// Package mount is the platform-neutral core of native on-demand files: it
// presents a dotfs root as a local file tree whose metadata is complete up
// front and whose file content arrives on first open.
//
// FS builds the tree from .fs/index, keeps it current from .fs/events,
// hydrates file content into a local cache keyed by content hash, and
// publishes writes with staged commits against the version a file was
// opened at. An OS backend (mount/fuse on Linux) turns kernel requests
// into FS calls and receives Changes to invalidate what the kernel caches.
//
// The tree and every hydrated file survive restarts in the cache
// directory, so a mount starts without the server and serves whatever is
// cached; changes made while the server is unreachable are committed once
// it is back, as long as the mount keeps running.
package mount

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"crypto/rand"
	"encoding/hex"

	"github.com/hugelgupf/p9/linux"

	"github.com/progrium/dotfs/client"
	"github.com/progrium/dotfs/fsproto"
	dsync "github.com/progrium/dotfs/sync"
)

// DefaultCacheSize is the content cache's default limit.
const DefaultCacheSize = 10 << 30

// Config configures an FS.
type Config struct {
	// Dial connects to the root. It is called again after the connection
	// drops.
	Dial func(ctx context.Context) (client.Conn, error)
	// Root names the root, as "<address>/<name>". A cache directory
	// remembers its root and refuses another one.
	Root string
	// CacheDir holds the saved tree, hydrated content, and files being
	// written.
	CacheDir string
	// CacheSize bounds the hydrated content in bytes; 0 means
	// DefaultCacheSize. Content being read and files not yet committed
	// are never evicted.
	CacheSize int64
	// Device names this client in conflict copy names; empty means one
	// made from the host name.
	Device string
	// NoPrefetch turns off fetching files flagged p right after the index
	// is read.
	NoPrefetch bool
	// Logf logs; nil means no logging.
	Logf func(format string, args ...any)
	// Now returns the current time; nil means time.Now. For tests.
	Now func() time.Time
}

// Stat is a path's metadata as the mount presents it.
type Stat struct {
	fsproto.Entry
	// Ino is a stable inode number for the path, for the life of the FS.
	Ino uint64
}

// Change tells a backend what to invalidate after a change it did not make
// itself, such as one from another device.
type Change struct {
	// Op is OpPut (Path was created or changed), OpDel (Path and
	// everything below it is gone), OpMv (Path moved to To), or OpReset
	// (anything may have changed).
	Op       fsproto.Op
	Path, To string
}

// FS is a dotfs root presented as on-demand files.
type FS struct {
	cfg    Config
	conn   *client.Redialer
	cache  *cache
	dir    string
	device string
	logf   func(string, ...any)
	now    func() time.Time

	mu      sync.Mutex
	t       *tree
	pending map[string]*pending // path -> file with local changes or open for writing
	notify  func(Change)
	online  bool

	retry chan struct{}
}

var badDevice = regexp.MustCompile(`[^A-Za-z0-9_.-]+`)

// New opens an FS. It reads the root's index, or, when the server cannot
// be reached and the cache directory has a saved tree, starts from that.
// Call Run to follow changes.
func New(ctx context.Context, cfg Config) (*FS, error) {
	if cfg.Dial == nil || cfg.CacheDir == "" {
		return nil, errors.New("mount: Dial and CacheDir are required")
	}
	f := &FS{
		cfg:     cfg,
		dir:     cfg.CacheDir,
		logf:    cfg.Logf,
		now:     cfg.Now,
		t:       newTree(),
		pending: map[string]*pending{},
		notify:  func(Change) {},
		retry:   make(chan struct{}, 1),
	}
	if f.logf == nil {
		f.logf = func(string, ...any) {}
	}
	if f.now == nil {
		f.now = time.Now
	}
	f.device = cfg.Device
	if f.device == "" {
		h, _ := os.Hostname()
		if h = badDevice.ReplaceAllString(h, "-"); h == "" {
			h = "device"
		}
		f.device = h + "-mount"
	}
	if err := os.MkdirAll(f.workDir(), 0o700); err != nil {
		return nil, err
	}
	if err := f.checkRoot(); err != nil {
		return nil, err
	}
	if left, _ := os.ReadDir(f.workDir()); len(left) > 0 {
		lost := filepath.Join(f.dir, "unsaved")
		os.MkdirAll(lost, 0o700)
		for _, l := range left {
			os.Rename(filepath.Join(f.workDir(), l.Name()), filepath.Join(lost, l.Name()))
		}
		f.logf("warning: %d files not committed before the last exit are kept in %s", len(left), lost)
	}
	size := cfg.CacheSize
	if size <= 0 {
		size = DefaultCacheSize
	}
	var err error
	if f.cache, err = openCache(filepath.Join(f.dir, "content"), size, f.logf); err != nil {
		return nil, err
	}
	f.conn = client.NewRedialer(cfg.Dial)
	if err := f.bootstrap(ctx); err != nil {
		if !client.IsTransport(err) && !errors.Is(err, context.DeadlineExceeded) {
			f.conn.Close()
			return nil, err
		}
		if lerr := f.loadTree(); lerr != nil {
			f.conn.Close()
			return nil, fmt.Errorf("mount: reading the index: %w (and no saved tree: %v)", err, lerr)
		}
		f.logf("offline: %v; serving the saved tree at seq %d", err, f.t.seq)
	}
	return f, nil
}

func (f *FS) workDir() string { return filepath.Join(f.dir, "work") }

// checkRoot records the root in the cache directory, or refuses a
// directory that belongs to another root.
func (f *FS) checkRoot() error {
	p := filepath.Join(f.dir, "root")
	b, err := os.ReadFile(p)
	if err == nil {
		if old := strings.TrimSpace(string(b)); old != f.cfg.Root {
			return fmt.Errorf("mount: cache %s belongs to %s, not %s", f.dir, old, f.cfg.Root)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.WriteFile(p, []byte(f.cfg.Root+"\n"), 0o600)
}

// Device returns the device id used in conflict copy names.
func (f *FS) Device() string { return f.device }

// SetNotify sets the function told about changes the FS did not make
// itself. It is called without any FS lock held, from the goroutine
// running Run.
func (f *FS) SetNotify(fn func(Change)) {
	f.mu.Lock()
	f.notify = fn
	f.mu.Unlock()
}

// Seq returns the seq the tree is current at.
func (f *FS) Seq() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t.seq
}

// bootstrap reads the whole index and replaces the tree with it.
func (f *FS) bootstrap(ctx context.Context) error {
	ir, err := f.conn.Index(ctx, []string{fsproto.EncodingZstd})
	if err != nil {
		return err
	}
	defer ir.Close()
	var entries []fsproto.Entry
	for {
		e, err := ir.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		entries = append(entries, e)
	}
	seq := ir.Header().Seq
	f.mu.Lock()
	f.t.reset(entries, seq)
	f.dropNested()
	f.keepPending()
	f.online = true
	f.mu.Unlock()
	f.saveTree()
	return nil
}

// dropNested empties nested scopes (flag i): the mount shows them as empty
// directories until following nested scopes is implemented.
func (f *FS) dropNested() {
	f.t.walk(func(p string, n *node) {
		if n.isDir() && n.e.Flags.Has(fsproto.FlagIndex) {
			n.kids = map[string]*node{}
		}
	})
}

// keepPending puts back placeholders for files with uncommitted changes
// that a new tree no longer has, and forgets clean ones whose content the
// new tree may have replaced.
func (f *FS) keepPending() {
	for p, pe := range f.pending {
		if !pe.dirty {
			if n := f.t.get(p); n == nil || n.e.Version != pe.base {
				f.detach(pe)
			}
			continue
		}
		if n := f.t.get(p); n == nil || n.e.Type != fsproto.TypeFile {
			f.t.put(fsproto.Entry{Path: p, Type: fsproto.TypeFile, Mode: pe.mode, Mtime: f.now().Unix()})
		}
	}
}

// saveTree writes the tree to the cache directory, so a restart without
// the server still has it.
func (f *FS) saveTree() {
	f.mu.Lock()
	var entries []fsproto.Entry
	f.t.walk(func(p string, n *node) {
		if n.e.Version == 0 && n.e.Type == fsproto.TypeFile && f.pending[p] != nil {
			return // created here and not committed yet
		}
		e := n.e
		e.Path = p
		entries = append(entries, e)
	})
	seq := f.t.seq
	f.mu.Unlock()
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	err := func() error {
		tmp, err := os.CreateTemp(f.dir, "index-")
		if err != nil {
			return err
		}
		defer os.Remove(tmp.Name())
		bw := bufio.NewWriter(tmp)
		iw, err := fsproto.NewIndexWriter(bw, fsproto.IndexHeader{Seq: seq}, nil)
		if err != nil {
			tmp.Close()
			return err
		}
		for _, e := range entries {
			if err := iw.Write(e); err != nil {
				tmp.Close()
				return err
			}
		}
		if err := iw.Close(); err != nil {
			tmp.Close()
			return err
		}
		if err := bw.Flush(); err != nil {
			tmp.Close()
			return err
		}
		if err := tmp.Close(); err != nil {
			return err
		}
		return os.Rename(tmp.Name(), filepath.Join(f.dir, "index"))
	}()
	if err != nil {
		f.logf("warning: saving the tree: %v", err)
	}
}

func (f *FS) loadTree() error {
	r, err := os.Open(filepath.Join(f.dir, "index"))
	if err != nil {
		return err
	}
	defer r.Close()
	ir, err := fsproto.NewIndexReader(bufio.NewReader(r), nil)
	if err != nil {
		return err
	}
	var entries []fsproto.Entry
	for {
		e, err := ir.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		entries = append(entries, e)
	}
	f.mu.Lock()
	f.t.reset(entries, ir.Header().Seq)
	f.mu.Unlock()
	return nil
}

// Run follows the root's events until ctx ends, fetches prefetch-flagged
// files, and retries commits that failed while the server was unreachable.
func (f *FS) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if !f.cfg.NoPrefetch {
		wg.Add(1)
		go func() { defer wg.Done(); f.prefetch(ctx) }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); f.retryLoop(ctx) }()
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(5 * time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				f.saveTree()
			}
		}
	}()
	return f.follow(ctx)
}

func (f *FS) follow(ctx context.Context) error {
	fl := client.NewFollower(f.conn.Events, fsproto.EventsSetup{Since: f.Seq(), HasSince: true, Coalesce: true})
	defer fl.Close()
	for {
		ev, err := fl.Next(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, client.ErrNeedsReset) {
			if err := f.bootstrap(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				f.logf("offline: re-reading the index: %v", err)
				sleep(ctx, 2*time.Second)
				continue
			}
			f.send([]Change{{Op: fsproto.OpReset}})
			fl.Restart(f.Seq())
			f.kick()
			continue
		}
		if err != nil {
			f.logf("events: %v", err)
			sleep(ctx, time.Second)
			continue
		}
		f.mu.Lock()
		wasOnline := f.online
		f.online = true
		f.mu.Unlock()
		if !wasOnline {
			f.logf("online again")
			f.kick()
		}
		if ev.Op == fsproto.OpReset || ev.Op == fsproto.OpSeq {
			continue
		}
		f.send(f.apply(ev))
		if ev.Op == fsproto.OpPut && ev.Entry.Flags.Has(fsproto.FlagPrefetch) && !f.cfg.NoPrefetch && ev.Entry.Type == fsproto.TypeFile {
			e := ev.Entry
			go f.cache.get(ctx, f.conn, e)
		}
	}
}

func (f *FS) send(cs []Change) {
	if len(cs) == 0 {
		return
	}
	f.mu.Lock()
	fn := f.notify
	f.mu.Unlock()
	for _, c := range cs {
		fn(c)
	}
}

// apply applies one event record to the tree and returns what changed.
// Records for changes this FS made itself are already applied and are
// skipped by comparing versions.
func (f *FS) apply(ev fsproto.Event) []Change {
	f.mu.Lock()
	defer f.mu.Unlock()
	if ev.Seq > f.t.seq {
		f.t.seq = ev.Seq
	}
	switch ev.Op {
	case fsproto.OpPut:
		p := ev.Entry.Path
		if n := f.t.get(p); n != nil && n.e.Version >= ev.Seq {
			return nil
		}
		if pe := f.pending[p]; pe != nil && !pe.dirty {
			f.detach(pe)
		}
		e := ev.Entry
		e.Version = ev.Seq
		n := f.t.put(e)
		if n.isDir() && e.Flags.Has(fsproto.FlagIndex) {
			n.kids = map[string]*node{}
		}
		return []Change{{Op: fsproto.OpPut, Path: p}}
	case fsproto.OpDel:
		n := f.t.get(ev.Path)
		if n == nil || n.e.Version > ev.Seq {
			return nil
		}
		f.t.del(ev.Path)
		var keep []*pending
		for p, pe := range f.pending {
			if fsproto.Within(p, ev.Path) {
				if pe.dirty {
					keep = append(keep, pe)
				} else {
					f.detach(pe)
				}
			}
		}
		cs := []Change{{Op: fsproto.OpDel, Path: ev.Path}}
		// An edit beats a delete: files with local changes stay, and
		// their commit recreates them.
		for _, pe := range keep {
			f.t.put(fsproto.Entry{Path: pe.path, Type: fsproto.TypeFile, Mode: pe.mode, Mtime: f.now().Unix()})
			cs = append(cs, Change{Op: fsproto.OpPut, Path: pe.path})
		}
		return cs
	case fsproto.OpMv:
		if f.t.get(ev.Path) == nil {
			return nil
		}
		if n := f.t.get(ev.To); n != nil && n.e.Version >= ev.Seq {
			return nil
		}
		f.t.mv(ev.Path, ev.To, ev.Seq)
		f.rekey(ev.Path, ev.To)
		return []Change{{Op: fsproto.OpMv, Path: ev.Path, To: ev.To}}
	case fsproto.OpFlags:
		n := f.t.get(ev.Path)
		if n == nil || n.e.Version >= ev.Seq {
			return nil
		}
		had := n.e.Flags.Has(fsproto.FlagIndex)
		now := n.isDir() && ev.Flags.Has(fsproto.FlagIndex)
		n.e.Flags, n.e.Version = ev.Flags, ev.Seq
		cs := []Change{{Op: fsproto.OpPut, Path: ev.Path}}
		switch {
		case now && !had:
			for name := range n.kids {
				f.t.del(fsproto.Join(ev.Path, name))
				cs = append(cs, Change{Op: fsproto.OpDel, Path: fsproto.Join(ev.Path, name)})
			}
		case had && !now:
			// A nested scope merged back: its children are only in
			// the parent's index now.
			go f.rebootstrap()
		}
		return cs
	}
	return nil
}

func (f *FS) rebootstrap() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := f.bootstrap(ctx); err != nil {
		f.logf("re-reading the index: %v", err)
		return
	}
	f.send([]Change{{Op: fsproto.OpReset}})
}

// rekey moves pending files below from to below to.
func (f *FS) rekey(from, to string) {
	for p, pe := range f.pending {
		if fsproto.Within(p, from) {
			delete(f.pending, p)
			pe.path = fsproto.Rebase(p, from, to)
			f.pending[pe.path] = pe
		}
	}
}

// Lookup returns the metadata at p.
func (f *FS) Lookup(p string) (Stat, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := f.t.get(p)
	if n == nil {
		return Stat{}, syscall.ENOENT
	}
	return f.stat(p, n), nil
}

// stat returns n's Stat, with local changes in a pending file applied.
func (f *FS) stat(p string, n *node) Stat {
	e := n.e
	e.Path = p
	if pe := f.pending[p]; pe != nil && n.e.Type == fsproto.TypeFile {
		e.Mode = pe.mode
		if fi, err := os.Stat(pe.work); err == nil {
			e.Length = fi.Size()
			if pe.dirty {
				e.Mtime = fi.ModTime().Unix()
			}
		}
		if pe.mtime != 0 {
			e.Mtime = pe.mtime
		}
	}
	return Stat{Entry: e, Ino: n.ino}
}

// ReadDir lists the directory at p, sorted by name.
func (f *FS) ReadDir(p string) ([]Stat, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := f.t.get(p)
	if n == nil {
		return nil, syscall.ENOENT
	}
	if !n.isDir() {
		return nil, syscall.ENOTDIR
	}
	names := make([]string, 0, len(n.kids))
	for name := range n.kids {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]Stat, len(names))
	for i, name := range names {
		cp := fsproto.Join(p, name)
		out[i] = f.stat(cp, n.kids[name])
	}
	return out, nil
}

// Cached reports whether p's content is local, so opening it needs no
// download.
func (f *FS) Cached(p string) bool {
	f.mu.Lock()
	n := f.t.get(p)
	pe := f.pending[p]
	var e fsproto.Entry
	if n != nil {
		e = f.t.entry(n)
	}
	f.mu.Unlock()
	if n == nil || n.e.Type != fsproto.TypeFile {
		return false
	}
	return pe != nil || f.cache.has(key(e))
}

// Online reports whether the last attempt to reach the server worked.
func (f *FS) Online() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.online
}

// errno maps an error from the client or the server's reply to an errno,
// noting transport failures as offline.
func (f *FS) errno(op, p string, err error) error {
	if err == nil {
		return nil
	}
	var en syscall.Errno
	if errors.As(err, &en) {
		return en
	}
	var le linux.Errno
	if errors.As(err, &le) {
		return syscall.Errno(le)
	}
	switch {
	case errors.Is(err, fsproto.ErrMissing):
		return syscall.ENOENT
	case errors.Is(err, fsproto.ErrConflict):
		return syscall.EBUSY
	case errors.Is(err, fsproto.ErrDenied):
		return syscall.EACCES
	case errors.Is(err, fsproto.ErrInvalid):
		return syscall.EINVAL
	case errors.Is(err, context.Canceled):
		return syscall.EINTR
	}
	if client.IsTransport(err) || errors.Is(err, context.DeadlineExceeded) {
		f.mu.Lock()
		was := f.online
		f.online = false
		f.mu.Unlock()
		if was {
			f.logf("offline: %s %s: %v", op, p, err)
		}
		return syscall.EIO
	}
	f.logf("%s %s: %v", op, p, err)
	return syscall.EIO
}

// prefetch fetches every file flagged p, or below a directory flagged p,
// that is not cached yet.
func (f *FS) prefetch(ctx context.Context) {
	var want []fsproto.Entry
	f.mu.Lock()
	var rec func(p string, n *node, on bool)
	rec = func(p string, n *node, on bool) {
		on = on || n.e.Flags.Has(fsproto.FlagPrefetch)
		if n.e.Type == fsproto.TypeFile && on {
			e := n.e
			e.Path = p
			want = append(want, e)
		}
		for name, k := range n.kids {
			rec(fsproto.Join(p, name), k, on)
		}
	}
	rec("/", f.t.root, false)
	f.mu.Unlock()
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	got := 0
	var mu sync.Mutex
	for _, e := range want {
		if f.cache.has(key(e)) {
			continue
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if _, err := f.cache.get(ctx, f.conn, e); err == nil {
				mu.Lock()
				got++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if got > 0 {
		f.logf("prefetched %d files", got)
	}
}

// Close commits what it can, saves the tree, and closes the connection.
func (f *FS) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f.commitAll(ctx)
	f.saveTree()
	f.mu.Lock()
	n := 0
	for _, pe := range f.pending {
		if pe.dirty {
			n++
		}
	}
	f.mu.Unlock()
	if n > 0 {
		f.logf("warning: %d files could not be committed; they will be moved to %s at the next start", n, filepath.Join(f.dir, "unsaved"))
	}
	return f.conn.Close()
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func randomID() string {
	var b [8]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// conflictName returns a free conflict copy name for p.
func (f *FS) conflictName(p string) string {
	dir, name := fsproto.Parent(p), fsproto.Name(p)
	dev := f.device
	for i := 2; ; i++ {
		cp := fsproto.Join(dir, dsync.ConflictName(name, f.now(), dev))
		f.mu.Lock()
		taken := f.t.get(cp) != nil
		f.mu.Unlock()
		if !taken {
			return cp
		}
		dev = fmt.Sprintf("%s-%d", f.device, i)
	}
}
