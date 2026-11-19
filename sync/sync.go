// Package sync is the dotfs sync engine: it mirrors one scope of a dotfs
// server into a plain local folder and keeps the two in step, using the
// index, events, path operations, and staged commit extensions (the sync
// profile in docs/spec.md).
//
// # Local state
//
// The engine keeps its state in <dir>/.dotfs/state.db (SQLite): the last
// seq it applied, and one row per synced path holding the server's index
// entry for it (the base version of any local edit) and the local file's
// size, mtime, and inode when it last matched that entry. It also holds the
// persistent commit queue. <dir>/.dotfs/ is never synced. Rows, the queue,
// and the seq are kept in memory and written through to state.db after
// every batch of changes, so a crash loses at most the last batch, which
// the engine rediscovers: an applied remote change looks like a local edit
// whose commit conflicts and is then adopted, and a local change is found
// again by the next scan.
//
// # How a cycle runs
//
//  1. Connect, send hello, and on the first run read the index and
//     reconcile it with the folder (bootstrap). Fetches run in parallel,
//     files flagged p first, and content already present locally under
//     another path is copied instead of fetched.
//  2. Scan the folder against the rows and queue local changes. Renames are
//     recognized by inode, then by hash, and queued as mv.
//  3. Catch up on events since the stored seq. A remote change to a path
//     that is unchanged locally is applied; one to a path edited locally is
//     a conflict (see below). Records for versions the engine committed
//     itself only advance the seq. A reset record re-reads the index and
//     reconciles.
//  4. Drain the queue: mkdir, then mv, then commit-hash or stage and commit
//     for files, then conditional rm, deepest first. Every change carries
//     its base version. On a conflict reply the engine catches up on
//     events, which resolves the conflict locally, and retries.
//
// Run does this continuously, with an fsnotify watcher (debounced, about a
// second) and a periodic full scan, and reconnects with backoff when the
// connection drops; local changes made while offline are queued and
// replayed after the remote changes on reconnect. Sync runs one cycle,
// which is what tests use.
//
// # Conflicts
//
//   - Both sides edited a file: the local copy is renamed to
//     name.sync-conflict-<YYYYMMDD-HHMMSS>-<device>.ext and committed as a
//     new file, and the server's version takes the path. Identical edits
//     are not conflicts.
//   - Edit versus delete: the edit wins. A remote delete of a locally
//     edited file leaves it to be committed as a new file; a local delete
//     of a remotely edited file restores the remote version.
//   - Rename versus edit: a remote rename moves the local file, edits and
//     all, and the edit is committed at the new path. A local rename of a
//     remotely edited file writes the new content under the new name before
//     the rename is committed.
//
// # Ignores
//
// See Ignores. Ignored paths are neither uploaded nor downloaded. Changing
// .dotfsignore makes the engine reconcile with the index again.
//
// # Limits
//
// Symlinks are downloaded but not uploaded, since no ctl command creates
// one. Directory modes and mtimes are not synced. A nested scope (flag i)
// is synced as a directory, but what is below it is left alone in both
// directions, since it has its own index and events. Names that collide on a
// case-insensitive filesystem are not handled.
package sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	gosync "sync"
	"sync/atomic"
	"time"

	"github.com/progrium/dotfs/client"
	"github.com/progrium/dotfs/fsproto"
)

// Config configures an Engine.
type Config struct {
	// Dir is the local folder to keep in sync.
	Dir string
	// Dial connects to the scope to mirror. The engine calls it again after
	// the connection fails.
	Dial func(ctx context.Context) (client.Conn, error)
	// Root identifies the scope as "<address>/<name>", such as
	// "host:port/home". It is stored in state.db. The engine refuses a
	// folder synced with a root of another name; when only the address
	// changed, it bootstraps again and merges, deleting nothing local.
	Root string
	// Device names this client in events and in conflict copy names. The
	// default is the one stored in state.db, or a new one made from the
	// host name.
	Device string
	// Debounce is how long the watcher waits for changes to settle before
	// committing; 0 means one second.
	Debounce time.Duration
	// ScanInterval is how often Run does a full scan; 0 means one minute.
	ScanInterval time.Duration
	// Fetchers is how many files bootstrap fetches at once; 0 means 8.
	Fetchers int
	// NoWatch turns off the filesystem watcher; Run then relies on the
	// periodic scan.
	NoWatch bool
	// Logf, when set, receives progress and problems.
	Logf func(format string, args ...any)
	// Now returns the time; nil means time.Now. Tests set it.
	Now func() time.Time
}

// Engine syncs one folder with one scope.
type Engine struct {
	cfg    Config
	dir    string
	device string
	st     *state
	ign    *Ignores
	ignKey string // hash of the ignore file the rules came from
	// watchIgn is ign for the watcher goroutine.
	watchIgn atomic.Pointer[Ignores]

	mu   gosync.Mutex // one cycle at a time
	conn client.Conn
	ev   *evReader

	hints     map[string]bool // dirs to scan, true for recursive
	reconcile bool            // the index must be re-read
	warned    map[string]bool
	prefetch  map[string]fetched
}

// New opens the state in cfg.Dir, creating it if needed.
func New(cfg Config) (*Engine, error) {
	if cfg.Dir == "" || cfg.Dial == nil {
		return nil, errors.New("sync: Dir and Dial are required")
	}
	dir, err := filepath.Abs(cfg.Dir)
	if err != nil {
		return nil, err
	}
	if fi, err := os.Stat(dir); err != nil {
		return nil, err
	} else if !fi.IsDir() {
		return nil, fmt.Errorf("sync: %s is not a directory", dir)
	}
	if cfg.Debounce == 0 {
		cfg.Debounce = time.Second
	}
	if cfg.ScanInterval == 0 {
		cfg.ScanInterval = time.Minute
	}
	if cfg.Fetchers == 0 {
		cfg.Fetchers = 8
	}
	st, err := openState(filepath.Join(dir, StateDir))
	if err != nil {
		return nil, err
	}
	e := &Engine{cfg: cfg, dir: dir, st: st, hints: map[string]bool{}, warned: map[string]bool{}}
	if old := st.meta["root"]; cfg.Root != "" && old != "" && old != cfg.Root {
		if rootName(old) != rootName(cfg.Root) {
			st.close()
			return nil, fmt.Errorf("sync: %s: %w (%s)", dir, errOtherRoot, old)
		}
		// Same root name at a new address: the server moved, or it is a
		// different server. Versions from the old address mean nothing
		// there, so forget them and bootstrap again, which merges the
		// folder with the server without deleting anything local: files
		// only here are uploaded, and differing ones become conflict
		// copies.
		e.logf("root moved from %s to %s; re-reading it and merging", old, cfg.Root)
		for p := range st.rows {
			st.del(p)
		}
		for _, q := range append([]*qent(nil), st.queue...) {
			st.dequeue(q)
		}
		st.setSeq(0)
		st.setMeta("bootstrapped", "")
	}
	if cfg.Root != "" {
		st.setMeta("root", cfg.Root)
	}
	e.device = cfg.Device
	if e.device == "" {
		e.device = st.meta["device"]
	}
	if e.device == "" {
		e.device = defaultDevice()
	}
	st.setMeta("device", e.device)
	for _, other := range []string{".stfolder", ".keepsync"} {
		if _, err := os.Lstat(filepath.Join(dir, other)); err == nil {
			e.logf("warning: %s also has %s; syncing one folder with two tools leads to churn", dir, other)
		}
	}
	if err := e.loadIgnores(); err != nil {
		st.close()
		return nil, err
	}
	os.RemoveAll(e.tmpDir())
	if err := st.flush(); err != nil {
		st.close()
		return nil, err
	}
	return e, nil
}

// rootName returns the root's name: the last segment of Config.Root.
func rootName(root string) string {
	return root[strings.LastIndexByte(root, '/')+1:]
}

var badDevice = regexp.MustCompile(`[^A-Za-z0-9_.-]+`)

func defaultDevice() string {
	h, _ := os.Hostname()
	h = badDevice.ReplaceAllString(h, "-")
	if h == "" {
		h = "device"
	}
	return h + "-" + randomID()[:6]
}

// Device returns the device id the engine uses.
func (e *Engine) Device() string { return e.device }

// Dir returns the synced folder.
func (e *Engine) Dir() string { return e.dir }

// Close disconnects and closes the state.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.disconnect()
	return e.st.close()
}

func (e *Engine) logf(format string, args ...any) {
	if e.cfg.Logf != nil {
		e.cfg.Logf(format, args...)
	}
}

func (e *Engine) now() time.Time {
	if e.cfg.Now != nil {
		return e.cfg.Now()
	}
	return time.Now()
}

// connError marks a failure of the connection, as opposed to a protocol
// outcome or a local problem: the cycle stops and the engine reconnects.
type connError struct{ err error }

func (c connError) Error() string { return c.err.Error() }
func (c connError) Unwrap() error { return c.err }

func isConnErr(err error) bool {
	var c connError
	return errors.As(err, &c) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// cerr wraps an error from a call on the connection.
func cerr(err error) error {
	if err == nil {
		return nil
	}
	var re *fsproto.ReplyError
	if errors.As(err, &re) || errors.Is(err, fsproto.ErrMissing) {
		return err
	}
	return connError{err}
}

// ctl sends one command and returns its reply; transport failures come
// back as connError.
func (e *Engine) ctl(ctx context.Context, cmd fsproto.Command) (fsproto.Reply, error) {
	r, err := e.conn.Ctl(ctx, cmd)
	if err != nil {
		return r, connError{err}
	}
	return r, nil
}

// evReader pumps one event stream into a channel, so a cycle can wait on
// events and other work at once.
type evReader struct {
	stream client.EventStream
	ch     chan evMsg
	cancel context.CancelFunc
}

type evMsg struct {
	ev  fsproto.Event
	err error
}

func startReader(s client.EventStream) *evReader {
	ctx, cancel := context.WithCancel(context.Background())
	r := &evReader{stream: s, ch: make(chan evMsg, 256), cancel: cancel}
	go func() {
		for {
			ev, err := s.Next(ctx)
			select {
			case r.ch <- evMsg{ev, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return r
}

func (r *evReader) close() {
	r.cancel()
	r.stream.Close()
}

// connect dials, says hello, bootstraps on the first run, and opens events
// since the stored seq.
func (e *Engine) connect(ctx context.Context) error {
	if e.conn != nil {
		return nil
	}
	c, err := e.cfg.Dial(ctx)
	if err != nil {
		return connError{err}
	}
	e.conn = c
	if r, err := e.ctl(ctx, fsproto.HelloCmd{Device: e.device}); err != nil {
		e.disconnect()
		return err
	} else if r.Status != fsproto.StatusOK {
		e.logf("hello: %s %s", r.Status, r.Message)
	}
	if e.st.meta["bootstrapped"] == "" {
		if err := e.reconcileIndex(ctx); err != nil {
			e.disconnect()
			return err
		}
	}
	if err := e.openEvents(ctx); err != nil {
		e.disconnect()
		return err
	}
	return nil
}

func (e *Engine) openEvents(ctx context.Context) error {
	if e.ev != nil {
		e.ev.close()
		e.ev = nil
	}
	s, err := e.conn.Events(ctx, fsproto.EventsSetup{Since: e.st.seq, HasSince: true})
	if err != nil {
		return connError{err}
	}
	e.ev = startReader(s)
	return nil
}

func (e *Engine) disconnect() {
	if e.ev != nil {
		e.ev.close()
		e.ev = nil
	}
	if e.conn != nil {
		e.conn.Close()
		e.conn = nil
	}
}

// currentSeq asks the server for the scope's current seq: a stream opened
// without since starts with it.
func (e *Engine) currentSeq(ctx context.Context) (uint64, error) {
	s, err := e.conn.Events(ctx, fsproto.EventsSetup{})
	if err != nil {
		return 0, connError{err}
	}
	defer s.Close()
	ev, err := s.Next(ctx)
	if err != nil {
		return 0, connError{err}
	}
	if ev.Op != fsproto.OpSeq {
		return 0, connError{fmt.Errorf("events: expected a seq record, got %s", ev.Op)}
	}
	return ev.Seq, nil
}

// catchUp applies events until the stored seq reaches the server's
// current seq, then reconciles with the index if something asked for it.
func (e *Engine) catchUp(ctx context.Context) error {
	target, err := e.currentSeq(ctx)
	if err != nil {
		return err
	}
	for e.st.seq < target {
		if err := e.nextEvent(ctx, true); err != nil {
			return err
		}
	}
	return e.afterRemote(ctx)
}

// afterRemote finishes a batch of remote changes: a pending reconcile runs,
// and the state is written out.
func (e *Engine) afterRemote(ctx context.Context) error {
	if e.reconcile {
		if err := e.reconcileIndex(ctx); err != nil {
			return err
		}
		if err := e.openEvents(ctx); err != nil {
			return err
		}
	}
	return e.st.flush()
}

// nextEvent receives and applies one event; with block unset it returns
// errNoEvent when none is waiting.
func (e *Engine) nextEvent(ctx context.Context, block bool) error {
	if e.ev == nil {
		return connError{errors.New("events: no stream")}
	}
	var m evMsg
	if block {
		select {
		case m = <-e.ev.ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	} else {
		select {
		case m = <-e.ev.ch:
		default:
			return errNoEvent
		}
	}
	if m.err != nil {
		if m.err == io.EOF {
			m.err = errors.New("events: stream ended")
		}
		return connError{m.err}
	}
	return e.handleEvent(ctx, m.ev)
}

var errNoEvent = errors.New("no event waiting")

// Sync runs one full cycle: connect if needed, scan the folder, catch up
// on remote changes, and push local ones. It returns once the folder and
// the server agreed as of the moment it caught up, or with the error that
// stopped it; a connection error leaves the engine disconnected, and the
// next Sync reconnects.
func (e *Engine) Sync(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	reused := e.conn != nil
	err := e.cycle(ctx, true)
	if isConnErr(err) && ctx.Err() == nil {
		e.disconnect()
		if reused {
			// The connection we held went stale; try once on a fresh one.
			err = e.cycle(ctx, true)
			if isConnErr(err) {
				e.disconnect()
			}
		}
	}
	return err
}

func (e *Engine) cycle(ctx context.Context, full bool) error {
	// Queue local changes first, even if the server is out of reach, so
	// they are kept and so remote changes are reconciled with them.
	if full {
		e.hints["/"] = true
	}
	if err := e.scanHints(); err != nil {
		return err
	}
	if err := e.st.flush(); err != nil {
		return err
	}
	if err := e.connect(ctx); err != nil {
		return err
	}
	if err := e.catchUp(ctx); err != nil {
		return err
	}
	if err := e.scanHints(); err != nil {
		return err
	}
	if err := e.afterRemote(ctx); err != nil {
		return err
	}
	if err := e.drain(ctx); err != nil {
		return err
	}
	return e.st.flush()
}

// Run keeps the folder in sync until ctx is done, reconnecting after
// connection failures.
func (e *Engine) Run(ctx context.Context) error {
	var hints <-chan hint
	if !e.cfg.NoWatch {
		w, err := e.watch()
		if err != nil {
			return err
		}
		defer w.close()
		hints = w.hints
	}
	backoff := time.Second
	for {
		err := e.session(ctx, hints)
		if ctx.Err() != nil {
			e.mu.Lock()
			e.disconnect()
			e.st.flush()
			e.mu.Unlock()
			return nil
		}
		e.logf("offline: %v (retrying in %s)", err, backoff)
		e.mu.Lock()
		e.disconnect()
		e.mu.Unlock()
		// While offline, keep noticing local changes so the queue is ready.
		retry := time.NewTimer(backoff)
	wait:
		for {
			select {
			case <-ctx.Done():
				retry.Stop()
				return nil
			case h := <-hints:
				e.mu.Lock()
				e.addHint(h)
				if err := e.scanHints(); err != nil {
					e.logf("scan: %v", err)
				}
				e.st.flush()
				e.mu.Unlock()
			case <-retry.C:
				break wait
			}
		}
		backoff = min(backoff*2, 30*time.Second)
		if err == nil {
			backoff = time.Second
		}
	}
}

// session runs one connection's worth of Run.
func (e *Engine) session(ctx context.Context, hints <-chan hint) error {
	locked := func(fn func() error) error {
		e.mu.Lock()
		defer e.mu.Unlock()
		return fn()
	}
	if err := locked(func() error { return e.cycle(ctx, true) }); err != nil {
		return err
	}
	e.logf("in sync at seq %d", e.st.seq)
	debounce := time.NewTimer(time.Hour)
	debounce.Stop()
	defer debounce.Stop()
	var firstHint time.Time
	scan := time.NewTicker(e.cfg.ScanInterval)
	defer scan.Stop()
	for {
		e.mu.Lock()
		evs := e.ev.ch
		e.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m := <-evs:
			err := locked(func() error {
				// Queue local changes first, so a remote change to a path
				// renamed or edited locally is reconciled with it.
				if err := e.scanHints(); err != nil {
					return err
				}
				if err := e.applyWaiting(ctx, m); err != nil {
					return err
				}
				if err := e.afterRemote(ctx); err != nil {
					return err
				}
				if err := e.scanHints(); err != nil {
					return err
				}
				return e.drain(ctx)
			})
			if err != nil {
				return err
			}
		case h := <-hints:
			e.mu.Lock()
			e.addHint(h)
			e.mu.Unlock()
			if firstHint.IsZero() {
				firstHint = time.Now()
			}
			// Wait for changes to settle, but not forever.
			d := e.cfg.Debounce
			if time.Since(firstHint) > 10*e.cfg.Debounce {
				d = 0
			}
			debounce.Reset(d)
		case <-debounce.C:
			firstHint = time.Time{}
			if err := locked(func() error { return e.cycle(ctx, false) }); err != nil {
				return err
			}
		case <-scan.C:
			if err := locked(func() error { return e.cycle(ctx, true) }); err != nil {
				return err
			}
		}
	}
}

// eventBatch bounds how many waiting events applyWaiting takes at once.
const eventBatch = 1024

// applyWaiting applies m and every event waiting after it, until none is
// left. The files a batch of events adds or changes are fetched in
// parallel first, so a burst of new files is not fetched one at a time.
func (e *Engine) applyWaiting(ctx context.Context, m evMsg) error {
	for {
		msgs := []evMsg{m}
	collect:
		for len(msgs) < eventBatch && m.err == nil {
			select {
			case m = <-e.ev.ch:
				msgs = append(msgs, m)
			default:
				break collect
			}
		}
		if err := e.applyBatch(ctx, msgs); err != nil {
			return err
		}
		select {
		case m = <-e.ev.ch:
		default:
			return nil
		}
	}
}

// applyBatch prefetches the files msgs put, then applies msgs in order.
func (e *Engine) applyBatch(ctx context.Context, msgs []evMsg) error {
	var ents []fsproto.Entry
	at := map[string]int{}
	for _, m := range msgs {
		ent := m.ev.Entry
		if m.err != nil || m.ev.Op != fsproto.OpPut || ent.Type != fsproto.TypeFile || m.ev.Seq <= e.st.seq || e.excluded(ent.Path, false) {
			continue
		}
		if r := e.st.get(ent.Path); r != nil && ent.Version <= r.Version {
			continue
		}
		if i, ok := at[ent.Path]; ok {
			ents[i] = ent // only the last version is worth fetching
			continue
		}
		at[ent.Path] = len(ents)
		ents = append(ents, ent)
	}
	if len(ents) > 1 {
		e.prefetchAll(ctx, ents)
		defer e.dropPrefetch()
	}
	for _, m := range msgs {
		if err := e.applyMsg(ctx, m); err != nil {
			return err
		}
		if m.ev.Op == fsproto.OpReset {
			return nil // the rest came from the old stream
		}
	}
	return nil
}

func (e *Engine) applyMsg(ctx context.Context, m evMsg) error {
	if m.err != nil {
		if m.err == io.EOF {
			m.err = errors.New("events: stream ended")
		}
		return connError{m.err}
	}
	return e.handleEvent(ctx, m.ev)
}

// localErr reports whether err is a local filesystem problem.
func localErr(err error) bool {
	var pe *fs.PathError
	var le *os.LinkError
	return errors.As(err, &pe) || errors.As(err, &le)
}
