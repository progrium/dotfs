// Package shim serves the in-memory fakes from server/fake over 9P2000.L,
// with every dotfs extension under .fs/, so the conformance suite can run
// without the real server. It is a test fixture, not a server: it keeps
// everything in memory, trusts the attach name, and implements only the 9P
// messages the suite and simple clients use.
//
// Attach with any user name; the attach name picks the root (default
// "conform"), which is created on first use.
package shim

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"hash/fnv"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/progrium/dotfs/conformance/internal/wire"
	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/fake"
)

// DefaultRoot is the root served for an empty attach name.
const DefaultRoot = "conform"

// Options configure a shim.
type Options struct {
	// QueueLen bounds each event stream's queue (fake.DefaultQueueLen if
	// 0). A small value makes overflow resets easy to provoke.
	QueueLen int
	// Extensions limits which .fs/ files exist: any of "index", "events",
	// "ctl", and "stage". Nil means all four.
	Extensions []string
	// Bugs turns on deliberate protocol violations, so the suite's own
	// tests can check that it catches them. See the Bug constants.
	Bugs []Bug
}

// Bug is a deliberate protocol violation.
type Bug string

// The bugs a shim can have.
const (
	// BugUnsortedIndex serves index lines in reverse order.
	BugUnsortedIndex Bug = "unsorted-index"
	// BugDropEvents drops every fifth event record.
	BugDropEvents Bug = "drop-events"
	// BugIgnoreFlush answers Tflush without canceling the request, as
	// hugelgupf/p9's server does.
	BugIgnoreFlush Bug = "ignore-flush"
	// BugQidVers reports qid.vers 0 for every real file.
	BugQidVers Bug = "qid-vers"
)

// Server is a 9P server over a set of fakes.
type Server struct {
	Store *fake.Store
	Meta  *fake.Meta
	Hub   *fake.Hub

	ext  map[string]bool
	bugs map[Bug]bool
	mu   sync.Mutex
}

// New returns a shim over fresh fakes.
func New(o Options) *Server {
	st := fake.NewStore()
	m := fake.NewMeta(st, nil)
	h := fake.NewHub(m, o.QueueLen)
	m.SetPublisher(h)
	s := &Server{Store: st, Meta: m, Hub: h, ext: map[string]bool{}, bugs: map[Bug]bool{}}
	for _, b := range o.Bugs {
		s.bugs[b] = true
	}
	exts := o.Extensions
	if exts == nil {
		exts = []string{"index", "events", "ctl", "stage"}
	}
	for _, e := range exts {
		s.ext[e] = true
	}
	return s
}

// Serve accepts connections on l until it fails.
func (s *Server) Serve(l net.Listener) error {
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		go s.ServeConn(c)
	}
}

// Pipe returns the client end of an in-process connection.
func (s *Server) Pipe() net.Conn {
	a, b := net.Pipe()
	go s.ServeConn(b)
	return a
}

// Trim drops the log of root's scope at or below seq, as retention would.
func (s *Server) Trim(ctx context.Context, user, root string, seq uint64) error {
	return s.Meta.Trim(ctx, server.RootScope(server.NewRoot(user, rootName(root))), seq)
}

func rootName(aname string) string {
	if aname == "" {
		return DefaultRoot
	}
	return aname
}

func (s *Server) ensureRoot(ctx context.Context, sc server.Scope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.Meta.CreateScope(ctx, sc)
	if errors.Is(err, server.ErrScopeExists) {
		return nil
	}
	return err
}

// ServeConn serves one connection until it closes.
func (s *Server) ServeConn(rwc io.ReadWriteCloser) {
	c := &conn{srv: s, rwc: rwc, fids: map[uint32]*fid{}, reqs: map[uint16]*req{}, msize: wire.DefaultMsize}
	c.serve()
}

type conn struct {
	srv   *Server
	rwc   io.ReadWriteCloser
	msize uint32

	wmu sync.Mutex

	mu   sync.Mutex
	fids map[uint32]*fid
	reqs map[uint16]*req
}

type req struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (c *conn) send(tag uint16, m wire.Msg) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	wire.WriteMsg(c.rwc, tag, m)
}

func (c *conn) serve() {
	defer c.shutdown()
	for {
		typ, tag, m, err := wire.ReadMsg(c.rwc, c.msize+64)
		if errors.Is(err, wire.ErrUnknownType) {
			c.send(tag, &wire.Rlerror{Ecode: wire.ENOSYS})
			continue
		}
		if err != nil {
			return
		}
		_ = typ
		switch m := m.(type) {
		case *wire.Tversion:
			c.version(tag, m)
			continue
		case *wire.Tflush:
			c.flush(tag, m)
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		r := &req{cancel: cancel, done: make(chan struct{})}
		c.mu.Lock()
		c.reqs[tag] = r
		c.mu.Unlock()
		go func() {
			defer close(r.done)
			resp := c.handle(ctx, m)
			c.mu.Lock()
			delete(c.reqs, tag)
			c.mu.Unlock()
			if ctx.Err() != nil && resp == nil {
				return // flushed: no answer
			}
			cancel()
			if resp == nil {
				resp = &wire.Rlerror{Ecode: wire.EINTR}
			}
			c.send(tag, resp)
		}()
	}
}

func (c *conn) version(tag uint16, m *wire.Tversion) {
	msize := min(m.Msize, wire.DefaultMsize)
	v := wire.Version
	if !strings.HasPrefix(m.Version, wire.Version) {
		v = "unknown"
	}
	c.mu.Lock()
	c.msize = msize
	c.mu.Unlock()
	c.send(tag, &wire.Rversion{Msize: msize, Version: v})
}

// flush cancels the request with the old tag, waits for it to finish (it
// may still answer), then answers the flush.
func (c *conn) flush(tag uint16, m *wire.Tflush) {
	c.mu.Lock()
	r := c.reqs[m.OldTag]
	c.mu.Unlock()
	go func() {
		if r != nil && !c.srv.bugs[BugIgnoreFlush] {
			r.cancel()
			<-r.done
		}
		c.send(tag, &wire.Rflush{})
	}()
}

func (c *conn) shutdown() {
	c.rwc.Close()
	c.mu.Lock()
	for _, r := range c.reqs {
		r.cancel()
	}
	fids := c.fids
	c.fids = map[uint32]*fid{}
	c.mu.Unlock()
	for _, f := range fids {
		f.release(context.Background())
	}
}

// attach is what every fid of one Tattach shares.
type attach struct {
	srv  *Server
	user string
	root server.Scope

	mu   sync.Mutex
	ctls map[server.Scope]*server.Ctl
}

func (a *attach) ctl(sc server.Scope) *server.Ctl {
	a.mu.Lock()
	defer a.mu.Unlock()
	if c := a.ctls[sc]; c != nil {
		return c
	}
	c := server.NewCtl(a.srv.Meta, a.srv.Store, sc, a.user)
	if !a.srv.ext["stage"] {
		c.Caps = fsproto.PathCommands
	}
	if rc := a.ctls[a.root]; rc != nil && sc != a.root {
		// A nested scope's ctl reports the same device.
		c.Exec(context.Background(), fsproto.HelloCmd{Device: rc.Origin().Dev})
	}
	a.ctls[sc] = c
	return c
}

func (a *attach) origin() server.Origin { return a.ctl(a.root).Origin() }

type kind int

const (
	kReal kind = iota
	kFsDir
	kIndex
	kEvents
	kCtl
	kStageDir
	kStage
)

// fid is the server side of one fid.
type fid struct {
	a     *attach
	kind  kind
	scope server.Scope // the .fs scope for synthetic files
	path  string       // root-relative path for real files; stage name for kStage

	mu     sync.Mutex
	opened bool
	flags  uint32

	// index
	accept []string
	index  []byte
	// events
	setup    fsproto.EventsSetup
	stream   server.Stream
	pending  []byte
	readMu   sync.Mutex
	finished atomic.Bool
	// ctl
	reply []byte
	// real files
	content server.ContentFile
	plain   string // stage name of a plain write in progress
	created uint32 // mode for a file created with Tlcreate
	// stage
	stage server.StageFile
}

func (f *fid) clone() *fid {
	return &fid{a: f.a, kind: f.kind, scope: f.scope, path: f.path}
}

func (c *conn) getFid(n uint32) *fid {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fids[n]
}

func (c *conn) setFid(n uint32, f *fid) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fids[n] != nil {
		return false
	}
	c.fids[n] = f
	return true
}

func errno(n uint32) wire.Msg { return &wire.Rlerror{Ecode: n} }

// errMsg maps an error from the fakes to an Rlerror.
func errMsg(err error) wire.Msg {
	switch {
	case errors.Is(err, fsproto.ErrMissing):
		return errno(wire.ENOENT)
	case errors.Is(err, fsproto.ErrConflict):
		return errno(wire.EEXIST)
	case errors.Is(err, fsproto.ErrInvalid):
		return errno(wire.EINVAL)
	case errors.Is(err, fsproto.ErrDenied):
		return errno(wire.EACCES)
	case errors.Is(err, context.Canceled):
		return nil
	}
	return errno(wire.EIO)
}

func (c *conn) handle(ctx context.Context, m wire.Msg) wire.Msg {
	switch m := m.(type) {
	case *wire.Tattach:
		return c.attach(ctx, m)
	case *wire.Tstatfs:
		return &wire.Rstatfs{Bsize: 4096, Namelen: 255}
	case *wire.Tclunk:
		c.mu.Lock()
		f := c.fids[m.Fid]
		delete(c.fids, m.Fid)
		c.mu.Unlock()
		if f == nil {
			return errno(wire.EBADF)
		}
		if err := f.release(ctx); err != nil {
			return errMsg(err)
		}
		return &wire.Rclunk{}
	}
	var n uint32
	switch m := m.(type) {
	case *wire.Twalk:
		n = m.Fid
	case *wire.Tlopen:
		n = m.Fid
	case *wire.Tlcreate:
		n = m.Fid
	case *wire.Tsymlink:
		n = m.Fid
	case *wire.Treadlink:
		n = m.Fid
	case *wire.Tgetattr:
		n = m.Fid
	case *wire.Tsetattr:
		n = m.Fid
	case *wire.Treaddir:
		n = m.Fid
	case *wire.Tmkdir:
		n = m.Dfid
	case *wire.Trenameat:
		n = m.OldDir
	case *wire.Tunlinkat:
		n = m.Dir
	case *wire.Tread:
		n = m.Fid
	case *wire.Twrite:
		n = m.Fid
	case *wire.Tremove:
		n = m.Fid
	default:
		return errno(wire.ENOSYS)
	}
	f := c.getFid(n)
	if f == nil {
		return errno(wire.EBADF)
	}
	switch m := m.(type) {
	case *wire.Twalk:
		return c.walk(ctx, f, m)
	case *wire.Tlopen:
		return f.open(ctx, m.Flags)
	case *wire.Tlcreate:
		return f.create(ctx, m)
	case *wire.Tsymlink:
		return f.symlink(ctx, m)
	case *wire.Treadlink:
		e, err := f.entry(ctx)
		if err != nil {
			return errMsg(err)
		}
		if e.Type != fsproto.TypeSymlink {
			return errno(wire.EINVAL)
		}
		return &wire.Rreadlink{Target: e.Target}
	case *wire.Tgetattr:
		a, err := f.attr(ctx)
		if err != nil {
			return errMsg(err)
		}
		return &wire.Rgetattr{Attr: a}
	case *wire.Tsetattr:
		return f.setattr(ctx, m)
	case *wire.Treaddir:
		return f.readdir(ctx, m)
	case *wire.Tmkdir:
		return f.mkdir(ctx, m)
	case *wire.Trenameat:
		to := c.getFid(m.NewDir)
		if to == nil {
			return errno(wire.EBADF)
		}
		return f.rename(ctx, m.OldName, to, m.NewName)
	case *wire.Tunlinkat:
		return f.unlink(ctx, m.Name)
	case *wire.Tread:
		return f.read(ctx, m)
	case *wire.Twrite:
		return f.write(ctx, m)
	case *wire.Tremove:
		c.mu.Lock()
		delete(c.fids, m.Fid)
		c.mu.Unlock()
		f.release(ctx)
		if f.kind != kReal || f.path == "/" {
			return errno(wire.EACCES)
		}
		dir, err := f.walkTo(ctx, fsproto.Parent(f.path))
		if err != nil {
			return errMsg(err)
		}
		if r, ok := dir.unlink(ctx, fsproto.Name(f.path)).(*wire.Rlerror); ok {
			return r
		}
		return &wire.Rremove{}
	}
	return errno(wire.ENOSYS)
}

func (c *conn) attach(ctx context.Context, m *wire.Tattach) wire.Msg {
	if m.Afid != wire.NoFid {
		return errno(wire.EINVAL)
	}
	user := m.Uname
	if user == "" {
		user = "conform"
	}
	sc := server.RootScope(server.NewRoot(user, rootName(m.Aname)))
	if err := c.srv.ensureRoot(ctx, sc); err != nil {
		return errMsg(err)
	}
	a := &attach{srv: c.srv, user: user, root: sc, ctls: map[server.Scope]*server.Ctl{}}
	f := &fid{a: a, kind: kReal, scope: sc, path: "/"}
	if !c.setFid(m.Fid, f) {
		return errno(wire.EBADF)
	}
	q, err := f.qid(ctx)
	if err != nil {
		return errMsg(err)
	}
	return &wire.Rattach{Qid: q}
}

// walkTo returns a new fid for a real path in f's root.
func (f *fid) walkTo(ctx context.Context, p string) (*fid, error) {
	if _, err := f.a.srv.Meta.Stat(ctx, f.a.root, p); err != nil {
		return nil, err
	}
	return &fid{a: f.a, kind: kReal, scope: f.a.root, path: p}, nil
}

func (c *conn) walk(ctx context.Context, f *fid, m *wire.Twalk) wire.Msg {
	f.mu.Lock()
	opened := f.opened
	f.mu.Unlock()
	if opened {
		return errno(wire.EBADF)
	}
	cur := f.clone()
	var qids []wire.Qid
	for i, name := range m.Names {
		next, err := cur.step(ctx, name)
		if err != nil {
			if i == 0 {
				return errMsg(err)
			}
			return &wire.Rwalk{Qids: qids}
		}
		q, err := next.qid(ctx)
		if err != nil {
			return errMsg(err)
		}
		qids = append(qids, q)
		cur = next
	}
	if m.NewFid == m.Fid {
		c.mu.Lock()
		c.fids[m.Fid] = cur
		c.mu.Unlock()
	} else if !c.setFid(m.NewFid, cur) {
		return errno(wire.EBADF)
	}
	return &wire.Rwalk{Qids: qids}
}

// step walks one name from f.
func (f *fid) step(ctx context.Context, name string) (*fid, error) {
	srv := f.a.srv
	switch f.kind {
	case kReal:
		if name == ".." {
			return &fid{a: f.a, kind: kReal, scope: f.a.root, path: fsproto.Parent(f.path)}, nil
		}
		if name == "." || name == "" || strings.Contains(name, "/") {
			return nil, fsproto.Missing("")
		}
		if name == server.ReservedName {
			sc, ok := f.fsScope(ctx)
			if !ok {
				return nil, fsproto.Missing("")
			}
			return &fid{a: f.a, kind: kFsDir, scope: sc}, nil
		}
		e, err := srv.Meta.Stat(ctx, f.a.root, f.path)
		if err != nil {
			return nil, err
		}
		if e.Type != fsproto.TypeDir {
			return nil, fsproto.Missing("")
		}
		return f.walkTo(ctx, fsproto.Join(f.path, name))
	case kFsDir:
		var k kind
		switch name {
		case "index":
			k = kIndex
		case "events":
			k = kEvents
		case "ctl":
			k = kCtl
		case "stage":
			k = kStageDir
		default:
			return nil, fsproto.Missing("")
		}
		if !srv.ext[name] {
			return nil, fsproto.Missing("")
		}
		return &fid{a: f.a, kind: k, scope: f.scope}, nil
	case kStageDir:
		if _, err := srv.Store.StatStage(f.a.user, name); err != nil {
			return nil, err
		}
		return &fid{a: f.a, kind: kStage, scope: f.scope, path: name}, nil
	}
	return nil, fsproto.Missing("")
}

// fsScope returns the scope whose .fs/ lives in directory f. The shim has
// only the root scope: the fakes do not implement nested scopes as
// docs/spec.md settles them, so ctl refuses flag i instead.
func (f *fid) fsScope(ctx context.Context) (server.Scope, bool) {
	if f.path == "/" {
		return f.a.root, len(f.a.srv.ext) > 0
	}
	return server.Scope{}, false
}

func qidPath(parts ...string) uint64 {
	h := fnv.New64a()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return h.Sum64()
}

func (f *fid) entry(ctx context.Context) (fsproto.Entry, error) {
	if f.kind != kReal {
		return fsproto.Entry{}, fsproto.Invalid("")
	}
	return f.a.srv.Meta.Stat(ctx, f.a.root, f.path)
}

func (f *fid) qid(ctx context.Context) (wire.Qid, error) {
	a, err := f.attr(ctx)
	return a.Qid, err
}

func (f *fid) attr(ctx context.Context) (wire.Attr, error) {
	srv := f.a.srv
	root := string(f.a.root.Root)
	a := wire.Attr{Valid: wire.GetattrBasic, Nlink: 1, Blksize: 4096}
	dir := func(perm uint32, id ...string) {
		a.Qid = wire.Qid{Type: wire.QTDir, Path: qidPath(id...)}
		a.Mode = wire.SIFDIR | perm
		a.Nlink = 2
	}
	file := func(perm uint32, id ...string) {
		a.Qid = wire.Qid{Type: wire.QTFile, Path: qidPath(id...)}
		a.Mode = wire.SIFREG | perm
	}
	sc := string(f.scope.Root) + f.scope.Dir
	switch f.kind {
	case kReal:
		e, err := srv.Meta.Stat(ctx, f.a.root, f.path)
		if err != nil {
			return a, err
		}
		id := qidPath(root, f.path)
		switch e.Type {
		case fsproto.TypeDir:
			a.Qid = wire.Qid{Type: wire.QTDir, Path: id}
			a.Mode = wire.SIFDIR | e.Mode
			a.Nlink = 2
		case fsproto.TypeSymlink:
			a.Qid = wire.Qid{Type: wire.QTSymlink, Path: id}
			a.Mode = wire.SIFLNK | e.Mode
		default:
			a.Qid = wire.Qid{Type: wire.QTFile, Path: id}
			a.Mode = wire.SIFREG | e.Mode
		}
		a.Qid.Version = uint32(e.Version)
		if srv.bugs[BugQidVers] {
			a.Qid.Version = 0
		}
		a.Size = uint64(e.Length)
		a.MtimeSec, a.CtimeSec, a.AtimeSec = uint64(e.Mtime), uint64(e.Mtime), uint64(e.Mtime)
	case kFsDir:
		dir(0o555, sc, ".fs")
	case kIndex:
		file(0o444, sc, ".fs/index")
		seq, err := srv.Meta.Seq(ctx, f.scope)
		if err != nil {
			return a, err
		}
		a.Qid.Version = uint32(seq)
	case kEvents:
		file(0o444, sc, ".fs/events")
	case kCtl:
		file(0o666, sc, ".fs/ctl")
	case kStageDir:
		dir(0o700, sc, ".fs/stage", f.a.user)
	case kStage:
		file(0o600, sc, ".fs/stage", f.a.user, f.path)
		info, err := srv.Store.StatStage(f.a.user, f.path)
		if err != nil {
			return a, err
		}
		a.Size = uint64(info.Size)
		a.MtimeSec = uint64(info.ModTime.Unix())
	}
	a.Blocks = (a.Size + 511) / 512
	return a, nil
}

func (f *fid) open(ctx context.Context, flags uint32) wire.Msg {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.opened {
		return errno(wire.EBADF)
	}
	acc := flags & 3
	srv := f.a.srv
	switch f.kind {
	case kReal:
		e, err := srv.Meta.Stat(ctx, f.a.root, f.path)
		if err != nil {
			return errMsg(err)
		}
		switch e.Type {
		case fsproto.TypeSymlink:
			return errno(wire.EINVAL)
		case fsproto.TypeDir:
			if acc != wire.ORDONLY {
				return errno(wire.EISDIR)
			}
		default:
			if acc == wire.ORDONLY {
				cf, err := srv.Store.Open(f.a.root.Root, f.path)
				if err != nil {
					return errMsg(err)
				}
				f.content = cf
			} else if err := f.startPlain(ctx, flags&wire.OTRUNC == 0); err != nil {
				return errMsg(err)
			}
		}
	case kFsDir, kStageDir, kIndex, kEvents:
		if f.kind == kFsDir || f.kind == kStageDir {
			if acc != wire.ORDONLY {
				return errno(wire.EISDIR)
			}
		}
	case kCtl:
	case kStage:
		sf, err := srv.Store.OpenStage(f.a.user, f.path, false)
		if err != nil {
			return errMsg(err)
		}
		f.stage = sf
	}
	f.opened, f.flags = true, flags
	f.mu.Unlock()
	q, err := f.qid(ctx)
	f.mu.Lock()
	if err != nil {
		return errMsg(err)
	}
	return &wire.Rlopen{Qid: q}
}

func newStageName(prefix string) string {
	var b [8]byte
	rand.Read(b[:])
	return prefix + hex.EncodeToString(b[:])
}

// startPlain begins a plain 9P write to f's path through a hidden stage
// file, committed unconditionally at clunk. Holds f.mu.
func (f *fid) startPlain(ctx context.Context, keep bool) error {
	srv := f.a.srv
	name := newStageName(".plain-")
	sf, err := srv.Store.OpenStage(f.a.user, name, true)
	if err != nil {
		return err
	}
	if keep {
		if cf, err := srv.Store.Open(f.a.root.Root, f.path); err == nil {
			b, err := io.ReadAll(cf)
			cf.Close()
			if err != nil {
				sf.Close()
				srv.Store.RemoveStage(f.a.user, name)
				return err
			}
			sf.WriteAt(b, 0)
		}
	}
	f.stage, f.plain = sf, name
	return nil
}

func (f *fid) create(ctx context.Context, m *wire.Tlcreate) wire.Msg {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.opened {
		return errno(wire.EBADF)
	}
	srv := f.a.srv
	switch f.kind {
	case kStageDir:
		if err := fsproto.CheckStageName(m.Name); err != nil || strings.HasPrefix(m.Name, ".plain-") {
			return errno(wire.EINVAL)
		}
		sf, err := srv.Store.OpenStage(f.a.user, m.Name, true)
		if err != nil {
			return errMsg(err)
		}
		f.kind, f.path, f.stage = kStage, m.Name, sf
	case kReal:
		p := fsproto.Join(f.path, m.Name)
		if err := server.CheckPath(p); err != nil {
			return errMsg(err)
		}
		if _, err := srv.Meta.Stat(ctx, f.a.root, p); err == nil {
			return errno(wire.EEXIST)
		}
		f.path = p
		if err := f.startPlain(ctx, false); err != nil {
			return errMsg(err)
		}
		f.created = m.Mode & 0o7777
		if f.created == 0 {
			f.created = 0o644
		}
	default:
		return errno(wire.EACCES)
	}
	f.opened, f.flags = true, m.Flags
	q := wire.Qid{Type: wire.QTFile, Path: qidPath(string(f.a.root.Root), f.path)}
	if f.kind == kStage {
		q.Path = qidPath(string(f.scope.Root)+f.scope.Dir, ".fs/stage", f.a.user, f.path)
	}
	return &wire.Rlcreate{Qid: q}
}

func (f *fid) dirPath(name string) (string, error) {
	if f.kind != kReal {
		return "", fsproto.Denied("")
	}
	p := fsproto.Join(f.path, name)
	if err := server.CheckPath(p); err != nil {
		return "", err
	}
	return p, nil
}

func (f *fid) writer() (server.Writer, error) { return f.a.srv.Meta.Writer(f.a.root) }

func (f *fid) mkdir(ctx context.Context, m *wire.Tmkdir) wire.Msg {
	p, err := f.dirPath(m.Name)
	if err != nil {
		return errMsg(err)
	}
	w, err := f.writer()
	if err != nil {
		return errMsg(err)
	}
	if _, err := w.Mkdir(ctx, f.a.origin(), fsproto.MkdirCmd{Path: p}); err != nil {
		return errMsg(err)
	}
	nf := &fid{a: f.a, kind: kReal, scope: f.a.root, path: p}
	q, err := nf.qid(ctx)
	if err != nil {
		return errMsg(err)
	}
	return &wire.Rmkdir{Qid: q}
}

func (f *fid) symlink(ctx context.Context, m *wire.Tsymlink) wire.Msg {
	p, err := f.dirPath(m.Name)
	if err != nil {
		return errMsg(err)
	}
	w, err := f.writer()
	if err != nil {
		return errMsg(err)
	}
	if _, err := w.Symlink(ctx, f.a.origin(), p, m.Target, fsproto.At(0)); err != nil {
		return errMsg(err)
	}
	nf := &fid{a: f.a, kind: kReal, scope: f.a.root, path: p}
	q, err := nf.qid(ctx)
	if err != nil {
		return errMsg(err)
	}
	return &wire.Rsymlink{Qid: q}
}

func (f *fid) rename(ctx context.Context, oldName string, to *fid, newName string) wire.Msg {
	from, err := f.dirPath(oldName)
	if err != nil {
		return errMsg(err)
	}
	dst, err := to.dirPath(newName)
	if err != nil {
		return errMsg(err)
	}
	w, err := f.writer()
	if err != nil {
		return errMsg(err)
	}
	if _, err := w.Mv(ctx, f.a.origin(), fsproto.MvCmd{From: from, To: dst}); err != nil {
		return errMsg(err)
	}
	return &wire.Rrenameat{}
}

func (f *fid) unlink(ctx context.Context, name string) wire.Msg {
	if f.kind == kStageDir {
		if err := f.a.srv.Store.RemoveStage(f.a.user, name); err != nil {
			return errMsg(err)
		}
		return &wire.Runlinkat{}
	}
	p, err := f.dirPath(name)
	if err != nil {
		return errMsg(err)
	}
	w, err := f.writer()
	if err != nil {
		return errMsg(err)
	}
	if _, err := w.Rm(ctx, f.a.origin(), fsproto.RmCmd{Path: p}); err != nil {
		if errors.Is(err, fsproto.ErrInvalid) {
			return errno(wire.ENOTEMPTY)
		}
		return errMsg(err)
	}
	return &wire.Runlinkat{}
}

func (f *fid) setattr(ctx context.Context, m *wire.Tsetattr) wire.Msg {
	if f.kind != kReal {
		return errno(wire.EPERM)
	}
	var mode *uint32
	var mtime *int64
	if m.Valid&wire.SetattrMode != 0 {
		v := m.Mode & 0o7777
		mode = &v
	}
	if m.Valid&wire.SetattrMtime != 0 {
		v := time.Now().Unix()
		if m.Valid&wire.SetattrMtimeSet != 0 {
			v = int64(m.MtimeSec)
		}
		mtime = &v
	}
	if m.Valid&wire.SetattrSize != 0 {
		e, err := f.entry(ctx)
		if err != nil {
			return errMsg(err)
		}
		if uint64(e.Length) != m.Size {
			return errno(wire.ENOSYS) // truncate through open(O_TRUNC) instead
		}
	}
	if mode == nil && mtime == nil {
		return &wire.Rsetattr{}
	}
	w, err := f.writer()
	if err != nil {
		return errMsg(err)
	}
	if _, err := w.SetAttr(ctx, f.a.origin(), f.path, mode, mtime); err != nil {
		return errMsg(err)
	}
	return &wire.Rsetattr{}
}

func direntType(q wire.Qid) uint8 {
	switch q.Type {
	case wire.QTDir:
		return 4
	case wire.QTSymlink:
		return 10
	}
	return 8
}

func (f *fid) readdir(ctx context.Context, m *wire.Treaddir) wire.Msg {
	srv := f.a.srv
	var all []wire.Dirent
	add := func(name string, nf *fid) error {
		q, err := nf.qid(ctx)
		if err != nil {
			return err
		}
		all = append(all, wire.Dirent{Qid: q, Offset: uint64(len(all) + 1), Type: direntType(q), Name: name})
		return nil
	}
	switch f.kind {
	case kReal:
		es, err := srv.Meta.ReadDir(ctx, f.a.root, f.path)
		if err != nil {
			return errMsg(err)
		}
		for _, e := range es {
			if err := add(fsproto.Name(e.Path), &fid{a: f.a, kind: kReal, scope: f.a.root, path: e.Path}); err != nil {
				return errMsg(err)
			}
		}
	case kFsDir:
		for _, n := range []string{"ctl", "events", "index", "stage"} {
			if srv.ext[n] {
				nf, _ := f.step(ctx, n)
				if err := add(n, nf); err != nil {
					return errMsg(err)
				}
			}
		}
	case kStageDir:
		ss, err := srv.Store.ListStages(f.a.user)
		if err != nil {
			return errMsg(err)
		}
		for _, s := range ss {
			if strings.HasPrefix(s.Name, ".plain-") {
				continue
			}
			if err := add(s.Name, &fid{a: f.a, kind: kStage, scope: f.scope, path: s.Name}); err != nil {
				return errMsg(err)
			}
		}
	default:
		return errno(wire.ENOTDIR)
	}
	var out []wire.Dirent
	size := 0
	for _, de := range all {
		if de.Offset <= m.Offset {
			continue
		}
		size += wire.DirentSize(de)
		if size > int(m.Count) {
			break
		}
		out = append(out, de)
	}
	return &wire.Rreaddir{Entries: out}
}

func sliceAt(b []byte, off uint64, count uint32) []byte {
	if off >= uint64(len(b)) {
		return nil
	}
	b = b[off:]
	if uint64(len(b)) > uint64(count) {
		b = b[:count]
	}
	return append([]byte(nil), b...)
}

func (f *fid) read(ctx context.Context, m *wire.Tread) wire.Msg {
	f.mu.Lock()
	if !f.opened || f.flags&3 == wire.OWRONLY {
		f.mu.Unlock()
		return errno(wire.EBADF)
	}
	switch f.kind {
	case kIndex:
		defer f.mu.Unlock()
		if f.index == nil {
			b, err := f.makeIndex(ctx)
			if err != nil {
				return errMsg(err)
			}
			f.index = b
		}
		return &wire.Rread{Data: sliceAt(f.index, m.Offset, m.Count)}
	case kCtl:
		defer f.mu.Unlock()
		return &wire.Rread{Data: sliceAt(f.reply, m.Offset, m.Count)}
	case kEvents:
		f.mu.Unlock()
		return f.readEvents(ctx, m.Count)
	case kStage:
		defer f.mu.Unlock()
		buf := make([]byte, m.Count)
		n, err := f.stage.ReadAt(buf, int64(m.Offset))
		if err != nil && !errors.Is(err, io.EOF) {
			return errMsg(err)
		}
		return &wire.Rread{Data: buf[:n]}
	case kReal:
		defer f.mu.Unlock()
		var r io.ReaderAt
		switch {
		case f.content != nil:
			r = f.content
		case f.stage != nil:
			r = f.stage
		default:
			return errno(wire.EISDIR)
		}
		buf := make([]byte, m.Count)
		n, err := r.ReadAt(buf, int64(m.Offset))
		if err != nil && !errors.Is(err, io.EOF) {
			return errMsg(err)
		}
		return &wire.Rread{Data: buf[:n]}
	}
	f.mu.Unlock()
	return errno(wire.EISDIR)
}

// makeIndex generates the index file, re-encoding the fake's plain index
// as zstd when the client accepted it. Holds f.mu.
func (f *fid) makeIndex(ctx context.Context) ([]byte, error) {
	rc, _, err := f.a.srv.Meta.OpenIndex(ctx, f.scope, f.accept)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	zstdOK := false
	for _, e := range f.accept {
		if e == fsproto.EncodingZstd {
			zstdOK = true
		}
	}
	if srv := f.a.srv; srv.bugs[BugUnsortedIndex] {
		nl := bytes.IndexByte(b, '\n')
		lines := bytes.Split(bytes.TrimSuffix(b[nl+1:], []byte("\n")), []byte("\n"))
		var out []byte
		out = append(out, b[:nl+1]...)
		for i := len(lines) - 1; i >= 0; i-- {
			out = append(append(out, lines[i]...), '\n')
		}
		b = out
	}
	if !zstdOK {
		return b, nil
	}
	nl := bytes.IndexByte(b, '\n')
	h, err := fsproto.ParseIndexHeader(string(b[:nl]))
	if err != nil {
		return nil, err
	}
	if h.Encoding != "" {
		return b, nil
	}
	h.Encoding = fsproto.EncodingZstd
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	out := append([]byte(h.Encode()+"\n"), enc.EncodeAll(b[nl+1:], nil)...)
	return out, enc.Close()
}

func (f *fid) readEvents(ctx context.Context, count uint32) wire.Msg {
	f.readMu.Lock()
	defer f.readMu.Unlock()
	f.mu.Lock()
	if f.stream == nil && !f.finished.Load() {
		st, err := f.a.srv.Hub.Subscribe(ctx, f.scope, f.setup)
		if err != nil {
			f.mu.Unlock()
			return errMsg(err)
		}
		f.stream = st
	}
	st := f.stream
	f.mu.Unlock()
	for len(f.pending) == 0 && !f.finished.Load() {
		evs, err := st.Next(ctx)
		if errors.Is(err, io.EOF) {
			f.finished.Store(true)
			break
		}
		if err != nil {
			return errMsg(err)
		}
		for _, ev := range evs {
			if f.a.srv.bugs[BugDropEvents] && ev.Seq%5 == 0 && ev.Op != fsproto.OpReset && ev.Op != fsproto.OpSeq {
				continue
			}
			f.pending = append(f.pending, ev.Encode()...)
			f.pending = append(f.pending, '\n')
		}
	}
	// Return as many whole records as fit.
	n := 0
	for n < len(f.pending) {
		i := bytes.IndexByte(f.pending[n:], '\n')
		if n+i+1 > int(count) {
			break
		}
		n += i + 1
	}
	if n == 0 && len(f.pending) > 0 {
		return errno(wire.EINVAL) // the next record does not fit in count
	}
	out := append([]byte(nil), f.pending[:n]...)
	f.pending = f.pending[n:]
	return &wire.Rread{Data: out}
}

func (f *fid) write(ctx context.Context, m *wire.Twrite) wire.Msg {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.opened || f.flags&3 == wire.ORDONLY && f.kind != kIndex && f.kind != kEvents {
		return errno(wire.EBADF)
	}
	switch f.kind {
	case kIndex:
		if f.index != nil {
			return errno(wire.EINVAL)
		}
		acc, err := fsproto.ParseAccept(strings.TrimRight(string(m.Data), "\n"))
		if err != nil {
			return errno(wire.EINVAL)
		}
		f.accept = acc
	case kEvents:
		if f.stream != nil || f.finished.Load() {
			return errno(wire.EINVAL)
		}
		if err := f.setup.Parse(string(m.Data)); err != nil {
			return errno(wire.EINVAL)
		}
	case kCtl:
		line := strings.TrimSuffix(string(m.Data), "\n")
		if cmd, err := fsproto.ParseCommand(line); err == nil {
			if fc, ok := cmd.(fsproto.FlagsCmd); ok && fc.Flags.Has(fsproto.FlagIndex) {
				f.reply = []byte("invalid msg=nested%20scopes%20not%20supported\n")
				break
			}
		}
		r, err := f.a.ctl(f.scope).ExecLine(ctx, line)
		if err != nil {
			return errMsg(err)
		}
		f.reply = []byte(r + "\n")
	case kStage, kReal:
		if f.stage == nil {
			return errno(wire.EBADF)
		}
		if _, err := f.stage.WriteAt(m.Data, int64(m.Offset)); err != nil {
			return errMsg(err)
		}
	default:
		return errno(wire.EBADF)
	}
	return &wire.Rwrite{Count: uint32(len(m.Data))}
}

// release closes what f holds and commits a plain write.
func (f *fid) release(ctx context.Context) error {
	f.mu.Lock()
	st := f.stream
	f.stream = nil
	f.finished.Store(true)
	f.mu.Unlock()
	if st != nil {
		st.Close()
	}
	f.readMu.Lock()
	defer f.readMu.Unlock()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.content != nil {
		f.content.Close()
		f.content = nil
	}
	if f.stage != nil {
		f.stage.Close()
		f.stage = nil
	}
	if f.plain == "" {
		return nil
	}
	srv := f.a.srv
	name := f.plain
	f.plain = ""
	mode := f.created
	if mode == 0 {
		e, err := srv.Meta.Stat(ctx, f.a.root, f.path)
		if err != nil {
			srv.Store.RemoveStage(f.a.user, name)
			return err
		}
		mode = e.Mode
	}
	w, err := f.writer()
	if err != nil {
		return err
	}
	_, err = w.Commit(ctx, f.a.origin(), fsproto.CommitCmd{
		Stage: name, Path: f.path, Base: fsproto.Any, Mtime: time.Now().Unix(), Mode: mode,
	})
	if err != nil {
		srv.Store.RemoveStage(f.a.user, name)
	}
	return err
}
