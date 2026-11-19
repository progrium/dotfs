package fake

import (
	"bytes"
	"context"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

type scopeState struct {
	seq      uint64
	entries  map[string]fsproto.Entry
	log      []fsproto.Event // changes after logStart, in seq order
	logStart uint64
}

// Meta is an in-memory server.Meta. One mutex orders every change, which is
// a valid (if coarse) way to serialize each scope's writer.
type Meta struct {
	mu     sync.Mutex
	store  server.Store
	pub    server.Publisher
	scopes map[server.Scope]*scopeState
	now    func() time.Time
}

var _ server.Meta = (*Meta)(nil)

// NewMeta returns a Meta that writes content through st and publishes each
// change to pub, which may be nil and set later with SetPublisher.
func NewMeta(st server.Store, pub server.Publisher) *Meta {
	return &Meta{store: st, pub: pub, scopes: map[server.Scope]*scopeState{}, now: time.Now}
}

// SetPublisher sets where changes are published. Call it before any change.
func (m *Meta) SetPublisher(p server.Publisher) {
	m.mu.Lock()
	m.pub = p
	m.mu.Unlock()
}

func (m *Meta) Close() error { return nil }

func (m *Meta) scope(s server.Scope) (*scopeState, error) {
	st := m.scopes[s]
	if st == nil {
		return nil, server.ErrNoScope
	}
	return st, nil
}

func (m *Meta) CreateScope(ctx context.Context, s server.Scope) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.scopes[s] != nil {
		return server.ErrScopeExists
	}
	if s.Dir == "" {
		s.Dir = "/"
	}
	st := &scopeState{entries: map[string]fsproto.Entry{}}
	if s.Dir == "/" {
		if err := m.store.CreateRoot(s.Root); err != nil {
			return err
		}
		st.entries["/"] = fsproto.Entry{Path: "/", Type: fsproto.TypeDir, Mode: 0o755, Mtime: m.now().Unix()}
	} else {
		parent := m.scopes[server.RootScope(s.Root)]
		if parent == nil {
			return server.ErrNoScope
		}
		d, ok := parent.entries[s.Dir]
		if !ok || d.Type != fsproto.TypeDir {
			return fsproto.Missing("no such directory")
		}
		for p, e := range parent.entries {
			if fsproto.Within(p, s.Dir) {
				e.Path = fsproto.Rebase(p, s.Dir, "/")
				e.Version = 0
				st.entries[e.Path] = e
			}
		}
	}
	m.scopes[s] = st
	return nil
}

func (m *Meta) Writer(s server.Scope) (server.Writer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.scope(s); err != nil {
		return nil, err
	}
	return &writer{m: m, s: s}, nil
}

func (m *Meta) Stat(ctx context.Context, s server.Scope, path string) (fsproto.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, err := m.scope(s)
	if err != nil {
		return fsproto.Entry{}, err
	}
	e, ok := st.entries[path]
	if !ok {
		return e, fsproto.Missing("")
	}
	return e, nil
}

func (m *Meta) ReadDir(ctx context.Context, s server.Scope, dir string) ([]fsproto.Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, err := m.scope(s)
	if err != nil {
		return nil, err
	}
	d, ok := st.entries[dir]
	if !ok {
		return nil, fsproto.Missing("")
	}
	if d.Type != fsproto.TypeDir {
		return nil, fsproto.Invalid("not a directory")
	}
	var out []fsproto.Entry
	for p, e := range st.entries {
		if p != "/" && fsproto.Parent(p) == dir {
			out = append(out, e)
		}
	}
	sortEntries(out)
	return out, nil
}

func sortEntries(es []fsproto.Entry) {
	sort.Slice(es, func(i, j int) bool { return fsproto.ComparePaths(es[i].Path, es[j].Path) < 0 })
}

func (m *Meta) Seq(ctx context.Context, s server.Scope) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, err := m.scope(s)
	if err != nil {
		return 0, err
	}
	return st.seq, nil
}

func (m *Meta) ReadLog(ctx context.Context, s server.Scope, since uint64, fn func(fsproto.Event) error) error {
	m.mu.Lock()
	st, err := m.scope(s)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	if since < st.logStart || since > st.seq {
		m.mu.Unlock()
		return server.ErrTrimmed
	}
	evs := append([]fsproto.Event(nil), st.log[since-st.logStart:]...)
	m.mu.Unlock()
	for _, ev := range evs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(ev); err != nil {
			return err
		}
	}
	return nil
}

func (m *Meta) Trim(ctx context.Context, s server.Scope, seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, err := m.scope(s)
	if err != nil {
		return err
	}
	if seq > st.seq {
		seq = st.seq
	}
	if seq <= st.logStart {
		return nil
	}
	st.log = append([]fsproto.Event(nil), st.log[seq-st.logStart:]...)
	st.logStart = seq
	return nil
}

func (m *Meta) Entries(ctx context.Context, s server.Scope, fn func(fsproto.Entry) error) (uint64, error) {
	m.mu.Lock()
	st, err := m.scope(s)
	if err != nil {
		m.mu.Unlock()
		return 0, err
	}
	seq := st.seq
	es := make([]fsproto.Entry, 0, len(st.entries))
	for _, e := range st.entries {
		es = append(es, e)
	}
	m.mu.Unlock()
	sortEntries(es)
	for _, e := range es {
		if err := fn(e); err != nil {
			return seq, err
		}
	}
	return seq, nil
}

func (m *Meta) OpenIndex(ctx context.Context, s server.Scope, accept []string) (io.ReadCloser, server.IndexInfo, error) {
	var buf bytes.Buffer
	var es []fsproto.Entry
	seq, err := m.Entries(ctx, s, func(e fsproto.Entry) error { es = append(es, e); return nil })
	if err != nil {
		return nil, server.IndexInfo{}, err
	}
	w, err := fsproto.NewIndexWriter(&buf, fsproto.IndexHeader{Seq: seq}, nil)
	if err != nil {
		return nil, server.IndexInfo{}, err
	}
	for _, e := range es {
		if err := w.Write(e); err != nil {
			return nil, server.IndexInfo{}, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, server.IndexInfo{}, err
	}
	return io.NopCloser(&buf), server.IndexInfo{Seq: seq, Size: int64(buf.Len())}, nil
}

// writer serializes changes to one scope through Meta's mutex.
type writer struct {
	m *Meta
	s server.Scope
}

// change runs fn with the scope locked. fn checks preconditions, applies the
// change to the store, and returns the event to record (without its seq); the
// writer assigns the seq, logs, and publishes before returning.
func (w *writer) change(o server.Origin, fn func(st *scopeState, seq uint64) (fsproto.Event, string, error)) (server.Result, error) {
	m := w.m
	m.mu.Lock()
	st, err := m.scope(w.s)
	if err != nil {
		m.mu.Unlock()
		return server.Result{}, err
	}
	seq := st.seq + 1
	ev, hash, err := fn(st, seq)
	if err != nil {
		m.mu.Unlock()
		return server.Result{}, err
	}
	ev.Seq = seq
	ev.Dev = o.Dev
	st.seq = seq
	st.log = append(st.log, ev)
	pub := m.pub
	// Publishing under the lock keeps records in seq order; Hub.Publish
	// must not block or call back into Meta.
	if pub != nil {
		pub.Publish(w.s, ev)
	}
	m.mu.Unlock()
	return server.Result{Version: seq, Hash: hash, Event: ev}, nil
}

func version(st *scopeState, p string) uint64 {
	return st.entries[p].Version // 0 when missing
}

func exists(st *scopeState, p string) bool {
	_, ok := st.entries[p]
	return ok
}

func checkParent(st *scopeState, p string) error {
	d, ok := st.entries[fsproto.Parent(p)]
	if !ok {
		return fsproto.Missing("no parent directory")
	}
	if d.Type != fsproto.TypeDir {
		return fsproto.Invalid("parent is not a directory")
	}
	return nil
}

// checkBase returns a conflict if base does not match p's current version.
func checkBase(st *scopeState, p string, base fsproto.Base) error {
	if cur := version(st, p); !base.Matches(cur) {
		return fsproto.Conflict(cur)
	}
	return nil
}

func (w *writer) root() server.Root { return w.s.Root }

func (w *writer) Mkdir(ctx context.Context, o server.Origin, c fsproto.MkdirCmd) (server.Result, error) {
	if err := server.CheckPath(c.Path); err != nil {
		return server.Result{}, err
	}
	return w.change(o, func(st *scopeState, seq uint64) (fsproto.Event, string, error) {
		if err := checkBase(st, c.Path, c.Base); err != nil {
			return fsproto.Event{}, "", err
		}
		if exists(st, c.Path) {
			return fsproto.Event{}, "", fsproto.Conflict(version(st, c.Path))
		}
		if err := checkParent(st, c.Path); err != nil {
			return fsproto.Event{}, "", err
		}
		mtime := w.m.now().Unix()
		if err := w.m.store.Mkdir(w.root(), w.s.RootPath(c.Path), 0o755, mtime); err != nil {
			return fsproto.Event{}, "", err
		}
		e := fsproto.Entry{Path: c.Path, Type: fsproto.TypeDir, Mode: 0o755, Mtime: mtime, Version: seq}
		st.entries[c.Path] = e
		return fsproto.Event{Op: fsproto.OpPut, Entry: e}, "", nil
	})
}

func (w *writer) remove(o server.Origin, p string, base fsproto.Base, recursive bool) (server.Result, error) {
	if err := server.CheckPath(p); err != nil {
		return server.Result{}, err
	}
	if p == "/" {
		return server.Result{}, fsproto.Invalid("cannot remove the root")
	}
	return w.change(o, func(st *scopeState, seq uint64) (fsproto.Event, string, error) {
		if !exists(st, p) {
			return fsproto.Event{}, "", fsproto.Missing("")
		}
		if err := checkBase(st, p, base); err != nil {
			return fsproto.Event{}, "", err
		}
		var below []string
		for q := range st.entries {
			if q != p && fsproto.Within(q, p) {
				below = append(below, q)
			}
		}
		if len(below) > 0 && !recursive {
			return fsproto.Event{}, "", fsproto.Invalid("directory not empty")
		}
		if err := w.m.store.Remove(w.root(), w.s.RootPath(p), recursive); err != nil {
			return fsproto.Event{}, "", err
		}
		for _, q := range below {
			delete(st.entries, q)
		}
		delete(st.entries, p)
		return fsproto.Event{Op: fsproto.OpDel, Path: p}, "", nil
	})
}

func (w *writer) Rm(ctx context.Context, o server.Origin, c fsproto.RmCmd) (server.Result, error) {
	return w.remove(o, c.Path, c.Base, false)
}

func (w *writer) Rmtree(ctx context.Context, o server.Origin, c fsproto.RmtreeCmd) (server.Result, error) {
	return w.remove(o, c.Path, c.Base, true)
}

func (w *writer) Mv(ctx context.Context, o server.Origin, c fsproto.MvCmd) (server.Result, error) {
	for _, p := range []string{c.From, c.To} {
		if err := server.CheckPath(p); err != nil {
			return server.Result{}, err
		}
	}
	if c.From == "/" || c.To == "/" || c.From == c.To || fsproto.Within(c.To, c.From) {
		return server.Result{}, fsproto.Invalid("bad move")
	}
	return w.change(o, func(st *scopeState, seq uint64) (fsproto.Event, string, error) {
		src, ok := st.entries[c.From]
		if !ok {
			return fsproto.Event{}, "", fsproto.Missing("")
		}
		if err := checkBase(st, c.From, c.Base); err != nil {
			return fsproto.Event{}, "", err
		}
		if err := checkBase(st, c.To, c.ToBase); err != nil {
			return fsproto.Event{}, "", err
		}
		if err := checkParent(st, c.To); err != nil {
			return fsproto.Event{}, "", err
		}
		if dst, ok := st.entries[c.To]; ok && (dst.Type == fsproto.TypeDir || src.Type == fsproto.TypeDir) {
			return fsproto.Event{}, "", fsproto.Invalid("cannot replace a directory")
		}
		if err := w.m.store.Rename(w.root(), w.s.RootPath(c.From), w.s.RootPath(c.To)); err != nil {
			return fsproto.Event{}, "", err
		}
		moved := map[string]fsproto.Entry{}
		for p, e := range st.entries {
			if fsproto.Within(p, c.From) {
				e.Path = fsproto.Rebase(p, c.From, c.To)
				if p == c.From {
					e.Version = seq
				}
				moved[e.Path] = e
				delete(st.entries, p)
			}
		}
		for p, e := range moved {
			st.entries[p] = e
		}
		return fsproto.Event{Op: fsproto.OpMv, Path: c.From, To: c.To}, "", nil
	})
}

func (w *writer) Cp(ctx context.Context, o server.Origin, c fsproto.CpCmd) (server.Result, error) {
	for _, p := range []string{c.From, c.To} {
		if err := server.CheckPath(p); err != nil {
			return server.Result{}, err
		}
	}
	if c.From == c.To {
		return server.Result{}, fsproto.Invalid("bad copy")
	}
	return w.change(o, func(st *scopeState, seq uint64) (fsproto.Event, string, error) {
		src, ok := st.entries[c.From]
		if !ok {
			return fsproto.Event{}, "", fsproto.Missing("")
		}
		if src.Type == fsproto.TypeDir {
			return fsproto.Event{}, "", fsproto.Invalid("cannot copy a directory")
		}
		if err := checkBase(st, c.To, c.ToBase); err != nil {
			return fsproto.Event{}, "", err
		}
		if err := checkParent(st, c.To); err != nil {
			return fsproto.Event{}, "", err
		}
		if dst, ok := st.entries[c.To]; ok && dst.Type == fsproto.TypeDir {
			return fsproto.Event{}, "", fsproto.Invalid("is a directory")
		}
		if err := w.m.store.Copy(w.root(), w.s.RootPath(c.From), w.s.RootPath(c.To)); err != nil {
			return fsproto.Event{}, "", err
		}
		e := src
		e.Path, e.Version, e.Flags, e.Attrs = c.To, seq, "", nil
		st.entries[c.To] = e
		return fsproto.Event{Op: fsproto.OpPut, Entry: e}, e.Hash, nil
	})
}

func (w *writer) SetFlags(ctx context.Context, o server.Origin, c fsproto.FlagsCmd) (server.Result, error) {
	if err := server.CheckPath(c.Path); err != nil {
		return server.Result{}, err
	}
	flags := c.Flags.Normalize()
	return w.change(o, func(st *scopeState, seq uint64) (fsproto.Event, string, error) {
		e, ok := st.entries[c.Path]
		if !ok {
			return fsproto.Event{}, "", fsproto.Missing("")
		}
		if flags.Has(fsproto.FlagIndex) && e.Type != fsproto.TypeDir {
			return fsproto.Event{}, "", fsproto.Invalid("flag i needs a directory")
		}
		e.Flags, e.Version = flags, seq
		st.entries[c.Path] = e
		return fsproto.Event{Op: fsproto.OpFlags, Path: c.Path, Flags: flags}, "", nil
	})
}

// putFile records a published file, keeping the old entry's flags.
func putFile(st *scopeState, p string, seq uint64, mode uint32, mtime, length int64, hash string) fsproto.Entry {
	e := fsproto.Entry{Path: p, Type: fsproto.TypeFile, Mode: mode, Length: length, Mtime: mtime, Version: seq, Hash: hash}
	if old, ok := st.entries[p]; ok {
		e.Flags = old.Flags
	}
	st.entries[p] = e
	return e
}

func (w *writer) checkFileTarget(st *scopeState, p string, base fsproto.Base) error {
	if p == "/" {
		return fsproto.Invalid("cannot replace the root")
	}
	if err := checkBase(st, p, base); err != nil {
		return err
	}
	if e, ok := st.entries[p]; ok && e.Type == fsproto.TypeDir {
		return fsproto.Invalid("is a directory")
	}
	return checkParent(st, p)
}

func (w *writer) Commit(ctx context.Context, o server.Origin, c fsproto.CommitCmd) (server.Result, error) {
	if err := server.CheckPath(c.Path); err != nil {
		return server.Result{}, err
	}
	return w.change(o, func(st *scopeState, seq uint64) (fsproto.Event, string, error) {
		if err := w.checkFileTarget(st, c.Path, c.Base); err != nil {
			return fsproto.Event{}, "", err
		}
		hash, n, err := w.m.store.Publish(o.User, c.Stage, w.root(), w.s.RootPath(c.Path), c.Mode, c.Mtime, c.Hash)
		if err != nil {
			return fsproto.Event{}, "", err
		}
		e := putFile(st, c.Path, seq, c.Mode, c.Mtime, n, hash)
		return fsproto.Event{Op: fsproto.OpPut, Entry: e}, hash, nil
	})
}

func (w *writer) CommitHash(ctx context.Context, o server.Origin, c fsproto.CommitHashCmd) (server.Result, error) {
	if err := server.CheckPath(c.Path); err != nil {
		return server.Result{}, err
	}
	return w.change(o, func(st *scopeState, seq uint64) (fsproto.Event, string, error) {
		if err := w.checkFileTarget(st, c.Path, c.Base); err != nil {
			return fsproto.Event{}, "", err
		}
		n, err := w.m.store.PublishHash(o.User, c.Hash, w.root(), w.s.RootPath(c.Path), c.Mode, c.Mtime)
		if err != nil {
			return fsproto.Event{}, "", err
		}
		e := putFile(st, c.Path, seq, c.Mode, c.Mtime, n, c.Hash)
		return fsproto.Event{Op: fsproto.OpPut, Entry: e}, c.Hash, nil
	})
}

func (w *writer) Symlink(ctx context.Context, o server.Origin, path, target string, base fsproto.Base) (server.Result, error) {
	if err := server.CheckPath(path); err != nil {
		return server.Result{}, err
	}
	return w.change(o, func(st *scopeState, seq uint64) (fsproto.Event, string, error) {
		if err := w.checkFileTarget(st, path, base); err != nil {
			return fsproto.Event{}, "", err
		}
		mtime := w.m.now().Unix()
		if err := w.m.store.Symlink(w.root(), w.s.RootPath(path), target, mtime); err != nil {
			return fsproto.Event{}, "", err
		}
		e := fsproto.Entry{Path: path, Type: fsproto.TypeSymlink, Mode: 0o777, Length: int64(len(target)), Mtime: mtime, Version: seq, Target: target}
		st.entries[path] = e
		return fsproto.Event{Op: fsproto.OpPut, Entry: e}, "", nil
	})
}

func (w *writer) SetAttr(ctx context.Context, o server.Origin, path string, mode *uint32, mtime *int64) (server.Result, error) {
	if err := server.CheckPath(path); err != nil {
		return server.Result{}, err
	}
	return w.change(o, func(st *scopeState, seq uint64) (fsproto.Event, string, error) {
		e, ok := st.entries[path]
		if !ok {
			return fsproto.Event{}, "", fsproto.Missing("")
		}
		if err := w.m.store.SetAttr(w.root(), w.s.RootPath(path), mode, mtime); err != nil {
			return fsproto.Event{}, "", err
		}
		if mode != nil {
			e.Mode = *mode
		}
		if mtime != nil {
			e.Mtime = *mtime
		}
		e.Version = seq
		st.entries[path] = e
		return fsproto.Event{Op: fsproto.OpPut, Entry: e}, e.Hash, nil
	})
}
