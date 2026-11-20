package mount

import (
	"context"
	"errors"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/progrium/dotfs/client"
	"github.com/progrium/dotfs/fsproto"
)

// pending is a file with local changes, or one open for writing. Its
// content lives in a private working copy, never in the shared cache, and
// is committed against base, the version it started from.
type pending struct {
	path  string
	work  string // working copy
	base  uint64 // version the content started from; 0 for a new file
	mode  uint32
	mtime int64 // an explicit mtime to commit; 0 means the commit time
	dirty bool
	gen   uint64 // bumped by each change, so a commit knows if it raced one
	refs  int    // open handles
	// detached means the path no longer refers to this working copy (a
	// remote change replaced it, or it was committed elsewhere as a
	// conflict copy); handles still open on it keep reading it.
	detached bool
	commitMu chan struct{} // held while committing
}

// Handle is an open file. Reads and writes go to File directly; call
// Wrote after each write so the change is committed.
type Handle struct {
	// File is the open content: a read-only cache file, or a working copy.
	File *os.File

	fs       *FS
	pe       *pending // nil for a read of cached content
	key      string   // the cache key, for a read of cached content
	writable bool
}

// Cached reports whether h reads immutable cached content, which a backend
// may hand to the kernel directly (FUSE passthrough).
func (h *Handle) Cached() bool { return h.pe == nil }

// Wrote marks the file changed after a write through File.
func (h *Handle) Wrote() {
	if h.pe == nil {
		return
	}
	h.fs.mu.Lock()
	h.pe.dirty = true
	h.pe.gen++
	h.fs.mu.Unlock()
}

// Flush commits the file's changes, if it has any. If the server cannot be
// reached, the changes stay local and are committed later.
func (h *Handle) Flush(ctx context.Context) error {
	if h.pe == nil || !h.writable {
		return nil
	}
	return h.fs.commit(ctx, h.pe)
}

// Release closes the handle. The last handle on a committed file moves its
// working copy into the cache.
func (h *Handle) Release() error {
	err := h.File.Close()
	if h.pe == nil {
		h.fs.cache.release(h.key)
		return err
	}
	f := h.fs
	f.mu.Lock()
	pe := h.pe
	pe.refs--
	done := pe.refs == 0 && !pe.dirty
	if done && !pe.detached {
		delete(f.pending, pe.path)
	}
	f.mu.Unlock()
	if done {
		f.retire(pe)
	} else if pe.refs == 0 {
		f.kick()
	}
	return err
}

// retire moves a clean working copy into the content cache, or removes it.
func (f *FS) retire(pe *pending) {
	hash, n, err := hashFile(pe.work)
	if err == nil && !pe.detached {
		if f.cache.adopt(pe.work, hashKey(hash), n) == nil {
			return
		}
	}
	os.Remove(pe.work)
}

func hashFile(p string) (string, int64, error) {
	r, err := os.Open(p)
	if err != nil {
		return "", 0, err
	}
	defer r.Close()
	return fsproto.HashReader(r)
}

// detach forgets pe at its path. Call with f.mu held.
func (f *FS) detach(pe *pending) {
	if f.pending[pe.path] == pe {
		delete(f.pending, pe.path)
	}
	pe.detached = true
	if pe.refs == 0 {
		os.Remove(pe.work)
	}
}

func (f *FS) newWork() (*os.File, error) {
	return os.CreateTemp(f.workDir(), "w-")
}

// Open opens the file at p. With write, reads and writes go to a private
// working copy that Flush commits; trunc empties it first.
func (f *FS) Open(ctx context.Context, p string, write, trunc bool) (*Handle, error) {
	f.mu.Lock()
	n := f.t.get(p)
	if n == nil {
		f.mu.Unlock()
		return nil, syscall.ENOENT
	}
	if n.isDir() {
		f.mu.Unlock()
		return nil, syscall.EISDIR
	}
	if n.e.Type != fsproto.TypeFile {
		f.mu.Unlock()
		return nil, syscall.EINVAL
	}
	if pe := f.pending[p]; pe != nil {
		h, err := f.openPending(pe, write, trunc)
		f.mu.Unlock()
		return h, err
	}
	e := f.t.entry(n)
	f.mu.Unlock()

	if write && trunc {
		w, err := f.newWork()
		if err != nil {
			return nil, err
		}
		w.Close()
		return f.addPending(p, e, w.Name(), true)
	}
	k, err := f.cache.get(ctx, f.conn, e)
	if err != nil {
		return nil, f.errno("read", p, err)
	}
	if !write {
		r, err := f.cache.openFile(k)
		if err != nil {
			return nil, err
		}
		return &Handle{File: r, fs: f, key: k}, nil
	}
	w, err := f.newWork()
	if err != nil {
		return nil, err
	}
	r, err := os.Open(f.cache.file(k))
	if err == nil {
		_, err = io.Copy(w, r)
		r.Close()
	}
	if cerr := w.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(w.Name())
		return nil, err
	}
	return f.addPending(p, e, w.Name(), false)
}

// addPending makes a working copy the path's pending file and opens it,
// unless another open got there first.
func (f *FS) addPending(p string, e fsproto.Entry, work string, dirty bool) (*Handle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if pe := f.pending[p]; pe != nil {
		os.Remove(work)
		return f.openPending(pe, true, dirty)
	}
	if n := f.t.get(p); n == nil || n.e.Version != e.Version {
		os.Remove(work)
		return nil, syscall.EAGAIN
	}
	pe := &pending{path: p, work: work, base: e.Version, mode: e.Mode, dirty: dirty, commitMu: make(chan struct{}, 1)}
	f.pending[p] = pe
	return f.openPending(pe, true, false)
}

// openPending opens pe's working copy. Call with f.mu held.
func (f *FS) openPending(pe *pending, write, trunc bool) (*Handle, error) {
	flag := os.O_RDONLY
	if write {
		flag = os.O_RDWR
	}
	w, err := os.OpenFile(pe.work, flag, 0)
	if err != nil {
		return nil, err
	}
	if write && trunc {
		if err := w.Truncate(0); err != nil {
			w.Close()
			return nil, err
		}
		pe.dirty = true
		pe.gen++
	}
	pe.refs++
	return &Handle{File: w, fs: f, pe: pe, writable: write}, nil
}

// Create creates an empty file at p and opens it for writing. The file
// exists locally at once and on the server at its first Flush.
func (f *FS) Create(ctx context.Context, p string, mode uint32) (*Handle, error) {
	w, err := f.newWork()
	if err != nil {
		return nil, err
	}
	w.Close()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.t.get(p) != nil {
		os.Remove(w.Name())
		return nil, syscall.EEXIST
	}
	if d := f.t.get(fsproto.Parent(p)); d == nil || !d.isDir() {
		os.Remove(w.Name())
		return nil, syscall.ENOENT
	}
	f.t.put(fsproto.Entry{Path: p, Type: fsproto.TypeFile, Mode: mode & 0o7777, Mtime: f.now().Unix()})
	pe := &pending{path: p, work: w.Name(), mode: mode & 0o7777, dirty: true, gen: 1, commitMu: make(chan struct{}, 1)}
	f.pending[p] = pe
	return f.openPending(pe, true, false)
}

// commit publishes pe's working copy if it has changes. When the file
// changed on the server since pe's base, the server's version stays at the
// path and the local one is committed beside it as a conflict copy. A
// transport failure leaves the changes pending for a later retry.
func (f *FS) commit(ctx context.Context, pe *pending) error {
	pe.commitMu <- struct{}{}
	defer func() { <-pe.commitMu }()
	f.mu.Lock()
	if !pe.dirty || pe.detached {
		f.mu.Unlock()
		return nil
	}
	gen, p, base, mode, mtime := pe.gen, pe.path, pe.base, pe.mode, pe.mtime
	flags := fsproto.Flags("")
	if n := f.t.get(p); n != nil {
		flags = n.e.Flags
	}
	f.mu.Unlock()

	r, err := os.Open(pe.work)
	if err != nil {
		return err
	}
	defer r.Close()
	fi, err := r.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()
	put := func(path string, b fsproto.Base) (fsproto.Reply, error) {
		reply, err := client.Put(ctx, f.conn, path, b, r, size, client.PutOptions{Mode: mode, Mtime: mtime})
		if errors.Is(err, fsproto.ErrMissing) {
			// The parent directory is gone; an edit beats a delete,
			// so recreate it.
			if f.mkdirs(ctx, fsproto.Parent(path)) == nil {
				reply, err = client.Put(ctx, f.conn, path, b, r, size, client.PutOptions{Mode: mode, Mtime: mtime})
			}
		}
		return reply, err
	}
	reply, err := put(p, fsproto.At(base))
	var re *fsproto.ReplyError
	if errors.As(err, &re) && re.Status == fsproto.StatusConflict && re.Version == 0 {
		// Deleted on the server: the edit wins.
		reply, err = put(p, fsproto.At(0))
	}
	if errors.As(err, &re) && re.Status == fsproto.StatusConflict {
		cp := f.conflictName(p)
		creply, cerr := put(cp, fsproto.At(0))
		if cerr != nil {
			return f.errno("commit", cp, cerr)
		}
		f.logf("conflict: %s changed on the server; kept the local copy as %s", p, cp)
		f.mu.Lock()
		f.t.put(f.committed(cp, size, mode, mtime, "", creply))
		pe.dirty = false
		f.detach(pe)
		f.mu.Unlock()
		f.send([]Change{{Op: fsproto.OpPut, Path: cp}, {Op: fsproto.OpPut, Path: p}})
		if st, err := client.Call(ctx, f.conn, fsproto.StatCmd{Path: p}); err == nil && st.Entry != nil {
			f.apply(fsproto.Event{Seq: st.Entry.Version, Op: fsproto.OpPut, Entry: *st.Entry})
			f.send([]Change{{Op: fsproto.OpPut, Path: p}})
		}
		return nil
	}
	if err != nil {
		if client.IsTransport(err) || errors.Is(err, context.DeadlineExceeded) {
			f.errno("commit", p, err)
			f.kick()
			return nil // kept locally; retried when the server is back
		}
		return f.errno("commit", p, err)
	}
	f.mu.Lock()
	if pe.path == p && !pe.detached {
		f.t.put(f.committed(p, size, mode, mtime, flags, reply))
		pe.base = reply.Version
		pe.mtime = 0
		if pe.gen == gen {
			pe.dirty = false
		}
	}
	retire := pe.refs == 0 && !pe.dirty
	if retire {
		f.detachKeep(pe)
	}
	f.mu.Unlock()
	if retire {
		f.retire(pe)
	}
	return nil
}

// detachKeep forgets pe at its path without removing its working copy.
func (f *FS) detachKeep(pe *pending) {
	if f.pending[pe.path] == pe {
		delete(f.pending, pe.path)
	}
}

func (f *FS) committed(p string, size int64, mode uint32, mtime int64, flags fsproto.Flags, r fsproto.Reply) fsproto.Entry {
	if mtime == 0 {
		mtime = f.now().Unix()
	}
	return fsproto.Entry{Path: p, Type: fsproto.TypeFile, Mode: mode, Length: size, Mtime: mtime, Version: r.Version, Hash: r.Hash, Flags: flags}
}

// mkdirs creates p and its missing parents on the server.
func (f *FS) mkdirs(ctx context.Context, p string) error {
	if p == "/" {
		return nil
	}
	if err := f.mkdirs(ctx, fsproto.Parent(p)); err != nil {
		return err
	}
	r, err := client.Call(ctx, f.conn, fsproto.MkdirCmd{Path: p, Base: fsproto.At(0)})
	if errors.Is(err, fsproto.ErrConflict) {
		return nil
	}
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.t.put(fsproto.Entry{Path: p, Type: fsproto.TypeDir, Mode: 0o755, Mtime: f.now().Unix(), Version: r.Version})
	f.mu.Unlock()
	return nil
}

// kick wakes the retry loop.
func (f *FS) kick() {
	select {
	case f.retry <- struct{}{}:
	default:
	}
}

func (f *FS) retryLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-f.retry:
		case <-t.C:
		}
		f.commitAll(ctx)
	}
}

// commitAll commits every pending file that has changes and no open
// writer (open files are committed when they are flushed).
func (f *FS) commitAll(ctx context.Context) {
	f.mu.Lock()
	var todo []*pending
	for _, pe := range f.pending {
		if pe.dirty && pe.refs == 0 {
			todo = append(todo, pe)
		}
	}
	f.mu.Unlock()
	for _, pe := range todo {
		if ctx.Err() != nil {
			return
		}
		f.commit(ctx, pe)
	}
}

// flushBelow commits every pending file at or below p, so a move or a
// delete acts on what the user sees.
func (f *FS) flushBelow(ctx context.Context, p string) error {
	f.mu.Lock()
	var todo []*pending
	for q, pe := range f.pending {
		if fsproto.Within(q, p) && pe.dirty {
			todo = append(todo, pe)
		}
	}
	f.mu.Unlock()
	for _, pe := range todo {
		if err := f.commit(ctx, pe); err != nil {
			return err
		}
		f.mu.Lock()
		dirty := pe.dirty && !pe.detached
		f.mu.Unlock()
		if dirty {
			return syscall.EIO // not committed: the server is unreachable
		}
	}
	return nil
}

// Mkdir creates a directory.
func (f *FS) Mkdir(ctx context.Context, p string) (Stat, error) {
	f.mu.Lock()
	if f.t.get(p) != nil {
		f.mu.Unlock()
		return Stat{}, syscall.EEXIST
	}
	f.mu.Unlock()
	r, err := client.Call(ctx, f.conn, fsproto.MkdirCmd{Path: p, Base: fsproto.At(0)})
	if errors.Is(err, fsproto.ErrConflict) {
		return Stat{}, syscall.EEXIST
	}
	if err != nil {
		return Stat{}, f.errno("mkdir", p, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	n := f.t.put(fsproto.Entry{Path: p, Type: fsproto.TypeDir, Mode: 0o755, Mtime: f.now().Unix(), Version: r.Version})
	return f.stat(p, n), nil
}

// Remove removes the file or empty directory at p, if it has not changed
// on the server since this FS last saw it. A file changed elsewhere in the
// meantime is kept, and Remove fails with EBUSY.
func (f *FS) Remove(ctx context.Context, p string, dir bool) error {
	f.mu.Lock()
	n := f.t.get(p)
	if n == nil {
		f.mu.Unlock()
		return syscall.ENOENT
	}
	if dir != n.isDir() {
		f.mu.Unlock()
		if dir {
			return syscall.ENOTDIR
		}
		return syscall.EISDIR
	}
	if dir && len(n.kids) > 0 {
		f.mu.Unlock()
		return syscall.ENOTEMPTY
	}
	v := n.e.Version
	if pe := f.pending[p]; pe != nil {
		f.detach(pe)
		if pe.base == 0 {
			// Never committed: nothing to remove on the server.
			f.t.del(p)
			f.mu.Unlock()
			return nil
		}
		v = pe.base
	}
	f.mu.Unlock()
	_, err := client.Call(ctx, f.conn, fsproto.RmCmd{Path: p, Base: fsproto.At(v)})
	switch {
	case err == nil, errors.Is(err, fsproto.ErrMissing):
	case errors.Is(err, fsproto.ErrConflict):
		f.logf("not removing %s: it changed on the server", p)
		return syscall.EBUSY
	case errors.Is(err, fsproto.ErrInvalid) && dir:
		return syscall.ENOTEMPTY
	default:
		return f.errno("rm", p, err)
	}
	f.mu.Lock()
	if n := f.t.get(p); n != nil && n.e.Version <= v {
		f.t.del(p)
	}
	f.mu.Unlock()
	return nil
}

// Rename moves from to to, replacing a file at to unless noReplace.
func (f *FS) Rename(ctx context.Context, from, to string, noReplace bool) error {
	if from == to {
		return nil
	}
	if fsproto.Within(to, from) {
		return syscall.EINVAL
	}
	if err := f.flushBelow(ctx, from); err != nil {
		return err
	}
	f.mu.Lock()
	src := f.t.get(from)
	if src == nil {
		f.mu.Unlock()
		return syscall.ENOENT
	}
	if d := f.t.get(fsproto.Parent(to)); d == nil || !d.isDir() {
		f.mu.Unlock()
		return syscall.ENOENT
	}
	var toBase uint64
	if dst := f.t.get(to); dst != nil {
		if noReplace {
			f.mu.Unlock()
			return syscall.EEXIST
		}
		switch {
		case dst.isDir() && !src.isDir():
			f.mu.Unlock()
			return syscall.EISDIR
		case !dst.isDir() && src.isDir():
			f.mu.Unlock()
			return syscall.ENOTDIR
		case dst.isDir() && len(dst.kids) > 0:
			f.mu.Unlock()
			return syscall.ENOTEMPTY
		}
		toBase = dst.e.Version
		if pe := f.pending[to]; pe != nil {
			f.detach(pe)
			toBase = pe.base
		}
		if dst.isDir() {
			// mv cannot replace a directory; remove the empty one first.
			f.mu.Unlock()
			if err := f.Remove(ctx, to, true); err != nil {
				return err
			}
			f.mu.Lock()
			toBase = 0
		}
	}
	base := src.e.Version
	f.mu.Unlock()
	r, err := client.Call(ctx, f.conn, fsproto.MvCmd{From: from, To: to, Base: fsproto.At(base), ToBase: fsproto.At(toBase)})
	switch {
	case err == nil:
	case errors.Is(err, fsproto.ErrConflict):
		f.logf("not moving %s to %s: one of them changed on the server", from, to)
		return syscall.EBUSY
	case errors.Is(err, fsproto.ErrInvalid):
		return syscall.EXDEV // for example, across a nested scope
	default:
		return f.errno("mv", from, err)
	}
	f.mu.Lock()
	if f.t.get(from) != nil {
		f.t.mv(from, to, r.Version)
		f.rekey(from, to)
	}
	f.mu.Unlock()
	return nil
}

// SetAttr changes a file's mode, mtime, or size; nil leaves one as it is.
// Directory modes and mtimes are not stored by the protocol, so they are
// accepted and ignored.
func (f *FS) SetAttr(ctx context.Context, p string, mode *uint32, mtime *time.Time, size *int64) (Stat, error) {
	f.mu.Lock()
	n := f.t.get(p)
	if n == nil {
		f.mu.Unlock()
		return Stat{}, syscall.ENOENT
	}
	if n.e.Type != fsproto.TypeFile {
		st := f.stat(p, n)
		f.mu.Unlock()
		if size != nil {
			return Stat{}, syscall.EISDIR
		}
		return st, nil
	}
	pe := f.pending[p]
	f.mu.Unlock()

	if pe == nil && size == nil {
		// Mode and mtime alone: commit the same content again with
		// commit-hash, so nothing is uploaded.
		f.mu.Lock()
		e := f.t.entry(n)
		f.mu.Unlock()
		if e.Hash != "" {
			m, t := e.Mode, e.Mtime
			if mode != nil {
				m = *mode & 0o7777
			}
			if mtime != nil {
				t = mtime.Unix()
			}
			if m == e.Mode && t == e.Mtime {
				return f.Lookup(p)
			}
			r, err := client.Call(ctx, f.conn, fsproto.CommitHashCmd{Hash: e.Hash, Path: p, Base: fsproto.At(e.Version), Mtime: t, Mode: m})
			if err == nil {
				e.Mode, e.Mtime, e.Version = m, t, r.Version
				f.mu.Lock()
				f.t.put(e)
				f.mu.Unlock()
				return f.Lookup(p)
			}
			if !errors.Is(err, fsproto.ErrMissing) && !errors.Is(err, fsproto.ErrInvalid) {
				return Stat{}, f.errno("setattr", p, err)
			}
		}
	}
	// Otherwise change a working copy and commit it.
	h, err := f.Open(ctx, p, true, size != nil && *size == 0)
	if err != nil {
		return Stat{}, err
	}
	f.mu.Lock()
	if mode != nil {
		h.pe.mode = *mode & 0o7777
	}
	if mtime != nil {
		h.pe.mtime = mtime.Unix()
	}
	h.pe.dirty = true
	h.pe.gen++
	f.mu.Unlock()
	if size != nil && *size != 0 {
		if err := h.File.Truncate(*size); err != nil {
			h.Release()
			return Stat{}, err
		}
	}
	f.mu.Lock()
	others := h.pe.refs > 1
	f.mu.Unlock()
	if !others {
		err = h.Flush(ctx)
	}
	h.Release()
	if err != nil {
		return Stat{}, err
	}
	return f.Lookup(p)
}

// Statfs reports the cache's limit and how much of it is in use.
func (f *FS) Statfs() (total, used int64) {
	f.cache.mu.Lock()
	defer f.cache.mu.Unlock()
	return f.cache.limit, f.cache.size
}
