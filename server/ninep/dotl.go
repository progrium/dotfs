package ninep

import (
	"context"
	"sync"

	"github.com/hugelgupf/p9/fsimpl/templatefs"
	"github.com/hugelgupf/p9/linux"
	"github.com/hugelgupf/p9/p9"
)

// dotlAttacher is the p9 Attacher. Its file is the top of a namespace of
// <user>/<root> directories; the 9P2000.L layer checks each Tattach and
// rewrites its name to the canonical "<user>/<root>", which p9 walks from
// here.
type dotlAttacher struct{ srv *Server }

func (a dotlAttacher) Attach() (p9.File, error) {
	return &lfile{srv: a.srv, n: &node{top: a.srv, kind: kTop}}, nil
}

// lfile is a p9.File over a node.
type lfile struct {
	p9.DefaultWalkGetAttr
	templatefs.XattrUnimplemented
	templatefs.NotLockable
	templatefs.NoopRenamed

	srv *Server
	n   *node

	mu   sync.Mutex
	h    handle
	list []dirent // directory listing taken at Readdir offset 0
}

var _ p9.File = (*lfile)(nil)

func lerr(err error) error {
	if err == nil {
		return nil
	}
	return errno(err)
}

func lqid(q qid) p9.QID {
	return p9.QID{Type: p9.QIDType(q.typ), Version: q.vers, Path: q.path}
}

func (f *lfile) lattr(a attr) p9.Attr {
	m := p9.FileMode(a.mode & 0o7777)
	switch {
	case a.qid.typ&qtDir != 0:
		m |= p9.ModeDirectory
	case a.qid.typ&qtSymlink != 0:
		m |= p9.ModeSymlink
	default:
		m |= p9.ModeRegular
	}
	size := uint64(a.size)
	nlink := p9.NLink(1)
	if a.qid.typ&qtDir != 0 {
		nlink = 2
	}
	return p9.Attr{
		Mode: m, UID: p9.UID(f.srv.cfg.UID), GID: p9.GID(f.srv.cfg.GID), NLink: nlink,
		Size: size, BlockSize: 4096, Blocks: (size + 511) / 512,
		ATimeSeconds: uint64(a.mtime), MTimeSeconds: uint64(a.mtime), CTimeSeconds: uint64(a.mtime),
	}
}

var allValid = p9.AttrMask{Mode: true, NLink: true, UID: true, GID: true, Size: true,
	Blocks: true, ATime: true, MTime: true, CTime: true}

func (f *lfile) newFile(n *node) *lfile { return &lfile{srv: f.srv, n: n} }

func (f *lfile) Walk(names []string) ([]p9.QID, p9.File, error) {
	ctx := context.Background()
	if len(names) == 0 {
		a, err := f.n.attr(ctx)
		if err != nil {
			return nil, nil, lerr(err)
		}
		return []p9.QID{lqid(a.qid)}, f.newFile(f.n), nil
	}
	n := f.n
	qids := make([]p9.QID, 0, len(names))
	for _, name := range names {
		c, a, err := n.walk1(ctx, name)
		if err != nil {
			return nil, nil, lerr(err)
		}
		qids = append(qids, lqid(a.qid))
		n = c
	}
	return qids, f.newFile(n), nil
}

func (f *lfile) handle() handle {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.h
}

func (f *lfile) GetAttr(req p9.AttrMask) (p9.QID, p9.AttrMask, p9.Attr, error) {
	a, err := f.n.attr(context.Background())
	if err != nil {
		return p9.QID{}, p9.AttrMask{}, p9.Attr{}, lerr(err)
	}
	if w, ok := f.handle().(*plainWrite); ok {
		if sz := w.size(); sz >= 0 {
			a.size = sz
		}
	}
	return lqid(a.qid), allValid, f.lattr(a), nil
}

func (f *lfile) StatFS() (p9.FSStat, error) {
	return p9.FSStat{Type: 0x01021997, BlockSize: 4096, NameLength: 255}, nil
}

func (f *lfile) SetAttr(valid p9.SetAttrMask, sa p9.SetAttr) error {
	ctx := context.Background()
	w, _ := f.handle().(*plainWrite)
	if valid.Size {
		if w != nil {
			if err := w.truncate(int64(sa.Size)); err != nil {
				return lerr(err)
			}
		} else if err := f.n.truncate(ctx, int64(sa.Size)); err != nil {
			return lerr(err)
		}
	}
	var mode *uint32
	var mtime *int64
	if valid.Permissions {
		m := uint32(sa.Permissions.Permissions())
		mode = &m
	}
	if valid.MTime {
		t := f.srv.cfg.Now().Unix()
		if valid.MTimeNotSystemTime {
			t = int64(sa.MTimeSeconds)
		}
		mtime = &t
	}
	if mode == nil && mtime == nil {
		return nil // atime, ctime, and owners are not kept
	}
	if f.n.kind == kStage {
		return nil
	}
	if w != nil {
		// Commit pending content first, so an explicit mtime is not
		// replaced by the commit's at clunk.
		if err := w.flush(ctx); err != nil {
			return lerr(err)
		}
	}
	return lerr(f.n.setattr(ctx, mode, mtime))
}

func (f *lfile) Close() error {
	f.mu.Lock()
	h := f.h
	f.h = nil
	f.mu.Unlock()
	if h != nil {
		return lerr(h.close(context.Background()))
	}
	return nil
}

func (f *lfile) Open(mode p9.OpenFlags) (p9.QID, uint32, error) {
	ctx := context.Background()
	fl := openFlags{
		write: mode.Mode() != p9.ReadOnly,
		trunc: mode&dotlTrunc != 0,
	}
	if f.n.kind == kIndex || f.n.kind == kCtl {
		fl.trunc = false
	}
	h, err := f.n.open(ctx, fl)
	if err != nil {
		return p9.QID{}, 0, lerr(err)
	}
	a, err := f.n.attr(ctx)
	if err != nil {
		if h != nil {
			h.close(ctx)
		}
		return p9.QID{}, 0, lerr(err)
	}
	f.mu.Lock()
	f.h = h
	f.mu.Unlock()
	return lqid(a.qid), 0, nil
}

func (f *lfile) ReadAt(p []byte, off int64) (int, error) {
	h := f.handle()
	if h == nil {
		return 0, linux.EISDIR
	}
	n, err := h.read(context.Background(), p, off)
	return n, lerr(err)
}

func (f *lfile) WriteAt(p []byte, off int64) (int, error) {
	h := f.handle()
	if h == nil {
		return 0, linux.EISDIR
	}
	n, err := h.write(context.Background(), p, off)
	return n, lerr(err)
}

func (f *lfile) FSync() error {
	if w, ok := f.handle().(*plainWrite); ok {
		return lerr(w.flush(context.Background()))
	}
	return nil
}

func (f *lfile) Create(name string, flags p9.OpenFlags, perm p9.FileMode, _ p9.UID, _ p9.GID) (p9.File, p9.QID, uint32, error) {
	c, a, h, err := f.n.create(context.Background(), name, uint32(perm.Permissions()))
	if err != nil {
		return nil, p9.QID{}, 0, lerr(err)
	}
	nf := f.newFile(c)
	nf.h = h
	return nf, lqid(a.qid), 0, nil
}

func (f *lfile) Mkdir(name string, _ p9.FileMode, _ p9.UID, _ p9.GID) (p9.QID, error) {
	_, a, err := f.n.mkdir(context.Background(), name)
	if err != nil {
		return p9.QID{}, lerr(err)
	}
	return lqid(a.qid), nil
}

func (f *lfile) Symlink(target, name string, _ p9.UID, _ p9.GID) (p9.QID, error) {
	_, a, err := f.n.symlink(context.Background(), name, target)
	if err != nil {
		return p9.QID{}, lerr(err)
	}
	return lqid(a.qid), nil
}

func (f *lfile) Link(p9.File, string) error { return linux.EPERM }

func (f *lfile) Mknod(string, p9.FileMode, uint32, uint32, p9.UID, p9.GID) (p9.QID, error) {
	return p9.QID{}, linux.EPERM
}

func (f *lfile) Rename(newDir p9.File, newName string) error {
	d, ok := newDir.(*lfile)
	if !ok || f.n.parent == nil {
		return linux.EINVAL
	}
	return lerr(f.n.parent.rename(context.Background(), f.n.name, d.n, newName))
}

func (f *lfile) RenameAt(oldName string, newDir p9.File, newName string) error {
	d, ok := newDir.(*lfile)
	if !ok {
		return linux.EINVAL
	}
	return lerr(f.n.rename(context.Background(), oldName, d.n, newName))
}

func (f *lfile) UnlinkAt(name string, _ uint32) error {
	return lerr(f.n.unlink(context.Background(), name))
}

// direntFixed is an encoded dirent's size without its name.
const direntFixed = 13 + 8 + 1 + 2

func (f *lfile) Readdir(offset uint64, count uint32) (p9.Dirents, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if offset == 0 || f.list == nil {
		list, err := f.n.readdir(context.Background())
		if err != nil {
			return nil, lerr(err)
		}
		f.list = list
	}
	var out p9.Dirents
	used := 0
	for i := int(min(offset, uint64(len(f.list)))); i < len(f.list); i++ {
		d := f.list[i]
		if used+direntFixed+len(d.name) > int(count) {
			break
		}
		used += direntFixed + len(d.name)
		q := lqid(d.attr.qid)
		out = append(out, p9.Dirent{QID: q, Type: q.Type, Name: d.name, Offset: uint64(i) + 1})
	}
	return out, nil
}

func (f *lfile) Readlink() (string, error) {
	a, err := f.n.attr(context.Background())
	if err != nil {
		return "", lerr(err)
	}
	if a.qid.typ&qtSymlink == 0 {
		return "", linux.EINVAL
	}
	return a.target, nil
}

// dotlTrunc is the 9P2000.L open flag for O_TRUNC.
const dotlTrunc = p9.OpenFlags(0o1000)
