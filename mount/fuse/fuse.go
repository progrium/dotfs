//go:build linux

// Package fuse is the Linux backend for native on-demand files: it serves a
// mount.FS through FUSE with hanwen/go-fuse.
//
// The kernel caches entries, attributes, and file pages for a long
// timeout, and the backend invalidates them when a change arrives from
// another device, so repeated stats and reads of hydrated files are
// answered by the kernel, and the page cache keeps file content between
// opens.
//
// With Options.Passthrough, on kernels with FUSE passthrough (6.9 and
// newer) and when the mounting process may use it (it needs
// CAP_SYS_ADMIN), reads of hydrated files go from the kernel straight to
// the cached file. It is off by default: the kernel refuses (EIO) to open a
// file for writing while it is open for passthrough reading, which breaks
// programs that append to a file another one is reading.
package fuse

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/mount"
)

// DefaultTimeout is how long the kernel caches entries and attributes.
// Changes from elsewhere invalidate them right away, so it only bounds how
// long a missed invalidation could last.
const DefaultTimeout = time.Hour

// Options configure a mount.
type Options struct {
	// Timeout is how long the kernel caches entries and attributes; 0
	// means DefaultTimeout.
	Timeout time.Duration
	// Passthrough lets the kernel read hydrated files directly.
	Passthrough bool
	// AllowOther lets other users see the mount (needs user_allow_other
	// in /etc/fuse.conf for a non-root user).
	AllowOther bool
	// Name is shown as the mount's source in /proc/mounts.
	Name string
	// Debug logs every FUSE request.
	Debug bool
}

// Mount serves f at dir. Unmount with the returned server's Unmount, and
// wait for it with Wait.
func Mount(dir string, f *mount.FS, opts Options) (*fuse.Server, error) {
	if opts.Timeout == 0 {
		opts.Timeout = DefaultTimeout
	}
	if opts.Name == "" {
		opts.Name = "dotfs"
	}
	b := &backend{fs: f, opts: opts, uid: uint32(os.Getuid()), gid: uint32(os.Getgid()), changes: make(chan mount.Change, 1024)}
	root := &node{b: b}
	b.root = root
	to := opts.Timeout
	srv, err := fs.Mount(dir, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName:        opts.Name,
			Name:          "dotfs",
			AllowOther:    opts.AllowOther,
			DirectMount:   true,
			DisableXAttrs: true,
			MaxWrite:      1 << 20,
			Debug:         opts.Debug,
		},
		EntryTimeout:    &to,
		AttrTimeout:     &to,
		NegativeTimeout: &to,
		UID:             b.uid,
		GID:             b.gid,
	})
	if err != nil {
		return nil, err
	}
	go b.invalidate()
	f.SetNotify(func(c mount.Change) {
		select {
		case b.changes <- c:
		default:
			// Too far behind: drop it and invalidate everything.
			b.overflow.Store(true)
		}
	})
	go func() {
		srv.Wait()
		f.SetNotify(func(mount.Change) {})
		close(b.changes)
	}()
	return srv, nil
}

type backend struct {
	fs       *mount.FS
	opts     Options
	root     *node
	uid, gid uint32
	changes  chan mount.Change
	overflow atomic.Bool
}

// node is one inode. Its path comes from the go-fuse inode tree, which
// follows renames and removals.
type node struct {
	fs.Inode
	b *backend
}

var (
	_ fs.NodeGetattrer  = (*node)(nil)
	_ fs.NodeSetattrer  = (*node)(nil)
	_ fs.NodeLookuper   = (*node)(nil)
	_ fs.NodeReaddirer  = (*node)(nil)
	_ fs.NodeOpener     = (*node)(nil)
	_ fs.NodeCreater    = (*node)(nil)
	_ fs.NodeMkdirer    = (*node)(nil)
	_ fs.NodeUnlinker   = (*node)(nil)
	_ fs.NodeRmdirer    = (*node)(nil)
	_ fs.NodeRenamer    = (*node)(nil)
	_ fs.NodeReadlinker = (*node)(nil)
	_ fs.NodeSymlinker  = (*node)(nil)
	_ fs.NodeLinker     = (*node)(nil)
	_ fs.NodeMknoder    = (*node)(nil)
	_ fs.NodeStatfser   = (*node)(nil)

	_ fs.NodeOpendirHandler = (*node)(nil)
)

func (n *node) path() string { return "/" + n.Path(nil) }

func (n *node) child(name string) string { return fsproto.Join(n.path(), name) }

func errno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if en, ok := err.(syscall.Errno); ok {
		return en
	}
	if os.IsNotExist(err) {
		return syscall.ENOENT
	}
	return syscall.EIO
}

func typeBits(t fsproto.Type) uint32 {
	switch t {
	case fsproto.TypeDir:
		return syscall.S_IFDIR
	case fsproto.TypeSymlink:
		return syscall.S_IFLNK
	}
	return syscall.S_IFREG
}

func (b *backend) fill(st mount.Stat, a *fuse.Attr) {
	a.Ino = st.Ino
	a.Mode = typeBits(st.Type) | st.Mode&0o7777
	a.Size = uint64(st.Length)
	a.Blocks = (a.Size + 511) / 512
	a.Blksize = 1 << 16
	a.Nlink = 1
	if st.Type == fsproto.TypeDir {
		a.Nlink = 2
	}
	t := uint64(max(st.Mtime, 0))
	a.Mtime, a.Ctime, a.Atime = t, t, t
	a.Owner = fuse.Owner{Uid: b.uid, Gid: b.gid}
}

func (n *node) newChild(ctx context.Context, st mount.Stat, out *fuse.EntryOut) *fs.Inode {
	n.b.fill(st, &out.Attr)
	return n.NewInode(ctx, &node{b: n.b}, fs.StableAttr{Mode: typeBits(st.Type), Ino: st.Ino})
}

func (n *node) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	st, err := n.b.fs.Lookup(n.path())
	if err != nil {
		return errno(err)
	}
	n.b.fill(st, &out.Attr)
	return 0
}

func (n *node) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	var mode *uint32
	var mtime *time.Time
	var size *int64
	if m, ok := in.GetMode(); ok {
		mode = &m
	}
	if t, ok := in.GetMTime(); ok {
		mtime = &t
	}
	if s, ok := in.GetSize(); ok {
		v := int64(s)
		size = &v
	}
	// A truncate through an open writable handle changes that handle's
	// working copy, which is committed when the handle is flushed.
	if h, ok := fh.(*handle); ok && size != nil && h.writable {
		if err := h.File.Truncate(*size); err != nil {
			return errno(err)
		}
		h.Wrote()
		size = nil
	}
	if _, ok := in.GetUID(); ok {
		return syscall.EPERM
	}
	if _, ok := in.GetGID(); ok {
		return syscall.EPERM
	}
	var st mount.Stat
	var err error
	if mode != nil || mtime != nil || size != nil {
		st, err = n.b.fs.SetAttr(ctx, n.path(), mode, mtime, size)
	} else {
		st, err = n.b.fs.Lookup(n.path())
	}
	if err != nil {
		return errno(err)
	}
	n.b.fill(st, &out.Attr)
	return 0
}

func (n *node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	st, err := n.b.fs.Lookup(n.child(name))
	if err != nil {
		return nil, errno(err)
	}
	return n.newChild(ctx, st, out), 0
}

func (n *node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	list, err := n.b.fs.ReadDir(n.path())
	if err != nil {
		return nil, errno(err)
	}
	out := make([]fuse.DirEntry, len(list))
	for i, st := range list {
		out[i] = fuse.DirEntry{Name: fsproto.Name(st.Path), Mode: typeBits(st.Type), Ino: st.Ino}
	}
	return fs.NewListDirStream(out), 0
}

// OpendirHandle lets the kernel cache directory listings; changes from
// elsewhere invalidate them.
func (n *node) OpendirHandle(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	ds, en := n.Readdir(ctx)
	if en != 0 {
		return nil, 0, en
	}
	return &dirHandle{ds: ds}, fuse.FOPEN_CACHE_DIR | fuse.FOPEN_KEEP_CACHE, 0
}

type dirHandle struct{ ds fs.DirStream }

var (
	_ fs.FileReaddirenter = (*dirHandle)(nil)
	_ fs.FileSeekdirer    = (*dirHandle)(nil)
	_ fs.FileReleasedirer = (*dirHandle)(nil)
)

func (d *dirHandle) Readdirent(ctx context.Context) (*fuse.DirEntry, syscall.Errno) {
	if !d.ds.HasNext() {
		return nil, 0
	}
	e, en := d.ds.Next()
	return &e, en
}

func (d *dirHandle) Seekdir(ctx context.Context, off uint64) syscall.Errno {
	if sd, ok := d.ds.(fs.FileSeekdirer); ok {
		return sd.Seekdir(ctx, off)
	}
	return syscall.ENOTSUP
}

func (d *dirHandle) Releasedir(ctx context.Context, flags uint32) { d.ds.Close() }

func (n *node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	write := flags&syscall.O_ACCMODE != syscall.O_RDONLY
	h, err := n.b.fs.Open(ctx, n.path(), write, flags&syscall.O_TRUNC != 0)
	if err != nil {
		return nil, 0, errno(err)
	}
	fl := uint32(fuse.FOPEN_KEEP_CACHE)
	if !write {
		// Nothing to commit when a read-only handle closes.
		fl |= fuse.FOPEN_NOFLUSH
	}
	return n.b.wrap(h, write), fl, 0
}

func (n *node) Create(ctx context.Context, name string, flags, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	p := n.child(name)
	h, err := n.b.fs.Create(ctx, p, mode)
	if err != nil {
		return nil, nil, 0, errno(err)
	}
	st, err := n.b.fs.Lookup(p)
	if err != nil {
		h.Release()
		return nil, nil, 0, errno(err)
	}
	return n.newChild(ctx, st, out), n.b.wrap(h, true), fuse.FOPEN_KEEP_CACHE, 0
}

func (n *node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	st, err := n.b.fs.Mkdir(ctx, n.child(name))
	if err != nil {
		return nil, errno(err)
	}
	return n.newChild(ctx, st, out), 0
}

func (n *node) Unlink(ctx context.Context, name string) syscall.Errno {
	return errno(n.b.fs.Remove(ctx, n.child(name), false))
}

func (n *node) Rmdir(ctx context.Context, name string) syscall.Errno {
	return errno(n.b.fs.Remove(ctx, n.child(name), true))
}

const (
	renameNoReplace = 1 // RENAME_NOREPLACE
	renameExchange  = 2 // RENAME_EXCHANGE
)

func (n *node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if flags&renameExchange != 0 {
		return syscall.EINVAL
	}
	to := fsproto.Join("/"+newParent.EmbeddedInode().Path(nil), newName)
	return errno(n.b.fs.Rename(ctx, n.child(name), to, flags&renameNoReplace != 0))
}

func (n *node) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	st, err := n.b.fs.Lookup(n.path())
	if err != nil {
		return nil, errno(err)
	}
	if st.Type != fsproto.TypeSymlink {
		return nil, syscall.EINVAL
	}
	return []byte(st.Target), 0
}

// Symlinks, hard links, and device files cannot be created through the
// protocol's commands.

func (n *node) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EPERM
}

func (n *node) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EPERM
}

func (n *node) Mknod(ctx context.Context, name string, mode, dev uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.EPERM
}

func (n *node) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	total, used := n.b.fs.Statfs()
	const bs = 1 << 16
	out.Bsize, out.Frsize = bs, bs
	out.Blocks = uint64(total) / bs
	free := uint64(max(total-used, 0)) / bs
	out.Bfree, out.Bavail = free, free
	out.Files, out.Ffree = 1<<32, 1<<31
	out.NameLen = 255
	return 0
}

// handle is an open file.
type handle struct {
	*mount.Handle
	writable    bool
	passthrough bool
}

// passHandle is a handle the kernel may read directly (FUSE passthrough).
type passHandle struct{ *handle }

func (b *backend) wrap(h *mount.Handle, write bool) fs.FileHandle {
	fh := &handle{Handle: h, writable: write}
	if !write && h.Cached() && b.opts.Passthrough {
		return &passHandle{fh}
	}
	return fh
}

var (
	_ fs.FileReader          = (*handle)(nil)
	_ fs.FileWriter          = (*handle)(nil)
	_ fs.FileFlusher         = (*handle)(nil)
	_ fs.FileFsyncer         = (*handle)(nil)
	_ fs.FileReleaser        = (*handle)(nil)
	_ fs.FilePassthroughFder = (*passHandle)(nil)
)

func (h *passHandle) PassthroughFd() (int, bool) { return int(h.File.Fd()), true }

func (h *handle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n, err := h.File.ReadAt(dest, off)
	if err != nil && n == 0 && !errors.Is(err, io.EOF) {
		return nil, errno(err)
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func (h *handle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if !h.writable {
		return 0, syscall.EBADF
	}
	n, err := h.File.WriteAt(data, off)
	if n > 0 {
		h.Wrote()
	}
	if err != nil {
		return uint32(n), errno(err)
	}
	return uint32(n), 0
}

func (h *handle) Flush(ctx context.Context) syscall.Errno {
	return errno(h.Handle.Flush(ctx))
}

func (h *handle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return errno(h.Handle.Flush(ctx))
}

func (h *handle) Release(ctx context.Context) syscall.Errno {
	return errno(h.Handle.Release())
}

// invalidate tells the kernel about changes made elsewhere. It runs on its
// own goroutine, since a notification sent while serving a request can
// deadlock with the kernel.
func (b *backend) invalidate() {
	for c := range b.changes {
		if b.overflow.Swap(false) {
			b.resetAll(b.root.EmbeddedInode())
		}
		switch c.Op {
		case fsproto.OpReset:
			b.resetAll(b.root.EmbeddedInode())
		case fsproto.OpPut:
			b.put(c.Path)
		case fsproto.OpDel:
			if dir, name, child := b.find(c.Path); dir != nil {
				dir.NotifyContent(0, 0)
				if child != nil {
					dir.NotifyDelete(name, child)
					dir.RmChild(name)
				} else {
					dir.NotifyEntry(name)
				}
			}
		case fsproto.OpMv:
			dir, name, child := b.find(c.Path)
			ndir, nname, _ := b.find(c.To)
			if dir != nil {
				dir.NotifyContent(0, 0)
				dir.NotifyEntry(name)
			}
			if ndir != nil {
				ndir.NotifyContent(0, 0)
				ndir.NotifyEntry(nname)
			}
			if child != nil {
				if ndir != nil {
					dir.MvChild(name, ndir, nname, true)
				} else {
					dir.RmChild(name)
				}
			}
		}
	}
}

func (b *backend) put(p string) {
	dir, name, child := b.find(p)
	if dir == nil {
		return
	}
	if child != nil {
		st, err := b.fs.Lookup(p)
		if err != nil || typeBits(st.Type) != child.StableAttr().Mode || st.Ino != child.StableAttr().Ino {
			dir.RmChild(name)
			dir.NotifyContent(0, 0)
		} else {
			child.NotifyContent(0, 0)
		}
	} else {
		dir.NotifyContent(0, 0) // a new name in the listing
	}
	dir.NotifyEntry(name)
}

// find returns the inode the kernel knows for p's parent, p's name, and
// p's inode, each nil when the kernel has not looked it up.
func (b *backend) find(p string) (dir *fs.Inode, name string, child *fs.Inode) {
	if p == "/" {
		return nil, "", nil
	}
	dir = b.root.EmbeddedInode()
	parts := strings.Split(p[1:], "/")
	for _, c := range parts[:len(parts)-1] {
		if dir = dir.GetChild(c); dir == nil {
			return nil, "", nil
		}
	}
	name = parts[len(parts)-1]
	return dir, name, dir.GetChild(name)
}

func (b *backend) resetAll(n *fs.Inode) {
	n.NotifyContent(0, 0)
	for name, c := range n.Children() {
		n.NotifyEntry(name)
		b.resetAll(c)
	}
}
