package ninep

import (
	"context"
	"errors"

	"github.com/hugelgupf/p9/linux"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

// Changes made with plain 9P. Each one goes through the scope's Writer, so
// it gets a version and an event like a ctl command. Only kReal
// directories (and .fs/stage/ for files) can be changed.

func (n *node) needRealDir() error {
	if n.kind != kReal {
		return linux.EPERM
	}
	return nil
}

func (n *node) childPath(name string) (string, error) {
	if checkName(name) != nil || name == server.ReservedName {
		return "", linux.EINVAL
	}
	return fsproto.Join(n.path, name), nil
}

// mkdir creates directory name in n.
func (n *node) mkdir(ctx context.Context, name string) (*node, attr, error) {
	if err := n.needRealDir(); err != nil {
		return nil, attr{}, err
	}
	p, err := n.childPath(name)
	if err != nil {
		return nil, attr{}, err
	}
	w, err := n.sess.writer(n.scope)
	if err != nil {
		return nil, attr{}, err
	}
	if _, err := w.Mkdir(ctx, n.sess.origin(), fsproto.MkdirCmd{Path: p}); err != nil {
		return nil, attr{}, err
	}
	return n.walk1(ctx, name)
}

// create creates file name in n and opens it for writing. In a real
// directory the empty file is committed right away, so the path exists
// for walks before the first clunk (Linux walks to a file it just
// created); its content is committed at clunk.
func (n *node) create(ctx context.Context, name string, mode uint32) (*node, attr, handle, error) {
	srv := n.srv()
	switch n.kind {
	case kStageDir:
		if err := fsproto.CheckStageName(name); err != nil {
			return nil, attr{}, nil, linux.EINVAL
		}
		if _, err := srv.cfg.Store.StatStage(n.sess.user, name); err == nil {
			return nil, attr{}, nil, linux.EEXIST
		}
		f, err := srv.cfg.Store.OpenStage(n.sess.user, name, true)
		if err != nil {
			return nil, attr{}, nil, err
		}
		c, a, err := n.walk1(ctx, name)
		if err != nil {
			f.Close()
			return nil, attr{}, nil, err
		}
		return c, a, &stageHandle{f: f}, nil
	case kReal:
	default:
		return nil, attr{}, nil, linux.EPERM
	}
	p, err := n.childPath(name)
	if err != nil {
		return nil, attr{}, nil, err
	}
	w, err := n.sess.writer(n.scope)
	if err != nil {
		return nil, attr{}, nil, err
	}
	stage := plainStageName()
	f, err := srv.cfg.Store.OpenStage(n.sess.user, stage, true)
	if err != nil {
		return nil, attr{}, nil, err
	}
	f.Close()
	_, err = w.Commit(ctx, n.sess.origin(), fsproto.CommitCmd{
		Stage: stage, Path: p, Base: fsproto.At(0),
		Mtime: srv.cfg.Now().Unix(), Mode: mode & 0o7777,
	})
	if err != nil {
		srv.cfg.Store.RemoveStage(n.sess.user, stage)
		return nil, attr{}, nil, err
	}
	c, a, err := n.walk1(ctx, name)
	if err != nil {
		return nil, attr{}, nil, err
	}
	return c, a, newPlainWrite(c, true), nil
}

// symlink creates symlink name in n pointing at target.
func (n *node) symlink(ctx context.Context, name, target string) (*node, attr, error) {
	if err := n.needRealDir(); err != nil {
		return nil, attr{}, err
	}
	p, err := n.childPath(name)
	if err != nil {
		return nil, attr{}, err
	}
	w, err := n.sess.writer(n.scope)
	if err != nil {
		return nil, attr{}, err
	}
	if _, err := w.Symlink(ctx, n.sess.origin(), p, target, fsproto.At(0)); err != nil {
		return nil, attr{}, err
	}
	return n.walk1(ctx, name)
}

// unlink removes name from n. A non-empty directory is ENOTEMPTY.
func (n *node) unlink(ctx context.Context, name string) error {
	srv := n.srv()
	if n.kind == kStageDir {
		if _, err := srv.cfg.Store.StatStage(n.sess.user, name); err != nil {
			return err
		}
		return srv.cfg.Store.RemoveStage(n.sess.user, name)
	}
	if err := n.needRealDir(); err != nil {
		return err
	}
	p, err := n.childPath(name)
	if err != nil {
		return err
	}
	w, err := n.sess.writer(n.scope)
	if err != nil {
		return err
	}
	_, err = w.Rm(ctx, n.sess.origin(), fsproto.RmCmd{Path: p})
	if errors.Is(err, fsproto.ErrInvalid) {
		if e, serr := srv.cfg.Meta.Stat(ctx, n.scope, p); serr == nil && e.Type == fsproto.TypeDir {
			return linux.ENOTEMPTY
		}
	}
	return err
}

// remove removes n itself, through its parent.
func (n *node) remove(ctx context.Context) error {
	if n.parent == nil || (n.kind != kReal && n.kind != kStage) {
		return linux.EPERM
	}
	return n.parent.unlink(ctx, n.name)
}

// rename moves name in n to newName in dir. Both must be in one scope.
func (n *node) rename(ctx context.Context, name string, dir *node, newName string) error {
	if err := n.needRealDir(); err != nil {
		return err
	}
	if err := dir.needRealDir(); err != nil {
		return err
	}
	if n.scope != dir.scope || n.sess != dir.sess {
		return linux.EXDEV
	}
	from, err := n.childPath(name)
	if err != nil {
		return err
	}
	to, err := dir.childPath(newName)
	if err != nil {
		return err
	}
	if from == to {
		return nil
	}
	w, err := n.sess.writer(n.scope)
	if err != nil {
		return err
	}
	_, err = w.Mv(ctx, n.sess.origin(), fsproto.MvCmd{From: from, To: to})
	return err
}

// setattr changes a real path's mode or mtime.
func (n *node) setattr(ctx context.Context, mode *uint32, mtime *int64) error {
	if n.kind != kReal {
		return linux.EPERM
	}
	if mode == nil && mtime == nil {
		return nil
	}
	w, err := n.sess.writer(n.scope)
	if err != nil {
		return err
	}
	_, err = w.SetAttr(ctx, n.sess.origin(), n.path, mode, mtime)
	return err
}

// truncate sets a real file's length now, outside any open handle.
func (n *node) truncate(ctx context.Context, size int64) error {
	if n.kind == kStage {
		if size != 0 {
			return linux.EINVAL
		}
		st := n.srv().cfg.Store
		if err := st.RemoveStage(n.sess.user, n.path); err != nil {
			return err
		}
		f, err := st.OpenStage(n.sess.user, n.path, true)
		if err != nil {
			return err
		}
		return f.Close()
	}
	if n.kind != kReal {
		return linux.EPERM
	}
	w := newPlainWrite(n, false)
	if err := w.truncate(size); err != nil {
		w.close(ctx)
		return err
	}
	return w.close(ctx)
}
