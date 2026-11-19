package meta

import (
	"context"
	"errors"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/store"
)

// writer is a scope's server.Writer.
type writer struct {
	m  *Meta
	sc *scope
}

var _ server.Writer = (*writer)(nil)

func isStatus(err, target error) bool { return errors.Is(err, target) }

// change is one change being built while the scope is locked.
type change struct {
	ctx  context.Context
	m    *Meta
	sc   *scope
	seq  uint64
	plan plan
	ev   fsproto.Event
	hash string
	done []func() // run after the change commits or fails
}

func (c *change) get(p string) (fsproto.Entry, bool, error) {
	return getEntry(c.ctx, c.m.rdb, c.sc.id, p)
}

// version returns p's version, 0 when it does not exist.
func (c *change) version(p string) (uint64, error) {
	e, _, err := c.get(p)
	return e.Version, err
}

func (c *change) checkBase(p string, base fsproto.Base) error {
	v, err := c.version(p)
	if err != nil {
		return err
	}
	if !base.Matches(v) {
		return fsproto.Conflict(v)
	}
	return nil
}

func (c *change) checkParent(p string) error {
	d, ok, err := c.get(fsproto.Parent(p))
	if err != nil {
		return err
	}
	if !ok {
		return fsproto.Missing("no parent directory")
	}
	if d.Type != fsproto.TypeDir {
		return fsproto.Invalid("parent is not a directory")
	}
	return nil
}

// checkPath checks a path the change touches: valid, not reserved, and not
// inside a nested scope.
func (c *change) checkPath(p string) error {
	if err := server.CheckPath(p); err != nil {
		return err
	}
	if c.sc.inNested(p) {
		return fsproto.Invalid("path is in a nested scope")
	}
	return nil
}

// rootPath converts a scope path to a root path.
func (c *change) rootPath(p string) string { return c.sc.key.RootPath(p) }

func (c *change) fs(op fsOp) { c.plan.FS = append(c.plan.FS, op) }

func (c *change) mut(mu mutation) { c.plan.Muts = append(c.plan.Muts, mu) }

func (c *change) put(e fsproto.Entry) {
	c.mut(mutation{Kind: mutPut, Entry: e})
	c.ev = fsproto.Event{Op: fsproto.OpPut, Entry: e}
}

func (c *change) place(prep store.Prepared, p string) {
	c.fs(fsOp{Kind: fsPlace, Prep: prep, Path: c.rootPath(p)})
}

// change runs build with the scope locked, then commits the plan it made
// and publishes the record.
func (w *writer) change(ctx context.Context, o server.Origin, build func(c *change) error) (server.Result, error) {
	sc := w.sc
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.gone {
		return server.Result{}, server.ErrNoScope
	}
	if sc.broken != nil {
		if err := w.m.repair(sc); err != nil {
			return server.Result{}, err
		}
	}
	c := &change{ctx: ctx, m: w.m, sc: sc, seq: sc.seq.Load() + 1}
	defer func() {
		for _, f := range c.done {
			f()
		}
	}()
	if err := build(c); err != nil {
		w.m.discard(&c.plan)
		return server.Result{}, err
	}
	ev := c.ev
	ev.Seq, ev.Dev = c.seq, o.Dev
	c.plan.Seq, c.plan.Event = c.seq, ev.Encode()
	if err := w.m.commitPlan(sc, &c.plan); err != nil {
		return server.Result{}, err
	}
	w.m.publish(sc.key, ev)
	return server.Result{Version: c.seq, Hash: c.hash, Event: ev}, nil
}

func now(c *change) int64 { return c.m.opts.Now().Unix() }

func (w *writer) Mkdir(ctx context.Context, o server.Origin, cmd fsproto.MkdirCmd) (server.Result, error) {
	return w.change(ctx, o, func(c *change) error {
		if err := c.checkPath(cmd.Path); err != nil {
			return err
		}
		cur, ok, err := c.get(cmd.Path)
		if err != nil {
			return err
		}
		if !cmd.Base.Matches(cur.Version) || ok {
			return fsproto.Conflict(cur.Version)
		}
		if err := c.checkParent(cmd.Path); err != nil {
			return err
		}
		mode, mtime := uint32(0o755), now(c)
		c.fs(fsOp{Kind: fsMkdir, Path: c.rootPath(cmd.Path), Mode: &mode, Mtime: &mtime})
		c.put(fsproto.Entry{Path: cmd.Path, Type: fsproto.TypeDir, Mode: mode, Mtime: mtime, Version: c.seq})
		return nil
	})
}

func (w *writer) remove(ctx context.Context, o server.Origin, p string, base fsproto.Base, recursive bool) (server.Result, error) {
	return w.change(ctx, o, func(c *change) error {
		if err := c.checkPath(p); err != nil {
			return err
		}
		if p == "/" {
			return fsproto.Invalid("cannot remove the root")
		}
		e, ok, err := c.get(p)
		if err != nil {
			return err
		}
		if !ok {
			return fsproto.Missing("")
		}
		if !base.Matches(e.Version) {
			return fsproto.Conflict(e.Version)
		}
		if c.sc.holdsNested(p) {
			return fsproto.Invalid("holds a nested scope")
		}
		dir := e.Type == fsproto.TypeDir
		if dir {
			below, err := hasBelow(ctx, c.m.rdb, c.sc.id, p)
			if err != nil {
				return err
			}
			if below && !recursive {
				return fsproto.Invalid("directory not empty")
			}
		}
		c.fs(fsOp{Kind: fsRemove, Path: c.rootPath(p), Recursive: recursive && dir})
		c.mut(mutation{Kind: mutDel, Path: p})
		c.ev = fsproto.Event{Op: fsproto.OpDel, Path: p}
		return nil
	})
}

func (w *writer) Rm(ctx context.Context, o server.Origin, cmd fsproto.RmCmd) (server.Result, error) {
	return w.remove(ctx, o, cmd.Path, cmd.Base, false)
}

func (w *writer) Rmtree(ctx context.Context, o server.Origin, cmd fsproto.RmtreeCmd) (server.Result, error) {
	return w.remove(ctx, o, cmd.Path, cmd.Base, true)
}

func (w *writer) Mv(ctx context.Context, o server.Origin, cmd fsproto.MvCmd) (server.Result, error) {
	return w.change(ctx, o, func(c *change) error {
		for _, p := range []string{cmd.From, cmd.To} {
			if err := c.checkPath(p); err != nil {
				return err
			}
		}
		if cmd.From == "/" || cmd.To == "/" || cmd.From == cmd.To || fsproto.Within(cmd.To, cmd.From) {
			return fsproto.Invalid("bad move")
		}
		src, ok, err := c.get(cmd.From)
		if err != nil {
			return err
		}
		if !ok {
			return fsproto.Missing("")
		}
		if !cmd.Base.Matches(src.Version) {
			return fsproto.Conflict(src.Version)
		}
		dst, dok, err := c.get(cmd.To)
		if err != nil {
			return err
		}
		if !cmd.ToBase.Matches(dst.Version) {
			return fsproto.Conflict(dst.Version)
		}
		if err := c.checkParent(cmd.To); err != nil {
			return err
		}
		if dok && (dst.Type == fsproto.TypeDir || src.Type == fsproto.TypeDir) {
			return fsproto.Invalid("cannot replace a directory")
		}
		if c.sc.holdsNested(cmd.From) {
			return fsproto.Invalid("holds a nested scope")
		}
		c.fs(fsOp{Kind: fsRename, Path: c.rootPath(cmd.From), To: c.rootPath(cmd.To)})
		c.mut(mutation{Kind: mutMove, Path: cmd.From, To: cmd.To, Version: c.seq})
		c.ev = fsproto.Event{Op: fsproto.OpMv, Path: cmd.From, To: cmd.To}
		return nil
	})
}

func (w *writer) Cp(ctx context.Context, o server.Origin, cmd fsproto.CpCmd) (server.Result, error) {
	return w.change(ctx, o, func(c *change) error {
		for _, p := range []string{cmd.From, cmd.To} {
			if err := c.checkPath(p); err != nil {
				return err
			}
		}
		if cmd.From == cmd.To {
			return fsproto.Invalid("bad copy")
		}
		src, ok, err := c.get(cmd.From)
		if err != nil {
			return err
		}
		if !ok {
			return fsproto.Missing("")
		}
		if src.Type == fsproto.TypeDir {
			return fsproto.Invalid("cannot copy a directory")
		}
		if err := c.checkBase(cmd.To, cmd.ToBase); err != nil {
			return err
		}
		if err := c.checkParent(cmd.To); err != nil {
			return err
		}
		if dst, ok, err := c.get(cmd.To); err != nil {
			return err
		} else if ok && dst.Type == fsproto.TypeDir {
			return fsproto.Invalid("is a directory")
		}
		prep, err := c.m.st.PrepareCopy(c.sc.key.Root, c.rootPath(cmd.From))
		if err != nil {
			return err
		}
		c.place(prep, cmd.To)
		e := src
		e.Path, e.Version, e.Flags, e.Attrs = cmd.To, c.seq, "", nil
		c.put(e)
		c.hash = e.Hash
		return nil
	})
}

func (w *writer) SetFlags(ctx context.Context, o server.Origin, cmd fsproto.FlagsCmd) (server.Result, error) {
	return w.change(ctx, o, func(c *change) error {
		if err := c.checkPath(cmd.Path); err != nil {
			return err
		}
		flags := cmd.Flags.Normalize()
		e, ok, err := c.get(cmd.Path)
		if err != nil {
			return err
		}
		if !ok {
			return fsproto.Missing("")
		}
		nest := flags.Has(fsproto.FlagIndex)
		if nest && e.Type != fsproto.TypeDir {
			return fsproto.Invalid("flag i needs a directory")
		}
		was := c.sc.nested[cmd.Path]
		switch {
		case nest && !was:
			if cmd.Path == "/" {
				return fsproto.Invalid("flag i on the scope root")
			}
			if c.sc.holdsNested(cmd.Path) {
				return fsproto.Invalid("holds a nested scope")
			}
			c.mut(mutation{Kind: mutNest, Path: cmd.Path})
		case !nest && was:
			child, err := c.m.scope(server.Scope{Root: c.sc.key.Root, Dir: c.rootPath(cmd.Path)})
			if err != nil {
				return err
			}
			// The child's lock keeps its own changes out until the merge
			// has committed.
			child.mu.Lock()
			c.done = append(c.done, child.mu.Unlock)
			if len(child.nested) > 0 {
				return fsproto.Invalid("holds a nested scope")
			}
			if child.broken != nil {
				if err := c.m.repair(child); err != nil {
					return err
				}
			}
			c.mut(mutation{Kind: mutUnnest, Path: cmd.Path, Version: c.seq})
		}
		e.Flags, e.Version = flags, c.seq
		c.mut(mutation{Kind: mutPut, Entry: e})
		c.ev = fsproto.Event{Op: fsproto.OpFlags, Path: cmd.Path, Flags: flags}
		return nil
	})
}

// fileTarget checks that a file may be published at p with base.
func (c *change) fileTarget(p string, base fsproto.Base) (fsproto.Entry, error) {
	if err := c.checkPath(p); err != nil {
		return fsproto.Entry{}, err
	}
	if p == "/" {
		return fsproto.Entry{}, fsproto.Invalid("cannot replace the root")
	}
	old, ok, err := c.get(p)
	if err != nil {
		return old, err
	}
	if !base.Matches(old.Version) {
		return old, fsproto.Conflict(old.Version)
	}
	if ok && old.Type == fsproto.TypeDir {
		return old, fsproto.Invalid("is a directory")
	}
	return old, c.checkParent(p)
}

// putFile records a published file, keeping the old entry's flags.
func (c *change) putFile(old fsproto.Entry, p string, mode uint32, mtime, length int64, hash string) {
	c.put(fsproto.Entry{Path: p, Type: fsproto.TypeFile, Mode: mode, Length: length, Mtime: mtime, Version: c.seq, Hash: hash, Flags: old.Flags})
	c.hash = hash
}

func (w *writer) Commit(ctx context.Context, o server.Origin, cmd fsproto.CommitCmd) (server.Result, error) {
	// Hash and sync the stage file before taking the lock, so that other
	// changes do not wait on it; PrepareStage reuses the result if nothing
	// was written since, and otherwise does the work and reports any error.
	w.m.st.VerifyStage(o.User, cmd.Stage)
	return w.change(ctx, o, func(c *change) error {
		old, err := c.fileTarget(cmd.Path, cmd.Base)
		if err != nil {
			return err
		}
		prep, err := c.m.st.PrepareStage(o.User, cmd.Stage, cmd.Hash, cmd.Mode, cmd.Mtime)
		if err != nil {
			return err
		}
		c.place(prep, cmd.Path)
		c.putFile(old, cmd.Path, cmd.Mode, cmd.Mtime, prep.Size, prep.Hash)
		return nil
	})
}

func (w *writer) CommitHash(ctx context.Context, o server.Origin, cmd fsproto.CommitHashCmd) (server.Result, error) {
	return w.change(ctx, o, func(c *change) error {
		old, err := c.fileTarget(cmd.Path, cmd.Base)
		if err != nil {
			return err
		}
		prep, err := c.m.st.PrepareHash(o.User, cmd.Hash, cmd.Mode, cmd.Mtime)
		if err != nil {
			return err
		}
		c.place(prep, cmd.Path)
		c.putFile(old, cmd.Path, cmd.Mode, cmd.Mtime, prep.Size, cmd.Hash)
		return nil
	})
}

func (w *writer) Symlink(ctx context.Context, o server.Origin, path, target string, base fsproto.Base) (server.Result, error) {
	return w.change(ctx, o, func(c *change) error {
		if _, err := c.fileTarget(path, base); err != nil {
			return err
		}
		prep, err := c.m.st.PrepareSymlink(target)
		if err != nil {
			return err
		}
		c.place(prep, path)
		c.put(fsproto.Entry{Path: path, Type: fsproto.TypeSymlink, Mode: 0o777, Length: int64(len(target)), Mtime: now(c), Version: c.seq, Target: target})
		return nil
	})
}

func (w *writer) SetAttr(ctx context.Context, o server.Origin, path string, mode *uint32, mtime *int64) (server.Result, error) {
	return w.change(ctx, o, func(c *change) error {
		if err := c.checkPath(path); err != nil {
			return err
		}
		e, ok, err := c.get(path)
		if err != nil {
			return err
		}
		if !ok {
			return fsproto.Missing("")
		}
		c.fs(fsOp{Kind: fsSetAttr, Path: c.rootPath(path), Mode: mode, Mtime: mtime})
		if mode != nil {
			e.Mode = *mode
		}
		if mtime != nil {
			e.Mtime = *mtime
		}
		e.Version = c.seq
		c.put(e)
		c.hash = e.Hash
		return nil
	})
}
