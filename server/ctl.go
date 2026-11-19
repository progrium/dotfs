package server

import (
	"context"
	"strings"
	"sync"

	"github.com/progrium/dotfs/fsproto"
)

// ReservedName is the synthetic directory name that no change may touch.
const ReservedName = ".fs"

// CheckPath validates a path a client wants to change: it must be a valid
// protocol path with no ".fs" component. It returns fsproto.ErrInvalid with
// a reason otherwise.
func CheckPath(p string) error {
	if err := fsproto.CheckPath(p); err != nil {
		return fsproto.Invalid("bad path")
	}
	if p == "/"+ReservedName || strings.HasPrefix(p, "/"+ReservedName+"/") ||
		strings.Contains(p, "/"+ReservedName+"/") || strings.HasSuffix(p, "/"+ReservedName) {
		return fsproto.Invalid("reserved path")
	}
	return nil
}

// Ctl executes ctl commands for one attach: it maps commands onto a scope's
// Writer, Meta, and Store, and remembers the device id from hello. It is
// shared by the 9P server and the in-process fake client so both behave the
// same. It is safe for concurrent use by several ctl fids.
type Ctl struct {
	Meta  Meta
	Store Store
	Scope Scope
	// Caps lists the commands offered; nil means every command.
	Caps []string

	mu     sync.Mutex
	origin Origin
}

// NewCtl returns a Ctl for user's attach to scope s.
func NewCtl(m Meta, st Store, s Scope, user string) *Ctl {
	return &Ctl{Meta: m, Store: st, Scope: s, origin: Origin{User: user}}
}

// Origin returns who changes made through this Ctl come from.
func (c *Ctl) Origin() Origin {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.origin
}

func (c *Ctl) caps() []string {
	if c.Caps != nil {
		return c.Caps
	}
	return append(append([]string{}, fsproto.PathCommands...), fsproto.StageCommands...)
}

func (c *Ctl) offers(name string) bool {
	for _, n := range c.caps() {
		if n == name {
			return true
		}
	}
	return false
}

// ExecLine parses one command line, executes it, and returns the reply
// line. Only errors that are not protocol outcomes (I/O failures) are
// returned, to be reported as 9P errors.
func (c *Ctl) ExecLine(ctx context.Context, line string) (string, error) {
	cmd, err := fsproto.ParseCommand(line)
	if err != nil {
		r, _ := fsproto.ErrorReply(err)
		return r.Encode(""), nil
	}
	r, err := c.Exec(ctx, cmd)
	if err != nil {
		return "", err
	}
	return r.Encode(cmd.Name()), nil
}

// Exec executes one command. Only errors that are not protocol outcomes
// are returned.
func (c *Ctl) Exec(ctx context.Context, cmd fsproto.Command) (fsproto.Reply, error) {
	if !c.offers(cmd.Name()) {
		return fsproto.Reply{Status: fsproto.StatusInvalid, Message: "unsupported command"}, nil
	}
	r, err := c.exec(ctx, cmd)
	if err != nil {
		return fsproto.ErrorReply(err)
	}
	return r, nil
}

func ok(res Result, err error) (fsproto.Reply, error) {
	if err != nil {
		return fsproto.Reply{}, err
	}
	return fsproto.Reply{Status: fsproto.StatusOK, Version: res.Version, Hash: res.Hash}, nil
}

func (c *Ctl) exec(ctx context.Context, cmd fsproto.Command) (fsproto.Reply, error) {
	switch cmd := cmd.(type) {
	case fsproto.CapsCmd:
		return fsproto.Reply{Status: fsproto.StatusOK, Caps: c.caps()}, nil
	case fsproto.HelloCmd:
		c.mu.Lock()
		c.origin.Dev = cmd.Device
		c.mu.Unlock()
		return fsproto.Reply{Status: fsproto.StatusOK}, nil
	case fsproto.StatCmd:
		e, err := c.Meta.Stat(ctx, c.Scope, cmd.Path)
		if err != nil {
			return fsproto.Reply{}, err
		}
		return fsproto.Reply{Status: fsproto.StatusOK, Entry: &e}, nil
	case fsproto.AbortCmd:
		if err := c.Store.RemoveStage(c.Origin().User, cmd.Stage); err != nil {
			return fsproto.Reply{}, err
		}
		return fsproto.Reply{Status: fsproto.StatusOK}, nil
	}
	w, err := c.Meta.Writer(c.Scope)
	if err != nil {
		return fsproto.Reply{}, err
	}
	o := c.Origin()
	switch cmd := cmd.(type) {
	case fsproto.MkdirCmd:
		return ok(w.Mkdir(ctx, o, cmd))
	case fsproto.RmCmd:
		return ok(w.Rm(ctx, o, cmd))
	case fsproto.RmtreeCmd:
		return ok(w.Rmtree(ctx, o, cmd))
	case fsproto.MvCmd:
		return ok(w.Mv(ctx, o, cmd))
	case fsproto.CpCmd:
		return ok(w.Cp(ctx, o, cmd))
	case fsproto.FlagsCmd:
		return ok(w.SetFlags(ctx, o, cmd))
	case fsproto.CommitCmd:
		if !cmd.Base.Set {
			return fsproto.Reply{}, fsproto.Invalid("commit needs a base")
		}
		return ok(w.Commit(ctx, o, cmd))
	case fsproto.CommitHashCmd:
		if !cmd.Base.Set {
			return fsproto.Reply{}, fsproto.Invalid("commit-hash needs a base")
		}
		return ok(w.CommitHash(ctx, o, cmd))
	}
	return fsproto.Reply{}, fsproto.Invalid("unknown command")
}
