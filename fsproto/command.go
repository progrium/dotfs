package fsproto

import (
	"errors"
	"strconv"
	"strings"
)

// Base is an optional expected version in a conditional command. The zero
// value, Any, makes the operation unconditional; At(0) means the path must
// not exist.
type Base struct {
	Version uint64
	Set     bool
}

// Any is the unconditional base.
var Any = Base{}

// At returns a base expecting version v.
func At(v uint64) Base { return Base{Version: v, Set: true} }

// Matches reports whether a path at version current satisfies b.
func (b Base) Matches(current uint64) bool { return !b.Set || b.Version == current }

// Encode returns the base field: the version, or "-" for Any.
func (b Base) Encode() string {
	if !b.Set {
		return "-"
	}
	return strconv.FormatUint(b.Version, 10)
}

func parseBase(s string) (Base, error) {
	if s == "-" {
		return Any, nil
	}
	v, err := parseUint(s, "base")
	return At(v), err
}

// optional returns field i of f, or "-" past the end or at a key=value.
func optional(f []string, i int) string {
	if i < len(f) && !strings.Contains(f[i], "=") {
		return f[i]
	}
	return "-"
}

// Command names.
const (
	CmdCaps       = "caps"
	CmdHello      = "hello"
	CmdMkdir      = "mkdir"
	CmdRm         = "rm"
	CmdRmtree     = "rmtree"
	CmdMv         = "mv"
	CmdCp         = "cp"
	CmdFlags      = "flags"
	CmdStat       = "stat"
	CmdCommit     = "commit"
	CmdCommitHash = "commit-hash"
	CmdAbort      = "abort"
)

// PathCommands are the path-operation commands (extension 3).
var PathCommands = []string{CmdCaps, CmdHello, CmdMkdir, CmdRm, CmdRmtree, CmdMv, CmdCp, CmdFlags, CmdStat}

// StageCommands are the commands staged commit adds to ctl (extension 4).
var StageCommands = []string{CmdCommit, CmdCommitHash, CmdAbort}

// ErrUnknownCommand is wrapped by ParseCommand's error for a command word it
// does not know; a server replies invalid.
var ErrUnknownCommand = errors.New("fsproto: unknown command")

// Command is one ctl command. Each command type is a plain struct whose
// paths are unescaped.
type Command interface {
	// Name returns the command word.
	Name() string
	// Encode returns the command line.
	Encode() string
}

type (
	// CapsCmd: caps.
	CapsCmd struct{}
	// HelloCmd: hello <device-id>.
	HelloCmd struct{ Device string }
	// MkdirCmd: mkdir <path> [<base>].
	MkdirCmd struct {
		Path string
		Base Base
	}
	// RmCmd: rm <path> [<base>].
	RmCmd struct {
		Path string
		Base Base
	}
	// RmtreeCmd: rmtree <path> [<base>].
	RmtreeCmd struct {
		Path string
		Base Base
	}
	// MvCmd: mv <from> <to> [<base>] [<tobase>].
	MvCmd struct {
		From, To     string
		Base, ToBase Base
	}
	// CpCmd: cp <from> <to> [<tobase>].
	CpCmd struct {
		From, To string
		ToBase   Base
	}
	// FlagsCmd: flags <path> <flags>.
	FlagsCmd struct {
		Path  string
		Flags Flags
	}
	// StatCmd: stat <path>.
	StatCmd struct{ Path string }
	// CommitCmd: commit <stage> <path> <base> <mtime> <mode> [hash=<hash>].
	// Base is required on the wire; servers use Any internally for plain
	// 9P writes.
	CommitCmd struct {
		Stage string
		Path  string
		Base  Base
		Mtime int64
		Mode  uint32
		Hash  string // optional; the server verifies it
	}
	// CommitHashCmd: commit-hash <hash> <path> <base> <mtime> <mode>.
	CommitHashCmd struct {
		Hash  string
		Path  string
		Base  Base
		Mtime int64
		Mode  uint32
	}
	// AbortCmd: abort <stage>.
	AbortCmd struct{ Stage string }
)

func (CapsCmd) Name() string       { return CmdCaps }
func (HelloCmd) Name() string      { return CmdHello }
func (MkdirCmd) Name() string      { return CmdMkdir }
func (RmCmd) Name() string         { return CmdRm }
func (RmtreeCmd) Name() string     { return CmdRmtree }
func (MvCmd) Name() string         { return CmdMv }
func (CpCmd) Name() string         { return CmdCp }
func (FlagsCmd) Name() string      { return CmdFlags }
func (StatCmd) Name() string       { return CmdStat }
func (CommitCmd) Name() string     { return CmdCommit }
func (CommitHashCmd) Name() string { return CmdCommitHash }
func (AbortCmd) Name() string      { return CmdAbort }

// join builds a command line, dropping trailing "-" fields.
func join(fields ...string) string {
	for len(fields) > 1 && fields[len(fields)-1] == "-" {
		fields = fields[:len(fields)-1]
	}
	return strings.Join(fields, " ")
}

func (c CapsCmd) Encode() string  { return CmdCaps }
func (c HelloCmd) Encode() string { return CmdHello + " " + encField(c.Device) }
func (c MkdirCmd) Encode() string { return join(CmdMkdir, EscapePath(c.Path), c.Base.Encode()) }
func (c RmCmd) Encode() string    { return join(CmdRm, EscapePath(c.Path), c.Base.Encode()) }
func (c RmtreeCmd) Encode() string {
	return join(CmdRmtree, EscapePath(c.Path), c.Base.Encode())
}
func (c MvCmd) Encode() string {
	return join(CmdMv, EscapePath(c.From), EscapePath(c.To), c.Base.Encode(), c.ToBase.Encode())
}
func (c CpCmd) Encode() string {
	return join(CmdCp, EscapePath(c.From), EscapePath(c.To), c.ToBase.Encode())
}
func (c FlagsCmd) Encode() string {
	return strings.Join([]string{CmdFlags, EscapePath(c.Path), c.Flags.Encode()}, " ")
}
func (c StatCmd) Encode() string { return join(CmdStat, EscapePath(c.Path)) }
func (c CommitCmd) Encode() string {
	s := strings.Join([]string{CmdCommit, encField(c.Stage), EscapePath(c.Path), c.Base.Encode(),
		strconv.FormatInt(c.Mtime, 10), fmtMode(c.Mode)}, " ")
	if c.Hash != "" {
		s += " hash=" + c.Hash
	}
	return s
}
func (c CommitHashCmd) Encode() string {
	return strings.Join([]string{CmdCommitHash, c.Hash, EscapePath(c.Path), c.Base.Encode(),
		strconv.FormatInt(c.Mtime, 10), fmtMode(c.Mode)}, " ")
}
func (c AbortCmd) Encode() string { return join(CmdAbort, encField(c.Stage)) }

// CheckStageName reports whether s is a valid name for a file in .fs/stage/.
func CheckStageName(s string) error {
	if s == "" || s == "." || s == ".." || strings.ContainsAny(s, "/\x00") {
		return syntaxErr("bad stage name %q", s)
	}
	return nil
}

func parseStageField(s string) (string, error) {
	n, err := decField(s)
	if err != nil {
		return "", err
	}
	return n, CheckStageName(n)
}

func need(f []string, n int, line string) error {
	if len(f) < n {
		return syntaxErr("%s needs %d arguments: %q", f[0], n-1, line)
	}
	return nil
}

// ParseCommand parses one ctl command line.
func ParseCommand(line string) (Command, error) {
	f, err := splitFields(line)
	if err != nil {
		return nil, err
	}
	pathAt := func(i int) (string, error) { return parsePathField(f[i]) }
	switch f[0] {
	case CmdCaps:
		return CapsCmd{}, nil
	case CmdHello:
		if err := need(f, 2, line); err != nil {
			return nil, err
		}
		d, err := decField(f[1])
		return HelloCmd{Device: d}, err
	case CmdMkdir, CmdRm, CmdRmtree:
		if err := need(f, 2, line); err != nil {
			return nil, err
		}
		p, err := pathAt(1)
		if err != nil {
			return nil, err
		}
		b, err := parseBase(optional(f, 2))
		if err != nil {
			return nil, err
		}
		switch f[0] {
		case CmdMkdir:
			return MkdirCmd{p, b}, nil
		case CmdRm:
			return RmCmd{p, b}, nil
		}
		return RmtreeCmd{p, b}, nil
	case CmdMv:
		if err := need(f, 3, line); err != nil {
			return nil, err
		}
		var c MvCmd
		if c.From, err = pathAt(1); err != nil {
			return nil, err
		}
		if c.To, err = pathAt(2); err != nil {
			return nil, err
		}
		if c.Base, err = parseBase(optional(f, 3)); err != nil {
			return nil, err
		}
		if c.ToBase, err = parseBase(optional(f, 4)); err != nil {
			return nil, err
		}
		return c, nil
	case CmdCp:
		if err := need(f, 3, line); err != nil {
			return nil, err
		}
		var c CpCmd
		if c.From, err = pathAt(1); err != nil {
			return nil, err
		}
		if c.To, err = pathAt(2); err != nil {
			return nil, err
		}
		if c.ToBase, err = parseBase(optional(f, 3)); err != nil {
			return nil, err
		}
		return c, nil
	case CmdFlags:
		if err := need(f, 3, line); err != nil {
			return nil, err
		}
		var c FlagsCmd
		if c.Path, err = pathAt(1); err != nil {
			return nil, err
		}
		if c.Flags, err = ParseFlags(f[2]); err != nil {
			return nil, err
		}
		return c, nil
	case CmdStat:
		if err := need(f, 2, line); err != nil {
			return nil, err
		}
		p, err := pathAt(1)
		return StatCmd{p}, err
	case CmdCommit:
		if err := need(f, 6, line); err != nil {
			return nil, err
		}
		var c CommitCmd
		if c.Stage, err = parseStageField(f[1]); err != nil {
			return nil, err
		}
		if c.Path, err = pathAt(2); err != nil {
			return nil, err
		}
		if c.Base, err = parseBase(f[3]); err != nil {
			return nil, err
		}
		if !c.Base.Set {
			return nil, syntaxErr("commit needs a base: %q", line)
		}
		if c.Mtime, err = parseInt(f[4], "mtime"); err != nil {
			return nil, err
		}
		if c.Mode, err = parseMode(f[5]); err != nil {
			return nil, err
		}
		attrs, err := parseAttrs(f[6:])
		if err != nil {
			return nil, err
		}
		if h, ok := attrs.Get("hash"); ok {
			if err := CheckHash(h); err != nil {
				return nil, err
			}
			c.Hash = h
		}
		return c, nil
	case CmdCommitHash:
		if err := need(f, 6, line); err != nil {
			return nil, err
		}
		var c CommitHashCmd
		if err := CheckHash(f[1]); err != nil {
			return nil, err
		}
		c.Hash = f[1]
		if c.Path, err = pathAt(2); err != nil {
			return nil, err
		}
		if c.Base, err = parseBase(f[3]); err != nil {
			return nil, err
		}
		if !c.Base.Set {
			return nil, syntaxErr("commit-hash needs a base: %q", line)
		}
		if c.Mtime, err = parseInt(f[4], "mtime"); err != nil {
			return nil, err
		}
		if c.Mode, err = parseMode(f[5]); err != nil {
			return nil, err
		}
		return c, nil
	case CmdAbort:
		if err := need(f, 2, line); err != nil {
			return nil, err
		}
		s, err := parseStageField(f[1])
		return AbortCmd{s}, err
	}
	return nil, errors.Join(ErrUnknownCommand, syntaxErr("unknown command %q", f[0]))
}
