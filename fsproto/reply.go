package fsproto

import (
	"fmt"
	"strconv"
	"strings"
)

// Status is a ctl reply's first word.
type Status string

const (
	StatusOK       Status = "ok"
	StatusConflict Status = "conflict" // a base did not match; carries the current version
	StatusMissing  Status = "missing"  // the path (or stage, or content) does not exist
	StatusDenied   Status = "denied"   // not allowed, or a resource limit was hit
	StatusInvalid  Status = "invalid"  // malformed, unknown, or impossible command
)

func (s Status) valid() bool {
	switch s {
	case StatusOK, StatusConflict, StatusMissing, StatusDenied, StatusInvalid:
		return true
	}
	return false
}

// Reply is a ctl reply. Which fields an ok reply carries depends on the
// command it answers:
//
//	hello, abort                       ok
//	mkdir, mv, cp, flags, commit-hash  ok <version>
//	rm, rmtree                         ok <seq>       (in Version)
//	commit                             ok <version> <hash>
//	caps                               ok <command>...
//	stat                               ok <index line>
//
// conflict carries the current version; any non-ok reply may carry a
// msg=<reason> field, kept in Message.
type Reply struct {
	Status  Status
	Version uint64
	Hash    string   // commit
	Caps    []string // caps
	Entry   *Entry   // stat
	Message string   // msg= on a non-ok reply
	Attrs   Attrs    // other key=value fields (not used with stat)
}

// OK returns an ok reply carrying version v.
func OK(v uint64) Reply { return Reply{Status: StatusOK, Version: v} }

// Err returns nil for an ok reply and a *ReplyError otherwise.
func (r Reply) Err() error {
	if r.Status == StatusOK {
		return nil
	}
	return &ReplyError{Status: r.Status, Version: r.Version, Message: r.Message}
}

// shape is the ok-reply payload kind for a command name.
type shape int

const (
	shapeNone shape = iota
	shapeVersion
	shapeVersionHash
	shapeCaps
	shapeEntry
)

func replyShape(cmd string) shape {
	switch cmd {
	case CmdMkdir, CmdRm, CmdRmtree, CmdMv, CmdCp, CmdFlags, CmdCommitHash:
		return shapeVersion
	case CmdCommit:
		return shapeVersionHash
	case CmdCaps:
		return shapeCaps
	case CmdStat:
		return shapeEntry
	}
	return shapeNone
}

// Encode returns the reply line for a reply to the named command.
func (r Reply) Encode(cmd string) string {
	var b strings.Builder
	b.WriteString(string(r.Status))
	if r.Status == StatusOK {
		switch replyShape(cmd) {
		case shapeVersion:
			b.WriteByte(' ')
			b.WriteString(strconv.FormatUint(r.Version, 10))
		case shapeVersionHash:
			b.WriteByte(' ')
			b.WriteString(strconv.FormatUint(r.Version, 10))
			b.WriteByte(' ')
			if r.Hash == "" {
				b.WriteByte('-')
			} else {
				b.WriteString(r.Hash)
			}
		case shapeCaps:
			for _, c := range r.Caps {
				b.WriteByte(' ')
				b.WriteString(c)
			}
		case shapeEntry:
			if r.Entry != nil {
				b.WriteByte(' ')
				r.Entry.encodeTo(&b)
				return b.String()
			}
		}
	} else if r.Status == StatusConflict {
		b.WriteByte(' ')
		b.WriteString(strconv.FormatUint(r.Version, 10))
	}
	if r.Message != "" && r.Status != StatusOK {
		b.WriteString(" msg=")
		b.WriteString(EscapePath(r.Message))
	}
	appendAttrs(&b, r.Attrs.Del("msg"))
	return b.String()
}

// ParseReply parses a reply line to the named command.
func ParseReply(cmd, line string) (Reply, error) {
	var r Reply
	f, err := splitFields(line)
	if err != nil {
		return r, err
	}
	r.Status = Status(f[0])
	if !r.Status.valid() {
		return r, syntaxErr("unknown reply status %q", f[0])
	}
	rest := f[1:]
	switch {
	case r.Status == StatusConflict:
		if len(rest) < 1 {
			return r, syntaxErr("conflict without version: %q", line)
		}
		if r.Version, err = parseUint(rest[0], "version"); err != nil {
			return r, err
		}
		rest = rest[1:]
	case r.Status == StatusOK:
		switch replyShape(cmd) {
		case shapeVersion, shapeVersionHash:
			if len(rest) < 1 {
				return r, syntaxErr("ok without version: %q", line)
			}
			if r.Version, err = parseUint(rest[0], "version"); err != nil {
				return r, err
			}
			rest = rest[1:]
			if replyShape(cmd) == shapeVersionHash {
				if len(rest) < 1 {
					return r, syntaxErr("commit ok without hash: %q", line)
				}
				if rest[0] != "-" {
					if err := CheckHash(rest[0]); err != nil {
						return r, err
					}
					r.Hash = rest[0]
				}
				rest = rest[1:]
			}
		case shapeCaps:
			for len(rest) > 0 && !strings.Contains(rest[0], "=") {
				r.Caps = append(r.Caps, rest[0])
				rest = rest[1:]
			}
		case shapeEntry:
			e, err := parseEntryFields(rest)
			if err != nil {
				return r, err
			}
			r.Entry = &e
			return r, nil
		}
	}
	if r.Attrs, err = parseAttrs(rest); err != nil {
		return r, err
	}
	if r.Status != StatusOK {
		r.Message, _ = r.Attrs.Get("msg")
	}
	r.Attrs = r.Attrs.Del("msg")
	return r, nil
}

// ReplyError is a non-ok ctl reply as a Go error. errors.Is matches it
// against ErrConflict, ErrMissing, ErrDenied, and ErrInvalid by status.
type ReplyError struct {
	Status  Status
	Version uint64 // the current version, for conflict
	Message string
}

func (e *ReplyError) Error() string {
	s := "dotfs: " + string(e.Status)
	if e.Status == StatusConflict {
		s += fmt.Sprintf(" (current version %d)", e.Version)
	}
	if e.Message != "" {
		s += ": " + e.Message
	}
	return s
}

// Is matches any *ReplyError with the same status.
func (e *ReplyError) Is(target error) bool {
	t, ok := target.(*ReplyError)
	return ok && t.Status == e.Status
}

// Sentinels for errors.Is. Use Conflict, Missing, Denied, and Invalid to
// make errors that carry details.
var (
	ErrConflict = &ReplyError{Status: StatusConflict}
	ErrMissing  = &ReplyError{Status: StatusMissing}
	ErrDenied   = &ReplyError{Status: StatusDenied}
	ErrInvalid  = &ReplyError{Status: StatusInvalid}
)

// Conflict returns a conflict error carrying the path's current version.
func Conflict(current uint64) error {
	return &ReplyError{Status: StatusConflict, Version: current}
}

// Missing returns a missing error with an optional reason.
func Missing(msg string) error { return &ReplyError{Status: StatusMissing, Message: msg} }

// Denied returns a denied error with an optional reason.
func Denied(msg string) error { return &ReplyError{Status: StatusDenied, Message: msg} }

// Invalid returns an invalid error with an optional reason.
func Invalid(msg string) error { return &ReplyError{Status: StatusInvalid, Message: msg} }

// ErrorReply converts an error to the reply a server sends. A *ReplyError
// (possibly wrapped) becomes its reply; a syntax error or unknown command
// becomes invalid. Any other error is returned as is, to be reported as a
// 9P error.
func ErrorReply(err error) (Reply, error) {
	if re, ok := asReplyError(err); ok {
		return Reply{Status: re.Status, Version: re.Version, Message: re.Message}, nil
	}
	if isSyntax(err) {
		return Reply{Status: StatusInvalid, Message: err.Error()}, nil
	}
	return Reply{}, err
}
