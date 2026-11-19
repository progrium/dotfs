package fsproto

import (
	"strconv"
	"strings"
)

// Op is an event record's operation.
type Op string

const (
	OpPut   Op = "put"   // create or modify; carries the whole index line
	OpDel   Op = "del"   // removal; for a directory, everything below it too
	OpMv    Op = "mv"    // rename; for a directory, everything below it too
	OpFlags Op = "flags" // index flags changed
	OpReset Op = "reset" // the stream fell behind; re-read the index
	// OpSeq is the "seq <current>" record that starts a stream opened
	// without since. It has no leading seq field.
	OpSeq Op = "seq"
)

// Event is one record read from .fs/events.
type Event struct {
	Seq   uint64
	Op    Op
	Entry Entry  // OpPut: the index line (Entry.Path is the path)
	Path  string // OpDel, OpFlags, and OpMv (the source)
	To    string // OpMv: the destination
	Flags Flags  // OpFlags
	Dev   string // the dev= field, when known
	Attrs Attrs  // other key=value fields
}

// Target returns the path the record is about: Entry.Path for put, Path for
// the others.
func (ev Event) Target() string {
	if ev.Op == OpPut {
		return ev.Entry.Path
	}
	return ev.Path
}

// Encode returns the record line.
func (ev Event) Encode() string {
	var b strings.Builder
	if ev.Op == OpSeq {
		b.WriteString("seq ")
		b.WriteString(strconv.FormatUint(ev.Seq, 10))
		return b.String()
	}
	b.WriteString(strconv.FormatUint(ev.Seq, 10))
	b.WriteByte(' ')
	b.WriteString(string(ev.Op))
	switch ev.Op {
	case OpPut:
		b.WriteByte(' ')
		e := ev.Entry
		e.Attrs = e.Attrs.Del("dev")
		e.encodeTo(&b)
	case OpDel:
		b.WriteByte(' ')
		b.WriteString(EscapePath(ev.Path))
	case OpMv:
		b.WriteByte(' ')
		b.WriteString(EscapePath(ev.Path))
		b.WriteByte(' ')
		b.WriteString(EscapePath(ev.To))
	case OpFlags:
		b.WriteByte(' ')
		b.WriteString(EscapePath(ev.Path))
		b.WriteByte(' ')
		b.WriteString(ev.Flags.Encode())
	case OpReset:
		return b.String()
	}
	if ev.Dev != "" {
		b.WriteString(" dev=")
		b.WriteString(EscapePath(ev.Dev))
	}
	appendAttrs(&b, ev.Attrs.Del("dev"))
	return b.String()
}

// ParseEvent parses one record line.
func ParseEvent(line string) (Event, error) {
	var ev Event
	f, err := splitFields(line)
	if err != nil {
		return ev, err
	}
	if f[0] == "seq" {
		if len(f) < 2 {
			return ev, syntaxErr("bad seq record %q", line)
		}
		ev.Op = OpSeq
		ev.Seq, err = parseUint(f[1], "seq")
		return ev, err
	}
	if len(f) < 2 {
		return ev, syntaxErr("bad event %q", line)
	}
	if ev.Seq, err = parseUint(f[0], "seq"); err != nil {
		return ev, err
	}
	ev.Op = Op(f[1])
	var rest []string
	switch ev.Op {
	case OpPut:
		if ev.Entry, err = parseEntryFields(f[2:]); err != nil {
			return ev, err
		}
		ev.Dev, _ = ev.Entry.Attrs.Get("dev")
		ev.Entry.Attrs = ev.Entry.Attrs.Del("dev")
		return ev, nil
	case OpDel:
		if len(f) < 3 {
			return ev, syntaxErr("bad del record %q", line)
		}
		if ev.Path, err = parsePathField(f[2]); err != nil {
			return ev, err
		}
		rest = f[3:]
	case OpMv:
		if len(f) < 4 {
			return ev, syntaxErr("bad mv record %q", line)
		}
		if ev.Path, err = parsePathField(f[2]); err != nil {
			return ev, err
		}
		if ev.To, err = parsePathField(f[3]); err != nil {
			return ev, err
		}
		rest = f[4:]
	case OpFlags:
		if len(f) < 4 {
			return ev, syntaxErr("bad flags record %q", line)
		}
		if ev.Path, err = parsePathField(f[2]); err != nil {
			return ev, err
		}
		if ev.Flags, err = ParseFlags(f[3]); err != nil {
			return ev, err
		}
		rest = f[4:]
	case OpReset:
		return ev, nil
	default:
		return ev, syntaxErr("unknown event op %q", f[1])
	}
	if ev.Attrs, err = parseAttrs(rest); err != nil {
		return ev, err
	}
	ev.Dev, _ = ev.Attrs.Get("dev")
	ev.Attrs = ev.Attrs.Del("dev")
	return ev, nil
}

// EventsSetup is what a client writes to an events fid before its first
// read: "since <N>", "path <dir>" (repeatable), and "coalesce".
type EventsSetup struct {
	Since    uint64
	HasSince bool     // without since, the stream starts with "seq <current>"
	Paths    []string // only changes at or below these directories; none means all
	Coalesce bool
}

// Encode returns the setup as newline-separated lines, or "" when there is
// nothing to write.
func (s EventsSetup) Encode() string {
	var lines []string
	if s.HasSince {
		lines = append(lines, "since "+strconv.FormatUint(s.Since, 10))
	}
	for _, p := range s.Paths {
		lines = append(lines, "path "+EscapePath(p))
	}
	if s.Coalesce {
		lines = append(lines, "coalesce")
	}
	return strings.Join(lines, "\n")
}

// Parse applies one write (one or more lines) to s.
func (s *EventsSetup) Parse(data string) error {
	data = strings.TrimSuffix(data, "\n")
	for _, line := range strings.Split(data, "\n") {
		f, err := splitFields(line)
		if err != nil {
			return err
		}
		switch f[0] {
		case "since":
			if len(f) < 2 {
				return syntaxErr("bad since %q", line)
			}
			if s.Since, err = parseUint(f[1], "seq"); err != nil {
				return err
			}
			s.HasSince = true
		case "path":
			if len(f) < 2 {
				return syntaxErr("bad path %q", line)
			}
			p, err := parsePathField(f[1])
			if err != nil {
				return err
			}
			s.Paths = append(s.Paths, p)
		case "coalesce":
			s.Coalesce = true
		default:
			return syntaxErr("unknown events setup %q", line)
		}
	}
	return nil
}

// Match reports whether a record passes the setup's path filters. A mv
// matches if either side is within a filter; reset and seq always match.
func (s EventsSetup) Match(ev Event) bool {
	if len(s.Paths) == 0 || ev.Op == OpReset || ev.Op == OpSeq {
		return true
	}
	for _, dir := range s.Paths {
		if Within(ev.Target(), dir) || (ev.Op == OpMv && Within(ev.To, dir)) {
			return true
		}
	}
	return false
}
