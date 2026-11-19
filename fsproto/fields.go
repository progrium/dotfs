package fsproto

import (
	"strconv"
	"strings"
)

// MaxLineLen is the longest line the readers in this package accept, and a
// sensible cap for servers on ctl command length.
const MaxLineLen = 1 << 20

// splitFields splits a line (with or without its trailing newline) on single
// spaces, rejecting empty lines and empty fields.
func splitFields(line string) ([]string, error) {
	line = strings.TrimSuffix(line, "\n")
	if line == "" {
		return nil, syntaxErr("empty line")
	}
	if len(line) > MaxLineLen {
		return nil, syntaxErr("line longer than %d bytes", MaxLineLen)
	}
	if strings.IndexByte(line, '\n') >= 0 {
		return nil, syntaxErr("embedded newline in %q", line)
	}
	f := strings.Split(line, " ")
	for _, s := range f {
		if s == "" {
			return nil, syntaxErr("empty field in %q", line)
		}
	}
	return f, nil
}

func parseUint(s, what string) (uint64, error) {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, syntaxErr("bad %s %q", what, s)
	}
	return v, nil
}

func parseInt(s, what string) (int64, error) {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, syntaxErr("bad %s %q", what, s)
	}
	return v, nil
}

func parseMode(s string) (uint32, error) {
	v, err := strconv.ParseUint(s, 8, 32)
	if err != nil {
		return 0, syntaxErr("bad mode %q", s)
	}
	return uint32(v), nil
}

func fmtMode(m uint32) string {
	s := strconv.FormatUint(uint64(m), 8)
	if len(s) < 4 {
		s = strings.Repeat("0", 4-len(s)) + s
	}
	return s
}

func parsePathField(s string) (string, error) {
	p, err := UnescapePath(s)
	if err != nil {
		return "", err
	}
	if err := CheckPath(p); err != nil {
		return "", err
	}
	return p, nil
}

// Attr is one optional key=value field. Value is unescaped.
type Attr struct {
	Key   string
	Value string
}

// Attrs is an ordered list of key=value fields.
type Attrs []Attr

// Get returns the value of the first attribute named key.
func (a Attrs) Get(key string) (string, bool) {
	for _, kv := range a {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return "", false
}

// Set replaces the value of key, or appends it.
func (a Attrs) Set(key, value string) Attrs {
	for i := range a {
		if a[i].Key == key {
			a[i].Value = value
			return a
		}
	}
	return append(a, Attr{key, value})
}

// Del removes every attribute named key.
func (a Attrs) Del(key string) Attrs {
	out := a[:0]
	for _, kv := range a {
		if kv.Key != key {
			out = append(out, kv)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func validKey(k string) bool {
	if k == "" {
		return false
	}
	for i := 0; i < len(k); i++ {
		c := k[i]
		if !('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '_' || c == '-' || c == '.') {
			return false
		}
	}
	return true
}

// parseAttrs collects key=value fields, ignoring fields without '=' (unknown
// trailing fields).
func parseAttrs(fields []string) (Attrs, error) {
	var a Attrs
	for _, f := range fields {
		k, v, ok := strings.Cut(f, "=")
		if !ok {
			continue
		}
		if !validKey(k) {
			return nil, syntaxErr("bad key in %q", f)
		}
		uv, err := UnescapePath(v)
		if err != nil {
			return nil, err
		}
		a = append(a, Attr{k, uv})
	}
	return a, nil
}

func appendAttrs(b *strings.Builder, a Attrs) {
	for _, kv := range a {
		b.WriteByte(' ')
		b.WriteString(kv.Key)
		b.WriteByte('=')
		b.WriteString(EscapePath(kv.Value))
	}
}

func checkAttrs(a Attrs) error {
	for _, kv := range a {
		if !validKey(kv.Key) {
			return syntaxErr("bad attribute key %q", kv.Key)
		}
	}
	return nil
}
