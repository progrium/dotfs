package fsproto

import (
	"errors"
	"fmt"
	"strings"
)

// ErrSyntax is wrapped by every error that reports a malformed line or field.
var ErrSyntax = errors.New("fsproto: syntax error")

func syntaxErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrSyntax, fmt.Sprintf(format, args...))
}

const upperhex = "0123456789ABCDEF"

// needsEscape reports whether b must be percent-escaped in a field: anything
// outside printable ASCII, plus space and '%'.
func needsEscape(b byte) bool {
	return b <= ' ' || b >= 0x7f || b == '%'
}

// EscapePath percent-escapes every byte of s outside printable ASCII, plus
// space and '%', using uppercase hex. It is used for paths and for every other
// free-form field (symlink targets, stage names, device ids, and key=value
// values). The empty string escapes to itself.
func EscapePath(s string) string {
	n := 0
	for i := 0; i < len(s); i++ {
		if needsEscape(s[i]) {
			n++
		}
	}
	if n == 0 {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 2*n)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if needsEscape(c) {
			b.WriteByte('%')
			b.WriteByte(upperhex[c>>4])
			b.WriteByte(upperhex[c&15])
		} else {
			b.WriteByte(c)
		}
	}
	return b.String()
}

func unhex(c byte) (byte, bool) {
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, true
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// UnescapePath reverses EscapePath. It accepts either case of hex digits and
// unnecessary escapes (such as %41), and rejects a malformed escape or a raw
// byte that should have been escaped.
func UnescapePath(s string) (string, error) {
	n := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '%' {
			n++
			continue
		}
		if needsEscape(c) {
			return "", syntaxErr("unescaped byte %#02x in %q", c, s)
		}
	}
	if n == 0 {
		return s, nil
	}
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c != '%' {
			b = append(b, c)
			continue
		}
		if i+2 >= len(s) {
			return "", syntaxErr("truncated escape in %q", s)
		}
		hi, ok1 := unhex(s[i+1])
		lo, ok2 := unhex(s[i+2])
		if !ok1 || !ok2 {
			return "", syntaxErr("bad escape in %q", s)
		}
		b = append(b, hi<<4|lo)
		i += 2
	}
	return string(b), nil
}

// encField encodes a free-form field: "-" for empty, and a value that is
// literally "-" as "%2D" so it is not read back as empty.
func encField(s string) string {
	if s == "" {
		return "-"
	}
	if s == "-" {
		return "%2D"
	}
	return EscapePath(s)
}

// decField reverses encField.
func decField(s string) (string, error) {
	if s == "-" {
		return "", nil
	}
	return UnescapePath(s)
}
