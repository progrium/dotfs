package fsproto

import (
	"slices"
	"strconv"
	"strings"
)

// Type is an index entry's type.
type Type byte

const (
	TypeDir     Type = 'd'
	TypeFile    Type = 'f'
	TypeSymlink Type = 'l'
)

func (t Type) valid() bool { return t == TypeDir || t == TypeFile || t == TypeSymlink }

func (t Type) String() string { return string(rune(t)) }

// Flags is a set of index flag letters, such as "p" or "ip". The empty set is
// encoded as "-". Unknown letters are kept.
type Flags string

const (
	FlagPrefetch = 'p' // fetch content right after reading the index
	FlagIndex    = 'i' // the directory has its own nested .fs/ scope
)

// Has reports whether f contains the flag letter c.
func (f Flags) Has(c byte) bool { return strings.IndexByte(string(f), c) >= 0 }

// With returns f plus c, normalized.
func (f Flags) With(c byte) Flags { return Flags(string(f) + string(c)).Normalize() }

// Without returns f minus c.
func (f Flags) Without(c byte) Flags {
	return Flags(strings.ReplaceAll(string(f), string(c), "")).Normalize()
}

// Normalize sorts and deduplicates the letters.
func (f Flags) Normalize() Flags {
	if len(f) < 2 {
		return f
	}
	b := []byte(f)
	slices.Sort(b)
	return Flags(slices.Compact(b))
}

// ParseFlags parses a flags field ("-" or lowercase letters).
func ParseFlags(s string) (Flags, error) {
	if s == "-" {
		return "", nil
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 'a' || s[i] > 'z' {
			return "", syntaxErr("bad flags %q", s)
		}
	}
	return Flags(s).Normalize(), nil
}

// Encode returns the flags field.
func (f Flags) Encode() string {
	if f == "" {
		return "-"
	}
	return string(f.Normalize())
}

func (f Flags) check() error {
	_, err := ParseFlags(f.Encode())
	return err
}

// Entry is one index line: a path's metadata at some seq.
type Entry struct {
	Path    string // unescaped, absolute from the scope root
	Type    Type
	Mode    uint32 // permission bits
	Length  int64  // bytes; 0 for directories, target length for symlinks
	Mtime   int64  // seconds since the epoch
	Version uint64 // seq of the path's last change; 0 for read-only trees
	Hash    string // "<algo>:<hex>" for files, or ""
	Flags   Flags
	Target  string // symlink target, unescaped; only for TypeSymlink
	Attrs   Attrs  // optional key=value fields
}

// Encode returns the index line for e.
func (e Entry) Encode() string {
	var b strings.Builder
	e.encodeTo(&b)
	return b.String()
}

func (e Entry) encodeTo(b *strings.Builder) {
	b.WriteString(EscapePath(e.Path))
	b.WriteByte(' ')
	b.WriteByte(byte(e.Type))
	b.WriteByte(' ')
	b.WriteString(fmtMode(e.Mode))
	b.WriteByte(' ')
	b.WriteString(strconv.FormatInt(e.Length, 10))
	b.WriteByte(' ')
	b.WriteString(strconv.FormatInt(e.Mtime, 10))
	b.WriteByte(' ')
	b.WriteString(strconv.FormatUint(e.Version, 10))
	b.WriteByte(' ')
	if e.Hash == "" {
		b.WriteByte('-')
	} else {
		b.WriteString(e.Hash)
	}
	b.WriteByte(' ')
	b.WriteString(e.Flags.Encode())
	if e.Type == TypeSymlink {
		b.WriteByte(' ')
		b.WriteString(encField(e.Target))
	}
	appendAttrs(b, e.Attrs)
}

// Check reports whether e can be encoded as a valid index line.
func (e Entry) Check() error {
	if err := CheckPath(e.Path); err != nil {
		return err
	}
	if !e.Type.valid() {
		return syntaxErr("bad type %q", byte(e.Type))
	}
	if e.Length < 0 {
		return syntaxErr("negative length")
	}
	if e.Hash != "" {
		if err := CheckHash(e.Hash); err != nil {
			return err
		}
	}
	if e.Target != "" && e.Type != TypeSymlink {
		return syntaxErr("target on a non-symlink")
	}
	if err := e.Flags.check(); err != nil {
		return err
	}
	return checkAttrs(e.Attrs)
}

// ParseEntry parses one index line.
func ParseEntry(line string) (Entry, error) {
	f, err := splitFields(line)
	if err != nil {
		return Entry{}, err
	}
	return parseEntryFields(f)
}

func parseEntryFields(f []string) (Entry, error) {
	var e Entry
	if len(f) < 8 {
		return e, syntaxErr("index line has %d fields, want at least 8", len(f))
	}
	var err error
	if e.Path, err = parsePathField(f[0]); err != nil {
		return e, err
	}
	if len(f[1]) != 1 || !Type(f[1][0]).valid() {
		return e, syntaxErr("bad type %q", f[1])
	}
	e.Type = Type(f[1][0])
	if e.Mode, err = parseMode(f[2]); err != nil {
		return e, err
	}
	if e.Length, err = parseInt(f[3], "length"); err != nil {
		return e, err
	}
	if e.Length < 0 {
		return e, syntaxErr("negative length %q", f[3])
	}
	if e.Mtime, err = parseInt(f[4], "mtime"); err != nil {
		return e, err
	}
	if e.Version, err = parseUint(f[5], "version"); err != nil {
		return e, err
	}
	if f[6] != "-" {
		if err := CheckHash(f[6]); err != nil {
			return e, err
		}
		e.Hash = f[6]
	}
	if e.Flags, err = ParseFlags(f[7]); err != nil {
		return e, err
	}
	rest := f[8:]
	if e.Type == TypeSymlink {
		if len(rest) == 0 {
			return e, syntaxErr("symlink line without target")
		}
		if e.Target, err = decField(rest[0]); err != nil {
			return e, err
		}
		rest = rest[1:]
	}
	if e.Attrs, err = parseAttrs(rest); err != nil {
		return e, err
	}
	return e, nil
}
