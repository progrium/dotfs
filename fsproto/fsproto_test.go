package fsproto

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
)

const h1 = "b3:5e1a00000000000000000000000000000000000000000000000000000000beef"

func TestEscape(t *testing.T) {
	for _, c := range []struct{ raw, esc string }{
		{"/Documents/Tax 2025.pdf", "/Documents/Tax%202025.pdf"},
		{"/100%", "/100%25"},
		{"/a\nb", "/a%0Ab"},
		{"/caf\u00e9", "/caf%C3%A9"},
		{"/tab\there", "/tab%09here"},
		{"/del\x7f", "/del%7F"},
		{"/plain-name_1.txt", "/plain-name_1.txt"},
		{"", ""},
	} {
		if got := EscapePath(c.raw); got != c.esc {
			t.Errorf("EscapePath(%q) = %q, want %q", c.raw, got, c.esc)
		}
		if got, err := UnescapePath(c.esc); err != nil || got != c.raw {
			t.Errorf("UnescapePath(%q) = %q, %v", c.esc, got, err)
		}
	}
	if got, err := UnescapePath("/%41%c3%a9"); err != nil || got != "/A\u00e9" {
		t.Errorf("lenient unescape: %q %v", got, err)
	}
	for _, bad := range []string{"/a%", "/a%2", "/a%zz", "/caf\u00e9", "/a\tb"} {
		if _, err := UnescapePath(bad); !errors.Is(err, ErrSyntax) {
			t.Errorf("UnescapePath(%q) err = %v", bad, err)
		}
	}
}

func TestPaths(t *testing.T) {
	for _, p := range []string{"/", "/a", "/a/b c", "/.fs/index", "/...", "/a.b"} {
		if err := CheckPath(p); err != nil {
			t.Errorf("CheckPath(%q) = %v", p, err)
		}
	}
	for _, p := range []string{"", "a", "/a/", "//a", "/a//b", "/.", "/a/..", "/a\x00"} {
		if err := CheckPath(p); err == nil {
			t.Errorf("CheckPath(%q) = nil", p)
		}
	}
	within := []struct {
		p, dir string
		ok     bool
	}{
		{"/a", "/", true}, {"/", "/", true}, {"/a", "/a", true}, {"/a/b", "/a", true},
		{"/ab", "/a", false}, {"/a", "/a/b", false},
	}
	for _, c := range within {
		if Within(c.p, c.dir) != c.ok {
			t.Errorf("Within(%q, %q) != %v", c.p, c.dir, c.ok)
		}
	}
	rebase := [][4]string{
		{"/a/b", "/a", "/x", "/x/b"}, {"/a", "/a", "/x/y", "/x/y"}, {"/q", "/a", "/x", "/q"},
		{"/a/b", "/", "/x", "/x/a/b"}, {"/a/b", "/a", "/", "/b"}, {"/a", "/a", "/", "/"},
	}
	for _, c := range rebase {
		if got := Rebase(c[0], c[1], c[2]); got != c[3] {
			t.Errorf("Rebase(%q, %q, %q) = %q, want %q", c[0], c[1], c[2], got, c[3])
		}
	}
	if Parent("/a/b") != "/a" || Parent("/a") != "/" || Parent("/") != "/" {
		t.Error("Parent")
	}
	if Name("/a/b") != "b" || Name("/") != "" || Join("/", "a") != "/a" || Join("/a", "b") != "/a/b" {
		t.Error("Name or Join")
	}
}

// specIndex is the spec's example, with real hashes in place of ellipses.
var specIndex = `fsindex 1 seq 48211
/ d 0755 0 1763491200 48000 - -
/.config d 0755 0 1763491100 47950 - p
/.config/git/config f 0644 212 1763400000 31002 ` + h1 + ` p
/Documents/Tax%202025.pdf f 0644 284113 1763480011 46990 b3:9f2c -
/Photos d 0755 0 1763491000 48100 - i
/bin l 0777 7 1763000000 120 - - usr/bin
`

func TestIndexSpecExample(t *testing.T) {
	r, err := NewIndexReader(strings.NewReader(specIndex), nil)
	if err != nil {
		t.Fatal(err)
	}
	if h := r.Header(); h.Seq != 48211 || h.Version != 1 || h.Encoding != "" {
		t.Fatalf("header %+v", h)
	}
	var got []Entry
	for {
		e, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, e)
	}
	if len(got) != 6 {
		t.Fatalf("got %d entries", len(got))
	}
	if e := got[3]; e.Path != "/Documents/Tax 2025.pdf" || e.Length != 284113 || e.Version != 46990 || e.Mode != 0o644 {
		t.Errorf("entry %+v", e)
	}
	if e := got[5]; e.Type != TypeSymlink || e.Target != "usr/bin" {
		t.Errorf("symlink %+v", e)
	}
	if !got[1].Flags.Has(FlagPrefetch) || !got[4].Flags.Has(FlagIndex) {
		t.Error("flags")
	}

	var buf bytes.Buffer
	w, err := NewIndexWriter(&buf, r.Header(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range got {
		if err := w.Write(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if buf.String() != specIndex {
		t.Errorf("re-encoded index differs:\n%s", buf.String())
	}
}

func TestIndexOrderAndEncoding(t *testing.T) {
	_, err := readAll("fsindex 1 seq 1\n/b f 0644 0 0 0 - -\n/a f 0644 0 0 0 - -\n", nil)
	if !errors.Is(err, ErrUnsorted) {
		t.Errorf("unsorted: %v", err)
	}
	_, err = readAll("fsindex 1 seq 1\n/a f 0644 0 0 0 - -\n/a f 0644 0 0 0 - -\n", nil)
	if !errors.Is(err, ErrUnsorted) {
		t.Errorf("duplicate: %v", err)
	}
	if _, err := readAll("fsindex 1 seq 1 zstd\n", nil); err == nil {
		t.Error("encoded body without decoder")
	}
	if _, err := readAll("fsindex 2 seq 1\n", nil); err == nil {
		t.Error("version 2 accepted")
	}
	// A pluggable encoding round-trips through Encoder and Decoder.
	up := func(enc string, w io.Writer) (io.WriteCloser, error) { return upperWriter{w}, nil }
	down := func(enc string, r io.Reader) (io.Reader, error) { return lowerReader{r}, nil }
	var buf bytes.Buffer
	w, _ := NewIndexWriter(&buf, IndexHeader{Seq: 9, Encoding: "upper"}, up)
	w.Write(Entry{Path: "/a", Type: TypeFile, Mode: 0o644})
	w.Close()
	if !strings.HasPrefix(buf.String(), "fsindex 1 seq 9 upper\n/A F") {
		t.Fatalf("encoded: %q", buf.String())
	}
	es, err := readAll(buf.String(), down)
	if err != nil || len(es) != 1 || es[0].Path != "/a" {
		t.Fatalf("decoded: %v %v", es, err)
	}
	// No trailing newline on the last line is accepted.
	if es, err := readAll("fsindex 1 seq 1\n/a f 0644 0 0 0 - -", nil); err != nil || len(es) != 1 {
		t.Errorf("no final newline: %v %v", es, err)
	}
}

type upperWriter struct{ w io.Writer }

func (u upperWriter) Write(p []byte) (int, error) { return u.w.Write(bytes.ToUpper(p)) }
func (upperWriter) Close() error                  { return nil }

type lowerReader struct{ r io.Reader }

func (l lowerReader) Read(p []byte) (int, error) {
	n, err := l.r.Read(p)
	copy(p, bytes.ToLower(p[:n]))
	return n, err
}

func readAll(s string, dec Decoder) ([]Entry, error) {
	r, err := NewIndexReader(strings.NewReader(s), dec)
	if err != nil {
		return nil, err
	}
	var es []Entry
	for {
		e, err := r.Next()
		if err == io.EOF {
			return es, nil
		}
		if err != nil {
			return es, err
		}
		es = append(es, e)
	}
}

func TestEntryRoundTrip(t *testing.T) {
	for _, e := range []Entry{
		{Path: "/", Type: TypeDir, Mode: 0o755},
		{Path: "/x y/z%", Type: TypeFile, Mode: 0o4755, Length: 5, Mtime: -1, Version: 1 << 63, Hash: h1, Flags: "ip"},
		{Path: "/l", Type: TypeSymlink, Mode: 0o777, Length: 1, Target: "-"},
		{Path: "/l", Type: TypeSymlink, Mode: 0o777, Target: ""},
		{Path: "/l", Type: TypeSymlink, Mode: 0o777, Length: 3, Target: "a b", Attrs: Attrs{{"k", "v w"}, {"e", ""}}},
		{Path: "/f", Type: TypeFile, Attrs: Attrs{{"x", "a=b"}}},
	} {
		line := e.Encode()
		got, err := ParseEntry(line)
		if err != nil {
			t.Errorf("ParseEntry(%q): %v", line, err)
			continue
		}
		if !reflect.DeepEqual(got, e) {
			t.Errorf("round trip %q:\n got %+v\nwant %+v", line, got, e)
		}
	}
	for _, bad := range []string{
		"/a f 0644 0 0 0 -", "a f 0644 0 0 0 - -", "/a x 0644 0 0 0 - -", "/a f 0999 0 0 0 - -",
		"/a f 0644 -1 0 0 - -", "/a f 0644 0 0 -1 - -", "/a f 0644 0 0 0 nohash -", "/a f 0644 0 0 0 - P",
		"/a l 0777 0 0 0 - -", "/a  f 0644 0 0 0 - -", "/a f 0644 0 0 0 - - =v", "/a f 0644 0 0 0 - - k=%zz",
	} {
		if _, err := ParseEntry(bad); err == nil {
			t.Errorf("ParseEntry(%q) succeeded", bad)
		}
	}
	// Unknown trailing fields without '=' are ignored.
	e, err := ParseEntry("/a f 0644 0 0 0 - - future k=v")
	if err != nil || len(e.Attrs) != 1 {
		t.Errorf("trailing field: %+v %v", e, err)
	}
}

func TestSpecEvents(t *testing.T) {
	lines := []string{
		"48212 put /Notes/todo.md f 0644 830 1763491250 48212 " + h1 + " p dev=mac-a1",
		"48213 del /Downloads/old.zip dev=linux-b2",
		"48214 mv /Draft.md /Posts/Final.md dev=mac-a1",
		"48215 flags /Projects p dev=mac-a1",
		"48216 reset",
		"seq 48216",
	}
	want := []Event{
		{Seq: 48212, Op: OpPut, Dev: "mac-a1", Entry: Entry{Path: "/Notes/todo.md", Type: TypeFile, Mode: 0o644, Length: 830, Mtime: 1763491250, Version: 48212, Hash: h1, Flags: "p"}},
		{Seq: 48213, Op: OpDel, Path: "/Downloads/old.zip", Dev: "linux-b2"},
		{Seq: 48214, Op: OpMv, Path: "/Draft.md", To: "/Posts/Final.md", Dev: "mac-a1"},
		{Seq: 48215, Op: OpFlags, Path: "/Projects", Flags: "p", Dev: "mac-a1"},
		{Seq: 48216, Op: OpReset},
		{Seq: 48216, Op: OpSeq},
	}
	for i, line := range lines {
		ev, err := ParseEvent(line + "\n")
		if err != nil {
			t.Fatalf("ParseEvent(%q): %v", line, err)
		}
		if !reflect.DeepEqual(ev, want[i]) {
			t.Errorf("ParseEvent(%q):\n got %+v\nwant %+v", line, ev, want[i])
		}
		if got := ev.Encode(); got != line {
			t.Errorf("Encode = %q, want %q", got, line)
		}
	}
	for _, bad := range []string{"put /a", "1 put /a f", "1 del", "1 mv /a", "1 flags /a", "1 bogus /a", "seq", "x reset"} {
		if _, err := ParseEvent(bad); err == nil {
			t.Errorf("ParseEvent(%q) succeeded", bad)
		}
	}
}

func TestEventsSetup(t *testing.T) {
	var s EventsSetup
	if err := s.Parse("since 42\npath /a%20b\n"); err != nil {
		t.Fatal(err)
	}
	if err := s.Parse("path /c"); err != nil {
		t.Fatal(err)
	}
	if err := s.Parse("coalesce"); err != nil {
		t.Fatal(err)
	}
	want := EventsSetup{Since: 42, HasSince: true, Paths: []string{"/a b", "/c"}, Coalesce: true}
	if !reflect.DeepEqual(s, want) {
		t.Fatalf("got %+v", s)
	}
	var s2 EventsSetup
	if err := s2.Parse(s.Encode()); err != nil || !reflect.DeepEqual(s2, s) {
		t.Fatalf("round trip %+v %v", s2, err)
	}
	for _, bad := range []string{"since", "since -1", "path a", "bogus", ""} {
		var s EventsSetup
		if err := s.Parse(bad); err == nil {
			t.Errorf("Parse(%q) succeeded", bad)
		}
	}
	if !s.Match(Event{Op: OpDel, Path: "/a b/x"}) || s.Match(Event{Op: OpDel, Path: "/d"}) ||
		!s.Match(Event{Op: OpMv, Path: "/d", To: "/c/d"}) || !s.Match(Event{Op: OpReset}) ||
		!s.Match(Event{Op: OpPut, Entry: Entry{Path: "/c"}}) {
		t.Error("Match")
	}
	if enc, err := ParseAccept("accept zstd gzip"); err != nil || !reflect.DeepEqual(enc, []string{"zstd", "gzip"}) {
		t.Errorf("ParseAccept: %v %v", enc, err)
	}
	if EncodeAccept("zstd") != "accept zstd" {
		t.Error("EncodeAccept")
	}
	if _, err := ParseAccept("accept"); err == nil {
		t.Error("empty accept")
	}
}

func TestCommands(t *testing.T) {
	cases := []struct {
		line string
		cmd  Command
	}{
		{"caps", CapsCmd{}},
		{"hello mac-a1", HelloCmd{"mac-a1"}},
		{"hello %2D", HelloCmd{"-"}},
		{"mkdir /a%20b", MkdirCmd{"/a b", Any}},
		{"mkdir /a 0", MkdirCmd{"/a", At(0)}},
		{"rm /a 7", RmCmd{"/a", At(7)}},
		{"rmtree /a", RmtreeCmd{"/a", Any}},
		{"mv /a /b", MvCmd{From: "/a", To: "/b"}},
		{"mv /a /b 3", MvCmd{From: "/a", To: "/b", Base: At(3)}},
		{"mv /a /b - 0", MvCmd{From: "/a", To: "/b", ToBase: At(0)}},
		{"mv /a /b 3 4", MvCmd{"/a", "/b", At(3), At(4)}},
		{"cp /a /b 0", CpCmd{"/a", "/b", At(0)}},
		{"cp /a /b", CpCmd{"/a", "/b", Any}},
		{"flags /a ip", FlagsCmd{"/a", "ip"}},
		{"flags /a -", FlagsCmd{"/a", ""}},
		{"stat /a", StatCmd{"/a"}},
		{"commit 1b4e /x/y.txt 0 1763491250 0644", CommitCmd{Stage: "1b4e", Path: "/x/y.txt", Base: At(0), Mtime: 1763491250, Mode: 0o644}},
		{"commit u%20v /y 12 5 0600 hash=" + h1, CommitCmd{"u v", "/y", At(12), 5, 0o600, h1}},
		{"commit-hash " + h1 + " /y 0 5 0644", CommitHashCmd{h1, "/y", At(0), 5, 0o644}},
		{"abort 1b4e", AbortCmd{"1b4e"}},
	}
	for _, c := range cases {
		cmd, err := ParseCommand(c.line)
		if err != nil {
			t.Errorf("ParseCommand(%q): %v", c.line, err)
			continue
		}
		if !reflect.DeepEqual(cmd, c.cmd) {
			t.Errorf("ParseCommand(%q) = %#v, want %#v", c.line, cmd, c.cmd)
		}
		if got := cmd.Encode(); got != c.line {
			t.Errorf("Encode = %q, want %q", got, c.line)
		}
		if cmd.Name() != strings.Fields(c.line)[0] {
			t.Errorf("Name = %q", cmd.Name())
		}
	}
	// Unknown trailing fields are ignored.
	if cmd, err := ParseCommand("rm /a 3 extra"); err != nil || cmd != (RmCmd{"/a", At(3)}) {
		t.Errorf("trailing: %v %v", cmd, err)
	}
	if cmd, err := ParseCommand("mkdir /a k=v"); err != nil || cmd != (MkdirCmd{"/a", Any}) {
		t.Errorf("trailing kv: %v %v", cmd, err)
	}
	for _, bad := range []string{
		"", "hello", "mkdir", "mkdir a", "mkdir /a x", "rm /a/../b", "mv /a", "cp /a", "flags /a",
		"flags /a X", "stat", "commit s /a - 1 0644", "commit s /a 0 1", "commit a/b /a 0 1 0644",
		"commit s /a 0 1 0644 hash=zz", "commit-hash nohash /a 0 1 0644", "commit-hash " + h1 + " /a - 1 0644",
		"abort", "abort ..", "rm  /a",
	} {
		if _, err := ParseCommand(bad); !errors.Is(err, ErrSyntax) {
			t.Errorf("ParseCommand(%q) err = %v", bad, err)
		}
	}
	if _, err := ParseCommand("frobnicate /a"); !errors.Is(err, ErrUnknownCommand) {
		t.Errorf("unknown command err = %v", err)
	}
}

func TestReplies(t *testing.T) {
	e := Entry{Path: "/a b", Type: TypeFile, Mode: 0o644, Length: 3, Version: 9, Hash: h1}
	cases := []struct {
		cmd, line string
		r         Reply
	}{
		{CmdHello, "ok", Reply{Status: StatusOK}},
		{CmdMkdir, "ok 12", OK(12)},
		{CmdRm, "ok 13", OK(13)},
		{CmdCommit, "ok 14 " + h1, Reply{Status: StatusOK, Version: 14, Hash: h1}},
		{CmdCaps, "ok caps hello mkdir", Reply{Status: StatusOK, Caps: []string{"caps", "hello", "mkdir"}}},
		{CmdStat, "ok /a%20b f 0644 3 0 9 " + h1 + " -", Reply{Status: StatusOK, Entry: &e}},
		{CmdMv, "conflict 40", Reply{Status: StatusConflict, Version: 40}},
		{CmdCommit, "conflict 0", Reply{Status: StatusConflict}},
		{CmdRm, "missing", Reply{Status: StatusMissing}},
		{CmdCommitHash, "missing msg=no%20content", Reply{Status: StatusMissing, Message: "no content"}},
		{CmdCommit, "invalid msg=hash%20mismatch", Reply{Status: StatusInvalid, Message: "hash mismatch"}},
		{CmdMkdir, "denied x=1", Reply{Status: StatusDenied, Attrs: Attrs{{"x", "1"}}}},
	}
	for _, c := range cases {
		r, err := ParseReply(c.cmd, c.line)
		if err != nil {
			t.Errorf("ParseReply(%s, %q): %v", c.cmd, c.line, err)
			continue
		}
		if !reflect.DeepEqual(r, c.r) {
			t.Errorf("ParseReply(%s, %q):\n got %+v\nwant %+v", c.cmd, c.line, r, c.r)
		}
		if got := r.Encode(c.cmd); got != c.line {
			t.Errorf("Encode = %q, want %q", got, c.line)
		}
	}
	for _, bad := range [][2]string{{CmdMkdir, "ok"}, {CmdMkdir, "conflict"}, {CmdMkdir, "maybe"}, {CmdCommit, "ok 1"}, {CmdStat, "ok /a"}} {
		if _, err := ParseReply(bad[0], bad[1]); err == nil {
			t.Errorf("ParseReply(%q, %q) succeeded", bad[0], bad[1])
		}
	}
}

func TestReplyErrors(t *testing.T) {
	err := Reply{Status: StatusConflict, Version: 7}.Err()
	if !errors.Is(err, ErrConflict) || errors.Is(err, ErrMissing) {
		t.Errorf("Is: %v", err)
	}
	var re *ReplyError
	if !errors.As(fmt.Errorf("wrapped: %w", err), &re) || re.Version != 7 {
		t.Errorf("As: %v", re)
	}
	if OK(1).Err() != nil {
		t.Error("ok reply has an error")
	}
	r, err := ErrorReply(fmt.Errorf("x: %w", Conflict(5)))
	if err != nil || r.Encode(CmdMv) != "conflict 5" {
		t.Errorf("ErrorReply conflict: %v %v", r, err)
	}
	_, perr := ParseCommand("bogus")
	if r, err := ErrorReply(perr); err != nil || r.Status != StatusInvalid {
		t.Errorf("ErrorReply syntax: %v %v", r, err)
	}
	io := errors.New("disk on fire")
	if _, err := ErrorReply(io); err != io {
		t.Errorf("ErrorReply other: %v", err)
	}
}

func TestHash(t *testing.T) {
	// BLAKE3-256 of the empty input.
	const empty = "b3:af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
	if got := HashBytes(nil); got != empty {
		t.Errorf("HashBytes(nil) = %s", got)
	}
	got, n, err := HashReader(strings.NewReader("hello"))
	if err != nil || n != 5 || got != HashBytes([]byte("hello")) {
		t.Errorf("HashReader: %s %d %v", got, n, err)
	}
	for _, bad := range []string{"", "b3", "b3:", ":ab", "B3:ab", "b3:abc", "b3:AB", "b-3:ab"} {
		if CheckHash(bad) == nil {
			t.Errorf("CheckHash(%q) = nil", bad)
		}
	}
}

func TestFlags(t *testing.T) {
	f := Flags("").With('p').With('i').With('p')
	if f != "ip" || !f.Has('i') || f.Without('i') != "p" || f.Without('p').Without('i').Encode() != "-" {
		t.Errorf("flags %q", f)
	}
}

func TestBase(t *testing.T) {
	if !Any.Matches(5) || !At(5).Matches(5) || At(0).Matches(5) || Any.Encode() != "-" || At(0).Encode() != "0" {
		t.Error("Base")
	}
}

func TestHeader(t *testing.T) {
	for _, line := range []string{"fsindex 1 seq 0", "fsindex 1 seq 18446744073709551615 zstd", "fsindex 1 seq 3 zstd k=v", "fsindex 1 seq 3 k=v"} {
		h, err := ParseIndexHeader(line)
		if err != nil {
			t.Errorf("%q: %v", line, err)
			continue
		}
		if h.Encode() != line {
			t.Errorf("Encode = %q, want %q", h.Encode(), line)
		}
	}
	for _, bad := range []string{"fsindex 1 seq", "fsindex x seq 1", "index 1 seq 1", "fsindex 1 seq -1", "fsindex 0 seq 1"} {
		if _, err := ParseIndexHeader(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
