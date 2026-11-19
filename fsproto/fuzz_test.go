package fsproto

import (
	"reflect"
	"testing"
)

func FuzzEscape(f *testing.F) {
	for _, s := range []string{"", "/a b", "%", "caf\u00e9", "\x00\xff", "-"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		esc := EscapePath(s)
		for i := 0; i < len(esc); i++ {
			if c := esc[i]; c <= ' ' || c >= 0x7f {
				t.Fatalf("EscapePath(%q) = %q has raw byte %#x", s, esc, c)
			}
		}
		got, err := UnescapePath(esc)
		if err != nil || got != s {
			t.Fatalf("UnescapePath(EscapePath(%q)) = %q, %v", s, got, err)
		}
		if f, err := decField(encField(s)); err != nil || f != s {
			t.Fatalf("field round trip of %q = %q, %v", s, f, err)
		}
	})
}

// roundTrip checks that a parsed value encodes to a line that parses back to
// the same value.
func roundTrip[T any](t *testing.T, line string, parse func(string) (T, error), enc func(T) string) {
	v, err := parse(line)
	if err != nil {
		return
	}
	line2 := enc(v)
	v2, err := parse(line2)
	if err != nil {
		t.Fatalf("%q parsed, encoded to %q, which fails: %v", line, line2, err)
	}
	if !reflect.DeepEqual(v, v2) {
		t.Fatalf("%q -> %+v -> %q -> %+v", line, v, line2, v2)
	}
	if line3 := enc(v2); line3 != line2 {
		t.Fatalf("encoding not stable: %q then %q", line2, line3)
	}
}

func FuzzEntry(f *testing.F) {
	for _, s := range []string{
		"/ d 0755 0 1763491200 48000 - -",
		"/Documents/Tax%202025.pdf f 0644 284113 1763480011 46990 b3:9f2c -",
		"/bin l 0777 7 1763000000 120 - - usr/bin k=v",
		"/x f 0644 0 0 0 - pi future a=%20",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, line string) {
		roundTrip(t, line, ParseEntry, Entry.Encode)
	})
}

func FuzzEvent(f *testing.F) {
	for _, s := range []string{
		"48212 put /Notes/todo.md f 0644 830 1763491250 48212 b3:77d0 p dev=mac-a1",
		"48213 del /Downloads/old.zip dev=linux-b2",
		"48214 mv /Draft.md /Posts/Final.md dev=mac-a1 x=y",
		"48215 flags /Projects p",
		"48216 reset",
		"seq 3",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, line string) {
		roundTrip(t, line, ParseEvent, Event.Encode)
	})
}

func FuzzCommand(f *testing.F) {
	for _, s := range []string{
		"caps", "hello mac", "mkdir /a 0", "rm /a", "rmtree /a 3", "mv /a /b - 0", "cp /a /b 4",
		"flags /a p", "stat /a%20b", "commit s /a 0 1 0644 hash=b3:00", "commit-hash b3:00 /a 0 1 0644", "abort s",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, line string) {
		roundTrip(t, line, ParseCommand, Command.Encode)
	})
}

func FuzzReply(f *testing.F) {
	cmds := append(append([]string{}, PathCommands...), StageCommands...)
	for i, s := range []string{"ok", "ok 3", "ok 3 b3:00", "ok caps rm", "ok /a f 0644 0 0 0 - -", "conflict 4", "missing msg=x", "invalid", "denied k=v"} {
		f.Add(uint8(i), s)
	}
	f.Fuzz(func(t *testing.T, ci uint8, line string) {
		cmd := cmds[int(ci)%len(cmds)]
		roundTrip(t, line,
			func(s string) (Reply, error) { return ParseReply(cmd, s) },
			func(r Reply) string { return r.Encode(cmd) })
	})
}

func FuzzEventsSetup(f *testing.F) {
	f.Add("since 4\npath /a\ncoalesce")
	f.Fuzz(func(t *testing.T, data string) {
		roundTrip(t, data,
			func(s string) (EventsSetup, error) { var e EventsSetup; err := e.Parse(s); return e, err },
			EventsSetup.Encode)
	})
}

func FuzzIndexHeader(f *testing.F) {
	f.Add("fsindex 1 seq 48211 zstd")
	f.Fuzz(func(t *testing.T, line string) {
		roundTrip(t, line, ParseIndexHeader, IndexHeader.Encode)
	})
}
