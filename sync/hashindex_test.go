package sync

import (
	"slices"
	"sort"
	"testing"

	"github.com/progrium/dotfs/fsproto"
)

func TestHashIndex(t *testing.T) {
	x := newHashIndex()
	a, b := fsproto.HashBytes([]byte("a")), fsproto.HashBytes([]byte("b"))
	other := "sha1:00ff"
	get := func(h string) []string {
		p := x.paths(h)
		sort.Strings(p)
		return p
	}
	x.add(a, "/1")
	x.add(a, "/1")
	x.add(a, "/2")
	x.add(a, "/3")
	x.add(b, "/4")
	x.add(other, "/5")
	if got := get(a); !slices.Equal(got, []string{"/1", "/2", "/3"}) {
		t.Fatalf("a: %v", got)
	}
	if got := get(other); !slices.Equal(got, []string{"/5"}) {
		t.Fatalf("other: %v", got)
	}
	if x.len() != 5 {
		t.Fatalf("len %d", x.len())
	}
	x.remove(a, "/1") // the one in x.one: another takes its place
	if got := get(a); !slices.Equal(got, []string{"/2", "/3"}) {
		t.Fatalf("a after removing /1: %v", got)
	}
	x.remove(a, "/3")
	x.remove(a, "/2")
	x.remove(a, "/9") // absent
	if got := get(a); got != nil {
		t.Fatalf("a after removing all: %v", got)
	}
	if len(x.more) != 0 || len(x.one) != 2 {
		t.Fatalf("leftovers: one %d, more %d", len(x.one), len(x.more))
	}
}

func TestStateWithHash(t *testing.T) {
	s, err := openState(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	h := fsproto.HashBytes([]byte("x"))
	file := func(p, h string) *row {
		return &row{Entry: fsproto.Entry{Path: p, Type: fsproto.TypeFile, Hash: h}}
	}
	s.put(&row{Entry: fsproto.Entry{Path: "/", Type: fsproto.TypeDir}})
	s.put(file("/b", h))
	s.put(file("/a", h))
	s.put(file("/c", fsproto.HashBytes([]byte("y"))))
	if got := s.withHash(h); !slices.Equal(got, []string{"/a", "/b"}) {
		t.Fatalf("withHash: %v", got)
	}
	s.put(file("/a", fsproto.HashBytes([]byte("z")))) // content changed
	s.del("/b")
	if got := s.withHash(h); got != nil {
		t.Fatalf("withHash after change and delete: %v", got)
	}
	s.rebase("/c", "/d", 2)
	if got := s.withHash(fsproto.HashBytes([]byte("y"))); !slices.Equal(got, []string{"/d"}) {
		t.Fatalf("withHash after rebase: %v", got)
	}
	if s.hashes.len() != 2 {
		t.Fatalf("index holds %d paths, want 2", s.hashes.len())
	}
}
