package store_test

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/servertest"
	"github.com/progrium/dotfs/server/store"
)

func newStore(t *testing.T, opts *store.Options) *store.Store {
	t.Helper()
	st, err := store.New(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestStore(t *testing.T) {
	servertest.TestStore(t, func(t *testing.T) server.Store { return newStore(t, nil) })
}

func TestStoreNoDedupe(t *testing.T) {
	// Without dedupe, commit-hash always misses, so only the stage and
	// namespace behavior applies.
	st := newStore(t, &store.Options{NoDedupe: true, NoSync: true})
	r := server.NewRoot("alice", "home")
	st.CreateRoot(r)
	stage(t, st, "s", "data")
	h, _, err := st.Publish("alice", "s", r, "/a", 0o644, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.PublishHash("alice", h, r, "/b", 0o644, 1); !errors.Is(err, fsproto.ErrMissing) {
		t.Fatalf("PublishHash without dedupe: %v", err)
	}
	if err := st.Copy(r, "/a", "/c"); err != nil {
		t.Fatal(err)
	}
	if got := read(t, st, r, "/c"); got != "data" {
		t.Fatalf("copy %q", got)
	}
}

func stage(t *testing.T, st *store.Store, name, data string) {
	t.Helper()
	f, err := st.OpenStage("alice", name, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte(data), 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
}

func read(t *testing.T, st *store.Store, r server.Root, p string) string {
	t.Helper()
	f, err := st.Open(r, p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b, _ := io.ReadAll(f)
	return string(b)
}

func TestDedupeAndGC(t *testing.T) {
	st := newStore(t, &store.Options{NoSync: true})
	r := server.NewRoot("alice", "home")
	st.CreateRoot(r)
	stage(t, st, "s1", "same")
	stage(t, st, "s2", "same")
	h, _, err := st.Publish("alice", "s1", r, "/a", 0o644, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Publish("alice", "s2", r, "/b", 0o644, 1, ""); err != nil {
		t.Fatal(err)
	}
	a, _ := os.Stat(filepath.Join(st.RootDir(r), "a"))
	b, _ := os.Stat(filepath.Join(st.RootDir(r), "b"))
	if !os.SameFile(a, b) {
		t.Fatal("identical files are not hard-linked")
	}
	if n, _ := st.GC(); n != 0 {
		t.Fatalf("GC removed %d referenced blobs", n)
	}
	st.Remove(r, "/a", false)
	st.Remove(r, "/b", false)
	if n, _ := st.GC(); n != 1 {
		t.Fatalf("GC removed %d, want 1", n)
	}
	if _, err := st.PublishHash("alice", h, r, "/c", 0o644, 1); !errors.Is(err, fsproto.ErrMissing) {
		t.Fatalf("PublishHash of deleted content: %v", err)
	}
}

func TestStageHashing(t *testing.T) {
	st := newStore(t, &store.Options{NoSync: true})
	r := server.NewRoot("alice", "home")
	st.CreateRoot(r)
	data := []byte("0123456789abcdef")
	want := fsproto.HashBytes(data)
	// In order across handles, out of order, and resumed after the
	// handle was closed: every case must hash the final bytes.
	cases := map[string][][2]int{
		"in order":     {{0, 4}, {4, 10}, {10, 16}},
		"out of order": {{8, 16}, {0, 8}},
		"overwrite":    {{0, 16}, {2, 6}},
	}
	for name, writes := range cases {
		for _, w := range writes {
			f, err := st.OpenStage("alice", name, true)
			if err != nil {
				t.Fatal(err)
			}
			f.WriteAt(data[w[0]:w[1]], int64(w[0]))
			f.Close()
		}
		h, n, err := st.Publish("alice", name, r, "/"+name, 0o644, 1, want)
		if err != nil || h != want || n != 16 {
			t.Fatalf("%s: %s %d %v", name, h, n, err)
		}
	}
}

func TestSealedStage(t *testing.T) {
	st := newStore(t, &store.Options{NoSync: true})
	r := server.NewRoot("alice", "home")
	st.CreateRoot(r)
	f, _ := st.OpenStage("alice", "s", true)
	f.WriteAt([]byte("x"), 0)
	if _, _, err := st.Publish("alice", "s", r, "/a", 0o644, 1, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("y"), 1); !errors.Is(err, store.ErrSealed) {
		t.Fatalf("write after commit: %v", err)
	}
	f.Close()
	if got := read(t, st, r, "/a"); got != "x" {
		t.Fatalf("content %q", got)
	}
}

func TestNoEscape(t *testing.T) {
	st := newStore(t, &store.Options{NoSync: true})
	r := server.NewRoot("alice", "home")
	st.CreateRoot(r)
	outside := filepath.Join(t.TempDir(), "secret")
	os.WriteFile(outside, []byte("secret"), 0o644)
	if err := st.Symlink(r, "/l", filepath.Dir(outside), 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Open(r, "/l/secret"); err == nil {
		t.Fatal("opened a file outside the root through a symlink")
	}
	if _, err := st.Open(r, "/l"); err == nil {
		t.Fatal("opened a symlink as a file")
	}
	for _, bad := range []server.Root{"../x", "alice/..", "alice/a/b", "/x"} {
		if err := st.CreateRoot(bad); err == nil {
			t.Fatalf("CreateRoot(%q) succeeded", bad)
		}
	}
}

func TestRemoveTreeAndPlaceRetry(t *testing.T) {
	st := newStore(t, &store.Options{NoSync: true})
	r := server.NewRoot("alice", "home")
	st.CreateRoot(r)
	st.Mkdir(r, "/d", 0o755, 1)
	stage(t, st, "s", "x")
	p, err := st.PrepareStage("alice", "s", "", 0o644, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Place(p, r, "/d/f"); err != nil {
		t.Fatal(err)
	}
	// Placing again (as recovery does) is a no-op.
	if err := st.Place(p, r, "/d/f"); err != nil {
		t.Fatal(err)
	}
	if err := st.Remove(r, "/d", true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(st.RootDir(r), "d")); !os.IsNotExist(err) {
		t.Fatal("tree survived")
	}
	des, _ := os.ReadDir(filepath.Join(st.Dir(), "tmp"))
	if len(des) != 0 {
		t.Fatalf("tmp not empty: %v", des)
	}
}
