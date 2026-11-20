//go:build linux

package fuse

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/progrium/dotfs/client"
	"github.com/progrium/dotfs/client/fake"
	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/mount"
	sfake "github.com/progrium/dotfs/server/fake"
)

type env struct {
	t      *testing.T
	dir    string
	remote *fake.Conn
	fs     *mount.FS
}

// setup mounts a fake root seeded by seed, or skips the test where FUSE
// mounts are not possible.
func setup(t *testing.T, seed map[string]string) *env {
	t.Helper()
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("no /dev/fuse")
	}
	srv := sfake.NewServer()
	if _, err := srv.CreateRoot("alice", "home"); err != nil {
		t.Fatal(err)
	}
	remote, err := fake.Dial(srv, "alice", "home")
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, dir: t.TempDir(), remote: remote}
	var paths []string
	for p := range seed {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		if strings.HasSuffix(p, "/") {
			if _, err := client.Call(context.Background(), remote, fsproto.MkdirCmd{Path: strings.TrimSuffix(p, "/")}); err != nil {
				t.Fatal(err)
			}
			continue
		}
		e.put(p, seed[p])
	}
	f, err := mount.New(context.Background(), mount.Config{
		Root:     "fake/home",
		CacheDir: t.TempDir(),
		Device:   "laptop",
		Dial:     func(ctx context.Context) (client.Conn, error) { return fake.Dial(srv, "alice", "home") },
		Logf:     t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	e.fs = f
	srvFuse, err := Mount(e.dir, f, Options{})
	if err != nil {
		f.Close()
		t.Skipf("cannot mount FUSE here: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.Run(ctx); close(done) }()
	t.Cleanup(func() {
		if err := srvFuse.Unmount(); err != nil {
			t.Errorf("unmount: %v", err)
		}
		cancel()
		<-done
		f.Close()
	})
	return e
}

func (e *env) put(p, content string) {
	e.t.Helper()
	base := fsproto.At(0)
	if st, err := client.Call(context.Background(), e.remote, fsproto.StatCmd{Path: p}); err == nil {
		base = fsproto.At(st.Entry.Version)
	}
	if _, err := client.Put(context.Background(), e.remote, p, base, strings.NewReader(content), int64(len(content)), client.PutOptions{}); err != nil {
		e.t.Fatalf("put %s: %v", p, err)
	}
}

func (e *env) path(p string) string { return filepath.Join(e.dir, p) }

func (e *env) remoteRead(p string) string {
	b, err := e.remote.Read(context.Background(), p)
	if err != nil {
		return "<" + err.Error() + ">"
	}
	return string(b)
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func readFile(p string) string {
	b, err := os.ReadFile(p)
	if err != nil {
		return "<" + err.Error() + ">"
	}
	return string(b)
}

func TestReadAndList(t *testing.T) {
	e := setup(t, map[string]string{"/a.txt": "hello", "/docs/": "", "/docs/b c.md": "spaces"})
	ents, err := os.ReadDir(e.dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, d := range ents {
		names = append(names, d.Name())
	}
	if got := strings.Join(names, " "); got != "a.txt docs" {
		t.Fatalf("ls = %q", got)
	}
	fi, err := os.Stat(e.path("docs/b c.md"))
	if err != nil || fi.Size() != 6 {
		t.Fatalf("stat = %v, %v", fi, err)
	}
	if got := readFile(e.path("docs/b c.md")); got != "spaces" {
		t.Fatalf("read = %q", got)
	}
	if _, err := os.Stat(e.path(".fs")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf(".fs is visible: %v", err)
	}
}

func TestWrites(t *testing.T) {
	e := setup(t, map[string]string{"/a.txt": "hello"})
	if err := os.WriteFile(e.path("new.txt"), []byte("fresh"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := e.remoteRead("/new.txt"); got != "fresh" {
		t.Fatalf("server new.txt = %q", got)
	}
	f, err := os.OpenFile(e.path("a.txt"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(" world")
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if got := e.remoteRead("/a.txt"); got != "hello world" {
		t.Fatalf("server a.txt = %q", got)
	}
	// An editor-style save: write a temporary file, then rename over.
	os.WriteFile(e.path(".a.txt.tmp"), []byte("saved"), 0o600)
	if err := os.Rename(e.path(".a.txt.tmp"), e.path("a.txt")); err != nil {
		t.Fatal(err)
	}
	if got := e.remoteRead("/a.txt"); got != "saved" {
		t.Fatalf("server a.txt after rename = %q", got)
	}
	if got := readFile(e.path("a.txt")); got != "saved" {
		t.Fatalf("local a.txt after rename = %q", got)
	}
	if err := os.Mkdir(e.path("d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(e.path("new.txt"), e.path("d/new.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(e.path("d")); !errors.Is(err, syscall.ENOTEMPTY) && !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("rmdir non-empty: %v", err)
	}
	if err := os.Remove(e.path("d/new.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(e.path("d")); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(e.path("a.txt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Stat(e.path("a.txt")); fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v", fi.Mode())
	}
	if err := os.Truncate(e.path("a.txt"), 2); err != nil {
		t.Fatal(err)
	}
	if got := e.remoteRead("/a.txt"); got != "sa" {
		t.Fatalf("server a.txt after truncate = %q", got)
	}
	if err := os.Symlink("a.txt", e.path("link")); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("symlink: %v", err)
	}
}

func TestRemoteChangesInvalidate(t *testing.T) {
	e := setup(t, map[string]string{"/a.txt": "one", "/d/": ""})
	if got := readFile(e.path("a.txt")); got != "one" {
		t.Fatal(got)
	}
	os.ReadDir(e.path("d"))
	if _, err := os.Stat(e.path("d/later")); err == nil {
		t.Fatal("d/later exists early")
	}
	e.put("/a.txt", "two, longer")
	eventually(t, "new content", func() bool { return readFile(e.path("a.txt")) == "two, longer" })
	e.put("/d/later", "x")
	eventually(t, "new file", func() bool { _, err := os.Stat(e.path("d/later")); return err == nil })
	eventually(t, "listing", func() bool { ents, _ := os.ReadDir(e.path("d")); return len(ents) == 1 })
	if _, err := client.Call(context.Background(), e.remote, fsproto.MvCmd{From: "/d", To: "/e"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "move", func() bool { return readFile(e.path("e/later")) == "x" })
	if _, err := os.Stat(e.path("d")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old name after move: %v", err)
	}
	if _, err := client.Call(context.Background(), e.remote, fsproto.RmCmd{Path: "/a.txt"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "delete", func() bool { _, err := os.Stat(e.path("a.txt")); return errors.Is(err, os.ErrNotExist) })
}

func TestReadWhileWriting(t *testing.T) {
	e := setup(t, map[string]string{"/log": "line1\n"})
	r, err := os.Open(e.path("log"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	w, err := os.OpenFile(e.path("log"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open for append while open for reading: %v", err)
	}
	w.WriteString("line2\n")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := e.remoteRead("/log"); got != "line1\nline2\n" {
		t.Fatalf("server log = %q", got)
	}
}
