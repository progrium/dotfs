package sync

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	gosync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/progrium/dotfs/client"
	"github.com/progrium/dotfs/client/fake"
	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	sfake "github.com/progrium/dotfs/server/fake"
)

var testTime = time.Date(2026, 11, 19, 4, 15, 0, 0, time.UTC)

// world is one fake server shared by several engines.
type world struct {
	t     *testing.T
	srv   *sfake.Server
	scope server.Scope
}

func newWorld(t *testing.T) *world {
	srv := sfake.NewServer()
	sc, err := srv.CreateRoot("alice", "home")
	if err != nil {
		t.Fatal(err)
	}
	return &world{t: t, srv: srv, scope: sc}
}

// node is one engine with a switchable connection.
type node struct {
	t      *testing.T
	name   string
	dir    string
	e      *Engine
	online atomic.Bool
	mu     gosync.Mutex
	conns  []*fake.Conn
	cfg    Config
}

func (w *world) node(name string) *node {
	w.t.Helper()
	n := &node{t: w.t, name: name, dir: w.t.TempDir()}
	n.online.Store(true)
	n.cfg = Config{
		Dir:    n.dir,
		Root:   "fake/home",
		Device: name,
		Now:    func() time.Time { return testTime },
		Dial: func(ctx context.Context) (client.Conn, error) {
			if !n.online.Load() {
				return nil, fake.ErrOffline
			}
			c, err := fake.Dial(w.srv, "alice", "home")
			if err != nil {
				return nil, err
			}
			n.mu.Lock()
			n.conns = append(n.conns, c)
			n.mu.Unlock()
			return c, nil
		},
		Logf: func(f string, args ...any) { w.t.Logf(name+": "+f, args...) },
	}
	n.open()
	w.t.Cleanup(func() { n.e.Close() })
	return n
}

func (n *node) open() {
	n.t.Helper()
	e, err := New(n.cfg)
	if err != nil {
		n.t.Fatal(err)
	}
	n.e = e
}

// restart closes the engine and opens a new one on the same folder.
func (n *node) restart() {
	n.t.Helper()
	n.e.Close()
	n.open()
}

func (n *node) setOnline(on bool) {
	n.online.Store(on)
	if !on {
		n.mu.Lock()
		for _, c := range n.conns {
			c.SetOffline(true)
		}
		n.conns = nil
		n.mu.Unlock()
	}
}

func (n *node) sync() {
	n.t.Helper()
	if err := n.e.Sync(context.Background()); err != nil {
		n.t.Fatalf("%s: sync: %v", n.name, err)
	}
}

func (n *node) path(p string) string { return filepath.Join(n.dir, filepath.FromSlash(p)) }

func (n *node) write(p, content string) {
	n.t.Helper()
	full := n.path(p)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		n.t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		n.t.Fatal(err)
	}
	// Make sure the change is visible to a size and mtime check even when
	// the size does not change within the filesystem's timestamp grain.
	bump := time.Now().Add(time.Duration(len(content)+1) * time.Millisecond)
	os.Chtimes(full, bump, bump)
}

func (n *node) remove(p string) {
	n.t.Helper()
	if err := os.RemoveAll(n.path(p)); err != nil {
		n.t.Fatal(err)
	}
}

func (n *node) rename(from, to string) {
	n.t.Helper()
	os.MkdirAll(filepath.Dir(n.path(to)), 0o755)
	if err := os.Rename(n.path(from), n.path(to)); err != nil {
		n.t.Fatal(err)
	}
}

// tree returns the folder as path -> content ("/" suffix for directories),
// without the state folder.
func (n *node) tree() map[string]string {
	n.t.Helper()
	out := map[string]string{}
	filepath.WalkDir(n.dir, func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			n.t.Fatal(err)
		}
		rel, _ := filepath.Rel(n.dir, name)
		if rel == "." {
			return nil
		}
		p := "/" + filepath.ToSlash(rel)
		if p == "/"+StateDir {
			return filepath.SkipDir
		}
		if d.IsDir() {
			out[p+"/"] = ""
			return nil
		}
		b, err := os.ReadFile(name)
		if err != nil {
			n.t.Fatal(err)
		}
		out[p] = string(b)
		return nil
	})
	return out
}

// serverTree returns the server's view in the same shape as node.tree.
func (w *world) serverTree() map[string]string {
	w.t.Helper()
	out := map[string]string{}
	ctx := context.Background()
	_, err := w.srv.Meta.Entries(ctx, w.scope, func(e fsproto.Entry) error {
		if e.Path == "/" {
			return nil
		}
		if e.Type == fsproto.TypeDir {
			out[e.Path+"/"] = ""
			return nil
		}
		f, err := w.srv.Store.Open(w.scope.Root, w.scope.RootPath(e.Path))
		if err != nil {
			return err
		}
		defer f.Close()
		b := make([]byte, e.Length)
		if _, err := f.ReadAt(b, 0); err != nil && e.Length > 0 {
			return err
		}
		out[e.Path] = string(b)
		return nil
	})
	if err != nil {
		w.t.Fatal(err)
	}
	return out
}

func show(m map[string]string) string {
	keys := slices.Sorted(maps.Keys(m))
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "  %s %q\n", k, m[k])
	}
	return b.String()
}

// converge syncs every node a few rounds and checks that all folders and
// the server agree, returning the common tree.
func (w *world) converge(nodes ...*node) map[string]string {
	w.t.Helper()
	for range 3 {
		for _, n := range nodes {
			n.sync()
		}
	}
	want := w.serverTree()
	for _, n := range nodes {
		if got := n.tree(); !maps.Equal(got, want) {
			w.t.Fatalf("%s differs from the server\n%s:\n%sserver:\n%s", n.name, n.name, show(got), show(want))
		}
		if q := n.e.st.pending(); len(q) != 0 {
			w.t.Fatalf("%s: queue not empty: %+v", n.name, q[0])
		}
	}
	return want
}

func expect(t *testing.T, got map[string]string, want map[string]string) {
	t.Helper()
	if !maps.Equal(got, want) {
		t.Fatalf("got:\n%swant:\n%s", show(got), show(want))
	}
}

func conflictCopies(tree map[string]string) []string {
	var out []string
	for p := range tree {
		if strings.Contains(p, ".sync-conflict-") {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

func TestBootstrapAndBasics(t *testing.T) {
	w := newWorld(t)
	a := w.node("a")
	a.write("/notes/todo.md", "buy milk")
	a.write("/notes/deep/x/y.txt", "y")
	a.write("/top.txt", "top")
	if err := os.MkdirAll(a.path("/empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	a.sync()

	b := w.node("b")
	expect(t, w.converge(a, b), map[string]string{
		"/empty/": "", "/notes/": "", "/notes/deep/": "", "/notes/deep/x/": "",
		"/notes/deep/x/y.txt": "y", "/notes/todo.md": "buy milk", "/top.txt": "top",
	})

	// Edit, delete, and rename on one side show up on the other.
	b.write("/notes/todo.md", "buy oat milk")
	b.remove("/top.txt")
	b.rename("/notes/deep", "/notes/shallow")
	b.rename("/empty", "/still-empty")
	b.sync()
	a.sync()
	expect(t, w.converge(a, b), map[string]string{
		"/still-empty/": "", "/notes/": "", "/notes/shallow/": "", "/notes/shallow/x/": "",
		"/notes/shallow/x/y.txt": "y", "/notes/todo.md": "buy oat milk",
	})

	// The directory rename went over as one mv, not deletes and creates.
	var mvs int
	w.srv.Meta.ReadLog(context.Background(), w.scope, 0, func(ev fsproto.Event) error {
		if ev.Op == fsproto.OpMv {
			mvs++
		}
		return nil
	})
	if mvs != 2 {
		t.Fatalf("got %d mv records, want 2", mvs)
	}

	// Removing a directory with content removes it everywhere.
	a.remove("/notes/shallow")
	expect(t, w.converge(a, b), map[string]string{
		"/still-empty/": "", "/notes/": "", "/notes/todo.md": "buy oat milk",
	})
}

func TestBootstrapIntoNonEmptyFolder(t *testing.T) {
	w := newWorld(t)
	a := w.node("a")
	a.write("/same.txt", "same")
	a.write("/differs.txt", "server")
	a.sync()

	b := w.node("b")
	b.e.Close()
	b.write("/same.txt", "same")
	b.write("/differs.txt", "local")
	b.write("/only-local.txt", "mine")
	b.open()
	got := w.converge(a, b)
	expect(t, got, map[string]string{
		"/same.txt":       "same",
		"/differs.txt":    "server",
		"/only-local.txt": "mine",
		"/differs.sync-conflict-20261119-041500-b.txt": "local",
	})
}

func TestConcurrentEdit(t *testing.T) {
	w := newWorld(t)
	a, b := w.node("a"), w.node("b")
	a.write("/notes.txt", "v1")
	w.converge(a, b)

	a.write("/notes.txt", "from a")
	b.write("/notes.txt", "from b")
	a.sync() // a commits first
	b.sync() // b commits second and gets the conflict
	got := w.converge(a, b)
	expect(t, got, map[string]string{
		"/notes.txt": "from a",
		"/notes.sync-conflict-20261119-041500-b.txt": "from b",
	})
}

func TestConcurrentEditOnline(t *testing.T) {
	// b learns about a's edit from a conflict reply rather than events.
	w := newWorld(t)
	a, b := w.node("a"), w.node("b")
	a.write("/f.md", "v1")
	w.converge(a, b)
	a.write("/f.md", "aaa")
	a.sync()
	b.write("/f.md", "bbb")
	// Commit directly, before b reads events, to force the conflict reply.
	b.e.mu.Lock()
	b.e.hints["/"] = true
	if err := b.e.scanHints(); err != nil {
		t.Fatal(err)
	}
	if err := b.e.drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	b.e.mu.Unlock()
	got := w.converge(a, b)
	if c := conflictCopies(got); len(c) != 1 || got[c[0]] != "bbb" || got["/f.md"] != "aaa" {
		t.Fatalf("want one conflict copy holding b's edit:\n%s", show(got))
	}
}

func TestIdenticalEditsDoNotConflict(t *testing.T) {
	w := newWorld(t)
	a, b := w.node("a"), w.node("b")
	a.write("/f", "v1")
	w.converge(a, b)
	a.write("/f", "same")
	b.write("/f", "same")
	a.sync()
	b.sync()
	expect(t, w.converge(a, b), map[string]string{"/f": "same"})
}

func TestEditVersusDelete(t *testing.T) {
	for _, deleteFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("deleteFirst=%v", deleteFirst), func(t *testing.T) {
			w := newWorld(t)
			a, b := w.node("a"), w.node("b")
			a.write("/doc.txt", "v1")
			a.write("/keep.txt", "k")
			w.converge(a, b)

			a.remove("/doc.txt")
			b.write("/doc.txt", "edited")
			if deleteFirst {
				a.sync()
				b.sync()
			} else {
				b.sync()
				a.sync()
			}
			expect(t, w.converge(a, b), map[string]string{"/doc.txt": "edited", "/keep.txt": "k"})
		})
	}
}

func TestEditVersusDeleteDirectory(t *testing.T) {
	w := newWorld(t)
	a, b := w.node("a"), w.node("b")
	a.write("/d/edited.txt", "v1")
	a.write("/d/other.txt", "o")
	w.converge(a, b)

	a.remove("/d")
	b.write("/d/edited.txt", "v2")
	a.sync()
	b.sync()
	expect(t, w.converge(a, b), map[string]string{"/d/": "", "/d/edited.txt": "v2"})
}

func TestRenameVersusEdit(t *testing.T) {
	for _, renameFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("renameFirst=%v", renameFirst), func(t *testing.T) {
			w := newWorld(t)
			a, b := w.node("a"), w.node("b")
			a.write("/draft.md", "v1")
			w.converge(a, b)

			a.rename("/draft.md", "/posts/final.md")
			b.write("/draft.md", "v1 with edits")
			if renameFirst {
				a.sync()
				b.sync()
			} else {
				b.sync()
				a.sync()
			}
			expect(t, w.converge(a, b), map[string]string{"/posts/": "", "/posts/final.md": "v1 with edits"})
		})
	}
}

func TestRenameDirectoryVersusEditInside(t *testing.T) {
	w := newWorld(t)
	a, b := w.node("a"), w.node("b")
	a.write("/src/main.go", "package main")
	a.write("/src/util.go", "package util")
	w.converge(a, b)

	a.rename("/src", "/lib")
	b.write("/src/main.go", "package main // edited")
	a.sync()
	b.sync()
	expect(t, w.converge(a, b), map[string]string{
		"/lib/": "", "/lib/main.go": "package main // edited", "/lib/util.go": "package util",
	})
}

func TestLongOffline(t *testing.T) {
	w := newWorld(t)
	a, b := w.node("a"), w.node("b")
	for i := range 20 {
		a.write(fmt.Sprintf("/shared/%02d.txt", i), "v1")
	}
	w.converge(a, b)

	b.setOnline(false)
	if err := b.e.Sync(context.Background()); err == nil {
		t.Fatal("sync while offline succeeded")
	}
	// Hundreds of changes on both sides.
	for i := range 150 {
		a.write(fmt.Sprintf("/from-a/%03d.txt", i), fmt.Sprint("a", i))
		b.write(fmt.Sprintf("/from-b/%03d.txt", i), fmt.Sprint("b", i))
	}
	for i := range 10 {
		a.write(fmt.Sprintf("/shared/%02d.txt", i), "a edit")
	}
	for i := 5; i < 15; i++ {
		b.write(fmt.Sprintf("/shared/%02d.txt", i), "b edit")
	}
	b.remove("/shared/19.txt")
	b.rename("/shared/18.txt", "/moved/18.txt")
	for range 30 {
		a.sync()
	}
	// Local changes keep queueing while offline, and survive a restart.
	b.e.Sync(context.Background())
	b.restart()
	if b.e.st.queueLen() == 0 {
		t.Fatal("offline changes were not queued")
	}
	b.setOnline(true)
	got := w.converge(a, b)

	if n := len(conflictCopies(got)); n != 5 {
		t.Fatalf("got %d conflict copies, want 5 (files 05 to 09):\n%s", n, show(got))
	}
	for i := range 20 {
		p := fmt.Sprintf("/shared/%02d.txt", i)
		want := "v1"
		switch {
		case i < 10:
			want = "a edit"
		case i < 15:
			want = "b edit"
		case i == 18 || i == 19:
			want = ""
		}
		if got[p] != want {
			t.Errorf("%s = %q, want %q", p, got[p], want)
		}
	}
	if got["/moved/18.txt"] != "v1" || got["/from-a/149.txt"] != "a149" || got["/from-b/149.txt"] != "b149" {
		t.Fatalf("missing changes:\n%s", show(got))
	}
}

func TestResetAfterTrimmedLog(t *testing.T) {
	w := newWorld(t)
	a, b := w.node("a"), w.node("b")
	a.write("/stay.txt", "s")
	a.write("/gone.txt", "g")
	a.write("/edit.txt", "v1")
	w.converge(a, b)

	b.setOnline(false)
	a.write("/edit.txt", "v2")
	a.remove("/gone.txt")
	a.rename("/stay.txt", "/moved.txt")
	a.write("/new/file.txt", "n")
	a.sync()
	seq, _ := w.srv.Meta.Seq(context.Background(), w.scope)
	if err := w.srv.Meta.Trim(context.Background(), w.scope, seq); err != nil {
		t.Fatal(err)
	}
	b.write("/local.txt", "while offline")
	b.setOnline(true)
	expect(t, w.converge(a, b), map[string]string{
		"/edit.txt": "v2", "/moved.txt": "s", "/new/": "", "/new/file.txt": "n", "/local.txt": "while offline",
	})
}

func TestConflictCopyName(t *testing.T) {
	// The scripted conflict from the spec: two devices edit one file.
	w := newWorld(t)
	a, b := w.node("mac-a1"), w.node("linux-b2")
	a.write("/Documents/Tax 2025.pdf", "original")
	w.converge(a, b)
	a.write("/Documents/Tax 2025.pdf", "mac edit")
	b.write("/Documents/Tax 2025.pdf", "linux edit")
	a.sync()
	b.sync()
	got := w.converge(a, b)
	want := "/Documents/Tax 2025.sync-conflict-20261119-041500-linux-b2.pdf"
	if c := conflictCopies(got); len(c) != 1 || c[0] != want {
		t.Fatalf("conflict copies %v, want [%s]", c, want)
	}
	if got[want] != "linux edit" || got["/Documents/Tax 2025.pdf"] != "mac edit" {
		t.Fatalf("wrong contents:\n%s", show(got))
	}
}

func TestConflictName(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"notes.txt", "notes.sync-conflict-20261119-041500-dev.txt"},
		{"archive.tar.gz", "archive.tar.sync-conflict-20261119-041500-dev.gz"},
		{"Makefile", "Makefile.sync-conflict-20261119-041500-dev"},
		{".bashrc", ".bashrc.sync-conflict-20261119-041500-dev"},
		{".config.json", ".config.sync-conflict-20261119-041500-dev.json"},
	} {
		if got := ConflictName(c.in, testTime, "dev"); got != c.want {
			t.Errorf("ConflictName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestIgnores(t *testing.T) {
	w := newWorld(t)
	a, b := w.node("a"), w.node("b")
	a.write("/.DS_Store", "x")
	a.write("/doc.txt.swp", "x")
	a.write("/build.log", "log")
	a.write("/"+IgnoreFile, "*.log\n/cache/\n")
	a.write("/cache/blob", "c")
	a.write("/src/cache/kept", "k")
	a.write("/doc.txt", "d")
	a.sync()
	got := w.serverTree()
	expect(t, got, map[string]string{
		"/" + IgnoreFile: "*.log\n/cache/\n", "/doc.txt": "d", "/src/": "", "/src/cache/": "", "/src/cache/kept": "k",
	})
	b.write("/local.log", "mine")
	b.sync()
	b.sync()
	if _, ok := b.tree()["/local.log"]; !ok {
		t.Fatal("ignored local file was removed")
	}
	if _, ok := b.tree()["/cache/blob"]; ok {
		t.Fatal("ignored file was downloaded")
	}

	// Un-ignoring *.log uploads a's log file.
	a.write("/"+IgnoreFile, "/cache/\n")
	a.sync()
	b.sync()
	a.sync()
	if got := w.serverTree(); got["/build.log"] != "log" || got["/local.log"] != "mine" {
		t.Fatalf("logs not synced after un-ignoring:\n%s", show(got))
	}
}

func TestIgnoreRules(t *testing.T) {
	ig, err := ParseIgnores(strings.NewReader("# comment\n*.log\n!keep.log\n/build\ndocs/drafts\ntmp/\n"))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		p    string
		dir  bool
		want bool
	}{
		{"/a.log", false, true},
		{"/x/y/a.log", false, true},
		{"/keep.log", false, false},
		{"/build", true, true},
		{"/build/out.o", false, true},
		{"/src/build", true, false},
		{"/docs/drafts/a.md", false, true},
		{"/tmp", false, false},
		{"/tmp", true, true},
		{"/a/tmp/f", false, true},
		{"/.dotfs/state.db", false, true},
		{"/x/.DS_Store", false, true},
		{"/notes~", false, true},
		{"/x/.fs", true, true},
		{"/ok.txt", false, false},
	} {
		if got := ig.Ignored(c.p, c.dir); got != c.want {
			t.Errorf("Ignored(%q, %v) = %v, want %v", c.p, c.dir, got, c.want)
		}
	}
}

func TestLargeFileResumesUpload(t *testing.T) {
	w := newWorld(t)
	a := w.node("a")
	big := strings.Repeat("0123456789abcdef", 1<<17) // 2 MiB: two upload chunks
	a.write("/big.bin", big)
	a.sync()
	if got := w.serverTree()["/big.bin"]; got != big {
		t.Fatalf("uploaded %d bytes, want %d", len(got), len(big))
	}
	// A copy of the same content commits by hash with no upload.
	a.write("/copy.bin", big)
	a.sync()
	b := w.node("b")
	got := w.converge(a, b)
	if got["/copy.bin"] != big {
		t.Fatal("copy differs")
	}
}

func TestOtherRootRefused(t *testing.T) {
	w := newWorld(t)
	a := w.node("a")
	a.e.Close()
	cfg := a.cfg
	cfg.Root = "elsewhere/root"
	if _, err := New(cfg); !errors.Is(err, errOtherRoot) {
		t.Fatalf("New with another root: %v", err)
	}
	a.open()
}

func TestRootMovedMerges(t *testing.T) {
	w := newWorld(t)
	a := w.node("a")
	a.write("/kept.txt", "k")
	a.write("/edited.txt", "v1")
	a.sync()

	// The server comes back at a new address with a different history:
	// another server with a root of the same name.
	w2 := newWorld(t)
	other := w2.node("other")
	other.write("/edited.txt", "theirs")
	other.write("/new.txt", "n")
	other.sync()

	a.e.Close()
	a.cfg.Root = "moved:1/home"
	a.cfg.Dial = other.cfg.Dial
	a.open()
	a.sync()
	a.sync()
	expect(t, a.tree(), map[string]string{
		"/kept.txt": "k", "/edited.txt": "theirs", "/new.txt": "n",
		"/edited.sync-conflict-20261119-041500-a.txt": "v1",
	})
	expect(t, w2.serverTree(), a.tree())
}

func TestRunWatchesAndFollows(t *testing.T) {
	w := newWorld(t)
	a, b := w.node("a"), w.node("b")
	for _, n := range []*node{a, b} {
		n.e.cfg.Debounce = 50 * time.Millisecond
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg gosync.WaitGroup
	for _, n := range []*node{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.e.Run(ctx)
		}()
	}
	defer func() {
		cancel()
		wg.Wait()
	}()
	waitFor := func(n *node, p, want string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			b, err := os.ReadFile(n.path(p))
			if err == nil && string(b) == want || errors.Is(err, fs.ErrNotExist) && want == "" {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("%s: %s never became %q", n.name, p, want)
	}
	// Give the watchers a moment to start.
	time.Sleep(100 * time.Millisecond)
	a.write("/hello.txt", "hi")
	waitFor(b, "/hello.txt", "hi")
	b.write("/sub/reply.txt", "hello back")
	waitFor(a, "/sub/reply.txt", "hello back")
	a.rename("/sub/reply.txt", "/sub/renamed.txt")
	waitFor(b, "/sub/renamed.txt", "hello back")
	waitFor(b, "/sub/reply.txt", "")
	b.remove("/hello.txt")
	waitFor(a, "/hello.txt", "")

	// Going offline and back.
	a.setOnline(false)
	a.write("/offline.txt", "queued")
	time.Sleep(200 * time.Millisecond)
	a.setOnline(true)
	waitFor(b, "/offline.txt", "queued")
}

func TestNestedScopeLeftAlone(t *testing.T) {
	w := newWorld(t)
	a, b := w.node("a"), w.node("b")
	a.write("/photos/one.jpg", "1")
	a.write("/notes.txt", "n")
	w.converge(a, b)

	// Another client makes /photos a nested scope.
	c, err := fake.Dial(w.srv, "alice", "home")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Call(context.Background(), c, fsproto.FlagsCmd{Path: "/photos", Flags: "i"}); err != nil {
		t.Fatal(err)
	}
	a.write("/photos/two.jpg", "2")
	a.write("/notes.txt", "n2")
	a.sync()
	b.sync()
	if _, ok := w.serverTree()["/photos/two.jpg"]; ok {
		t.Fatal("a file inside a nested scope was committed to the parent")
	}
	if got := b.tree(); got["/photos/one.jpg"] != "1" || got["/notes.txt"] != "n2" {
		t.Fatalf("b lost files or missed changes:\n%s", show(got))
	}
	if r := b.e.st.get("/photos/one.jpg"); r != nil {
		t.Fatal("b still tracks a path inside the nested scope")
	}
}
