package mount

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/progrium/dotfs/client"
	"github.com/progrium/dotfs/client/fake"
	"github.com/progrium/dotfs/fsproto"
	sfake "github.com/progrium/dotfs/server/fake"
)

var testTime = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

type world struct {
	t      *testing.T
	srv    *sfake.Server
	remote *fake.Conn // another device, writing straight to the server
}

func newWorld(t *testing.T) *world {
	t.Helper()
	srv := sfake.NewServer()
	if _, err := srv.CreateRoot("alice", "home"); err != nil {
		t.Fatal(err)
	}
	c, err := fake.Dial(srv, "alice", "home")
	if err != nil {
		t.Fatal(err)
	}
	return &world{t: t, srv: srv, remote: c}
}

func (w *world) put(p, content string) uint64 {
	w.t.Helper()
	base := fsproto.At(0)
	if st, err := client.Call(context.Background(), w.remote, fsproto.StatCmd{Path: p}); err == nil {
		base = fsproto.At(st.Entry.Version)
	}
	r, err := client.Put(context.Background(), w.remote, p, base, strings.NewReader(content), int64(len(content)), client.PutOptions{Mtime: testTime.Unix()})
	if err != nil {
		w.t.Fatalf("put %s: %v", p, err)
	}
	return r.Version
}

func (w *world) mkdir(p string) {
	w.t.Helper()
	if _, err := client.Call(context.Background(), w.remote, fsproto.MkdirCmd{Path: p}); err != nil {
		w.t.Fatalf("mkdir %s: %v", p, err)
	}
}

func (w *world) read(p string) (string, error) {
	b, err := w.remote.Read(context.Background(), p)
	return string(b), err
}

type mnt struct {
	*FS
	t      *testing.T
	online atomic.Bool
	cancel context.CancelFunc
	done   chan error
	cfg    Config
}

func (w *world) mount(dir string) *mnt {
	w.t.Helper()
	m := &mnt{t: w.t}
	m.online.Store(true)
	m.cfg = Config{
		Root:     "fake/home",
		CacheDir: dir,
		Device:   "laptop",
		Now:      func() time.Time { return testTime },
		Dial: func(ctx context.Context) (client.Conn, error) {
			if !m.online.Load() {
				return nil, fake.ErrOffline
			}
			return fake.Dial(w.srv, "alice", "home")
		},
		Logf: func(f string, args ...any) { w.t.Logf("mount: "+f, args...) },
	}
	m.start()
	return m
}

func (m *mnt) start() {
	m.t.Helper()
	fs, err := New(context.Background(), m.cfg)
	if err != nil {
		m.t.Fatal(err)
	}
	m.FS = fs
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel, m.done = cancel, make(chan error, 1)
	go func() { m.done <- fs.Run(ctx) }()
	m.t.Cleanup(m.stop)
}

func (m *mnt) stop() {
	if m.cancel == nil {
		return
	}
	m.cancel()
	<-m.done
	m.FS.Close()
	m.cancel = nil
}

// eventually waits for cond.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (m *mnt) readFile(p string) (string, error) {
	h, err := m.Open(context.Background(), p, false, false)
	if err != nil {
		return "", err
	}
	defer h.Release()
	b, err := io.ReadAll(io.NewSectionReader(h.File, 0, 1<<30))
	return string(b), err
}

func (m *mnt) writeFile(p, content string, create bool) error {
	ctx := context.Background()
	var h *Handle
	var err error
	if create {
		h, err = m.Create(ctx, p, 0o644)
	} else {
		h, err = m.Open(ctx, p, true, true)
	}
	if err != nil {
		return err
	}
	if _, err := h.File.WriteAt([]byte(content), 0); err != nil {
		return err
	}
	h.Wrote()
	if err := h.Flush(ctx); err != nil {
		return err
	}
	return h.Release()
}

func names(sts []Stat) string {
	var s []string
	for _, st := range sts {
		s = append(s, fsproto.Name(st.Path))
	}
	return strings.Join(s, " ")
}

func TestBootstrapAndHydrate(t *testing.T) {
	w := newWorld(t)
	w.mkdir("/docs")
	w.put("/docs/a b.txt", "hello")
	w.put("/top", "top")
	m := w.mount(t.TempDir())

	ls, err := m.ReadDir("/")
	if err != nil || names(ls) != "docs top" {
		t.Fatalf("readdir / = %q, %v", names(ls), err)
	}
	st, err := m.Lookup("/docs/a b.txt")
	if err != nil || st.Length != 5 || st.Type != fsproto.TypeFile {
		t.Fatalf("lookup = %+v, %v", st, err)
	}
	if m.Cached("/docs/a b.txt") {
		t.Fatal("cached before the first open")
	}
	if got, err := m.readFile("/docs/a b.txt"); err != nil || got != "hello" {
		t.Fatalf("read = %q, %v", got, err)
	}
	if !m.Cached("/docs/a b.txt") {
		t.Fatal("not cached after the first open")
	}
	if _, err := m.Lookup("/nope"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("lookup missing: %v", err)
	}
}

func TestRemoteChanges(t *testing.T) {
	w := newWorld(t)
	w.put("/a", "one")
	w.mkdir("/d")
	m := w.mount(t.TempDir())
	var changes atomic.Int32
	m.SetNotify(func(Change) { changes.Add(1) })
	ino := mustLookup(t, m.FS, "/a").Ino

	w.put("/a", "two!")
	eventually(t, "put", func() bool { st, _ := m.Lookup("/a"); return st.Length == 4 })
	if got, _ := m.readFile("/a"); got != "two!" {
		t.Fatalf("read after remote put = %q", got)
	}
	if st := mustLookup(t, m.FS, "/a"); st.Ino != ino {
		t.Fatalf("inode changed on a remote put: %d then %d", ino, st.Ino)
	}

	if _, err := client.Call(context.Background(), w.remote, fsproto.MvCmd{From: "/a", To: "/d/b"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "mv", func() bool { _, err := m.Lookup("/d/b"); return err == nil })
	if _, err := m.Lookup("/a"); err == nil {
		t.Fatal("/a still there after mv")
	}
	if _, err := client.Call(context.Background(), w.remote, fsproto.RmtreeCmd{Path: "/d"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "rmtree", func() bool { _, err := m.Lookup("/d"); return err != nil })
	if changes.Load() < 3 {
		t.Fatalf("got %d change notifications, want at least 3", changes.Load())
	}
}

func mustLookup(t *testing.T, f *FS, p string) Stat {
	t.Helper()
	st, err := f.Lookup(p)
	if err != nil {
		t.Fatalf("lookup %s: %v", p, err)
	}
	return st
}

func TestLocalWrites(t *testing.T) {
	w := newWorld(t)
	w.put("/a", "old")
	m := w.mount(t.TempDir())
	var marker atomic.Bool
	var changes atomic.Int32
	m.SetNotify(func(c Change) {
		changes.Add(1)
		if c.Path == "/marker" {
			marker.Store(true)
		}
	})

	if err := m.writeFile("/new", "fresh", true); err != nil {
		t.Fatal(err)
	}
	if got, err := w.read("/new"); err != nil || got != "fresh" {
		t.Fatalf("server /new = %q, %v", got, err)
	}
	if err := m.writeFile("/a", "newer", false); err != nil {
		t.Fatal(err)
	}
	if got, _ := w.read("/a"); got != "newer" {
		t.Fatalf("server /a = %q", got)
	}
	if got, _ := m.readFile("/a"); got != "newer" {
		t.Fatalf("local /a = %q", got)
	}
	if _, err := m.Mkdir(context.Background(), "/dir"); err != nil {
		t.Fatal(err)
	}
	if err := m.Rename(context.Background(), "/a", "/dir/a", false); err != nil {
		t.Fatal(err)
	}
	if got, err := w.read("/dir/a"); err != nil || got != "newer" {
		t.Fatalf("server /dir/a = %q, %v", got, err)
	}
	if err := m.Remove(context.Background(), "/new", false); err != nil {
		t.Fatal(err)
	}
	if _, err := w.read("/new"); !errors.Is(err, fsproto.ErrMissing) {
		t.Fatalf("server /new after remove: %v", err)
	}
	if err := m.Remove(context.Background(), "/dir", true); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("rmdir non-empty: %v", err)
	}
	mode := uint32(0o600)
	if _, err := m.SetAttr(context.Background(), "/dir/a", &mode, nil, nil); err != nil {
		t.Fatal(err)
	}
	st, err := client.Call(context.Background(), w.remote, fsproto.StatCmd{Path: "/dir/a"})
	if err != nil || st.Entry.Mode != 0o600 {
		t.Fatalf("server mode = %+v, %v", st.Entry, err)
	}
	size := int64(2)
	if _, err := m.SetAttr(context.Background(), "/dir/a", nil, nil, &size); err != nil {
		t.Fatal(err)
	}
	if got, _ := w.read("/dir/a"); got != "ne" {
		t.Fatalf("server after truncate = %q", got)
	}
	// Let the events for our own changes arrive. Most are skipped, since
	// the tree already has them; one that arrives before its own ctl reply
	// is reported, which only costs the kernel an extra lookup.
	w.put("/marker", "x")
	eventually(t, "marker", marker.Load)
	if n := changes.Load(); n > 4 {
		t.Fatalf("got %d change notifications for 9 local changes and 1 remote one", n)
	}
}

func TestConflictCopy(t *testing.T) {
	w := newWorld(t)
	w.put("/doc.txt", "base")
	m := w.mount(t.TempDir())
	ctx := context.Background()
	h, err := m.Open(ctx, "/doc.txt", true, true)
	if err != nil {
		t.Fatal(err)
	}
	h.File.WriteAt([]byte("mine"), 0)
	h.Wrote()
	w.put("/doc.txt", "theirs")
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	h.Release()
	if got, _ := w.read("/doc.txt"); got != "theirs" {
		t.Fatalf("server /doc.txt = %q", got)
	}
	cp := "/doc.sync-conflict-20261120-120000-laptop.txt"
	if got, err := w.read(cp); err != nil || got != "mine" {
		t.Fatalf("conflict copy = %q, %v", got, err)
	}
	eventually(t, "local view", func() bool { got, _ := m.readFile("/doc.txt"); return got == "theirs" })
	if _, err := m.Lookup(cp); err != nil {
		t.Fatal(err)
	}
}

func TestEditBeatsDelete(t *testing.T) {
	w := newWorld(t)
	w.put("/f", "base")
	m := w.mount(t.TempDir())
	ctx := context.Background()
	h, err := m.Open(ctx, "/f", true, false)
	if err != nil {
		t.Fatal(err)
	}
	h.File.WriteAt([]byte("BASE"), 0)
	h.Wrote()
	if _, err := client.Call(ctx, w.remote, fsproto.RmCmd{Path: "/f"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "del applied", func() bool { st, err := m.Lookup("/f"); return err == nil && st.Version == 0 })
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	h.Release()
	if got, err := w.read("/f"); err != nil || got != "BASE" {
		t.Fatalf("server /f = %q, %v", got, err)
	}
}

func TestOfflineRestartAndRetry(t *testing.T) {
	w := newWorld(t)
	w.put("/cached", "local bytes")
	w.put("/remote-only", "far away")
	dir := t.TempDir()
	m := w.mount(dir)
	if got, _ := m.readFile("/cached"); got != "local bytes" {
		t.Fatal(got)
	}
	m.stop()

	m.online.Store(false)
	m.start()
	if m.Online() {
		t.Fatal("online with the server unreachable")
	}
	if got, err := m.readFile("/cached"); err != nil || got != "local bytes" {
		t.Fatalf("offline read of cached file = %q, %v", got, err)
	}
	if _, err := m.readFile("/remote-only"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("offline read of uncached file: %v", err)
	}
	// A change made offline is committed once the server is back.
	if err := m.writeFile("/cached", "edited offline", false); err != nil {
		t.Fatal(err)
	}
	if got, _ := m.readFile("/cached"); got != "edited offline" {
		t.Fatalf("local view = %q", got)
	}
	m.online.Store(true)
	eventually(t, "retry", func() bool { got, _ := w.read("/cached"); return got == "edited offline" })
}

func TestRootMismatch(t *testing.T) {
	w := newWorld(t)
	dir := t.TempDir()
	m := w.mount(dir)
	m.stop()
	cfg := m.cfg
	cfg.Root = "fake/other"
	if _, err := New(context.Background(), cfg); err == nil {
		t.Fatal("opened a cache directory that belongs to another root")
	}
}

func TestPrefetch(t *testing.T) {
	w := newWorld(t)
	w.mkdir("/cfg")
	w.put("/cfg/rc", "settings")
	w.put("/big", "not prefetched")
	if _, err := client.Call(context.Background(), w.remote, fsproto.FlagsCmd{Path: "/cfg", Flags: "p"}); err != nil {
		t.Fatal(err)
	}
	m := w.mount(t.TempDir())
	eventually(t, "prefetch", func() bool { return m.Cached("/cfg/rc") })
	if m.Cached("/big") {
		t.Fatal("prefetched a file without p")
	}
}

func TestCacheEviction(t *testing.T) {
	dir := t.TempDir()
	c, err := openCache(dir, 100, t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "cccccccccccccccccccccccccccccccccccccccc"} {
		p := filepath.Join(dir, "tmp", "x")
		os.WriteFile(p, []byte(s), 0o600)
		old := time.Now().Add(time.Duration(i-10) * time.Minute)
		os.Chtimes(p, old, old)
		if err := c.adopt(p, hashKey(fsproto.HashBytes([]byte(s))), int64(len(s))); err != nil {
			t.Fatal(err)
		}
	}
	if c.has(hashKey(fsproto.HashBytes(bytes.Repeat([]byte("a"), 40)))) {
		t.Fatal("oldest content not evicted")
	}
	if !c.has(hashKey(fsproto.HashBytes(bytes.Repeat([]byte("c"), 40)))) {
		t.Fatal("newest content evicted")
	}
}
