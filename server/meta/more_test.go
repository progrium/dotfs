package meta_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/meta"
	"github.com/progrium/dotfs/server/store"
)

var (
	ctx   = context.Background()
	home  = server.RootScope(server.NewRoot("alice", "home"))
	alice = server.Origin{User: "alice", Dev: "d1"}
)

type env struct {
	t   *testing.T
	dir string
	st  *store.Store
	m   *meta.Meta
	w   server.Writer
	n   int
}

func newEnv(t *testing.T) *env {
	e := &env{t: t, dir: t.TempDir()}
	e.st, e.m = open(t, e.dir, nil, false)
	if err := e.m.CreateScope(ctx, home); err != nil {
		t.Fatal(err)
	}
	e.w, _ = e.m.Writer(home)
	t.Cleanup(func() { e.m.Close() })
	return e
}

func (e *env) reopen() {
	e.t.Helper()
	e.m.Close()
	e.st.Close()
	e.st, e.m = open(e.t, e.dir, nil, false)
	e.w, _ = e.m.Writer(home)
}

func ok(t *testing.T) func(server.Result, error) server.Result {
	return func(r server.Result, err error) server.Result {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
}

func (e *env) put(w server.Writer, p, data string) server.Result {
	e.t.Helper()
	e.n++
	name := string(rune('a'+e.n%26)) + string(rune('0'+e.n/26))
	f, _ := e.st.OpenStage("alice", name, true)
	f.WriteAt([]byte(data), 0)
	f.Close()
	return ok(e.t)(w.Commit(ctx, alice, fsproto.CommitCmd{Stage: name, Path: p, Mode: 0o644, Mtime: 1}))
}

func (e *env) entries(s server.Scope) (uint64, []string) {
	e.t.Helper()
	var ps []string
	seq, err := e.m.Entries(ctx, s, func(x fsproto.Entry) error { ps = append(ps, x.Path); return nil })
	if err != nil {
		e.t.Fatal(err)
	}
	return seq, ps
}

func TestOddPathBytes(t *testing.T) {
	e := newEnv(t)
	odd := "/d \xff%\x01é"
	ok(t)(e.w.Mkdir(ctx, alice, fsproto.MkdirCmd{Path: odd}))
	e.put(e.w, odd+"/f\xfe", "x")
	ok(t)(e.w.Mkdir(ctx, alice, fsproto.MkdirCmd{Path: odd + "/sub"}))
	e.put(e.w, odd+"/sub/g", "y")
	ok(t)(e.w.Mv(ctx, alice, fsproto.MvCmd{From: odd, To: "/moved\x80"}))
	_, ps := e.entries(home)
	want := []string{"/", "/moved\x80", "/moved\x80/f\xfe", "/moved\x80/sub", "/moved\x80/sub/g"}
	if !reflect.DeepEqual(ps, want) {
		t.Fatalf("entries %q, want %q", ps, want)
	}
	es, err := e.m.ReadDir(ctx, home, "/moved\x80")
	if err != nil || len(es) != 2 {
		t.Fatalf("ReadDir %+v %v", es, err)
	}
	es, _ = e.m.ReadDir(ctx, home, "/moved\x80/sub")
	if len(es) != 1 || es[0].Path != "/moved\x80/sub/g" {
		t.Fatalf("ReadDir sub %+v", es)
	}
}

func TestIndexEncodings(t *testing.T) {
	e := newEnv(t)
	e.put(e.w, "/a", "1")
	e.put(e.w, "/b", "2")
	read := func(accept []string) (server.IndexInfo, []fsproto.Entry) {
		t.Helper()
		rc, info, err := e.m.OpenIndex(ctx, home, accept)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(rc)
		rc.Close()
		if info.Size != int64(len(raw)) {
			t.Fatalf("size %d for %d bytes", info.Size, len(raw))
		}
		ir, err := fsproto.NewIndexReader(bytes.NewReader(raw), func(enc string, r io.Reader) (io.Reader, error) {
			return zstd.NewReader(r)
		})
		if err != nil {
			t.Fatal(err)
		}
		if ir.Header().Encoding != info.Encoding || ir.Header().Seq != info.Seq {
			t.Fatalf("header %+v info %+v", ir.Header(), info)
		}
		var es []fsproto.Entry
		for {
			x, err := ir.Next()
			if err == io.EOF {
				return info, es
			}
			if err != nil {
				t.Fatal(err)
			}
			es = append(es, x)
		}
	}
	pi, plain := read(nil)
	zi, z := read([]string{"br", "zstd"})
	if pi.Encoding != "" || zi.Encoding != "zstd" || !reflect.DeepEqual(plain, z) || len(plain) != 3 {
		t.Fatalf("plain %+v %d, zstd %+v %d", pi, len(plain), zi, len(z))
	}
	// After a change the snapshot is rebuilt.
	e.put(e.w, "/c", "3")
	if info, es := read([]string{"zstd"}); info.Seq != 3 || len(es) != 4 {
		t.Fatalf("after change: %+v %d", info, len(es))
	}
	// The zstd snapshot survives a restart, and plain is decoded from it.
	e.reopen()
	if info, es := read(nil); info.Seq != 3 || len(es) != 4 {
		t.Fatalf("after restart: %+v %d", info, len(es))
	}
}

func TestRestart(t *testing.T) {
	e := newEnv(t)
	ok(t)(e.w.Mkdir(ctx, alice, fsproto.MkdirCmd{Path: "/d"}))
	r := e.put(e.w, "/d/f", "hello")
	e.reopen()
	if seq, _ := e.m.Seq(ctx, home); seq != 2 {
		t.Fatalf("seq %d", seq)
	}
	got, err := e.m.Stat(ctx, home, "/d/f")
	if err != nil || got.Hash != r.Hash || got.Version != 2 {
		t.Fatalf("stat %+v %v", got, err)
	}
	if r := e.put(e.w, "/d/g", "x"); r.Version != 3 {
		t.Fatalf("version after restart %d", r.Version)
	}
	var n int
	e.m.ReadLog(ctx, home, 0, func(fsproto.Event) error { n++; return nil })
	if n != 3 {
		t.Fatalf("log has %d records", n)
	}
}

func TestLogRetention(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.New(dir+"/data", &store.Options{NoSync: true})
	m, err := meta.Open(dir+"/meta.db", st, nil, &meta.Options{NoSync: true, KeepLog: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	m.CreateScope(ctx, home)
	w, _ := m.Writer(home)
	for i := 0; i < 30; i++ {
		ok(t)(w.SetFlags(ctx, alice, fsproto.FlagsCmd{Path: "/", Flags: fsproto.Flags("p")[:i%2]}))
	}
	if err := m.ReadLog(ctx, home, 10, func(fsproto.Event) error { return nil }); !errors.Is(err, server.ErrTrimmed) {
		t.Fatalf("old records kept: %v", err)
	}
	var n int
	if err := m.ReadLog(ctx, home, 22, func(fsproto.Event) error { n++; return nil }); err != nil || n != 8 {
		t.Fatalf("recent records: %d %v", n, err)
	}
}

func TestNestedScope(t *testing.T) {
	e := newEnv(t)
	ok(t)(e.w.Mkdir(ctx, alice, fsproto.MkdirCmd{Path: "/P"}))                     // 1
	e.put(e.w, "/P/a.jpg", "a")                                                    // 2
	ok(t)(e.w.Mkdir(ctx, alice, fsproto.MkdirCmd{Path: "/P/2025"}))                // 3
	e.put(e.w, "/P/2025/b.jpg", "b")                                               // 4
	e.put(e.w, "/top", "t")                                                        // 5
	r := ok(t)(e.w.SetFlags(ctx, alice, fsproto.FlagsCmd{Path: "/P", Flags: "i"})) // 6
	if r.Event.Op != fsproto.OpFlags || r.Version != 6 {
		t.Fatalf("result %+v", r)
	}
	nested := server.Scope{Root: home.Root, Dir: "/P"}
	if err := e.m.CreateScope(ctx, nested); err != server.ErrScopeExists {
		t.Fatalf("CreateScope on a nested scope: %v", err)
	}
	// The parent lists the directory but not its children.
	if _, ps := e.entries(home); !reflect.DeepEqual(ps, []string{"/", "/P", "/top"}) {
		t.Fatalf("parent entries %q", ps)
	}
	seq, ps := e.entries(nested)
	if seq != 0 || !reflect.DeepEqual(ps, []string{"/", "/2025", "/2025/b.jpg", "/a.jpg"}) {
		t.Fatalf("nested entries %d %q", seq, ps)
	}
	if x, _ := e.m.Stat(ctx, nested, "/a.jpg"); x.Version != 0 {
		t.Fatalf("nested version %d", x.Version)
	}
	// The parent rejects changes inside, and moving or removing it.
	for what, err := range map[string]error{
		"mkdir inside": second(e.w.Mkdir(ctx, alice, fsproto.MkdirCmd{Path: "/P/x"})),
		"rmtree":       second(e.w.Rmtree(ctx, alice, fsproto.RmtreeCmd{Path: "/P"})),
		"mv":           second(e.w.Mv(ctx, alice, fsproto.MvCmd{From: "/P", To: "/Q"})),
		"cp out":       second(e.w.Cp(ctx, alice, fsproto.CpCmd{From: "/P/a.jpg", To: "/a"})),
	} {
		if !errors.Is(err, fsproto.ErrInvalid) {
			t.Fatalf("%s: %v", what, err)
		}
	}
	// The nested scope has its own writer, seqs, and content paths.
	nw, err := e.m.Writer(nested)
	if err != nil {
		t.Fatal(err)
	}
	if r := e.put(nw, "/c.jpg", "c"); r.Version != 1 {
		t.Fatalf("nested commit %+v", r)
	}
	f, err := e.st.Open(home.Root, "/P/c.jpg")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	ok(t)(nw.Mv(ctx, alice, fsproto.MvCmd{From: "/2025", To: "/y2025"}))
	// It survives a restart.
	e.reopen()
	nw, err = e.m.Writer(nested)
	if err != nil {
		t.Fatal(err)
	}
	if seq, _ := e.m.Seq(ctx, nested); seq != 2 {
		t.Fatalf("nested seq after restart %d", seq)
	}
	if !errors.Is(second(e.w.Mkdir(ctx, alice, fsproto.MkdirCmd{Path: "/P/x"})), fsproto.ErrInvalid) {
		t.Fatal("parent accepts changes inside after restart")
	}
	// Clearing the flag merges the children back at the new version.
	r = ok(t)(e.w.SetFlags(ctx, alice, fsproto.FlagsCmd{Path: "/P"}))
	if _, err := e.m.Writer(nested); err != server.ErrNoScope {
		t.Fatalf("nested scope still there: %v", err)
	}
	if !errors.Is(second(nw.Mkdir(ctx, alice, fsproto.MkdirCmd{Path: "/z"})), server.ErrNoScope) {
		t.Fatal("old nested writer still works")
	}
	_, ps = e.entries(home)
	if !reflect.DeepEqual(ps, []string{"/", "/P", "/P/a.jpg", "/P/c.jpg", "/P/y2025", "/P/y2025/b.jpg", "/top"}) {
		t.Fatalf("merged entries %q", ps)
	}
	if x, _ := e.m.Stat(ctx, home, "/P/y2025/b.jpg"); x.Version != r.Version {
		t.Fatalf("merged version %d, want %d", x.Version, r.Version)
	}
	// CreateScope sets flag i.
	if err := e.m.CreateScope(ctx, nested); err != nil {
		t.Fatal(err)
	}
	if x, _ := e.m.Stat(ctx, home, "/P"); x.Flags != "i" {
		t.Fatalf("flags %q", x.Flags)
	}
}

func second(_ server.Result, err error) error { return err }

// failingStore fails Place once, to check that a change whose filesystem
// step fails leaves nothing behind.
type failingStore struct {
	*store.Store
	fail bool
}

func (f *failingStore) Place(p store.Prepared, r server.Root, path string) error {
	if f.fail {
		f.fail = false
		return errors.New("disk on fire")
	}
	return f.Store.Place(p, r, path)
}

func TestFailedPlace(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.New(dir+"/data", &store.Options{NoSync: true})
	fs := &failingStore{Store: st}
	m, err := meta.Open(dir+"/meta.db", fs, nil, &meta.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	m.CreateScope(ctx, home)
	w, _ := m.Writer(home)
	f, _ := st.OpenStage("alice", "s", true)
	f.WriteAt([]byte("x"), 0)
	f.Close()
	fs.fail = true
	if _, err := w.Commit(ctx, alice, fsproto.CommitCmd{Stage: "s", Path: "/f", Mode: 0o644}); err == nil {
		t.Fatal("commit succeeded")
	}
	if seq, _ := m.Seq(ctx, home); seq != 0 {
		t.Fatalf("seq %d after failed commit", seq)
	}
	if _, err := m.Stat(ctx, home, "/f"); !errors.Is(err, fsproto.ErrMissing) {
		t.Fatalf("stat: %v", err)
	}
	if r, err := w.Mkdir(ctx, alice, fsproto.MkdirCmd{Path: "/d"}); err != nil || r.Version != 1 {
		t.Fatalf("next change %+v %v", r, err)
	}
}
