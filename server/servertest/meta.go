package servertest

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

type metaEnv struct {
	t     *testing.T
	st    server.Store
	m     server.Meta
	rec   *Recorder
	scope server.Scope
	w     server.Writer
	o     server.Origin
	n     int
}

func newMetaEnv(t *testing.T, newMeta NewMeta) *metaEnv {
	rec := &Recorder{}
	st, m := newMeta(t, rec)
	t.Cleanup(func() { m.Close() })
	e := &metaEnv{t: t, st: st, m: m, rec: rec, o: server.Origin{User: "alice", Dev: "dev-a"}}
	e.scope = server.RootScope(server.NewRoot("alice", "home"))
	check(t, "CreateScope", m.CreateScope(ctx, e.scope))
	e.w = must[server.Writer](t, "Writer")(m.Writer(e.scope))
	return e
}

func (e *metaEnv) stat(p string) fsproto.Entry {
	e.t.Helper()
	return must[fsproto.Entry](e.t, "Stat "+p)(e.m.Stat(ctx, e.scope, p))
}

func (e *metaEnv) missing(p string) {
	e.t.Helper()
	_, err := e.m.Stat(ctx, e.scope, p)
	wantStatus(e.t, "Stat "+p, err, fsproto.ErrMissing)
}

func (e *metaEnv) seq() uint64 {
	e.t.Helper()
	return must[uint64](e.t, "Seq")(e.m.Seq(ctx, e.scope))
}

func (e *metaEnv) mkdir(p string) server.Result {
	e.t.Helper()
	return must[server.Result](e.t, "mkdir "+p)(e.w.Mkdir(ctx, e.o, fsproto.MkdirCmd{Path: p}))
}

// commit stages data and commits it at p with base.
func (e *metaEnv) commit(p string, base fsproto.Base, data string) (server.Result, error) {
	e.t.Helper()
	e.n++
	name := fmt.Sprintf("stage-%d", e.n)
	stage(e.t, e.st, e.o.User, name, []byte(data))
	return e.w.Commit(ctx, e.o, fsproto.CommitCmd{Stage: name, Path: p, Base: base, Mtime: 1763491250, Mode: 0o644})
}

func (e *metaEnv) put(p, data string) server.Result {
	e.t.Helper()
	return must[server.Result](e.t, "commit "+p)(e.commit(p, fsproto.Any, data))
}

// log returns every logged change after since.
func (e *metaEnv) log(since uint64) []fsproto.Event {
	e.t.Helper()
	var evs []fsproto.Event
	check(e.t, "ReadLog", e.m.ReadLog(ctx, e.scope, since, func(ev fsproto.Event) error {
		evs = append(evs, ev)
		return nil
	}))
	return evs
}

// TestMeta runs the Meta, Writer, Log, and IndexSource behavior tests.
func TestMeta(t *testing.T, newMeta NewMeta) {
	t.Run("Scope", func(t *testing.T) {
		e := newMetaEnv(t, newMeta)
		if e.seq() != 0 {
			t.Fatalf("new scope seq %d", e.seq())
		}
		root := e.stat("/")
		if root.Type != fsproto.TypeDir || root.Path != "/" {
			t.Fatalf("root entry %+v", root)
		}
		if err := e.m.CreateScope(ctx, e.scope); err != server.ErrScopeExists {
			t.Fatalf("CreateScope again: %v", err)
		}
		other := server.RootScope(server.NewRoot("alice", "nope"))
		if _, err := e.m.Writer(other); err != server.ErrNoScope {
			t.Fatalf("Writer on missing scope: %v", err)
		}
		if _, err := e.m.Seq(ctx, other); err != server.ErrNoScope {
			t.Fatalf("Seq on missing scope: %v", err)
		}
	})

	t.Run("Mkdir", func(t *testing.T) {
		e := newMetaEnv(t, newMeta)
		r := e.mkdir("/a")
		if r.Version != 1 || e.stat("/a").Version != 1 || e.stat("/a").Type != fsproto.TypeDir {
			t.Fatalf("mkdir result %+v, entry %+v", r, e.stat("/a"))
		}
		_, err := e.w.Mkdir(ctx, e.o, fsproto.MkdirCmd{Path: "/a"})
		wantConflict(t, "mkdir existing", err, 1)
		_, err = e.w.Mkdir(ctx, e.o, fsproto.MkdirCmd{Path: "/a", Base: fsproto.At(0)})
		wantConflict(t, "mkdir existing with base 0", err, 1)
		_, err = e.w.Mkdir(ctx, e.o, fsproto.MkdirCmd{Path: "/b", Base: fsproto.At(5)})
		wantConflict(t, "mkdir with wrong base", err, 0)
		r = must[server.Result](t, "mkdir base 0")(e.w.Mkdir(ctx, e.o, fsproto.MkdirCmd{Path: "/a/b", Base: fsproto.At(0)}))
		if r.Version != 2 {
			t.Fatalf("version %d", r.Version)
		}
		_, err = e.w.Mkdir(ctx, e.o, fsproto.MkdirCmd{Path: "/x/y"})
		wantStatus(t, "mkdir without parent", err, fsproto.ErrMissing)
		_, err = e.w.Mkdir(ctx, e.o, fsproto.MkdirCmd{Path: "/.fs"})
		wantStatus(t, "mkdir .fs", err, fsproto.ErrInvalid)
		_, err = e.w.Mkdir(ctx, e.o, fsproto.MkdirCmd{Path: "/a/.fs/x"})
		wantStatus(t, "mkdir under .fs", err, fsproto.ErrInvalid)
		if e.seq() != 2 {
			t.Fatalf("failed changes moved seq to %d", e.seq())
		}
	})

	t.Run("Commit", func(t *testing.T) {
		e := newMetaEnv(t, newMeta)
		r, err := e.commit("/f.txt", fsproto.At(0), "one")
		check(t, "commit new", err)
		if r.Version != 1 || r.Hash != fsproto.HashBytes([]byte("one")) {
			t.Fatalf("result %+v", r)
		}
		want := fsproto.Entry{Path: "/f.txt", Type: fsproto.TypeFile, Mode: 0o644, Length: 3, Mtime: 1763491250, Version: 1, Hash: r.Hash}
		if got := e.stat("/f.txt"); !reflect.DeepEqual(got, want) {
			t.Fatalf("entry\n got %+v\nwant %+v", got, want)
		}
		if got := readAll(t, e.st, e.scope.Root, "/f.txt"); got != "one" {
			t.Fatalf("content %q", got)
		}
		if _, err := e.st.StatStage("alice", "stage-1"); err == nil {
			t.Fatal("stage file survived commit")
		}
		_, err = e.commit("/f.txt", fsproto.At(0), "two")
		wantConflict(t, "commit base 0 on existing", err, 1)
		_, err = e.commit("/f.txt", fsproto.At(7), "two")
		wantConflict(t, "commit wrong base", err, 1)
		r, err = e.commit("/f.txt", fsproto.At(1), "two")
		check(t, "commit right base", err)
		if r.Version != 2 || readAll(t, e.st, e.scope.Root, "/f.txt") != "two" {
			t.Fatalf("second commit %+v", r)
		}
		r = e.put("/f.txt", "three") // unconditional, as plain writes do
		if r.Version != 3 {
			t.Fatalf("unconditional commit %+v", r)
		}

		stage(t, e.st, "alice", "h", []byte("data"))
		_, err = e.w.Commit(ctx, e.o, fsproto.CommitCmd{Stage: "h", Path: "/g", Base: fsproto.At(0), Mode: 0o644, Hash: fsproto.HashBytes([]byte("other"))})
		wantStatus(t, "commit with wrong hash", err, fsproto.ErrInvalid)
		e.missing("/g")
		r = must[server.Result](t, "commit with hash")(e.w.Commit(ctx, e.o, fsproto.CommitCmd{Stage: "h", Path: "/g", Base: fsproto.At(0), Mode: 0o644, Hash: fsproto.HashBytes([]byte("data"))}))
		if r.Version != 4 {
			t.Fatalf("commit with hash %+v", r)
		}
		_, err = e.w.Commit(ctx, e.o, fsproto.CommitCmd{Stage: "gone", Path: "/h", Base: fsproto.At(0), Mode: 0o644})
		wantStatus(t, "commit missing stage", err, fsproto.ErrMissing)
		_, err = e.commit("/no/such/file", fsproto.At(0), "x")
		wantStatus(t, "commit without parent", err, fsproto.ErrMissing)
		e.mkdir("/dir")
		_, err = e.commit("/dir", fsproto.At(5), "x")
		wantStatus(t, "commit over a directory", err, fsproto.ErrInvalid)
		_, err = e.commit("/.fs/index", fsproto.At(0), "x")
		wantStatus(t, "commit to .fs", err, fsproto.ErrInvalid)
		if e.seq() != 5 {
			t.Fatalf("seq %d after failed commits", e.seq())
		}
		// Flags survive a commit.
		must[server.Result](t, "flags")(e.w.SetFlags(ctx, e.o, fsproto.FlagsCmd{Path: "/g", Flags: "p"}))
		e.put("/g", "new")
		if e.stat("/g").Flags != "p" {
			t.Fatalf("flags lost: %+v", e.stat("/g"))
		}
	})

	t.Run("CommitHash", func(t *testing.T) {
		e := newMetaEnv(t, newMeta)
		h := e.put("/a", "shared content").Hash
		r := must[server.Result](t, "commit-hash")(e.w.CommitHash(ctx, e.o, fsproto.CommitHashCmd{Hash: h, Path: "/b", Base: fsproto.At(0), Mtime: 9, Mode: 0o600}))
		if r.Version != 2 || r.Hash != h {
			t.Fatalf("result %+v", r)
		}
		if got := e.stat("/b"); got.Length != 14 || got.Hash != h || got.Mode != 0o600 || got.Mtime != 9 {
			t.Fatalf("entry %+v", got)
		}
		_, err := e.w.CommitHash(ctx, e.o, fsproto.CommitHashCmd{Hash: fsproto.HashBytes([]byte("nope")), Path: "/c", Base: fsproto.At(0), Mode: 0o644})
		wantStatus(t, "commit-hash miss", err, fsproto.ErrMissing)
		_, err = e.w.CommitHash(ctx, e.o, fsproto.CommitHashCmd{Hash: h, Path: "/b", Base: fsproto.At(0), Mode: 0o644})
		wantConflict(t, "commit-hash conflict", err, 2)
		// Another user cannot probe alice's content.
		bob := server.RootScope(server.NewRoot("bob", "home"))
		check(t, "CreateScope bob", e.m.CreateScope(ctx, bob))
		bw := must[server.Writer](t, "Writer bob")(e.m.Writer(bob))
		_, err = bw.CommitHash(ctx, server.Origin{User: "bob"}, fsproto.CommitHashCmd{Hash: h, Path: "/x", Base: fsproto.At(0), Mode: 0o644})
		wantStatus(t, "commit-hash across users", err, fsproto.ErrMissing)
	})

	t.Run("Rm", func(t *testing.T) {
		e := newMetaEnv(t, newMeta)
		e.mkdir("/d")
		e.put("/d/f", "x")
		_, err := e.w.Rm(ctx, e.o, fsproto.RmCmd{Path: "/nope"})
		wantStatus(t, "rm missing", err, fsproto.ErrMissing)
		_, err = e.w.Rm(ctx, e.o, fsproto.RmCmd{Path: "/d"})
		wantStatus(t, "rm non-empty dir", err, fsproto.ErrInvalid)
		_, err = e.w.Rm(ctx, e.o, fsproto.RmCmd{Path: "/d/f", Base: fsproto.At(1)})
		wantConflict(t, "rm wrong base", err, 2)
		_, err = e.w.Rm(ctx, e.o, fsproto.RmCmd{Path: "/"})
		wantStatus(t, "rm root", err, fsproto.ErrInvalid)
		r := must[server.Result](t, "rm")(e.w.Rm(ctx, e.o, fsproto.RmCmd{Path: "/d/f", Base: fsproto.At(2)}))
		if r.Version != 3 || r.Event.Op != fsproto.OpDel || r.Event.Path != "/d/f" {
			t.Fatalf("result %+v", r)
		}
		e.missing("/d/f")
		if _, err := e.st.Open(e.scope.Root, "/d/f"); err == nil {
			t.Fatal("content survived rm")
		}
		must[server.Result](t, "rm empty dir")(e.w.Rm(ctx, e.o, fsproto.RmCmd{Path: "/d"}))
		e.missing("/d")
	})

	t.Run("Rmtree", func(t *testing.T) {
		e := newMetaEnv(t, newMeta)
		e.mkdir("/d")
		e.mkdir("/d/e")
		e.put("/d/e/f", "x")
		e.put("/dx", "keep")
		_, err := e.w.Rmtree(ctx, e.o, fsproto.RmtreeCmd{Path: "/d", Base: fsproto.At(2)})
		wantConflict(t, "rmtree wrong base", err, 1)
		r := must[server.Result](t, "rmtree")(e.w.Rmtree(ctx, e.o, fsproto.RmtreeCmd{Path: "/d", Base: fsproto.At(1)}))
		if r.Version != 5 {
			t.Fatalf("result %+v", r)
		}
		e.missing("/d")
		e.missing("/d/e/f")
		e.stat("/dx")
		if evs := e.log(4); len(evs) != 1 || evs[0].Op != fsproto.OpDel || evs[0].Path != "/d" {
			t.Fatalf("rmtree logged %+v, want one del", evs)
		}
	})

	t.Run("Mv", func(t *testing.T) {
		e := newMetaEnv(t, newMeta)
		e.mkdir("/a")        // 1
		e.mkdir("/a/b")      // 2
		e.put("/a/b/f", "x") // 3
		e.mkdir("/z")        // 4
		_, err := e.w.Mv(ctx, e.o, fsproto.MvCmd{From: "/nope", To: "/q"})
		wantStatus(t, "mv missing", err, fsproto.ErrMissing)
		_, err = e.w.Mv(ctx, e.o, fsproto.MvCmd{From: "/a", To: "/z/a", Base: fsproto.At(9)})
		wantConflict(t, "mv wrong base", err, 1)
		_, err = e.w.Mv(ctx, e.o, fsproto.MvCmd{From: "/a", To: "/a/b/c"})
		wantStatus(t, "mv into itself", err, fsproto.ErrInvalid)
		_, err = e.w.Mv(ctx, e.o, fsproto.MvCmd{From: "/a", To: "/q/r"})
		wantStatus(t, "mv without parent", err, fsproto.ErrMissing)
		r := must[server.Result](t, "mv dir")(e.w.Mv(ctx, e.o, fsproto.MvCmd{From: "/a", To: "/z/a", Base: fsproto.At(1), ToBase: fsproto.At(0)}))
		if r.Version != 5 || r.Event.Op != fsproto.OpMv || r.Event.Path != "/a" || r.Event.To != "/z/a" {
			t.Fatalf("result %+v", r)
		}
		e.missing("/a")
		e.missing("/a/b/f")
		if got := e.stat("/z/a"); got.Version != 5 {
			t.Fatalf("moved dir version %d, want 5", got.Version)
		}
		if got := e.stat("/z/a/b/f"); got.Version != 3 {
			t.Fatalf("child version %d, want 3 (unchanged)", got.Version)
		}
		if readAll(t, e.st, e.scope.Root, "/z/a/b/f") != "x" {
			t.Fatal("content did not move")
		}
		if evs := e.log(4); len(evs) != 1 {
			t.Fatalf("dir mv logged %d records, want 1", len(evs))
		}
		// Destination base.
		e.put("/f1", "1") // 6
		e.put("/f2", "2") // 7
		_, err = e.w.Mv(ctx, e.o, fsproto.MvCmd{From: "/f1", To: "/f2", ToBase: fsproto.At(0)})
		wantConflict(t, "mv onto existing with tobase 0", err, 7)
		r = must[server.Result](t, "mv replace")(e.w.Mv(ctx, e.o, fsproto.MvCmd{From: "/f1", To: "/f2", ToBase: fsproto.At(7)}))
		if got := e.stat("/f2"); got.Version != r.Version || readAll(t, e.st, e.scope.Root, "/f2") != "1" {
			t.Fatalf("replaced entry %+v", got)
		}
		_, err = e.w.Mv(ctx, e.o, fsproto.MvCmd{From: "/f2", To: "/z"})
		wantStatus(t, "mv file over dir", err, fsproto.ErrInvalid)
	})

	t.Run("Cp", func(t *testing.T) {
		e := newMetaEnv(t, newMeta)
		src := e.put("/a", "data")
		e.mkdir("/d")
		r := must[server.Result](t, "cp")(e.w.Cp(ctx, e.o, fsproto.CpCmd{From: "/a", To: "/d/b", ToBase: fsproto.At(0)}))
		got := e.stat("/d/b")
		if r.Version != 3 || got.Version != 3 || got.Hash != src.Hash || got.Length != 4 {
			t.Fatalf("cp result %+v entry %+v", r, got)
		}
		if readAll(t, e.st, e.scope.Root, "/d/b") != "data" || readAll(t, e.st, e.scope.Root, "/a") != "data" {
			t.Fatal("cp content")
		}
		_, err := e.w.Cp(ctx, e.o, fsproto.CpCmd{From: "/a", To: "/d/b", ToBase: fsproto.At(0)})
		wantConflict(t, "cp onto existing", err, 3)
		_, err = e.w.Cp(ctx, e.o, fsproto.CpCmd{From: "/nope", To: "/c"})
		wantStatus(t, "cp missing", err, fsproto.ErrMissing)
		_, err = e.w.Cp(ctx, e.o, fsproto.CpCmd{From: "/d", To: "/e"})
		wantStatus(t, "cp dir", err, fsproto.ErrInvalid)
		if evs := e.log(2); len(evs) != 1 || evs[0].Op != fsproto.OpPut || evs[0].Entry.Path != "/d/b" {
			t.Fatalf("cp logged %+v", evs)
		}
	})

	t.Run("Flags", func(t *testing.T) {
		e := newMetaEnv(t, newMeta)
		e.mkdir("/d")
		e.put("/f", "x")
		r := must[server.Result](t, "flags")(e.w.SetFlags(ctx, e.o, fsproto.FlagsCmd{Path: "/d", Flags: "pi"}))
		if got := e.stat("/d"); got.Flags != "ip" || got.Version != r.Version || r.Version != 3 {
			t.Fatalf("entry %+v result %+v", got, r)
		}
		if r.Event.Op != fsproto.OpFlags || r.Event.Flags != "ip" {
			t.Fatalf("event %+v", r.Event)
		}
		_, err := e.w.SetFlags(ctx, e.o, fsproto.FlagsCmd{Path: "/nope", Flags: "p"})
		wantStatus(t, "flags missing", err, fsproto.ErrMissing)
		_, err = e.w.SetFlags(ctx, e.o, fsproto.FlagsCmd{Path: "/f", Flags: "i"})
		wantStatus(t, "flag i on a file", err, fsproto.ErrInvalid)
		must[server.Result](t, "clear flags")(e.w.SetFlags(ctx, e.o, fsproto.FlagsCmd{Path: "/d"}))
		if e.stat("/d").Flags != "" {
			t.Fatal("flags not cleared")
		}
	})

	t.Run("SymlinkAndSetAttr", func(t *testing.T) {
		e := newMetaEnv(t, newMeta)
		r := must[server.Result](t, "symlink")(e.w.Symlink(ctx, e.o, "/l", "a b", fsproto.At(0)))
		if got := e.stat("/l"); got.Type != fsproto.TypeSymlink || got.Target != "a b" || got.Length != 3 || got.Version != r.Version {
			t.Fatalf("symlink entry %+v", got)
		}
		_, err := e.w.Symlink(ctx, e.o, "/l", "x", fsproto.At(0))
		wantConflict(t, "symlink existing", err, 1)
		e.put("/f", "x")
		mode, mtime := uint32(0o600), int64(42)
		r = must[server.Result](t, "setattr")(e.w.SetAttr(ctx, e.o, "/f", &mode, &mtime))
		got := e.stat("/f")
		if got.Mode != 0o600 || got.Mtime != 42 || got.Version != r.Version || r.Event.Op != fsproto.OpPut {
			t.Fatalf("setattr entry %+v result %+v", got, r)
		}
		r = must[server.Result](t, "setattr mode only")(e.w.SetAttr(ctx, e.o, "/f", nil, nil))
		if e.stat("/f").Mtime != 42 {
			t.Fatal("nil mtime changed mtime")
		}
		_, err = e.w.SetAttr(ctx, e.o, "/nope", &mode, nil)
		wantStatus(t, "setattr missing", err, fsproto.ErrMissing)
	})

	t.Run("ReadDir", func(t *testing.T) {
		e := newMetaEnv(t, newMeta)
		e.mkdir("/d")
		e.put("/d/b", "x")
		e.put("/d/a", "x")
		e.mkdir("/d/c")
		e.put("/d/c/deep", "x")
		e.put("/top", "x")
		es := must[[]fsproto.Entry](t, "ReadDir")(e.m.ReadDir(ctx, e.scope, "/d"))
		var names []string
		for _, x := range es {
			names = append(names, x.Path)
		}
		if !reflect.DeepEqual(names, []string{"/d/a", "/d/b", "/d/c"}) {
			t.Fatalf("ReadDir /d = %v", names)
		}
		es = must[[]fsproto.Entry](t, "ReadDir /")(e.m.ReadDir(ctx, e.scope, "/"))
		if len(es) != 2 {
			t.Fatalf("ReadDir / = %+v", es)
		}
		_, err := e.m.ReadDir(ctx, e.scope, "/nope")
		wantStatus(t, "ReadDir missing", err, fsproto.ErrMissing)
	})

	t.Run("LogAndEvents", func(t *testing.T) {
		e := newMetaEnv(t, newMeta)
		var results []server.Result
		results = append(results, e.mkdir("/d"))
		results = append(results, e.put("/d/f", "x"))
		results = append(results, must[server.Result](t, "mv")(e.w.Mv(ctx, e.o, fsproto.MvCmd{From: "/d/f", To: "/g"})))
		results = append(results, must[server.Result](t, "flags")(e.w.SetFlags(ctx, e.o, fsproto.FlagsCmd{Path: "/g", Flags: "p"})))
		results = append(results, must[server.Result](t, "rm")(e.w.Rm(ctx, e.o, fsproto.RmCmd{Path: "/g"})))
		logged := e.log(0)
		published := e.rec.Get(e.scope)
		if len(logged) != 5 || len(published) != 5 {
			t.Fatalf("logged %d, published %d, want 5", len(logged), len(published))
		}
		for i, r := range results {
			if r.Version != uint64(i+1) || r.Event.Seq != r.Version || r.Event.Dev != "dev-a" {
				t.Fatalf("result %d: %+v", i, r)
			}
			if !reflect.DeepEqual(logged[i], r.Event) || !reflect.DeepEqual(published[i], r.Event) {
				t.Fatalf("change %d:\nresult    %+v\nlogged    %+v\npublished %+v", i, r.Event, logged[i], published[i])
			}
		}
		if got := e.log(5); len(got) != 0 {
			t.Fatalf("ReadLog at current: %+v", got)
		}
		if got := e.log(3); len(got) != 2 || got[0].Seq != 4 {
			t.Fatalf("ReadLog since 3: %+v", got)
		}
		err := e.m.ReadLog(ctx, e.scope, 6, func(fsproto.Event) error { return nil })
		if err != server.ErrTrimmed {
			t.Fatalf("ReadLog past current: %v", err)
		}
		check(t, "Trim", e.m.Trim(ctx, e.scope, 3))
		err = e.m.ReadLog(ctx, e.scope, 2, func(fsproto.Event) error { return nil })
		if err != server.ErrTrimmed {
			t.Fatalf("ReadLog before trim point: %v", err)
		}
		if got := e.log(3); len(got) != 2 {
			t.Fatalf("ReadLog at trim point: %+v", got)
		}
		// Each record encodes and parses back unchanged.
		for _, ev := range logged {
			back, err := fsproto.ParseEvent(ev.Encode())
			if err != nil || !reflect.DeepEqual(back, ev) {
				t.Fatalf("record %q does not round-trip: %+v %v", ev.Encode(), back, err)
			}
		}
	})

	t.Run("Index", func(t *testing.T) {
		e := newMetaEnv(t, newMeta)
		e.mkdir("/b")
		e.put("/b/x y", "1")
		e.put("/a", "2")
		must[server.Result](t, "symlink")(e.w.Symlink(ctx, e.o, "/c", "b/x y", fsproto.Any))
		var es []fsproto.Entry
		seq := must[uint64](t, "Entries")(e.m.Entries(ctx, e.scope, func(x fsproto.Entry) error {
			es = append(es, x)
			return nil
		}))
		if seq != 4 || len(es) != 5 || es[0].Path != "/" {
			t.Fatalf("Entries seq %d: %+v", seq, es)
		}
		for i := 1; i < len(es); i++ {
			if fsproto.ComparePaths(es[i-1].Path, es[i].Path) >= 0 {
				t.Fatalf("Entries out of order: %q then %q", es[i-1].Path, es[i].Path)
			}
			if !reflect.DeepEqual(es[i], e.stat(es[i].Path)) {
				t.Fatalf("Entries and Stat disagree on %q", es[i].Path)
			}
		}
		rc, info, err := e.m.OpenIndex(ctx, e.scope, nil)
		check(t, "OpenIndex", err)
		raw, _ := io.ReadAll(rc)
		rc.Close()
		if info.Seq != 4 || info.Encoding != "" || (info.Size >= 0 && info.Size != int64(len(raw))) {
			t.Fatalf("IndexInfo %+v for %d bytes", info, len(raw))
		}
		ir, err := fsproto.NewIndexReader(bytes.NewReader(raw), nil)
		check(t, "NewIndexReader", err)
		if ir.Header().Seq != 4 {
			t.Fatalf("header %+v", ir.Header())
		}
		for i := 0; ; i++ {
			x, err := ir.Next()
			if err == io.EOF {
				if i != len(es) {
					t.Fatalf("index has %d entries, want %d", i, len(es))
				}
				break
			}
			check(t, "index Next", err)
			if !reflect.DeepEqual(x, es[i]) {
				t.Fatalf("index entry %d\n got %+v\nwant %+v", i, x, es[i])
			}
		}
	})

	t.Run("ConcurrentWriters", func(t *testing.T) {
		e := newMetaEnv(t, newMeta)
		const workers, each = 8, 25
		var wg sync.WaitGroup
		errs := make(chan error, workers*each)
		for i := 0; i < workers; i++ {
			for j := 0; j < each; j++ {
				name := fmt.Sprintf("s-%d-%d", i, j)
				stage(t, e.st, "alice", name, []byte(name))
			}
		}
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				w := must[server.Writer](t, "Writer")(e.m.Writer(e.scope))
				for j := 0; j < each; j++ {
					name := fmt.Sprintf("s-%d-%d", i, j)
					_, err := w.Commit(ctx, e.o, fsproto.CommitCmd{Stage: name, Path: "/" + name, Base: fsproto.At(0), Mode: 0o644})
					errs <- err
				}
			}(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			check(t, "concurrent commit", err)
		}
		const total = workers * each
		if e.seq() != total {
			t.Fatalf("seq %d, want %d", e.seq(), total)
		}
		for i, ev := range e.log(0) {
			if ev.Seq != uint64(i+1) {
				t.Fatalf("log record %d has seq %d", i, ev.Seq)
			}
		}
		for i, ev := range e.rec.Get(e.scope) {
			if ev.Seq != uint64(i+1) {
				t.Fatalf("published record %d has seq %d", i, ev.Seq)
			}
		}
	})
}

// TestTokens runs the Tokens behavior tests.
func TestTokens(t *testing.T, newTokens NewTokens) {
	tk := newTokens(t)
	a := must[string](t, "CreateToken")(tk.CreateToken(ctx, "alice"))
	b := must[string](t, "CreateToken")(tk.CreateToken(ctx, "alice"))
	if a == b || len(a) < 16 {
		t.Fatalf("weak or repeated tokens %q %q", a, b)
	}
	if u := must[string](t, "LookupToken")(tk.LookupToken(ctx, a)); u != "alice" {
		t.Fatalf("LookupToken = %q", u)
	}
	_, err := tk.LookupToken(ctx, "bogus")
	wantStatus(t, "LookupToken bogus", err, fsproto.ErrDenied)
}
