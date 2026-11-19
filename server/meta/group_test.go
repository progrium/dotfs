package meta

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/store"
)

// placeFails fails PlaceNoSync for one path.
type placeFails struct {
	*store.Store
	path string
}

func (f *placeFails) PlaceNoSync(p store.Prepared, r server.Root, path string) error {
	if path == f.path {
		return errors.New("disk on fire")
	}
	return f.Store.PlaceNoSync(p, r, path)
}

type pubFunc func(server.Scope, fsproto.Event)

func (f pubFunc) Publish(s server.Scope, ev fsproto.Event) { f(s, ev) }

func TestGroupCommit(t *testing.T) {
	dir := t.TempDir()
	st, err := store.New(filepath.Join(dir, "data"), &store.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	fst := &placeFails{Store: st}
	var seqs []uint64
	m, err := Open(filepath.Join(dir, "meta.db"), fst, pubFunc(func(_ server.Scope, ev fsproto.Event) { seqs = append(seqs, ev.Seq) }), &Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	killSetup(t, st, m)
	w, _ := m.Writer(killScope)
	commit := func(stage, p string, base uint64) func() (server.Result, error) {
		stageData(t, st, stage, stage+" content")
		return func() (server.Result, error) {
			return w.Commit(ctx, alice, fsproto.CommitCmd{Stage: stage, Path: p, Base: fsproto.At(base), Mode: 0o644, Mtime: 1})
		}
	}

	// A stale base fails alone; the rest get consecutive seqs, in order.
	seqs = nil
	rs := inGroup(t, w, []func() (server.Result, error){
		commit("a", "/e/a", 0),
		commit("b", "/g", 1), // /g is at 3
		commit("c", "/e/a", killSetupSeq+1),
		func() (server.Result, error) { return w.Mkdir(ctx, alice, fsproto.MkdirCmd{Path: "/e/a"}) },
		commit("d", "/e/d", 0),
	})
	want := []struct {
		v   uint64
		err error
	}{{6, nil}, {0, fsproto.ErrConflict}, {7, nil}, {0, fsproto.ErrConflict}, {8, nil}}
	for i, r := range rs {
		if r.res.Version != want[i].v || !errors.Is(r.err, want[i].err) && r.err != want[i].err {
			t.Fatalf("change %d: %+v %v, want %d %v", i, r.res, r.err, want[i].v, want[i].err)
		}
	}
	if len(seqs) != 3 || seqs[0] != 6 || seqs[2] != 8 {
		t.Fatalf("published %v", seqs)
	}
	if e, err := m.Stat(ctx, killScope, "/e/a"); err != nil || e.Version != 7 || e.Hash != fsproto.HashBytes([]byte("c content")) {
		t.Fatalf("/e/a: %+v %v", e, err)
	}
	checkConsistent(t, st, m, killScope)

	// A change that fails at its filesystem step is dropped with the ones
	// after it; the ones before it commit.
	fst.path = "/e/y"
	rs = inGroup(t, w, []func() (server.Result, error){
		commit("x", "/e/x", 0),
		commit("y", "/e/y", 0),
		commit("z", "/e/z", 0),
	})
	if rs[0].err != nil || rs[0].res.Version != 9 {
		t.Fatalf("first: %+v %v", rs[0].res, rs[0].err)
	}
	var dropped errGroupDropped
	if rs[1].err == nil || errors.As(rs[1].err, &dropped) || !errors.As(rs[2].err, &dropped) {
		t.Fatalf("failed and dropped: %v; %v", rs[1].err, rs[2].err)
	}
	fst.path = ""
	checkConsistent(t, st, m, killScope)
	if r, err := w.Mkdir(ctx, alice, fsproto.MkdirCmd{Path: "/after"}); err != nil || r.Version != 10 {
		t.Fatalf("next change: %+v %v", r, err)
	}
}
