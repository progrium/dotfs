package servertest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/fake"
)

// hubEnv wires a Hub under test to a fake Meta, which logs and publishes.
type hubEnv struct {
	t     *testing.T
	m     *fake.Meta
	st    *fake.Store
	hub   server.Hub
	scope server.Scope
	w     server.Writer
	n     int
}

func newHubEnv(t *testing.T, newHub NewHub, queueLen int) *hubEnv {
	st := fake.NewStore()
	m := fake.NewMeta(st, nil)
	h := newHub(t, m, queueLen)
	m.SetPublisher(h)
	e := &hubEnv{t: t, m: m, st: st, hub: h}
	e.scope = server.RootScope(server.NewRoot("alice", "home"))
	check(t, "CreateScope", m.CreateScope(ctx, e.scope))
	e.w = must[server.Writer](t, "Writer")(m.Writer(e.scope))
	return e
}

// change makes one change (a mkdir) at p.
func (e *hubEnv) change(p string) uint64 {
	e.t.Helper()
	r, err := e.w.Mkdir(ctx, server.Origin{User: "alice", Dev: "d"}, fsproto.MkdirCmd{Path: p})
	check(e.t, "mkdir", err)
	return r.Version
}

func (e *hubEnv) changes(n int) {
	for i := 0; i < n; i++ {
		e.n++
		e.change(fmt.Sprintf("/c%d", e.n))
	}
}

func (e *hubEnv) subscribe(setup fsproto.EventsSetup) server.Stream {
	e.t.Helper()
	s, err := e.hub.Subscribe(ctx, e.scope, setup)
	check(e.t, "Subscribe", err)
	e.t.Cleanup(func() { s.Close() })
	return s
}

// collect reads records until it has n or a reset, failing after a timeout.
func collect(t *testing.T, s server.Stream, n int) []fsproto.Event {
	t.Helper()
	var out []fsproto.Event
	for len(out) < n {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		evs, err := s.Next(c)
		cancel()
		if err != nil {
			t.Fatalf("Next after %d records: %v", len(out), err)
		}
		if len(evs) == 0 {
			t.Fatal("Next returned no records and no error")
		}
		out = append(out, evs...)
		if evs[len(evs)-1].Op == fsproto.OpReset {
			break
		}
	}
	return out
}

// noMore checks that Next blocks.
func noMore(t *testing.T, s server.Stream) {
	t.Helper()
	c, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	evs, err := s.Next(c)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Next = %+v, %v; want it to block", evs, err)
	}
}

func wantSeqs(t *testing.T, evs []fsproto.Event, from, to uint64) {
	t.Helper()
	if uint64(len(evs)) != to-from+1 {
		t.Fatalf("got %d records, want seqs %d..%d", len(evs), from, to)
	}
	for i, ev := range evs {
		if ev.Seq != from+uint64(i) || ev.Op == fsproto.OpReset || ev.Op == fsproto.OpSeq {
			t.Fatalf("record %d is %+v, want seq %d", i, ev, from+uint64(i))
		}
	}
}

// TestHub runs the Hub behavior tests.
func TestHub(t *testing.T, newHub NewHub) {
	t.Run("Live", func(t *testing.T) {
		e := newHubEnv(t, newHub, 100)
		e.changes(3)
		s := e.subscribe(fsproto.EventsSetup{})
		first := collect(t, s, 1)
		if first[0].Op != fsproto.OpSeq || first[0].Seq != 3 {
			t.Fatalf("first record %+v, want seq 3", first[0])
		}
		noMore(t, s)
		e.changes(2)
		wantSeqs(t, collect(t, s, 2), 4, 5)
	})

	t.Run("Since", func(t *testing.T) {
		e := newHubEnv(t, newHub, 100)
		e.changes(5)
		s := e.subscribe(fsproto.EventsSetup{Since: 2, HasSince: true})
		wantSeqs(t, collect(t, s, 3), 3, 5)
		e.changes(2)
		wantSeqs(t, collect(t, s, 2), 6, 7)
		s0 := e.subscribe(fsproto.EventsSetup{Since: 7, HasSince: true})
		noMore(t, s0)
	})

	t.Run("SubscribeDuringWrites", func(t *testing.T) {
		e := newHubEnv(t, newHub, 10000)
		const total = 500
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.changes(total)
		}()
		time.Sleep(time.Millisecond)
		s := e.subscribe(fsproto.EventsSetup{Since: 0, HasSince: true})
		wantSeqs(t, collect(t, s, total), 1, total)
		wg.Wait()
		noMore(t, s)
	})

	t.Run("PathFilter", func(t *testing.T) {
		e := newHubEnv(t, newHub, 100)
		e.change("/a")
		e.change("/b")
		s := e.subscribe(fsproto.EventsSetup{Since: 0, HasSince: true, Paths: []string{"/a"}})
		e.change("/a/x")
		e.change("/b/x")
		e.change("/ab")
		evs := collect(t, s, 2)
		if len(evs) != 2 || evs[0].Entry.Path != "/a" || evs[1].Entry.Path != "/a/x" {
			t.Fatalf("filtered records %+v", evs)
		}
		noMore(t, s)
		// A move out of the filtered directory is reported.
		_, err := e.w.Mv(ctx, server.Origin{User: "alice"}, fsproto.MvCmd{From: "/a/x", To: "/b/y"})
		check(t, "mv", err)
		if evs := collect(t, s, 1); evs[0].Op != fsproto.OpMv {
			t.Fatalf("mv record %+v", evs)
		}
	})

	t.Run("SlowReaderGetsReset", func(t *testing.T) {
		const queueLen = 16
		e := newHubEnv(t, newHub, queueLen)
		slow := e.subscribe(fsproto.EventsSetup{Since: 0, HasSince: true})
		fast := e.subscribe(fsproto.EventsSetup{Since: 0, HasSince: true})
		// The writer goes in half-queue batches and the fast reader catches
		// up after each, while the slow reader never reads.
		start := time.Now()
		var got []fsproto.Event
		for len(got) < queueLen*8 {
			e.changes(queueLen / 2)
			got = append(got, collect(t, fast, queueLen/2)...)
		}
		if d := time.Since(start); d > 10*time.Second {
			t.Fatalf("writers took %v with a stalled stream", d)
		}
		wantSeqs(t, got, 1, queueLen*8)
		evs := collect(t, slow, queueLen*8+1)
		if last := evs[len(evs)-1]; last.Op != fsproto.OpReset {
			t.Fatalf("slow reader's last record %+v, want reset", last)
		}
		if len(evs) > queueLen*8 {
			t.Fatalf("slow reader got all %d records and a reset", len(evs))
		}
		if _, err := slow.Next(ctx); err != io.EOF {
			t.Fatalf("Next after reset: %v, want io.EOF", err)
		}
	})

	t.Run("TrimmedSinceResets", func(t *testing.T) {
		e := newHubEnv(t, newHub, 100)
		e.changes(10)
		check(t, "Trim", e.m.Trim(ctx, e.scope, 5))
		s := e.subscribe(fsproto.EventsSetup{Since: 2, HasSince: true})
		evs := collect(t, s, 1)
		if len(evs) != 1 || evs[0].Op != fsproto.OpReset || evs[0].Seq != 10 {
			t.Fatalf("records %+v, want one reset at 10", evs)
		}
		if _, err := s.Next(ctx); err != io.EOF {
			t.Fatalf("Next after reset: %v", err)
		}
		// A since the log still covers works.
		s2 := e.subscribe(fsproto.EventsSetup{Since: 5, HasSince: true})
		wantSeqs(t, collect(t, s2, 5), 6, 10)
		// A since in the future is answered with reset too.
		s3 := e.subscribe(fsproto.EventsSetup{Since: 99, HasSince: true})
		if evs := collect(t, s3, 1); evs[0].Op != fsproto.OpReset {
			t.Fatalf("future since: %+v", evs)
		}
	})

	t.Run("FlushLeavesStreamUsable", func(t *testing.T) {
		e := newHubEnv(t, newHub, 100)
		s := e.subscribe(fsproto.EventsSetup{Since: 0, HasSince: true})
		c, cancel := context.WithCancel(ctx)
		errc := make(chan error, 1)
		go func() {
			_, err := s.Next(c)
			errc <- err
		}()
		time.Sleep(10 * time.Millisecond)
		cancel()
		select {
		case err := <-errc:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled Next: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Next ignored cancellation")
		}
		e.changes(1)
		wantSeqs(t, collect(t, s, 1), 1, 1)
	})

	t.Run("Coalesce", func(t *testing.T) {
		// A Hub may merge records with coalesce, or not. Either way the
		// records arrive in increasing seq order, and applying them to the
		// empty tree gives the scope's tree at the seq last read. A reader
		// that reads in rounds, while changes continue, checks that at
		// every round.
		e := newHubEnv(t, newHub, 100000)
		co := e.subscribe(fsproto.EventsSetup{Since: 0, HasSince: true, Coalesce: true})
		all := e.subscribe(fsproto.EventsSetup{Since: 0, HasSince: true})
		got := map[string]fsproto.Entry{}
		var last, records uint64
		for round := 0; round < 60; round++ {
			hubWorkload(e, round, 40)
			cur := must[uint64](t, "Seq")(e.m.Seq(ctx, e.scope))
			if cur == last {
				continue
			}
			want := map[string]fsproto.Entry{}
			_ = must[uint64](t, "Entries")(e.m.Entries(ctx, e.scope, func(en fsproto.Entry) error {
				if en.Path != "/" {
					want[en.Path] = en
				}
				return nil
			}))
			wantSeqs(t, collectTo(t, all, cur), last+1, cur)
			evs := collectTo(t, co, cur)
			for i, ev := range evs {
				if ev.Seq <= last {
					t.Fatalf("round %d: record %d has seq %d after %d", round, i, ev.Seq, last)
				}
				last = ev.Seq
				if err := applyEvent(got, ev); err != nil {
					t.Fatalf("round %d: %v", round, err)
				}
			}
			records += uint64(len(evs))
			if g, w := treeString(got), treeString(want); g != w {
				t.Fatalf("round %d: records up to %d give\n%s\nwant\n%s", round, cur, g, w)
			}
		}
		t.Logf("%d records with coalesce for %d changes", records, last)
	})

	t.Run("Close", func(t *testing.T) {
		e := newHubEnv(t, newHub, 100)
		s := e.subscribe(fsproto.EventsSetup{})
		collect(t, s, 1)
		check(t, "Close", s.Close())
		e.changes(3) // publishing to a closed stream must not block or panic
		if _, err := s.Next(ctx); err == nil {
			t.Fatal("Next on a closed stream succeeded")
		}
	})
}

// hubWorkload makes n pseudo-random changes, varying with seed, over a few paths: files written
// and rewritten, directories made, flagged, moved, and removed. Changes that
// fail (a missing parent, a non-empty directory) are skipped.
func hubWorkload(e *hubEnv, seed, n int) {
	names := []string{"/a", "/b", "/c", "/a/x", "/a/y", "/b/x", "/b/y", "/a/x/z", "/b/x/z", "/f"}
	x := uint32(seed)*2654435761 + 1
	rnd := func(k int) int {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		return int(x % uint32(k))
	}
	o := server.Origin{User: "alice", Dev: "d"}
	if seed == 0 {
		// Cases a careless coalesce gets wrong: a directory removed after
		// children were added, a path reused after its parent was removed,
		// and a path reused after a move.
		e.w.Mkdir(ctx, o, fsproto.MkdirCmd{Path: "/a"})
		e.w.Mkdir(ctx, o, fsproto.MkdirCmd{Path: "/a/x"})
		e.w.Rmtree(ctx, o, fsproto.RmtreeCmd{Path: "/a"})
		e.w.Mkdir(ctx, o, fsproto.MkdirCmd{Path: "/b"})
		e.w.Mkdir(ctx, o, fsproto.MkdirCmd{Path: "/b/x"})
		e.w.Rmtree(ctx, o, fsproto.RmtreeCmd{Path: "/b"})
		e.w.Mkdir(ctx, o, fsproto.MkdirCmd{Path: "/b"})
		e.w.Mkdir(ctx, o, fsproto.MkdirCmd{Path: "/b/x"})
		e.w.SetFlags(ctx, o, fsproto.FlagsCmd{Path: "/b/x", Flags: "p"})
		e.w.Mv(ctx, o, fsproto.MvCmd{From: "/b", To: "/c"})
		e.w.Mkdir(ctx, o, fsproto.MkdirCmd{Path: "/b"})
		e.w.Mkdir(ctx, o, fsproto.MkdirCmd{Path: "/b/x"})
		e.w.SetFlags(ctx, o, fsproto.FlagsCmd{Path: "/b/x", Flags: "p"})
	}
	for i := 0; i < n; i++ {
		p := names[rnd(len(names))]
		switch op := rnd(11); {
		case op < 3:
			e.w.Mkdir(ctx, o, fsproto.MkdirCmd{Path: p})
		case op < 6:
			e.n++
			name := fmt.Sprintf("co-%d", e.n)
			stage(e.t, e.st, "alice", name, []byte(name))
			e.w.Commit(ctx, o, fsproto.CommitCmd{Stage: name, Path: p, Base: fsproto.Any, Mode: 0o644})
			e.st.RemoveStage("alice", name)
		case op < 8:
			e.w.SetFlags(ctx, o, fsproto.FlagsCmd{Path: p, Flags: fsproto.Flags([]string{"", "p"}[rnd(2)])})
		case op < 9:
			e.w.Rm(ctx, o, fsproto.RmCmd{Path: p})
		case op < 10:
			e.w.Rmtree(ctx, o, fsproto.RmtreeCmd{Path: p})
		default:
			e.w.Mv(ctx, o, fsproto.MvCmd{From: p, To: names[rnd(len(names))]})
		}
	}
}

// collectTo reads records until one has seq at least to.
func collectTo(t *testing.T, s server.Stream, to uint64) []fsproto.Event {
	t.Helper()
	var out []fsproto.Event
	for len(out) == 0 || out[len(out)-1].Seq < to {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		evs, err := s.Next(c)
		cancel()
		if err != nil {
			t.Fatalf("Next after %d records: %v", len(out), err)
		}
		for _, ev := range evs {
			if ev.Op == fsproto.OpReset || ev.Op == fsproto.OpSeq {
				t.Fatalf("unexpected record %+v", ev)
			}
		}
		out = append(out, evs...)
	}
	return out
}

// applyEvent applies a record to a tree of entries keyed by path. It fails
// on a record the tree is not ready for, such as a put whose parent does not
// exist, which a reordering coalesce would cause.
func applyEvent(tree map[string]fsproto.Entry, ev fsproto.Event) error {
	isDir := func(p string) bool {
		en, ok := tree[p]
		return p == "/" || ok && en.Type == fsproto.TypeDir
	}
	switch ev.Op {
	case fsproto.OpPut:
		if !isDir(path.Dir(ev.Entry.Path)) {
			return fmt.Errorf("%d put %s without its parent", ev.Seq, ev.Entry.Path)
		}
		tree[ev.Entry.Path] = ev.Entry
	case fsproto.OpDel:
		for p := range tree {
			if fsproto.Within(p, ev.Path) {
				delete(tree, p)
			}
		}
	case fsproto.OpMv:
		if _, ok := tree[ev.Path]; !ok {
			return fmt.Errorf("%d mv of missing %s", ev.Seq, ev.Path)
		}
		if !isDir(path.Dir(ev.To)) {
			return fmt.Errorf("%d mv to %s without its parent", ev.Seq, ev.To)
		}
		for p := range tree {
			if fsproto.Within(p, ev.To) {
				delete(tree, p)
			}
		}
		moved := map[string]fsproto.Entry{}
		for p, en := range tree {
			if fsproto.Within(p, ev.Path) {
				delete(tree, p)
				en.Path = fsproto.Rebase(p, ev.Path, ev.To)
				moved[en.Path] = en
			}
		}
		for p, en := range moved {
			tree[p] = en
		}
	case fsproto.OpFlags:
		en, ok := tree[ev.Path]
		if !ok {
			return fmt.Errorf("%d flags of missing %s", ev.Seq, ev.Path)
		}
		en.Flags = ev.Flags
		tree[ev.Path] = en
	}
	return nil
}

// treeString lists each path's type, hash, and flags, sorted. Versions are
// left out: a flags record does not carry the path's new version.
func treeString(tree map[string]fsproto.Entry) string {
	var lines []string
	for p, en := range tree {
		lines = append(lines, fmt.Sprintf("%s %v %s %s", p, en.Type, en.Hash, en.Flags.Encode()))
	}
	slices.Sort(lines)
	return strings.Join(lines, "\n")
}
