package servertest

import (
	"context"
	"errors"
	"fmt"
	"io"
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
