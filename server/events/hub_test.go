package events_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/events"
	"github.com/progrium/dotfs/server/fake"
)

var ctx = context.Background()

// env is a fake Meta publishing to an events.Hub.
type env struct {
	t     *testing.T
	st    *fake.Store
	n     int
	m     *fake.Meta
	hub   *events.Hub
	scope server.Scope
	w     server.Writer
}

func newEnv(t *testing.T, opts events.Options) *env {
	t.Helper()
	st := fake.NewStore()
	m := fake.NewMeta(st, nil)
	h := events.New(m, opts)
	m.SetPublisher(h)
	e := &env{t: t, st: st, m: m, hub: h, scope: server.RootScope(server.NewRoot("alice", "home"))}
	if err := m.CreateScope(ctx, e.scope); err != nil {
		t.Fatal(err)
	}
	w, err := m.Writer(e.scope)
	if err != nil {
		t.Fatal(err)
	}
	e.w = w
	return e
}

var origin = server.Origin{User: "alice", Dev: "d"}

func (e *env) mkdir(p string) {
	e.t.Helper()
	if _, err := e.w.Mkdir(ctx, origin, fsproto.MkdirCmd{Path: p}); err != nil {
		e.t.Fatalf("mkdir %s: %v", p, err)
	}
}

// write commits data at p, unconditionally.
func (e *env) write(p, data string) {
	e.t.Helper()
	e.n++
	name := fmt.Sprintf("stage-%d", e.n)
	f, err := e.st.OpenStage("alice", name, true)
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte(data), 0); err != nil {
		e.t.Fatal(err)
	}
	f.Close()
	if _, err := e.w.Commit(ctx, origin, fsproto.CommitCmd{Stage: name, Path: p, Base: fsproto.Any, Mode: 0o644}); err != nil {
		e.t.Fatalf("commit %s: %v", p, err)
	}
}

func (e *env) flags(p string, f fsproto.Flags) {
	e.t.Helper()
	if _, err := e.w.SetFlags(ctx, origin, fsproto.FlagsCmd{Path: p, Flags: f}); err != nil {
		e.t.Fatalf("flags %s: %v", p, err)
	}
}

func (e *env) rm(p string) {
	e.t.Helper()
	if _, err := e.w.Rm(ctx, origin, fsproto.RmCmd{Path: p}); err != nil {
		e.t.Fatalf("rm %s: %v", p, err)
	}
}

func (e *env) mv(from, to string) {
	e.t.Helper()
	if _, err := e.w.Mv(ctx, origin, fsproto.MvCmd{From: from, To: to}); err != nil {
		e.t.Fatalf("mv %s %s: %v", from, to, err)
	}
}

func (e *env) subscribe(setup fsproto.EventsSetup) server.Stream {
	e.t.Helper()
	s, err := e.hub.Subscribe(ctx, e.scope, setup)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { s.Close() })
	return s
}

// drain reads until Next would block.
func drain(t *testing.T, s server.Stream) []fsproto.Event {
	t.Helper()
	var out []fsproto.Event
	for {
		c, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		evs, err := s.Next(c)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) || err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, evs...)
	}
}

func lines(evs []fsproto.Event) []string {
	var out []string
	for _, ev := range evs {
		s := fmt.Sprintf("%d %s %s", ev.Seq, ev.Op, ev.Target())
		if ev.Op == fsproto.OpMv {
			s += " " + ev.To
		}
		if ev.Op == fsproto.OpFlags {
			s += " " + string(ev.Flags)
		}
		out = append(out, s)
	}
	return out
}

func wantLines(t *testing.T, evs []fsproto.Event, want ...string) {
	t.Helper()
	got := lines(evs)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("records\n got %q\nwant %q", got, want)
	}
}

// staticLog is a Log that only knows its current seq, for driving Publish
// directly.
type staticLog struct{ seq uint64 }

func (l *staticLog) Seq(context.Context, server.Scope) (uint64, error) { return l.seq, nil }
func (l *staticLog) ReadLog(_ context.Context, _ server.Scope, since uint64, _ func(fsproto.Event) error) error {
	if since != l.seq {
		return server.ErrTrimmed
	}
	return nil
}
func (l *staticLog) Trim(context.Context, server.Scope, uint64) error { return nil }

func put(seq uint64, p string) fsproto.Event {
	return fsproto.Event{Seq: seq, Op: fsproto.OpPut, Entry: fsproto.Entry{Path: p, Type: fsproto.TypeFile, Version: seq}}
}

// One stalled stream must not slow publishers or hold more than its queue,
// and it ends with a reset at the seq that overflowed it.
func TestStalledStreamNeverBlocksPublish(t *testing.T) {
	const queueLen = 64
	const n = 200_000
	scope := server.RootScope("alice/home")
	h := events.New(&staticLog{}, events.Options{QueueLen: queueLen})
	stalled, err := h.Subscribe(ctx, scope, fsproto.EventsSetup{})
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.Close()
	fast, err := h.Subscribe(ctx, scope, fsproto.EventsSetup{})
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Close()

	var got atomic.Uint64
	done := make(chan error, 1)
	go func() {
		for got.Load() < n {
			evs, err := fast.Next(ctx)
			if err != nil {
				done <- err
				return
			}
			for _, ev := range evs {
				if ev.Op == fsproto.OpSeq {
					continue
				}
				if ev.Seq != got.Load()+1 {
					done <- fmt.Errorf("fast reader got %d %s after %d", ev.Seq, ev.Op, got.Load())
					return
				}
				got.Store(ev.Seq)
			}
		}
		done <- nil
	}()

	start := time.Now()
	for i := uint64(1); i <= n; i++ {
		h.Publish(scope, put(i, "/f"))
		if i%8 == 0 {
			// Keep the fast reader within its queue; the stalled one never reads.
			for i-got.Load() >= queueLen/2 {
				select {
				case err := <-done:
					t.Fatalf("fast reader stopped at %d: %v", got.Load(), err)
				default:
				}
				time.Sleep(10 * time.Microsecond)
			}
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	t.Logf("%d publishes with one stalled stream in %v", n, time.Since(start))

	evs, err := stalled.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	last := evs[len(evs)-1]
	if last.Op != fsproto.OpReset || last.Seq != queueLen+1 {
		t.Fatalf("stalled stream ends with %+v, want reset %d", last, queueLen+1)
	}
	if len(evs) != 1 {
		t.Fatalf("stalled stream kept %d records with its reset", len(evs)-1)
	}
	if _, err := stalled.Next(ctx); err != io.EOF {
		t.Fatalf("Next after reset: %v", err)
	}
}

// Streams opened at random points with random since values, with a ring and
// queue small enough to force every path (log, ring, join, and live), each see
// every seq after their start exactly once.
func TestNoGapsUnderConcurrency(t *testing.T) {
	for _, ringLen := range []int{1, 7, 4096} {
		t.Run(fmt.Sprint("ring", ringLen), func(t *testing.T) {
			e := newEnv(t, events.Options{QueueLen: 1 << 20, RingLen: ringLen})
			const total = 3000
			const readers = 40
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 1; i <= total; i++ {
					e.mkdir(fmt.Sprintf("/d%d", i))
				}
			}()
			errs := make(chan error, readers)
			for r := 0; r < readers; r++ {
				wg.Add(1)
				go func(r int) {
					defer wg.Done()
					time.Sleep(time.Duration(rand.IntN(3000)) * time.Microsecond)
					setup := fsproto.EventsSetup{}
					if r%4 != 0 {
						cur, _ := e.m.Seq(ctx, e.scope)
						setup.HasSince = true
						setup.Since = uint64(rand.IntN(int(cur) + 1))
					}
					s, err := e.hub.Subscribe(ctx, e.scope, setup)
					if err != nil {
						errs <- err
						return
					}
					defer s.Close()
					next := setup.Since + 1
					first := true
					for next <= total {
						c, cancel := context.WithTimeout(ctx, 10*time.Second)
						evs, err := s.Next(c)
						cancel()
						if err != nil {
							errs <- fmt.Errorf("reader %d at %d: %v", r, next, err)
							return
						}
						for _, ev := range evs {
							if first && !setup.HasSince {
								if ev.Op != fsproto.OpSeq {
									errs <- fmt.Errorf("reader %d: first record %+v", r, ev)
									return
								}
								next = ev.Seq + 1
								first = false
								continue
							}
							if ev.Seq != next || ev.Op != fsproto.OpPut {
								errs <- fmt.Errorf("reader %d: got %d %s, want %d", r, ev.Seq, ev.Op, next)
								return
							}
							next++
						}
					}
					errs <- nil
				}(r)
			}
			wg.Wait()
			for r := 0; r < readers; r++ {
				if err := <-errs; err != nil {
					t.Error(err)
				}
			}
		})
	}
}

// A stream far behind replays in bounded batches, never resets while it
// catches up from the log, and then goes live without a gap.
func TestDeepCatchUp(t *testing.T) {
	e := newEnv(t, events.Options{QueueLen: 16, RingLen: 32})
	for i := 1; i <= 1000; i++ {
		e.mkdir(fmt.Sprintf("/d%d", i))
	}
	s := e.subscribe(fsproto.EventsSetup{Since: 10, HasSince: true})
	var seqs []uint64
	for len(seqs) < 990 {
		evs, err := s.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(evs) > 16 {
			t.Fatalf("batch of %d records with QueueLen 16", len(evs))
		}
		for _, ev := range evs {
			seqs = append(seqs, ev.Seq)
		}
		// Writes continue while the stream is behind.
		if len(seqs)%100 < 16 {
			e.mkdir(fmt.Sprintf("/x%d", len(seqs)))
		}
	}
	cur, _ := e.m.Seq(ctx, e.scope)
	for uint64(len(seqs)) < cur-10 {
		evs, err := s.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, ev := range evs {
			seqs = append(seqs, ev.Seq)
		}
	}
	for i, seq := range seqs {
		if seq != uint64(11+i) {
			t.Fatalf("record %d has seq %d", i, seq)
		}
	}
	e.mkdir("/live")
	if evs := drain(t, s); len(evs) != 1 || evs[0].Seq != cur+1 {
		t.Fatalf("live records %v", lines(evs))
	}
}

func TestTrimWhileCatchingUp(t *testing.T) {
	e := newEnv(t, events.Options{QueueLen: 8, RingLen: 8})
	for i := 1; i <= 100; i++ {
		e.mkdir(fmt.Sprintf("/d%d", i))
	}
	s := e.subscribe(fsproto.EventsSetup{Since: 0, HasSince: true})
	evs, err := s.Next(ctx)
	if err != nil || len(evs) != 8 {
		t.Fatalf("first batch %v, %v", lines(evs), err)
	}
	if err := e.m.Trim(ctx, e.scope, 50); err != nil {
		t.Fatal(err)
	}
	evs, err = s.Next(ctx)
	if err != nil || len(evs) != 1 || evs[0].Op != fsproto.OpReset || evs[0].Seq != 100 {
		t.Fatalf("after trim %v, %v; want reset 100", lines(evs), err)
	}
	if _, err := s.Next(ctx); err != io.EOF {
		t.Fatalf("Next after reset: %v", err)
	}
}

func TestCoalesce(t *testing.T) {
	co := fsproto.EventsSetup{HasSince: true, Coalesce: true}

	t.Run("FlagsReplaceFlags", func(t *testing.T) {
		e := newEnv(t, events.Options{})
		e.mkdir("/a")
		s := e.subscribe(co)
		e.flags("/a", "p")
		e.flags("/a", "")
		e.flags("/a", "p")
		wantLines(t, drain(t, s), "1 put /a", "4 flags /a p")
	})

	t.Run("PutReplacesPutAndFlags", func(t *testing.T) {
		e := newEnv(t, events.Options{})
		e.write("/f", "1")
		e.write("/g", "1")
		e.flags("/f", "p")
		e.write("/g", "2")
		e.write("/f", "2")
		// The put of /f at 5 replaces the flags at 3, which could not replace
		// the put at 1.
		wantLines(t, drain(t, e.subscribe(co)), "1 put /f", "4 put /g", "5 put /f")
	})

	t.Run("DelReplacesPutAndIsABarrier", func(t *testing.T) {
		e := newEnv(t, events.Options{})
		e.write("/f", "1")
		e.write("/g", "1")
		e.rm("/g")
		e.write("/f", "2")
		e.write("/g", "2")
		wantLines(t, drain(t, e.subscribe(co)), "1 put /f", "3 del /g", "4 put /f", "5 put /g")
	})

	t.Run("DelReplacesFlags", func(t *testing.T) {
		e := newEnv(t, events.Options{})
		e.mkdir("/a")
		e.mkdir("/b")
		s := e.subscribe(co)
		e.flags("/a", "p")
		e.flags("/b", "p")
		e.flags("/a", "")
		e.rm("/a")
		wantLines(t, drain(t, s), "1 put /a", "2 put /b", "4 flags /b p", "6 del /a")
	})

	t.Run("ShapeChangesAreBarriers", func(t *testing.T) {
		e := newEnv(t, events.Options{})
		e.mkdir("/a")
		e.mkdir("/a/x")
		s := e.subscribe(co)
		e.flags("/a/x", "p")
		e.mv("/a", "/b")
		e.mkdir("/a")
		e.mkdir("/a/x")
		e.flags("/a/x", "p")
		e.flags("/a/x", "")
		wantLines(t, drain(t, s),
			"1 put /a", "2 put /a/x", "3 flags /a/x p", "4 mv /a /b", "5 put /a", "6 put /a/x", "8 flags /a/x ")
	})

	t.Run("NotWithoutCoalesce", func(t *testing.T) {
		e := newEnv(t, events.Options{})
		e.mkdir("/a")
		s := e.subscribe(fsproto.EventsSetup{HasSince: true})
		e.flags("/a", "p")
		e.flags("/a", "")
		wantLines(t, drain(t, s), "1 put /a", "2 flags /a p", "3 flags /a ")
	})

	t.Run("KeepsSlowReaderAlive", func(t *testing.T) {
		e := newEnv(t, events.Options{QueueLen: 4})
		e.mkdir("/a")
		s := e.subscribe(fsproto.EventsSetup{Coalesce: true})
		for i := 0; i < 100; i++ {
			e.flags("/a", fsproto.Flags([]string{"p", ""}[i%2]))
		}
		wantLines(t, drain(t, s), "1 seq ", "101 flags /a ")
	})
}

func TestCloseWakesNext(t *testing.T) {
	e := newEnv(t, events.Options{})
	s := e.subscribe(fsproto.EventsSetup{HasSince: true})
	errc := make(chan error, 1)
	go func() {
		_, err := s.Next(ctx)
		errc <- err
	}()
	time.Sleep(10 * time.Millisecond)
	s.Close()
	select {
	case err := <-errc:
		if !errors.Is(err, events.ErrClosed) {
			t.Fatalf("Next on close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not wake Next")
	}
}

func TestNoScope(t *testing.T) {
	e := newEnv(t, events.Options{})
	other := server.RootScope("bob/home")
	if _, err := e.hub.Subscribe(ctx, other, fsproto.EventsSetup{}); !errors.Is(err, server.ErrNoScope) {
		t.Fatalf("Subscribe without since: %v", err)
	}
	if _, err := e.hub.Subscribe(ctx, other, fsproto.EventsSetup{HasSince: true}); !errors.Is(err, server.ErrNoScope) {
		t.Fatalf("Subscribe with since: %v", err)
	}
}
