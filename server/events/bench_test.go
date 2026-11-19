package events_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/events"
)

// BenchmarkFanout publishes to 1,000 live streams, each drained by its own
// reader, and reports the time per published record until every stream has
// it.
func BenchmarkFanout(b *testing.B) {
	for _, c := range []struct {
		name  string
		setup func(i int) fsproto.EventsSetup
	}{
		{"all", func(int) fsproto.EventsSetup { return fsproto.EventsSetup{} }},
		// Each stream watches one of 100 directories; records go to all of
		// them in turn, so a record reaches 10 streams.
		{"path", func(i int) fsproto.EventsSetup {
			return fsproto.EventsSetup{Paths: []string{fmt.Sprintf("/d%d", i%100)}}
		}},
		{"coalesce", func(int) fsproto.EventsSetup { return fsproto.EventsSetup{Coalesce: true} }},
	} {
		b.Run(c.name+"/streams=1000", func(b *testing.B) { benchFanout(b, 1000, c.setup) })
	}
}

func benchFanout(b *testing.B, streams int, setup func(int) fsproto.EventsSetup) {
	scope := server.RootScope("alice/home")
	h := events.New(&staticLog{}, events.Options{QueueLen: 1 << 16})
	paths := make([]string, 100)
	for i := range paths {
		paths[i] = fmt.Sprintf("/d%d/f%d", i, i)
	}
	h.Publish(scope, put(1, paths[0]))
	var ss []server.Stream
	for i := 0; i < streams; i++ {
		s, err := h.Subscribe(ctx, scope, setup(i))
		if err != nil {
			b.Fatal(err)
		}
		ss = append(ss, s)
	}
	n := uint64(b.N)
	// After seqs 2..n+1, a final round of one record per directory, seqs
	// above n+1, reaches every stream whatever its filter.
	var wg sync.WaitGroup
	for _, s := range ss {
		wg.Add(1)
		go func(s server.Stream) {
			defer wg.Done()
			for {
				evs, err := s.Next(ctx)
				if err != nil {
					b.Error(err)
					return
				}
				if last := evs[len(evs)-1]; last.Op == fsproto.OpReset {
					b.Error("reset")
					return
				} else if last.Seq > n+1 {
					return
				}
			}
		}(s)
	}
	b.ResetTimer()
	for i := uint64(2); i <= n+1; i++ {
		h.Publish(scope, put(i, paths[i%100]))
	}
	for i, p := range paths {
		h.Publish(scope, put(n+2+uint64(i), p))
	}
	wg.Wait()
	b.StopTimer()
	for _, s := range ss {
		s.Close()
	}
}

// BenchmarkPublish measures Publish alone with 1,000 live streams that do
// not read during the run, so it leaves out reader wakeups.
func BenchmarkPublish(b *testing.B) {
	scope := server.RootScope("alice/home")
	h := events.New(&staticLog{}, events.Options{QueueLen: b.N + 1})
	h.Publish(scope, put(1, "/f"))
	for i := 0; i < 1000; i++ {
		s, err := h.Subscribe(ctx, scope, fsproto.EventsSetup{})
		if err != nil {
			b.Fatal(err)
		}
		defer s.Close()
	}
	b.ResetTimer()
	for i := 2; i <= b.N+1; i++ {
		h.Publish(scope, put(uint64(i), "/f"))
	}
}
