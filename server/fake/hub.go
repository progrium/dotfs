package fake

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

// DefaultQueueLen is the default per-stream queue bound.
const DefaultQueueLen = 1024

// ErrClosed is returned by Next on a closed stream.
var ErrClosed = errors.New("fake: stream closed")

// Hub is an in-memory server.Hub. Publish never blocks: a stream whose
// queue is full gets a reset record and ends.
type Hub struct {
	log      server.Log
	queueLen int

	mu   sync.Mutex
	subs map[server.Scope]map[*stream]struct{}
}

var _ server.Hub = (*Hub)(nil)

// NewHub returns a Hub that replays from log and bounds each live queue at
// queueLen records.
func NewHub(log server.Log, queueLen int) *Hub {
	if queueLen <= 0 {
		queueLen = DefaultQueueLen
	}
	return &Hub{log: log, queueLen: queueLen, subs: map[server.Scope]map[*stream]struct{}{}}
}

func (h *Hub) Publish(s server.Scope, ev fsproto.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for st := range h.subs[s] {
		st.push(ev)
	}
}

func (h *Hub) Subscribe(ctx context.Context, s server.Scope, setup fsproto.EventsSetup) (server.Stream, error) {
	st := &stream{hub: h, scope: s, setup: setup, holding: true, notify: make(chan struct{}, 1)}
	// Register first, holding live records, then read the log: a change
	// lands in the log, the held records, or both, and duplicates are
	// dropped by seq.
	h.mu.Lock()
	if h.subs[s] == nil {
		h.subs[s] = map[*stream]struct{}{}
	}
	h.subs[s][st] = struct{}{}
	h.mu.Unlock()

	var replay []fsproto.Event
	var last uint64
	reset := false
	if setup.HasSince {
		last = setup.Since
		err := h.log.ReadLog(ctx, s, setup.Since, func(ev fsproto.Event) error {
			replay = append(replay, ev)
			last = ev.Seq
			return nil
		})
		if errors.Is(err, server.ErrTrimmed) {
			reset = true
			if last, err = h.log.Seq(ctx, s); err != nil {
				st.Close()
				return nil, err
			}
		} else if err != nil {
			st.Close()
			return nil, err
		}
	} else {
		cur, err := h.log.Seq(ctx, s)
		if err != nil {
			st.Close()
			return nil, err
		}
		last = cur
		replay = []fsproto.Event{{Op: fsproto.OpSeq, Seq: cur}}
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if reset {
		st.queue = []fsproto.Event{{Seq: last, Op: fsproto.OpReset}}
		st.ending = true
	} else {
		for _, ev := range replay {
			if setup.Match(ev) {
				st.queue = append(st.queue, ev)
			}
		}
		for _, ev := range st.held {
			if ev.Seq > last {
				last = ev.Seq
				if setup.Match(ev) {
					st.queue = append(st.queue, ev)
				}
			}
		}
	}
	st.held = nil
	st.holding = false
	st.last = last
	st.limit = len(st.queue) + h.queueLen
	st.signal()
	return st, nil
}

type stream struct {
	hub    *Hub
	scope  server.Scope
	setup  fsproto.EventsSetup
	notify chan struct{}

	mu      sync.Mutex
	holding bool            // subscribing: collect live records in held
	held    []fsproto.Event // live records seen while subscribing
	queue   []fsproto.Event
	limit   int
	last    uint64 // highest seq seen, for dropping duplicates
	ending  bool   // a reset is queued; drop everything else
	done    bool   // the reset was delivered
	closed  bool
}

func (st *stream) signal() {
	select {
	case st.notify <- struct{}{}:
	default:
	}
}

func (st *stream) push(ev fsproto.Event) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed || st.ending || st.done {
		return
	}
	if st.holding {
		st.held = append(st.held, ev)
		return
	}
	if ev.Seq <= st.last {
		return
	}
	st.last = ev.Seq
	if !st.setup.Match(ev) {
		return
	}
	if len(st.queue) >= st.limit {
		st.queue = []fsproto.Event{{Seq: ev.Seq, Op: fsproto.OpReset}}
		st.ending = true
	} else {
		st.queue = append(st.queue, ev)
	}
	st.signal()
}

func (st *stream) Next(ctx context.Context) ([]fsproto.Event, error) {
	for {
		st.mu.Lock()
		if st.closed {
			st.mu.Unlock()
			return nil, ErrClosed
		}
		if len(st.queue) > 0 {
			out := st.queue
			st.queue = nil
			st.limit = st.hub.queueLen
			if out[len(out)-1].Op == fsproto.OpReset {
				st.done = true
				st.ending = false
			}
			st.mu.Unlock()
			return out, nil
		}
		if st.done {
			st.mu.Unlock()
			return nil, io.EOF
		}
		st.mu.Unlock()
		select {
		case <-st.notify:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (st *stream) Close() error {
	st.mu.Lock()
	st.closed = true
	st.mu.Unlock()
	st.signal()
	h := st.hub
	h.mu.Lock()
	delete(h.subs[st.scope], st)
	h.mu.Unlock()
	return nil
}
