// Package events is the server's events fanout: a server.Hub that delivers
// each scope's changes to its open .fs/events streams.
//
// Each scope keeps an in-memory ring of its most recent records, filled by
// Publish. A stream opened with since replays first from the Log and then
// from the ring, a batch at a time as the reader asks for more, and joins
// the live fanout once it has caught up. Until then it is not registered
// with the scope, so a reader catching up costs publishers nothing. A live
// stream has a bounded queue; when a slow reader lets it fill, the stream
// gets a reset record and leaves the fanout, so Publish never blocks.
package events

import (
	"context"
	"errors"
	"sync"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

// Defaults for Options.
const (
	DefaultQueueLen = 1024
	DefaultRingLen  = 4096
)

// ErrClosed is returned by Next on a closed stream.
var ErrClosed = errors.New("events: stream closed")

// Options tunes a Hub. Zero values take the defaults.
type Options struct {
	// QueueLen bounds the records waiting in one live stream. A stream
	// that would exceed it gets a reset record instead. It is also the
	// largest batch read from the log or the ring while catching up.
	QueueLen int
	// RingLen is how many recent records each scope keeps in memory for
	// replay without reading the log.
	RingLen int
}

// Hub is a server.Hub. Publish must be called in seq order for each scope,
// as server.Writer does.
type Hub struct {
	log  server.Log
	opts Options

	mu     sync.RWMutex
	scopes map[server.Scope]*scope
}

var _ server.Hub = (*Hub)(nil)

// New returns a Hub that replays history from log.
func New(log server.Log, opts Options) *Hub {
	if opts.QueueLen <= 0 {
		opts.QueueLen = DefaultQueueLen
	}
	if opts.RingLen <= 0 {
		opts.RingLen = DefaultRingLen
	}
	return &Hub{log: log, opts: opts, scopes: map[server.Scope]*scope{}}
}

// scope is one scope's ring and live streams. Lock order: scope.mu, then
// stream.mu.
type scope struct {
	mu      sync.Mutex
	ring    ring
	streams map[*stream]struct{}
}

func (h *Hub) scope(s server.Scope) *scope {
	h.mu.RLock()
	sc := h.scopes[s]
	h.mu.RUnlock()
	if sc != nil {
		return sc
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if sc = h.scopes[s]; sc == nil {
		sc = &scope{ring: newRing(h.opts.RingLen), streams: map[*stream]struct{}{}}
		h.scopes[s] = sc
	}
	return sc
}

// Publish records ev in the scope's ring and queues it on every live stream.
// It never blocks on a reader.
func (h *Hub) Publish(s server.Scope, ev fsproto.Event) {
	sc := h.scope(s)
	p := &ev // one copy, shared by the ring and every queue
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if !sc.ring.add(p) {
		return
	}
	for st := range sc.streams {
		if !st.push(p) {
			delete(sc.streams, st)
		}
	}
}

// Subscribe opens a stream. Errors from the Log, such as server.ErrNoScope,
// are returned here; a since the log no longer covers is reported as a reset
// record, as the spec says.
func (h *Hub) Subscribe(ctx context.Context, s server.Scope, setup fsproto.EventsSetup) (server.Stream, error) {
	st := &stream{
		hub:     h,
		sc:      h.scope(s),
		scope:   s,
		setup:   setup,
		notify:  make(chan struct{}, 1),
		reader:  make(chan struct{}, 1),
		barrier: -1,
	}
	if setup.Coalesce {
		st.byPath = map[string]int{}
	}
	if !setup.HasSince {
		if err := st.startAtCurrent(ctx); err != nil {
			return nil, err
		}
		return st, nil
	}
	// Check that the log still holds the change after since. The ring may
	// hold changes retention has already trimmed from the log, but a client
	// asking for them gets the same answer whether or not they are cached.
	err := h.log.ReadLog(ctx, s, setup.Since, func(fsproto.Event) error { return errStop })
	switch {
	case errors.Is(err, server.ErrTrimmed):
		if err := st.resetAtCurrent(ctx); err != nil {
			return nil, err
		}
		return st, nil
	case err != nil && !errors.Is(err, errStop):
		return nil, err
	}
	// Catch up by one batch now, so a stream with little to replay is live
	// as soon as it opens.
	st.cursor = setup.Since
	if err := st.fill(ctx); err != nil {
		return nil, err
	}
	return st, nil
}

// errStop ends a ReadLog early.
var errStop = errors.New("events: stop")
