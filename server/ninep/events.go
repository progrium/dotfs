package ninep

import (
	"context"
	"io"
	"strings"
	"sync"

	"github.com/hugelgupf/p9/linux"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

// eventsHandle is an open .fs/events: setup writes, then blocking reads of
// whole records. Reads are serialized; a read waiting for records returns
// when its context is canceled (Tflush), leaving the stream usable.
type eventsHandle struct {
	srv   *Server
	scope server.Scope

	sem chan struct{} // held by the one read or write in progress

	mu      sync.Mutex
	setup   fsproto.EventsSetup
	stream  server.Stream
	pending []string // encoded records, each ending in "\n", not yet read
	eof     bool     // a reset was queued; after pending drains, reads end
	closed  bool
}

func newEventsHandle(srv *Server, sc server.Scope) *eventsHandle {
	return &eventsHandle{srv: srv, scope: sc, sem: make(chan struct{}, 1)}
}

func (h *eventsHandle) acquire(ctx context.Context) error {
	select {
	case h.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *eventsHandle) release() { <-h.sem }

func (h *eventsHandle) write(ctx context.Context, p []byte, _ int64) (int, error) {
	if err := h.acquire(ctx); err != nil {
		return 0, err
	}
	defer h.release()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stream != nil || h.closed {
		return 0, linux.EINVAL // setup comes before the first read
	}
	s := h.setup
	s.Paths = append([]string(nil), s.Paths...)
	if err := s.Parse(string(p)); err != nil {
		return 0, linux.EINVAL
	}
	h.setup = s
	return len(p), nil
}

// read is readDeliver for callers that always deliver what they read.
func (h *eventsHandle) read(ctx context.Context, p []byte, _ int64) (int, error) {
	var n int
	err := h.readDeliver(ctx, len(p), func(b []byte) bool {
		n = copy(p, b)
		return true
	})
	return n, err
}

// readDeliver waits for records and passes as many whole ones as fit in
// count to deliver, which reports whether they reached the client. Only
// delivered records are consumed, so a read flushed after records arrived
// loses nothing. An empty delivery is end of stream, after a reset.
func (h *eventsHandle) readDeliver(ctx context.Context, count int, deliver func([]byte) bool) error {
	if err := h.acquire(ctx); err != nil {
		return err
	}
	defer h.release()

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return linux.EBADF
	}
	if h.stream == nil {
		st, err := h.srv.cfg.Hub.Subscribe(h.srv.ctx, h.scope, h.setup)
		if err != nil {
			h.mu.Unlock()
			return err
		}
		h.stream = st
	}
	st := h.stream
	for len(h.pending) == 0 && !h.eof {
		h.mu.Unlock()
		evs, err := st.Next(ctx)
		h.mu.Lock()
		if h.closed {
			h.mu.Unlock()
			return linux.EBADF
		}
		if err == io.EOF {
			h.eof = true
			break
		}
		if err != nil {
			h.mu.Unlock()
			return err
		}
		for _, ev := range evs {
			h.pending = append(h.pending, ev.Encode()+"\n")
			if ev.Op == fsproto.OpReset {
				h.eof = true
			}
		}
	}
	var b strings.Builder
	n := 0
	for n < len(h.pending) && b.Len()+len(h.pending[n]) <= count {
		b.WriteString(h.pending[n])
		n++
	}
	if n == 0 && len(h.pending) > 0 {
		h.mu.Unlock()
		return linux.EMSGSIZE // a record never spans two reads
	}
	h.mu.Unlock()
	if deliver([]byte(b.String())) {
		h.mu.Lock()
		h.pending = h.pending[n:]
		h.mu.Unlock()
	}
	return nil
}

func (h *eventsHandle) close(context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	if h.stream != nil {
		return h.stream.Close()
	}
	return nil
}
