package client

import (
	"context"
	"errors"
	"sync"

	"github.com/progrium/dotfs/fsproto"
)

// Redialer is a Conn that dials lazily and redials after a transport
// failure. A call that fails with a transport error (see IsTransport)
// returns that error and drops the connection; the next call dials a new
// one. Calls are not retried, since a ctl command may have taken effect
// before the connection dropped. Files, stages, and streams opened earlier
// keep using the connection they were opened on.
//
// Per-connection state, such as the device id from hello, belongs in dial
// (Options.Device does this for Dial).
type Redialer struct {
	dial func(ctx context.Context) (Conn, error)

	mu     sync.Mutex
	cur    Conn
	closed bool
}

var _ Conn = (*Redialer)(nil)

// NewRedialer returns a Redialer that connects with dial.
func NewRedialer(dial func(ctx context.Context) (Conn, error)) *Redialer {
	return &Redialer{dial: dial}
}

// Conn returns the current connection, dialing if there is none.
func (r *Redialer) Conn(ctx context.Context) (Conn, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	if r.cur != nil {
		return r.cur, nil
	}
	c, err := r.dial(ctx)
	if err != nil {
		return nil, err
	}
	r.cur = c
	return c, nil
}

// fail drops c if err is a transport error and c is still current.
func (r *Redialer) fail(c Conn, err error) {
	if !IsTransport(err) {
		return
	}
	r.mu.Lock()
	if r.cur == c {
		r.cur = nil
	} else {
		c = nil
	}
	r.mu.Unlock()
	if c != nil {
		c.Close()
	}
}

func redial[T any](r *Redialer, ctx context.Context, fn func(Conn) (T, error)) (T, error) {
	c, err := r.Conn(ctx)
	if err != nil {
		var zero T
		return zero, err
	}
	v, err := fn(c)
	if err != nil {
		r.fail(c, err)
	}
	return v, err
}

func (r *Redialer) Index(ctx context.Context, accept []string) (IndexReader, error) {
	return redial(r, ctx, func(c Conn) (IndexReader, error) { return c.Index(ctx, accept) })
}

func (r *Redialer) Events(ctx context.Context, setup fsproto.EventsSetup) (EventStream, error) {
	return redial(r, ctx, func(c Conn) (EventStream, error) {
		st, err := c.Events(ctx, setup)
		if err != nil {
			return nil, err
		}
		return &redialStream{EventStream: st, r: r, c: c}, nil
	})
}

// redialStream reports a stream's transport failure to its Redialer, so
// a Follower's reopen dials a new connection.
type redialStream struct {
	EventStream
	r *Redialer
	c Conn
}

func (s *redialStream) Next(ctx context.Context) (fsproto.Event, error) {
	ev, err := s.EventStream.Next(ctx)
	if err != nil && ctx.Err() == nil && !errors.Is(err, ErrStreamEnded) {
		s.r.fail(s.c, err)
	}
	return ev, err
}

func (r *Redialer) Ctl(ctx context.Context, cmd fsproto.Command) (fsproto.Reply, error) {
	return redial(r, ctx, func(c Conn) (fsproto.Reply, error) { return c.Ctl(ctx, cmd) })
}

func (r *Redialer) Stage(ctx context.Context, name string) (Stage, error) {
	return redial(r, ctx, func(c Conn) (Stage, error) { return c.Stage(ctx, name) })
}

func (r *Redialer) Open(ctx context.Context, path string) (File, error) {
	return redial(r, ctx, func(c Conn) (File, error) { return c.Open(ctx, path) })
}

func (r *Redialer) Read(ctx context.Context, path string) ([]byte, error) {
	return redial(r, ctx, func(c Conn) ([]byte, error) { return c.Read(ctx, path) })
}

// Close closes the current connection; later calls fail with ErrClosed.
func (r *Redialer) Close() error {
	r.mu.Lock()
	c := r.cur
	r.cur, r.closed = nil, true
	r.mu.Unlock()
	if c != nil {
		return c.Close()
	}
	return nil
}
