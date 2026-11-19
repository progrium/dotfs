package client

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/progrium/dotfs/fsproto"
)

// OpenEvents opens an event stream; Conn.Events has this signature.
type OpenEvents func(ctx context.Context, setup fsproto.EventsSetup) (EventStream, error)

// ErrNeedsReset is returned by Follower.Next after a reset record until
// Restart is called.
var ErrNeedsReset = errors.New("dotfs: event stream was reset; re-read the index and Restart")

// Follower reads events across dropped connections. It remembers the seq
// of the last record it returned, and when a stream fails with a transport
// error (see IsTransport) it reopens one with since set to that seq, after
// a backoff, and drops any record at or below it, so the caller sees every
// change once, in order, without gaps (beyond those path filters and
// coalesce allow).
//
// Pair it with a Redialer's Events so a reopen also redials:
//
//	r := client.NewRedialer(dial)
//	f := client.NewFollower(r.Events, fsproto.EventsSetup{Since: n, HasSince: true})
//
// A Follower is not safe for concurrent use.
type Follower struct {
	open  OpenEvents
	setup fsproto.EventsSetup

	// MinBackoff and MaxBackoff bound the wait between reopen attempts;
	// the wait doubles after each failure.
	MinBackoff, MaxBackoff time.Duration

	st      EventStream
	started bool // last is valid
	last    uint64
	reset   bool
	backoff time.Duration
}

// NewFollower returns a Follower that opens streams with open. setup's
// path filters and coalesce apply to every stream; its since applies to
// the first one only. Without since, the first record returned is the
// stream's "seq <current>" record.
func NewFollower(open OpenEvents, setup fsproto.EventsSetup) *Follower {
	f := &Follower{open: open, setup: setup, MinBackoff: 50 * time.Millisecond, MaxBackoff: 5 * time.Second}
	if setup.HasSince {
		f.started, f.last = true, setup.Since
	}
	return f
}

// Seq returns the seq of the last record returned (or the starting since),
// and whether there is one yet.
func (f *Follower) Seq() (uint64, bool) { return f.last, f.started }

// Next returns the next record, reopening the stream as needed. It
// returns ctx.Err() when ctx ends, leaving the Follower usable. A reset
// record is returned like any other; after it, Next returns ErrNeedsReset
// until Restart.
func (f *Follower) Next(ctx context.Context) (fsproto.Event, error) {
	for {
		if f.reset {
			return fsproto.Event{}, ErrNeedsReset
		}
		if err := ctx.Err(); err != nil {
			return fsproto.Event{}, err
		}
		if f.st == nil {
			setup := f.setup
			setup.Since, setup.HasSince = f.last, f.started
			st, err := f.open(ctx, setup)
			if err != nil {
				if !IsTransport(err) {
					return fsproto.Event{}, err
				}
				if err := f.wait(ctx); err != nil {
					return fsproto.Event{}, err
				}
				continue
			}
			f.st = st
		}
		ev, err := f.st.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return fsproto.Event{}, ctx.Err()
			}
			f.st.Close()
			f.st = nil
			if !IsTransport(err) && err != io.EOF {
				return fsproto.Event{}, err
			}
			if err := f.wait(ctx); err != nil {
				return fsproto.Event{}, err
			}
			continue
		}
		f.backoff = 0
		switch ev.Op {
		case fsproto.OpReset:
			f.st.Close()
			f.st = nil
			f.reset = true
			return ev, nil
		case fsproto.OpSeq:
			if f.started {
				// Only a stream opened without since starts this way.
				continue
			}
		default:
			if f.started && ev.Seq <= f.last {
				continue
			}
		}
		f.started, f.last = true, ev.Seq
		return ev, nil
	}
}

func (f *Follower) wait(ctx context.Context) error {
	if f.backoff == 0 {
		f.backoff = f.MinBackoff
	} else {
		f.backoff = min(2*f.backoff, f.MaxBackoff)
	}
	t := time.NewTimer(f.backoff)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Restart continues after a reset (or anywhere else) from since: the next
// stream opens with since, typically the seq of a freshly read index.
func (f *Follower) Restart(since uint64) {
	if f.st != nil {
		f.st.Close()
		f.st = nil
	}
	f.reset = false
	f.started, f.last = true, since
	f.backoff = 0
}

// Close closes the current stream.
func (f *Follower) Close() error {
	if f.st == nil {
		return nil
	}
	err := f.st.Close()
	f.st = nil
	return err
}
