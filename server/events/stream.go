package events

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

// stream is one open events fid. It starts out catching up (reading the log
// and then the ring from cursor) and becomes live once it has every record
// up to the newest one the scope has published.
type stream struct {
	hub    *Hub
	sc     *scope
	scope  server.Scope
	setup  fsproto.EventsSetup
	notify chan struct{}
	reader chan struct{} // holds a token while a Next runs

	mu     sync.Mutex
	queue  []*fsproto.Event // records to deliver, shared with the ring and other streams; nil marks one merged away
	count  int              // records in queue that are not merged away
	limit  int              // live queue bound, QueueLen past what was queued on joining
	cursor uint64           // catching up: the last seq read
	last   uint64           // live: the last seq seen, for dropping duplicates
	live   bool             // registered with the scope
	done   bool             // a reset was delivered
	closed bool

	// For coalesce: the queue index of the last record about each path, and
	// of the last record that changes the tree's shape (mv, del, or a
	// directory put). A record merges into an earlier one for the same path
	// only if no shape change lies between them, so moving it later can never
	// put it before a record it depends on, or after one that depends on it.
	byPath  map[string]int
	barrier int
}

// push delivers a live record. It runs with the scope locked, and returns
// false when the stream should leave the fanout.
func (st *stream) push(ev *fsproto.Event) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return false
	}
	if ev.Seq <= st.last {
		return true
	}
	st.last = ev.Seq
	if !st.setup.Match(*ev) {
		return true
	}
	if !st.merge(ev) {
		if st.count >= st.limit {
			// The reader fell too far behind: replace what it has not read
			// with a reset at the current seq and stop following.
			st.clear()
			st.add(&fsproto.Event{Seq: ev.Seq, Op: fsproto.OpReset})
			st.live = false
			st.signal()
			return false
		}
		st.add(ev)
	}
	st.signal()
	return true
}

// enqueue adds a replayed record, if it passes the filters. st.mu is held.
func (st *stream) enqueue(ev *fsproto.Event) {
	st.cursor = ev.Seq
	if st.setup.Match(*ev) && !st.merge(ev) {
		st.add(ev)
	}
}

// merge coalesces ev into the queue, replacing an earlier record for the
// same path, and reports whether it did. st.mu is held.
func (st *stream) merge(ev *fsproto.Event) bool {
	if st.byPath == nil || ev.Op == fsproto.OpMv {
		return false
	}
	i, ok := st.byPath[ev.Target()]
	if !ok || i <= st.barrier || !supersedes(ev.Op, st.queue[i].Op) {
		return false
	}
	st.queue[i] = nil
	st.count--
	st.add(ev)
	return true
}

// supersedes reports whether a record with op x makes an earlier record
// with op y for the same path redundant. A put carries the whole index line,
// flags included, and a del ends the path; flags replace only flags.
func supersedes(x, y fsproto.Op) bool {
	switch x {
	case fsproto.OpPut, fsproto.OpDel:
		return y == fsproto.OpPut || y == fsproto.OpFlags
	case fsproto.OpFlags:
		return y == fsproto.OpFlags
	}
	return false
}

// add appends ev to the queue. st.mu is held.
func (st *stream) add(ev *fsproto.Event) {
	i := len(st.queue)
	st.queue = append(st.queue, ev)
	st.count++
	if st.byPath == nil {
		return
	}
	switch {
	case ev.Op == fsproto.OpMv, ev.Op == fsproto.OpDel,
		ev.Op == fsproto.OpPut && ev.Entry.Type == fsproto.TypeDir:
		st.barrier = i
	}
	if ev.Op != fsproto.OpMv {
		st.byPath[ev.Target()] = i
	}
}

// clear empties the queue. st.mu is held.
func (st *stream) clear() {
	st.queue = nil
	st.count = 0
	if st.byPath != nil {
		clear(st.byPath)
	}
	st.barrier = -1
}

func (st *stream) signal() {
	select {
	case st.notify <- struct{}{}:
	default:
	}
}

// Next returns every queued record, catching up from the log or the ring
// first when the stream is not live yet.
func (st *stream) Next(ctx context.Context) ([]fsproto.Event, error) {
	select {
	case st.reader <- struct{}{}:
		defer func() { <-st.reader }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	for {
		st.mu.Lock()
		if st.closed {
			st.mu.Unlock()
			return nil, ErrClosed
		}
		if st.count > 0 {
			out := make([]fsproto.Event, 0, st.count)
			for _, ev := range st.queue {
				if ev != nil {
					out = append(out, *ev)
				}
			}
			st.clear()
			st.limit = st.hub.opts.QueueLen
			if out[len(out)-1].Op == fsproto.OpReset {
				st.done = true
			}
			st.mu.Unlock()
			return out, nil
		}
		if st.done {
			st.mu.Unlock()
			return nil, io.EOF
		}
		live := st.live
		st.mu.Unlock()
		if !live {
			if err := st.fill(ctx); err != nil {
				return nil, err
			}
			continue
		}
		select {
		case <-st.notify:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// fill catches up by one batch: from the ring if it covers the cursor,
// otherwise from the log. Once nothing is left to replay, the stream joins
// the fanout.
func (st *stream) fill(ctx context.Context) error {
	sc := st.sc
	sc.mu.Lock()
	if sc.ring.covers(st.cursor) {
		st.join()
		sc.mu.Unlock()
		return nil
	}
	sc.mu.Unlock()

	max := st.hub.opts.QueueLen
	var batch []fsproto.Event
	err := st.hub.log.ReadLog(ctx, st.scope, st.cursor, func(ev fsproto.Event) error {
		batch = append(batch, ev)
		if len(batch) >= max {
			return errStop
		}
		return nil
	})
	switch {
	case errors.Is(err, server.ErrTrimmed):
		// Retention trimmed the log while this stream was behind.
		return st.resetAtCurrent(ctx)
	case err != nil && !errors.Is(err, errStop):
		return err // the batch is dropped, so a canceled read can be retried
	}
	st.mu.Lock()
	for i := range batch {
		st.enqueue(&batch[i])
	}
	st.mu.Unlock()
	if err != nil {
		return nil // a full batch; there may be more
	}

	// The log had nothing after cursor when it was read. Any later change was
	// published after that, so the ring holds it, unless the ring has already
	// moved past cursor, in which case read the log again.
	sc.mu.Lock()
	if !sc.ring.known || sc.ring.start <= st.cursor {
		st.join()
	}
	sc.mu.Unlock()
	return nil
}

// join copies records after the cursor from the ring, up to the queue bound,
// and registers the stream once it holds them all. sc.mu is held, and the
// ring is either unknown or starts at or before the cursor. A cursor past
// the ring's last seq is a change logged but not yet published, which the
// stream will drop as a duplicate when it arrives.
func (st *stream) join() {
	sc := st.sc
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return
	}
	r := &sc.ring
	if r.known && st.cursor < r.last {
		if room := st.hub.opts.QueueLen - st.count; room > 0 {
			r.after(st.cursor, room, st.enqueue)
		}
		if st.cursor < r.last {
			return
		}
	}
	st.live = true
	st.last = st.cursor
	st.limit = st.hub.opts.QueueLen + st.count
	sc.streams[st] = struct{}{}
}

// startAtCurrent queues the "seq <current>" record for a stream opened
// without since and makes it live after current.
func (st *stream) startAtCurrent(ctx context.Context) error {
	sc := st.sc
	sc.mu.Lock()
	if sc.ring.known {
		st.cursor = sc.ring.last
		st.mu.Lock()
		st.add(&fsproto.Event{Op: fsproto.OpSeq, Seq: st.cursor})
		st.mu.Unlock()
		st.join()
		sc.mu.Unlock()
		return nil
	}
	sc.mu.Unlock()
	cur, err := st.hub.log.Seq(ctx, st.scope)
	if err != nil {
		return err
	}
	st.cursor = cur
	st.mu.Lock()
	st.add(&fsproto.Event{Op: fsproto.OpSeq, Seq: cur})
	st.mu.Unlock()
	// As in fill: the log was at cur, so anything later is in the ring,
	// unless the ring has moved past cur, and then Next reads the log.
	sc.mu.Lock()
	if !sc.ring.known || sc.ring.start <= cur {
		st.join()
	}
	sc.mu.Unlock()
	return nil
}

// resetAtCurrent replaces the queue with a reset at the scope's current seq,
// which ends the stream.
func (st *stream) resetAtCurrent(ctx context.Context) error {
	cur, err := st.hub.log.Seq(ctx, st.scope)
	if err != nil {
		return err
	}
	st.mu.Lock()
	st.clear()
	st.add(&fsproto.Event{Seq: cur, Op: fsproto.OpReset})
	st.mu.Unlock()
	st.signal()
	return nil
}

// Close ends the stream and removes it from the fanout. A blocked Next
// returns ErrClosed.
func (st *stream) Close() error {
	st.mu.Lock()
	st.closed = true
	st.live = false
	st.mu.Unlock()
	st.signal()
	st.sc.mu.Lock()
	delete(st.sc.streams, st)
	st.sc.mu.Unlock()
	return nil
}
