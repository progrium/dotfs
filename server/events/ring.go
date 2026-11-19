package events

import "github.com/progrium/dotfs/fsproto"

// ring holds a scope's most recent records, which always have consecutive
// seqs start+1 through last.
type ring struct {
	buf   []*fsproto.Event
	head  int // index of the oldest record
	n     int
	start uint64 // seq just before the oldest record
	last  uint64 // seq of the newest record, or start when empty
	known bool   // whether anything was published since the hub started
}

func newRing(size int) ring { return ring{buf: make([]*fsproto.Event, size)} }

// add appends ev. A seq at or below the last one is a duplicate and is
// dropped (add returns false). A seq that skips ahead starts the ring over,
// since it can only hold consecutive records.
func (r *ring) add(ev *fsproto.Event) bool {
	if r.known && ev.Seq <= r.last {
		return false
	}
	if !r.known || ev.Seq != r.last+1 {
		r.head, r.n = 0, 0
		r.start = ev.Seq - 1
		r.known = true
	}
	if r.n == len(r.buf) {
		r.buf[r.head] = nil
		r.head = (r.head + 1) % len(r.buf)
		r.n--
		r.start++
	}
	r.buf[(r.head+r.n)%len(r.buf)] = ev
	r.n++
	r.last = ev.Seq
	return true
}

// covers reports whether the ring holds every record after seq.
func (r *ring) covers(seq uint64) bool {
	return r.known && r.start <= seq && seq <= r.last
}

// after calls fn for up to max records after seq, which the ring must cover,
// and returns how many it visited.
func (r *ring) after(seq uint64, max int, fn func(*fsproto.Event)) int {
	i := int(seq - r.start)
	k := 0
	for ; i < r.n && k < max; i, k = i+1, k+1 {
		fn(r.buf[(r.head+i)%len(r.buf)])
	}
	return k
}
