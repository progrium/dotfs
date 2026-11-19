package ninep

import (
	"context"
	"io"
	"sync"

	"github.com/hugelgupf/p9/linux"
	"github.com/hugelgupf/p9/p9"

	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/ninep/frame"
)

// lconn is one 9P2000.L connection. Messages pass through to a p9 server,
// except that:
//
//   - Tattach is checked against the auth policy and its name rewritten
//     to the canonical "<user>/<root>", because p9 does not show the
//     attach name or user to the file system;
//   - fids that walk to a .fs/events file (recognized by qid in Rwalk) are
//     opened, written, read, and clunked here, so a blocked read can be
//     canceled by Tflush, which p9 cannot do: its Tflush waits for the
//     read to finish.
type lconn struct {
	srv  *Server
	t    frame.Transport
	user string // authenticated by the transport, or ""
	pw   *io.PipeWriter

	wmu sync.Mutex // one message at a time to t

	mu     sync.Mutex
	walks  map[uint16]walkReq      // outstanding Twalks, by tag
	events map[uint32]*eventsFid   // fids at an events file
	reads  map[uint16]*pendingRead // outstanding events reads, by tag
}

type walkReq struct {
	fid, newfid uint32
	n           int
}

type eventsFid struct {
	q     qid
	scope server.Scope
	h     *eventsHandle // nil until opened
}

type pendingRead struct {
	cancel  context.CancelFunc
	flushed bool
}

func (s *Server) serveDotL(t frame.Transport, user string, first []byte) error {
	c := &lconn{
		srv: s, t: t, user: user,
		walks:  map[uint16]walkReq{},
		events: map[uint32]*eventsFid{},
		reads:  map[uint16]*pendingRead{},
	}
	pr, pw := io.Pipe()
	c.pw = pw
	asm := &frame.Assembler{Emit: c.fromP9}
	ps := p9.NewServer(dotlAttacher{srv: s})
	done := make(chan struct{})
	go func() {
		ps.Handle(pr, nopCloser{asm})
		close(done)
	}()
	err := c.in(first)
	for err == nil {
		var f []byte
		if f, err = t.ReadFrame(); err == nil {
			err = c.in(f)
		}
	}
	c.shutdown()
	pw.CloseWithError(io.EOF)
	<-done
	return err
}

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

func (c *lconn) send(f []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.t.WriteFrame(f)
}

func (c *lconn) forward(f []byte) error {
	_, err := c.pw.Write(f)
	return err
}

func (c *lconn) lerror(tag uint16, err error) error {
	return c.send(newEnc(rlerror, tag).u32(uint32(errno(err))).bytes())
}

// fromP9 sees each reply from p9 before it goes out, to learn which fids
// walked to an events file.
func (c *lconn) fromP9(f []byte) error {
	switch frame.Type(f) {
	case rwalk:
		c.mu.Lock()
		w, ok := c.walks[frame.Tag(f)]
		delete(c.walks, frame.Tag(f))
		if ok {
			d := newDec(f)
			nq := int(d.u16())
			var last qid
			for i := 0; i < nq; i++ {
				last = d.qid()
			}
			if !d.bad && nq == w.n {
				src := c.events[w.fid]
				delete(c.events, w.newfid)
				if w.n == 0 && src != nil {
					c.events[w.newfid] = &eventsFid{q: src.q, scope: src.scope}
				} else if w.n > 0 {
					if sc, ok := c.srv.events.Load(last.path); ok {
						c.events[w.newfid] = &eventsFid{q: last, scope: sc.(server.Scope)}
					}
				}
			}
		}
		c.mu.Unlock()
	case rlerror:
		c.mu.Lock()
		delete(c.walks, frame.Tag(f))
		c.mu.Unlock()
	}
	return c.send(f)
}

func (c *lconn) evfid(fid uint32) *eventsFid {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.events[fid]
}

// in handles one message from the client.
func (c *lconn) in(f []byte) error {
	tag := frame.Tag(f)
	d := newDec(f)
	switch frame.Type(f) {
	case tversion:
		c.shutdown()
		return c.forward(f)
	case tattach:
		return c.attach(tag, d, f)
	case twalk:
		fid, newfid, n := d.u32(), d.u32(), int(d.u16())
		if !d.bad {
			c.mu.Lock()
			c.walks[tag] = walkReq{fid: fid, newfid: newfid, n: n}
			c.mu.Unlock()
		}
	case tlopen:
		if ev := c.evfid(d.u32()); ev != nil && !d.bad {
			c.mu.Lock()
			opened := ev.h != nil
			if !opened {
				ev.h = newEventsHandle(c.srv, ev.scope)
			}
			c.mu.Unlock()
			if opened {
				return c.lerror(tag, linux.EBADF)
			}
			return c.send(newEnc(rlopen, tag).qid(ev.q).u32(0).bytes())
		}
	case twrite:
		fid, off, count := d.u32(), d.u64(), d.u32()
		if ev := c.evfid(fid); ev != nil && ev.h != nil && !d.bad {
			data := d.take(int(count))
			if d.bad {
				return c.lerror(tag, linux.EINVAL)
			}
			n, err := ev.h.write(c.srv.ctx, data, int64(off))
			if err != nil {
				return c.lerror(tag, err)
			}
			return c.send(newEnc(rwrite, tag).u32(uint32(n)).bytes())
		}
	case tread:
		fid, _, count := d.u32(), d.u64(), d.u32()
		if ev := c.evfid(fid); ev != nil && ev.h != nil && !d.bad {
			c.read(tag, ev.h, int(min(count, frame.MaxSize-11)))
			return nil
		}
	case tclunk, tremove:
		fid := d.u32()
		c.mu.Lock()
		ev := c.events[fid]
		delete(c.events, fid)
		c.mu.Unlock()
		if ev != nil && ev.h != nil {
			ev.h.close(c.srv.ctx)
		}
	case tflush:
		old := d.u16()
		c.mu.Lock()
		pr := c.reads[old]
		if pr != nil {
			pr.flushed = true
			delete(c.reads, old)
		}
		c.mu.Unlock()
		if pr != nil {
			// The read's reply is now never sent, so Rflush can go out
			// right away.
			pr.cancel()
			return c.send(newEnc(rflush, tag).bytes())
		}
	}
	return c.forward(f)
}

// read serves a Tread on an events fid in its own goroutine.
func (c *lconn) read(tag uint16, h *eventsHandle, count int) {
	ctx, cancel := context.WithCancel(c.srv.ctx)
	pr := &pendingRead{cancel: cancel}
	c.mu.Lock()
	c.reads[tag] = pr
	c.mu.Unlock()
	go func() {
		defer cancel()
		err := h.readDeliver(ctx, count, func(b []byte) bool {
			c.mu.Lock()
			defer c.mu.Unlock()
			if pr.flushed {
				return false
			}
			delete(c.reads, tag)
			return c.send(newEnc(rread, tag).data(b).bytes()) == nil
		})
		if err != nil {
			c.mu.Lock()
			defer c.mu.Unlock()
			if !pr.flushed {
				delete(c.reads, tag)
				c.lerror(tag, err)
			}
		}
	}()
}

// attach checks a Tattach and passes it on with the canonical name.
func (c *lconn) attach(tag uint16, d *dec, f []byte) error {
	fid, afid := d.u32(), d.u32()
	uname, aname := d.str(), d.str()
	nuname := d.u32()
	if d.bad {
		return c.lerror(tag, linux.EINVAL)
	}
	if afid != noFID {
		return c.forward(f) // p9 rejects authentication fids
	}
	root, err := c.srv.cfg.Auth.Attach(c.srv.ctx, c.user, uname, aname)
	if err != nil {
		return c.lerror(tag, err)
	}
	m := newEnc(tattach, tag).u32(fid).u32(afid).str(uname).
		str(root.User() + "/" + root.Name()).u32(nuname)
	return c.forward(m.bytes())
}

// shutdown ends every events fid and pending read, for a new Tversion or
// the end of the connection.
func (c *lconn) shutdown() {
	c.mu.Lock()
	for tag, pr := range c.reads {
		pr.flushed = true
		pr.cancel()
		delete(c.reads, tag)
	}
	evs := c.events
	c.events = map[uint32]*eventsFid{}
	c.walks = map[uint16]walkReq{}
	c.mu.Unlock()
	for _, ev := range evs {
		if ev.h != nil {
			ev.h.close(c.srv.ctx)
		}
	}
}
