package ninep

import (
	"net"
	"testing"
	"time"

	"9fans.net/go/plan9"

	"github.com/progrium/dotfs/server/ninep/frame"
)

type pfcall = plan9.Fcall

const (
	tversionP = plan9.Tversion
	tattachP  = plan9.Tattach
	rattachP  = plan9.Rattach
	twalkP    = plan9.Twalk
	topenP    = plan9.Topen
	tcreateP  = plan9.Tcreate
	rcreateP  = plan9.Rcreate
	treadP    = plan9.Tread
	twriteP   = plan9.Twrite
	tclunkP   = plan9.Tclunk
	tflushP   = plan9.Tflush
	rflushP   = plan9.Rflush
	rwstatP   = plan9.Rwstat
	rwriteP   = plan9.Rwrite
	rerrorP   = plan9.Rerror
)

type rawPlain struct {
	t    *testing.T
	tr   frame.Transport
	msgs chan *plan9.Fcall
	tag  uint16
}

func newRawPlain(t *testing.T, c net.Conn) *rawPlain {
	r := &rawPlain{t: t, tr: frame.NewStream(c), msgs: make(chan *plan9.Fcall, 16)}
	go func() {
		defer close(r.msgs)
		for {
			b, err := r.tr.ReadFrame()
			if err != nil {
				return
			}
			f, err := plan9.UnmarshalFcall(b)
			if err != nil {
				t.Errorf("bad reply: %v", err)
				return
			}
			r.msgs <- f
		}
	}()
	return r
}

func (r *rawPlain) sendTag(f *plan9.Fcall, tag uint16) {
	r.t.Helper()
	f.Tag = tag
	b, err := f.Bytes()
	if err != nil {
		r.t.Fatal(err)
	}
	if err := r.tr.WriteFrame(b); err != nil {
		r.t.Fatal(err)
	}
}

func (r *rawPlain) recv() *plan9.Fcall {
	r.t.Helper()
	select {
	case f, ok := <-r.msgs:
		if !ok {
			r.t.Fatal("connection closed")
		}
		return f
	case <-time.After(5 * time.Second):
		r.t.Fatal("no reply")
	}
	return nil
}

func (r *rawPlain) quiet(d time.Duration) {
	r.t.Helper()
	select {
	case f := <-r.msgs:
		r.t.Fatalf("unexpected %v", f)
	case <-time.After(d):
	}
}

func (r *rawPlain) rpc(f *plan9.Fcall) *plan9.Fcall {
	r.t.Helper()
	r.tag++
	tag := r.tag
	if f.Type == plan9.Tversion {
		tag = plan9.NOTAG
	}
	r.sendTag(f, tag)
	g := r.recv()
	if g.Tag != tag {
		r.t.Fatalf("reply tag %d, want %d: %v", g.Tag, tag, g)
	}
	return g
}

func wstatName(fid uint32, name string) *plan9.Fcall {
	var d plan9.Dir
	d.Null()
	d.Name = name
	b, _ := d.Bytes()
	return &plan9.Fcall{Type: plan9.Twstat, Fid: fid, Stat: b}
}
