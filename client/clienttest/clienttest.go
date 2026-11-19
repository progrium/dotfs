// Package clienttest has behavior tests that every client.Conn must pass.
// Call TestConn from a _test.go file in the implementing package with a
// function that sets up one fresh, empty, writable root and returns a way
// to dial it:
//
//	func TestConn(t *testing.T) {
//		clienttest.TestConn(t, func(t *testing.T) clienttest.Dial {
//			addr := startServer(t)
//			return func(t *testing.T) client.Conn { return dial(t, addr) }
//		})
//	}
package clienttest

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/progrium/dotfs/client"
	"github.com/progrium/dotfs/fsproto"
)

// Dial returns a new connection to the test's root. Each call is a
// separate client.
type Dial func(t *testing.T) client.Conn

// NewRoot sets up a fresh, empty, writable root and returns how to dial it.
type NewRoot func(t *testing.T) Dial

var ctx = context.Background()

func check(t *testing.T, what string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

func call(t *testing.T, c client.Conn, cmd fsproto.Command) fsproto.Reply {
	t.Helper()
	r, err := client.Call(ctx, c, cmd)
	check(t, cmd.Encode(), err)
	return r
}

// upload stages data under name and commits it at p.
func upload(t *testing.T, c client.Conn, name, p string, base fsproto.Base, data string) (fsproto.Reply, error) {
	t.Helper()
	s, err := c.Stage(ctx, name)
	check(t, "Stage", err)
	_, err = s.WriteAt([]byte(data), 0)
	check(t, "WriteAt", err)
	check(t, "Close stage", s.Close())
	return client.Call(ctx, c, fsproto.CommitCmd{Stage: name, Path: p, Base: base, Mtime: 1763491250, Mode: 0o644})
}

func dial(t *testing.T, d Dial) client.Conn {
	c := d(t)
	t.Cleanup(func() { c.Close() })
	return c
}

func index(t *testing.T, c client.Conn) (fsproto.IndexHeader, []fsproto.Entry) {
	t.Helper()
	r, err := c.Index(ctx, []string{fsproto.EncodingZstd})
	check(t, "Index", err)
	defer r.Close()
	var es []fsproto.Entry
	for {
		e, err := r.Next()
		if err == io.EOF {
			return r.Header(), es
		}
		check(t, "index Next", err)
		es = append(es, e)
	}
}

func next(t *testing.T, s client.EventStream) fsproto.Event {
	t.Helper()
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ev, err := s.Next(c)
	check(t, "events Next", err)
	return ev
}

// TestConn runs the client.Conn behavior tests.
func TestConn(t *testing.T, newRoot NewRoot) {
	t.Run("CapsAndHello", func(t *testing.T) {
		c := dial(t, newRoot(t))
		r := call(t, c, fsproto.CapsCmd{})
		for _, want := range []string{fsproto.CmdMkdir, fsproto.CmdCommit, fsproto.CmdStat} {
			found := false
			for _, got := range r.Caps {
				found = found || got == want
			}
			if !found {
				t.Fatalf("caps %v lacks %s", r.Caps, want)
			}
		}
		call(t, c, fsproto.HelloCmd{Device: "dev-a"})
	})

	t.Run("EmptyIndex", func(t *testing.T) {
		c := dial(t, newRoot(t))
		h, es := index(t, c)
		if h.Seq != 0 || len(es) != 1 || es[0].Path != "/" || es[0].Type != fsproto.TypeDir {
			t.Fatalf("empty root index %+v %+v", h, es)
		}
	})

	t.Run("CommitReadAndIndex", func(t *testing.T) {
		c := dial(t, newRoot(t))
		call(t, c, fsproto.MkdirCmd{Path: "/docs", Base: fsproto.At(0)})
		r, err := upload(t, c, "u1", "/docs/a b.txt", fsproto.At(0), "hello")
		check(t, "commit", err)
		if r.Version != 2 || r.Hash != fsproto.HashBytes([]byte("hello")) {
			t.Fatalf("commit reply %+v", r)
		}
		b, err := c.Read(ctx, "/docs/a b.txt")
		if err != nil || string(b) != "hello" {
			t.Fatalf("Read = %q, %v", b, err)
		}
		f, err := c.Open(ctx, "/docs/a b.txt")
		check(t, "Open", err)
		buf := make([]byte, 3)
		if n, _ := f.ReadAt(buf, 2); n != 3 || string(buf) != "llo" || f.Size() != 5 {
			t.Fatalf("ReadAt = %q, size %d", buf, f.Size())
		}
		f.Close()
		h, es := index(t, c)
		if h.Seq != 2 || len(es) != 3 || es[2].Path != "/docs/a b.txt" || es[2].Version != 2 || es[2].Length != 5 {
			t.Fatalf("index %+v %+v", h, es)
		}
		st := call(t, c, fsproto.StatCmd{Path: "/docs/a b.txt"})
		if st.Entry == nil || !reflect.DeepEqual(*st.Entry, es[2]) {
			t.Fatalf("stat %+v, index %+v", st.Entry, es[2])
		}
		_, err = c.Read(ctx, "/nope")
		if !errors.Is(err, fsproto.ErrMissing) {
			t.Fatalf("Read missing: %v", err)
		}
	})

	t.Run("Conflict", func(t *testing.T) {
		d := newRoot(t)
		a, b := dial(t, d), dial(t, d)
		_, err := upload(t, a, "a1", "/f", fsproto.At(0), "from a")
		check(t, "a commits", err)
		_, err = upload(t, b, "b1", "/f", fsproto.At(0), "from b")
		var re *fsproto.ReplyError
		if !errors.As(err, &re) || re.Status != fsproto.StatusConflict || re.Version != 1 {
			t.Fatalf("b's commit: %v, want conflict 1", err)
		}
		_, err = upload(t, b, "b1", "/f", fsproto.At(1), "from b")
		check(t, "b commits on top", err)
		if got, _ := a.Read(ctx, "/f"); string(got) != "from b" {
			t.Fatalf("a reads %q", got)
		}
		_, err = client.Call(ctx, a, fsproto.RmCmd{Path: "/f", Base: fsproto.At(1)})
		if !errors.Is(err, fsproto.ErrConflict) {
			t.Fatalf("stale rm: %v", err)
		}
	})

	t.Run("ResumeStage", func(t *testing.T) {
		c := dial(t, newRoot(t))
		s, err := c.Stage(ctx, "big")
		check(t, "Stage", err)
		s.WriteAt([]byte("first half,"), 0)
		s.Close()
		s, err = c.Stage(ctx, "big")
		check(t, "Stage again", err)
		n, err := s.Size()
		check(t, "Size", err)
		if n != 11 {
			t.Fatalf("resumed size %d", n)
		}
		s.WriteAt([]byte(" second half"), n)
		s.Close()
		data := "first half, second half"
		r := call(t, c, fsproto.CommitCmd{Stage: "big", Path: "/big", Base: fsproto.At(0), Mode: 0o644, Hash: fsproto.HashBytes([]byte(data))})
		if r.Hash != fsproto.HashBytes([]byte(data)) {
			t.Fatalf("reply %+v", r)
		}
		if got, _ := c.Read(ctx, "/big"); string(got) != data {
			t.Fatalf("content %q", got)
		}
		// abort discards a stage file; committing it then fails.
		s, _ = c.Stage(ctx, "gone")
		s.WriteAt([]byte("x"), 0)
		s.Close()
		call(t, c, fsproto.AbortCmd{Stage: "gone"})
		if _, err := client.Call(ctx, c, fsproto.CommitCmd{Stage: "gone", Path: "/g", Base: fsproto.At(0), Mode: 0o644}); err == nil {
			t.Fatal("commit of an aborted stage succeeded")
		}
	})

	t.Run("Events", func(t *testing.T) {
		d := newRoot(t)
		a, b := dial(t, d), dial(t, d)
		call(t, a, fsproto.HelloCmd{Device: "dev-a"})
		call(t, a, fsproto.MkdirCmd{Path: "/d"})
		live, err := b.Events(ctx, fsproto.EventsSetup{})
		check(t, "Events", err)
		defer live.Close()
		if ev := next(t, live); ev.Op != fsproto.OpSeq || ev.Seq != 1 {
			t.Fatalf("first record %+v", ev)
		}
		// A canceled read leaves the stream usable.
		c, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
		_, err = live.Next(c)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Next with nothing pending: %v", err)
		}
		r, err := upload(t, a, "s", "/d/f", fsproto.At(0), "x")
		check(t, "commit", err)
		ev := next(t, live)
		if ev.Op != fsproto.OpPut || ev.Seq != r.Version || ev.Entry.Path != "/d/f" || ev.Dev != "dev-a" || ev.Entry.Hash != r.Hash {
			t.Fatalf("put record %+v for reply %+v", ev, r)
		}
		call(t, a, fsproto.MvCmd{From: "/d/f", To: "/g"})
		if ev := next(t, live); ev.Op != fsproto.OpMv || ev.Path != "/d/f" || ev.To != "/g" {
			t.Fatalf("mv record %+v", ev)
		}
		// Replay from a seq, filtered by path.
		replay, err := b.Events(ctx, fsproto.EventsSetup{Since: 0, HasSince: true, Paths: []string{"/d"}})
		check(t, "Events since", err)
		defer replay.Close()
		if ev := next(t, replay); ev.Seq != 1 || ev.Entry.Path != "/d" {
			t.Fatalf("replay 1: %+v", ev)
		}
		if ev := next(t, replay); ev.Seq != 2 {
			t.Fatalf("replay 2: %+v", ev)
		}
		if ev := next(t, replay); ev.Seq != 3 || ev.Op != fsproto.OpMv {
			t.Fatalf("replay 3: %+v", ev)
		}
	})
}
