package client_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/progrium/dotfs/client"
	"github.com/progrium/dotfs/client/clienttest"
	cfake "github.com/progrium/dotfs/client/fake"
	"github.com/progrium/dotfs/client/internal/ninetest"
	"github.com/progrium/dotfs/fsproto"
	sfake "github.com/progrium/dotfs/server/fake"
)

var ctx = context.Background()

func dial(t *testing.T, addr string, opts client.Options) *client.Client {
	t.Helper()
	c, err := client.Dial(ctx, addr, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestConnTCP(t *testing.T) {
	clienttest.TestConn(t, func(t *testing.T) clienttest.Dial {
		s := ninetest.New(t)
		return func(t *testing.T) client.Conn {
			return dial(t, s.Addr(), client.Options{Root: "home"})
		}
	})
}

func TestConnWebSocket(t *testing.T) {
	clienttest.TestConn(t, func(t *testing.T) clienttest.Dial {
		s := ninetest.New(t)
		s.Token = "sekrit"
		url := s.WebSocket(t)
		return func(t *testing.T) client.Conn {
			return dial(t, url, client.Options{Root: "home", Token: "sekrit"})
		}
	})
}

func TestWebSocketBadToken(t *testing.T) {
	s := ninetest.New(t)
	s.Token = "sekrit"
	url := s.WebSocket(t)
	if c, err := client.Dial(ctx, url, client.Options{Root: "home", Token: "wrong"}); err == nil {
		c.Close()
		t.Fatal("dial with a bad token succeeded")
	}
}

func TestIndexEncodings(t *testing.T) {
	s := ninetest.New(t)
	c := dial(t, s.Addr(), client.Options{Root: "home", Device: "dev-a"})
	for i := range 50 {
		name := "/f" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		if _, err := client.Put(ctx, c, name, fsproto.At(0), bytes.NewReader([]byte(name)), int64(len(name)), client.PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, accept := range [][]string{nil, {fsproto.EncodingZstd}} {
		r, err := c.Index(ctx, accept)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for {
			_, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			n++
		}
		r.Close()
		want := ""
		if accept != nil {
			want = fsproto.EncodingZstd
		}
		if h := r.Header(); h.Encoding != want || h.Seq != 50 || n != 51 {
			t.Fatalf("accept %v: header %+v, %d entries", accept, h, n)
		}
	}
}

// failConn fails every write once limit bytes have been written.
type failConn struct {
	net.Conn
	limit   int64
	written atomic.Int64
}

func (c *failConn) Write(p []byte) (int, error) {
	if c.written.Add(int64(len(p))) > c.limit {
		c.Conn.Close()
		return 0, errors.New("injected failure")
	}
	return c.Conn.Write(p)
}

func TestUploadResume(t *testing.T) {
	defer func(n int) { client.UploadChunk = n }(client.UploadChunk)
	client.UploadChunk = 64 << 10
	s := ninetest.New(t)
	data := make([]byte, 3<<20+12345)
	rand.New(rand.NewSource(1)).Read(data)
	size := int64(len(data))

	nc, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.New(ctx, &failConn{Conn: nc, limit: size / 2}, client.Options{Root: "home"})
	if err != nil {
		t.Fatal(err)
	}
	n1, err := client.Upload(ctx, c, "big", bytes.NewReader(data), size)
	c.Close()
	if err == nil {
		t.Fatal("upload over a failing connection succeeded")
	}
	if n1 == 0 || n1 >= size {
		t.Fatalf("first attempt wrote %d of %d", n1, size)
	}

	c2 := dial(t, s.Addr(), client.Options{Root: "home"})
	n2, err := client.Upload(ctx, c2, "big", bytes.NewReader(data), size)
	if err != nil {
		t.Fatal(err)
	}
	// The second attempt starts where the stage file ends: together the
	// two wrote the file once, plus at most the chunk that was in flight.
	if n1+n2 != size {
		t.Fatalf("attempts wrote %d + %d, want %d", n1, n2, size)
	}
	if got := s.StageBytes.Load(); got > size+int64(client.UploadChunk) {
		t.Fatalf("server received %d stage bytes for a %d-byte file", got, size)
	}
	hash := fsproto.HashBytes(data)
	if _, err := client.Call(ctx, c2, fsproto.CommitCmd{Stage: "big", Path: "/big", Base: fsproto.At(0), Mode: 0o644, Hash: hash}); err != nil {
		t.Fatal(err)
	}
	got, err := c2.Read(ctx, "/big")
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read back %d bytes, %v", len(got), err)
	}
}

func TestPutUsesCommitHash(t *testing.T) {
	s := ninetest.New(t)
	c := dial(t, s.Addr(), client.Options{Root: "home"})
	data := bytes.Repeat([]byte("dedupe me "), 1000)
	r1, err := client.Put(ctx, c, "/a", fsproto.At(0), bytes.NewReader(data), int64(len(data)), client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	staged := s.StageBytes.Load()
	r2, err := client.Put(ctx, c, "/b", fsproto.At(0), bytes.NewReader(data), int64(len(data)), client.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if s.StageBytes.Load() != staged || r2.Hash != r1.Hash || r2.Version != r1.Version+1 {
		t.Fatalf("second put staged %d bytes; replies %+v %+v", s.StageBytes.Load()-staged, r1, r2)
	}
	_, err = client.Put(ctx, c, "/a", fsproto.At(0), bytes.NewReader(data), int64(len(data)), client.PutOptions{})
	if !errors.Is(err, fsproto.ErrConflict) {
		t.Fatalf("stale put: %v", err)
	}
}

// writeN makes n changes (mkdir /d0 ... /dn-1) through c.
func writeN(t *testing.T, c client.Conn, from, n int) {
	for i := from; i < from+n; i++ {
		if _, err := client.Call(ctx, c, fsproto.MkdirCmd{Path: "/d" + itoa(i)}); err != nil {
			t.Error(err)
			return
		}
	}
}

func itoa(i int) string {
	return string(rune('0'+i/100%10)) + string(rune('0'+i/10%10)) + string(rune('0'+i%10))
}

// collect reads records from f until it has seen seq want, checking that
// seqs arrive once each, in order, without gaps.
func collect(t *testing.T, f *client.Follower, first, want uint64) {
	t.Helper()
	next := first
	for next <= want {
		c, cancel := context.WithTimeout(ctx, 10*time.Second)
		ev, err := f.Next(c)
		cancel()
		if err != nil {
			t.Fatalf("waiting for seq %d: %v", next, err)
		}
		if ev.Seq != next {
			t.Fatalf("got seq %d (%s), want %d", ev.Seq, ev.Op, next)
		}
		next++
	}
}

func TestFollowerSurvivesDrops(t *testing.T) {
	s := ninetest.New(t)
	writer := dial(t, s.Addr(), client.Options{Root: "home", Device: "w"})
	var dials atomic.Int32
	r := client.NewRedialer(func(ctx context.Context) (client.Conn, error) {
		dials.Add(1)
		return client.Dial(ctx, s.Addr(), client.Options{Root: "home"})
	})
	defer r.Close()
	f := client.NewFollower(r.Events, fsproto.EventsSetup{Since: 0, HasSince: true})
	f.MinBackoff = time.Millisecond
	defer f.Close()

	const total = 300
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < total; i += 30 {
			writeN(t, writer, i, 30)
			// Drop the follower's connection (and the writer's) mid-stream.
			s.DropConns()
			time.Sleep(5 * time.Millisecond)
			w, err := client.Dial(ctx, s.Addr(), client.Options{Root: "home"})
			if err != nil {
				t.Error(err)
				return
			}
			writer.Close()
			writer = w
		}
		writer.Close()
	}()
	collect(t, f, 1, total)
	wg.Wait()
	if dials.Load() < 2 {
		t.Fatalf("follower dialed %d times; the drops did not reach it", dials.Load())
	}
}

func TestFollowerFakeOffline(t *testing.T) {
	srv := sfake.NewServer()
	srv.CreateRoot("alice", "home")
	reader, _ := cfake.Dial(srv, "alice", "home")
	writer, _ := cfake.Dial(srv, "alice", "home")
	f := client.NewFollower(reader.Events, fsproto.EventsSetup{})
	f.MinBackoff = time.Millisecond
	if ev, err := f.Next(ctx); err != nil || ev.Op != fsproto.OpSeq || ev.Seq != 0 {
		t.Fatalf("first record %+v, %v", ev, err)
	}
	writeN(t, writer, 0, 5)
	collect(t, f, 1, 3)
	reader.SetOffline(true)
	writeN(t, writer, 5, 5)
	go func() {
		time.Sleep(20 * time.Millisecond)
		reader.SetOffline(false)
	}()
	collect(t, f, 4, 10)
}

func TestFollowerReset(t *testing.T) {
	s := ninetest.New(t)
	c := dial(t, s.Addr(), client.Options{Root: "home"})
	writeN(t, c, 0, 10)
	if err := s.Fake.Meta.Trim(ctx, s.Scope, 5); err != nil {
		t.Fatal(err)
	}
	f := client.NewFollower(c.Events, fsproto.EventsSetup{Since: 2, HasSince: true})
	defer f.Close()
	ev, err := f.Next(ctx)
	if err != nil || ev.Op != fsproto.OpReset {
		t.Fatalf("got %+v, %v; want reset", ev, err)
	}
	if _, err := f.Next(ctx); !errors.Is(err, client.ErrNeedsReset) {
		t.Fatalf("after reset: %v", err)
	}
	f.Restart(10)
	writeN(t, c, 10, 2)
	collect(t, f, 11, 12)
}

func TestEventsCancelKeepsRecord(t *testing.T) {
	s := ninetest.New(t)
	c := dial(t, s.Addr(), client.Options{Root: "home"})
	st, err := c.Events(ctx, fsproto.EventsSetup{Since: 0, HasSince: true})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for range 3 {
		cc, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		if _, err := st.Next(cc); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Next with nothing pending: %v", err)
		}
		cancel()
	}
	writeN(t, c, 0, 1)
	cc, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if ev, err := st.Next(cc); err != nil || ev.Seq != 1 {
		t.Fatalf("after cancels: %+v, %v", ev, err)
	}
}

func TestMissing(t *testing.T) {
	s := ninetest.New(t)
	c := dial(t, s.Addr(), client.Options{Root: "home"})
	for _, p := range []string{"/nope", "/a/b/c"} {
		if _, err := c.Open(ctx, p); !errors.Is(err, fsproto.ErrMissing) {
			t.Fatalf("Open %s: %v", p, err)
		}
	}
	if _, err := c.Open(ctx, "relative"); err == nil {
		t.Fatal("Open of a relative path succeeded")
	}
}
