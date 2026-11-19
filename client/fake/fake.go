// Package fake has an in-process client.Conn backed by server/fake, for
// testing code built on client.Conn (such as the sync engine) without a
// network. Several Conns dialed to one fake.Server share its state, so two
// sync engines can be tested against one server.
package fake

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/progrium/dotfs/client"
	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	sfake "github.com/progrium/dotfs/server/fake"
)

// ErrOffline is returned by every call on a Conn set offline.
var ErrOffline = errors.New("fake: connection offline")

// Conn is a client.Conn calling straight into a fake server.
type Conn struct {
	srv   *sfake.Server
	scope server.Scope
	user  string
	ctl   *server.Ctl

	mu      sync.Mutex
	offline bool
	closed  bool
	streams map[*eventStream]struct{}
}

var _ client.Conn = (*Conn)(nil)

// Dial connects user to the root named root on srv, as an attach would.
// It fails with fsproto.ErrDenied if the root does not exist.
func Dial(srv *sfake.Server, user, root string) (*Conn, error) {
	sc := server.RootScope(server.NewRoot(user, root))
	if _, err := srv.Meta.Seq(context.Background(), sc); err != nil {
		return nil, fsproto.Denied("no such root")
	}
	return &Conn{
		srv:     srv,
		scope:   sc,
		user:    user,
		ctl:     server.NewCtl(srv.Meta, srv.Store, sc, user),
		streams: map[*eventStream]struct{}{},
	}, nil
}

// SetOffline simulates losing (true) or regaining (false) the connection.
// Going offline ends every open event stream with ErrOffline.
func (c *Conn) SetOffline(off bool) {
	c.mu.Lock()
	c.offline = off
	var streams []*eventStream
	if off {
		for s := range c.streams {
			streams = append(streams, s)
		}
	}
	c.mu.Unlock()
	for _, s := range streams {
		s.fail(ErrOffline)
	}
}

func (c *Conn) check() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("fake: connection closed")
	}
	if c.offline {
		return ErrOffline
	}
	return nil
}

func (c *Conn) Close() error {
	c.mu.Lock()
	c.closed = true
	var streams []*eventStream
	for s := range c.streams {
		streams = append(streams, s)
	}
	c.mu.Unlock()
	for _, s := range streams {
		s.Close()
	}
	return nil
}

type indexReader struct {
	*fsproto.IndexReader
	rc io.Closer
}

func (r indexReader) Close() error { return r.rc.Close() }

func (c *Conn) Index(ctx context.Context, accept []string) (client.IndexReader, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	rc, _, err := c.srv.Meta.OpenIndex(ctx, c.scope, accept)
	if err != nil {
		return nil, err
	}
	ir, err := fsproto.NewIndexReader(rc, nil)
	if err != nil {
		rc.Close()
		return nil, err
	}
	return indexReader{ir, rc}, nil
}

func (c *Conn) Events(ctx context.Context, setup fsproto.EventsSetup) (client.EventStream, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	st, err := c.srv.Hub.Subscribe(ctx, c.scope, setup)
	if err != nil {
		return nil, err
	}
	s := &eventStream{c: c, st: st}
	c.mu.Lock()
	c.streams[s] = struct{}{}
	c.mu.Unlock()
	return s, nil
}

type eventStream struct {
	c  *Conn
	st server.Stream

	mu  sync.Mutex
	buf []fsproto.Event
	err error
}

func (s *eventStream) fail(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
	s.st.Close()
}

func (s *eventStream) Next(ctx context.Context) (fsproto.Event, error) {
	s.mu.Lock()
	if len(s.buf) > 0 {
		ev := s.buf[0]
		s.buf = s.buf[1:]
		s.mu.Unlock()
		return ev, nil
	}
	if s.err != nil {
		err := s.err
		s.mu.Unlock()
		return fsproto.Event{}, err
	}
	s.mu.Unlock()
	evs, err := s.st.Next(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return fsproto.Event{}, s.err
	}
	if err != nil {
		return fsproto.Event{}, err
	}
	s.buf = evs[1:]
	return evs[0], nil
}

func (s *eventStream) Close() error {
	s.c.mu.Lock()
	delete(s.c.streams, s)
	s.c.mu.Unlock()
	s.fail(errors.New("fake: stream closed"))
	return nil
}

func (c *Conn) Ctl(ctx context.Context, cmd fsproto.Command) (fsproto.Reply, error) {
	if err := c.check(); err != nil {
		return fsproto.Reply{}, err
	}
	// Round-trip through the wire format, as a real connection does.
	line := cmd.Encode()
	parsed, err := fsproto.ParseCommand(line)
	if err != nil {
		r, _ := fsproto.ErrorReply(err)
		return r, nil
	}
	reply, err := c.ctl.ExecLine(ctx, line)
	if err != nil {
		return fsproto.Reply{}, err
	}
	return fsproto.ParseReply(parsed.Name(), reply)
}

type stage struct {
	server.StageFile
	c *Conn
}

func (s stage) WriteAt(p []byte, off int64) (int, error) {
	if err := s.c.check(); err != nil {
		return 0, err
	}
	return s.StageFile.WriteAt(p, off)
}

func (c *Conn) Stage(ctx context.Context, name string) (client.Stage, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	f, err := c.srv.Store.OpenStage(c.user, name, true)
	if err != nil {
		return nil, err
	}
	return stage{f, c}, nil
}

type file struct {
	*bytes.Reader
	size int64
}

func (f file) Size() int64  { return f.size }
func (f file) Close() error { return nil }

func (c *Conn) Open(ctx context.Context, path string) (client.File, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	if err := fsproto.CheckPath(path); err != nil {
		return nil, err
	}
	cf, err := c.srv.Store.Open(c.scope.Root, c.scope.RootPath(path))
	if err != nil {
		return nil, err
	}
	defer cf.Close()
	b, err := io.ReadAll(cf)
	if err != nil {
		return nil, err
	}
	return file{bytes.NewReader(b), int64(len(b))}, nil
}

func (c *Conn) Read(ctx context.Context, path string) ([]byte, error) {
	f, err := c.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}
