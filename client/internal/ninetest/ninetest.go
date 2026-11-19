// Package ninetest is a small 9P2000.L server for the client's tests: it
// serves one root of a server/fake over hugelgupf/p9, with .fs/index,
// .fs/events, .fs/ctl, and .fs/stage/ built straight from fsproto and
// server.Ctl. It stands in for the real 9P server until that lands, and
// lets tests drop connections and count stage bytes.
package ninetest

import (
	"bytes"
	"context"
	"errors"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/coder/websocket"
	"github.com/hugelgupf/p9/fsimpl/templatefs"
	"github.com/hugelgupf/p9/linux"
	"github.com/hugelgupf/p9/p9"
	"github.com/klauspost/compress/zstd"

	"github.com/progrium/dotfs/client"
	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	sfake "github.com/progrium/dotfs/server/fake"
)

// Server serves the root "home" of user "alice" on a fake server.
type Server struct {
	Fake  *sfake.Server
	Scope server.Scope
	User  string
	// Token, when set, is required as a bearer token on WebSocket
	// upgrades.
	Token string

	// StageBytes counts bytes written to stage files.
	StageBytes atomic.Int64

	ln    net.Listener
	mu    sync.Mutex
	conns map[io.Closer]struct{}
	wg    sync.WaitGroup
}

// New starts a server on a loopback TCP port; it stops when t ends.
func New(t testing.TB) *Server {
	f := sfake.NewServer()
	sc, err := f.CreateRoot("alice", "home")
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{Fake: f, Scope: sc, User: "alice", ln: ln, conns: map[io.Closer]struct{}{}}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.Serve(c)
			}()
		}
	}()
	t.Cleanup(s.Close)
	return s
}

// Addr returns the TCP address.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// WebSocket starts an HTTP server whose /9p endpoint serves 9P over
// WebSocket, and returns its ws:// URL.
func (s *Server) WebSocket(t testing.TB) string {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.Token != "" && r.Header.Get("Authorization") != "Bearer "+s.Token {
			http.Error(w, "bad token", http.StatusUnauthorized)
			return
		}
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		s.Serve(client.WebSocketConn(ws, 1<<20))
	})
	hs := httptest.NewServer(h)
	t.Cleanup(hs.Close)
	return "ws" + strings.TrimPrefix(hs.URL, "http") + "/9p"
}

// Close stops listening and drops every connection.
func (s *Server) Close() {
	s.ln.Close()
	s.DropConns()
	s.wg.Wait()
}

// DropConns closes every open connection from the server's side.
func (s *Server) DropConns() {
	s.mu.Lock()
	var cs []io.Closer
	for c := range s.conns {
		cs = append(cs, c)
	}
	s.mu.Unlock()
	for _, c := range cs {
		c.Close()
	}
}

// Serve serves one connection until it fails.
func (s *Server) Serve(rwc io.ReadWriteCloser) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cc := &connCloser{rwc: rwc, cancel: cancel}
	s.mu.Lock()
	s.conns[cc] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.conns, cc)
		s.mu.Unlock()
	}()
	a := &attach{
		s:   s,
		ctx: ctx,
		ctl: server.NewCtl(s.Fake.Meta, s.Fake.Store, s.Scope, s.User),
	}
	p9.NewServer(a).Handle(cc, cc)
}

// connCloser cancels the connection's context once reading fails, so
// blocked event reads end and p9's server can finish.
type connCloser struct {
	rwc    io.ReadWriteCloser
	cancel context.CancelFunc
}

func (c *connCloser) Read(p []byte) (int, error) {
	n, err := c.rwc.Read(p)
	if err != nil {
		c.cancel()
	}
	return n, err
}

func (c *connCloser) Write(p []byte) (int, error) { return c.rwc.Write(p) }

func (c *connCloser) Close() error {
	c.cancel()
	return c.rwc.Close()
}

type attach struct {
	s   *Server
	ctx context.Context
	ctl *server.Ctl
}

// Attach returns a directory holding the user's one root: p9's server
// walks the attach name from it.
func (a *attach) Attach() (p9.File, error) { return &node{a: a, kind: kTop}, nil }

type kind int

const (
	kDir kind = iota
	kFile
	kFS
	kIndex
	kEvents
	kCtl
	kStageDir
	kStage
	kTop // the attacher's root, holding the root "home"
)

type node struct {
	templatefs.NoopFile
	a    *attach
	kind kind
	path string // scope path for kDir and kFile, stage name for kStage

	mu      sync.Mutex
	content server.ContentFile
	stage   server.StageFile
	accept  []string
	index   []byte
	setup   fsproto.EventsSetup
	stream  server.Stream
	pending []byte // encoded records not yet read
	ended   bool
	reply   []byte
}

func (n *node) child(k kind, path string) *node { return &node{a: n.a, kind: k, path: path} }

func (n *node) qid() p9.QID {
	h := fnv.New64a()
	h.Write([]byte{byte(n.kind)})
	h.Write([]byte(n.path))
	q := p9.QID{Path: h.Sum64(), Type: p9.TypeRegular}
	if n.isDir() {
		q.Type = p9.TypeDir
	}
	return q
}

func (n *node) isDir() bool {
	return n.kind == kDir || n.kind == kFS || n.kind == kStageDir || n.kind == kTop
}

func (n *node) Walk(names []string) ([]p9.QID, p9.File, error) {
	if len(names) == 0 {
		c := n.child(n.kind, n.path)
		return nil, c, nil
	}
	c, err := n.walk1(names[0])
	if err != nil {
		return nil, nil, err
	}
	return []p9.QID{c.qid()}, c, nil
}

func (n *node) walk1(name string) (*node, error) {
	ctx := n.a.ctx
	switch n.kind {
	case kTop:
		if name == n.a.s.Scope.Root.Name() {
			return n.child(kDir, "/"), nil
		}
	case kDir:
		if n.path == "/" && name == server.ReservedName {
			return n.child(kFS, "/.fs"), nil
		}
		p := fsproto.Join(n.path, name)
		e, err := n.a.s.Fake.Meta.Stat(ctx, n.a.s.Scope, p)
		if err != nil {
			return nil, linux.ENOENT
		}
		if e.Type == fsproto.TypeDir {
			return n.child(kDir, p), nil
		}
		return n.child(kFile, p), nil
	case kFS:
		switch name {
		case "index":
			return n.child(kIndex, ""), nil
		case "events":
			return n.child(kEvents, ""), nil
		case "ctl":
			return n.child(kCtl, ""), nil
		case "stage":
			return n.child(kStageDir, ""), nil
		}
	case kStageDir:
		if _, err := n.a.s.Fake.Store.StatStage(n.a.s.User, name); err == nil {
			return n.child(kStage, name), nil
		}
	}
	return nil, linux.ENOENT
}

func (n *node) GetAttr(req p9.AttrMask) (p9.QID, p9.AttrMask, p9.Attr, error) {
	attr := p9.Attr{Mode: p9.ModeRegular | 0o644}
	switch n.kind {
	case kDir, kFS, kStageDir, kTop:
		attr.Mode = p9.ModeDirectory | 0o755
	case kFile:
		e, err := n.a.s.Fake.Meta.Stat(n.a.ctx, n.a.s.Scope, n.path)
		if err != nil {
			return p9.QID{}, p9.AttrMask{}, p9.Attr{}, linux.ENOENT
		}
		attr.Mode = p9.ModeRegular | p9.FileMode(e.Mode&0o777)
		attr.Size = uint64(e.Length)
		attr.MTimeSeconds = uint64(e.Mtime)
	case kStage:
		info, err := n.a.s.Fake.Store.StatStage(n.a.s.User, n.path)
		if err != nil {
			return p9.QID{}, p9.AttrMask{}, p9.Attr{}, linux.ENOENT
		}
		attr.Size = uint64(info.Size)
	}
	return n.qid(), p9.AttrMask{Mode: true, Size: true, MTime: true}, attr, nil
}

func (n *node) Open(mode p9.OpenFlags) (p9.QID, uint32, error) {
	var err error
	switch n.kind {
	case kFile:
		n.content, err = n.a.s.Fake.Store.Open(n.a.s.Scope.Root, n.a.s.Scope.RootPath(n.path))
	case kStage:
		n.stage, err = n.a.s.Fake.Store.OpenStage(n.a.s.User, n.path, false)
	}
	if err != nil {
		return p9.QID{}, 0, errno(err)
	}
	return n.qid(), 0, nil
}

func (n *node) Create(name string, flags p9.OpenFlags, perm p9.FileMode, _ p9.UID, _ p9.GID) (p9.File, p9.QID, uint32, error) {
	if n.kind != kStageDir {
		return nil, p9.QID{}, 0, linux.EPERM
	}
	if err := fsproto.CheckStageName(name); err != nil {
		return nil, p9.QID{}, 0, linux.EINVAL
	}
	if _, err := n.a.s.Fake.Store.StatStage(n.a.s.User, name); err == nil {
		return nil, p9.QID{}, 0, linux.EEXIST
	}
	f, err := n.a.s.Fake.Store.OpenStage(n.a.s.User, name, true)
	if err != nil {
		return nil, p9.QID{}, 0, errno(err)
	}
	c := n.child(kStage, name)
	c.stage = f
	return c, c.qid(), 0, nil
}

func errno(err error) error {
	if errors.Is(err, fsproto.ErrMissing) {
		return linux.ENOENT
	}
	return linux.EIO
}

func readFrom(b []byte, p []byte, off int64) (int, error) {
	if off >= int64(len(b)) {
		return 0, nil
	}
	return copy(p, b[off:]), nil
}

func (n *node) ReadAt(p []byte, off int64) (int, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	switch n.kind {
	case kFile:
		m, err := n.content.ReadAt(p, off)
		if err == io.EOF {
			err = nil
		}
		return m, err
	case kStage:
		m, err := n.stage.ReadAt(p, off)
		if err == io.EOF {
			err = nil
		}
		return m, err
	case kCtl:
		return readFrom(n.reply, p, off)
	case kIndex:
		if n.index == nil {
			b, err := n.buildIndex()
			if err != nil {
				return 0, linux.EIO
			}
			n.index = b
		}
		return readFrom(n.index, p, off)
	case kEvents:
		return n.readEvents(p)
	}
	return 0, linux.EISDIR
}

// buildIndex reads the fake's plain index and, when the client accepted
// zstd, re-encodes the body.
func (n *node) buildIndex() ([]byte, error) {
	rc, _, err := n.a.s.Fake.Meta.OpenIndex(n.a.ctx, n.a.s.Scope, nil)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	plain, err := io.ReadAll(rc)
	if err != nil || !slices.Contains(n.accept, fsproto.EncodingZstd) {
		return plain, err
	}
	head, body, _ := bytes.Cut(plain, []byte("\n"))
	h, err := fsproto.ParseIndexHeader(string(head))
	if err != nil {
		return nil, err
	}
	h.Encoding = fsproto.EncodingZstd
	var out bytes.Buffer
	out.WriteString(h.Encode() + "\n")
	zw, _ := zstd.NewWriter(&out)
	zw.Write(body)
	zw.Close()
	return out.Bytes(), nil
}

// readEvents returns whole records, blocking while there are none.
func (n *node) readEvents(p []byte) (int, error) {
	for len(n.pending) == 0 {
		if n.ended {
			return 0, nil
		}
		if n.stream == nil {
			st, err := n.a.s.Fake.Hub.Subscribe(n.a.ctx, n.a.s.Scope, n.setup)
			if err != nil {
				return 0, linux.EIO
			}
			n.stream = st
		}
		// Let writes and clunks on other fids through while blocked.
		n.mu.Unlock()
		evs, err := n.stream.Next(n.a.ctx)
		n.mu.Lock()
		if err == io.EOF {
			n.ended = true
			continue
		}
		if err != nil {
			return 0, linux.EINTR
		}
		for _, ev := range evs {
			n.pending = append(n.pending, ev.Encode()+"\n"...)
		}
	}
	// Whole records only.
	m := 0
	for m < len(n.pending) {
		i := bytes.IndexByte(n.pending[m:], '\n')
		if m+i+1 > len(p) {
			break
		}
		m += i + 1
	}
	if m == 0 {
		return 0, linux.ENOBUFS
	}
	copy(p, n.pending[:m])
	n.pending = n.pending[m:]
	return m, nil
}

func (n *node) WriteAt(p []byte, off int64) (int, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	switch n.kind {
	case kStage:
		m, err := n.stage.WriteAt(p, off)
		n.a.s.StageBytes.Add(int64(m))
		return m, err
	case kCtl:
		reply, err := n.a.ctl.ExecLine(n.a.ctx, string(p))
		if err != nil {
			return 0, linux.EIO
		}
		n.reply = []byte(reply + "\n")
		return len(p), nil
	case kIndex:
		acc, err := fsproto.ParseAccept(string(p))
		if err != nil {
			return 0, linux.EINVAL
		}
		n.accept = acc
		return len(p), nil
	case kEvents:
		if n.stream != nil {
			return 0, linux.EINVAL
		}
		if err := n.setup.Parse(string(p)); err != nil {
			return 0, linux.EINVAL
		}
		return len(p), nil
	}
	return 0, linux.EPERM
}

func (n *node) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.content != nil {
		n.content.Close()
	}
	if n.stage != nil {
		n.stage.Close()
	}
	if n.stream != nil {
		n.stream.Close()
	}
	return nil
}
