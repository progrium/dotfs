package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hugelgupf/p9/linux"
	"github.com/hugelgupf/p9/p9"

	"github.com/progrium/dotfs/fsproto"
)

// DefaultMessageSize is the 9P message size a Client asks for. Larger
// messages mean fewer round trips for content reads and stage writes.
const DefaultMessageSize = 256 << 10

// Options configure a Client.
type Options struct {
	// Root is the attach name: the root to attach to. For tcp and unix
	// listeners that check a static token, it carries the token as the
	// server expects.
	Root string
	// Token is a bearer token sent on the WebSocket upgrade.
	Token string
	// MessageSize is the 9P message size; 0 means DefaultMessageSize.
	MessageSize uint32
	// Device, when set, is sent with hello right after attaching, so
	// changes made through the Client carry dev= in events.
	Device string
	// IdleCtl is how many open ctl fids to keep for reuse; 0 means 4, and
	// a negative value keeps none.
	IdleCtl int
}

func (o Options) msize() uint32 {
	if o.MessageSize == 0 {
		return DefaultMessageSize
	}
	return o.MessageSize
}

// Client is a client.Conn over a 9P2000.L connection, using hugelgupf/p9.
// Every call walks its own fids from the attach root, so a Client is safe
// for concurrent use.
type Client struct {
	p9c   *p9.Client
	root  p9.File
	msize uint32

	mu      sync.Mutex
	closed  bool
	idleCtl []p9.File
	maxIdle int
}

var _ Conn = (*Client)(nil)

// ErrClosed is returned by calls on a closed Client.
var ErrClosed = errors.New("dotfs: client closed")

// New runs the 9P version exchange and attach over rwc and returns a
// Client that owns rwc. If ctx has a deadline and rwc is a net.Conn, the
// handshake is bounded by it.
func New(ctx context.Context, rwc io.ReadWriteCloser, opts Options) (*Client, error) {
	if nc, ok := rwc.(net.Conn); ok {
		if dl, ok := ctx.Deadline(); ok {
			nc.SetDeadline(dl)
			defer nc.SetDeadline(time.Time{})
		}
	}
	stop := context.AfterFunc(ctx, func() { rwc.Close() })
	defer stop()
	p9c, err := p9.NewClient(rwc, p9.WithMessageSize(opts.msize()))
	if err != nil {
		rwc.Close()
		return nil, ctxErr(ctx, fmt.Errorf("dotfs: 9P version: %w", err))
	}
	root, err := p9c.Attach(opts.Root)
	if err != nil {
		p9c.Close()
		return nil, ctxErr(ctx, fmt.Errorf("dotfs: attach %q: %w", opts.Root, mapErrno(err)))
	}
	c := &Client{p9c: p9c, root: root, msize: opts.msize(), maxIdle: opts.IdleCtl}
	if c.maxIdle == 0 {
		c.maxIdle = 4
	}
	if opts.Device != "" {
		if _, err := Call(ctx, c, fsproto.HelloCmd{Device: opts.Device}); err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}

// ctxErr prefers the context's error when the context ended, since closing
// the connection on cancellation surfaces as an I/O error.
func ctxErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// Dial connects to addr and attaches. addr is one of:
//
//	ws://host/path, wss://host/path   WebSocket, with opts.Token as a bearer token
//	unix:/path/to/socket              a Unix socket
//	tcp:host:port, or host:port       TCP
func Dial(ctx context.Context, addr string, opts Options) (*Client, error) {
	switch {
	case strings.HasPrefix(addr, "ws://"), strings.HasPrefix(addr, "wss://"):
		return DialWebSocket(ctx, addr, opts)
	case strings.HasPrefix(addr, "unix:"):
		return dialNet(ctx, "unix", strings.TrimPrefix(addr, "unix:"), opts)
	default:
		return dialNet(ctx, "tcp", strings.TrimPrefix(addr, "tcp:"), opts)
	}
}

func dialNet(ctx context.Context, network, addr string, opts Options) (*Client, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	return New(ctx, nc, opts)
}

// Close closes the connection. Open files, streams, and stages fail after
// it.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.idleCtl = nil
	c.mu.Unlock()
	return c.p9c.Close()
}

func (c *Client) check() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	return nil
}

// readSize is the largest read that p9 sends as one Tread: a little under
// the message size, on a 512-byte boundary like p9's own payload size.
func (c *Client) readSize() int {
	return int((c.msize - 4096) &^ 511)
}

// walk walks names from the attach root and returns the new fid. A walk
// that stops short (a server may answer a partial walk with fewer qids)
// is ErrMissing.
func (c *Client) walk(names []string) (p9.File, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	qids, f, err := c.root.Walk(names)
	if err != nil {
		return nil, err
	}
	if len(qids) != len(names) {
		f.Close()
		return nil, linux.ENOENT
	}
	return f, nil
}

// walkGetAttr is walk plus the file's attributes, in one round trip when
// the server supports Twalkgetattr.
func (c *Client) walkGetAttr(names []string) (p9.File, p9.Attr, error) {
	if err := c.check(); err != nil {
		return nil, p9.Attr{}, err
	}
	qids, f, _, attr, err := c.root.WalkGetAttr(names)
	if err != nil {
		return nil, p9.Attr{}, err
	}
	if len(qids) != len(names) {
		f.Close()
		return nil, p9.Attr{}, linux.ENOENT
	}
	return f, attr, nil
}

// splitPath turns a protocol path into walk names.
func splitPath(p string) ([]string, error) {
	if err := fsproto.CheckPath(p); err != nil {
		return nil, err
	}
	if p == "/" {
		return nil, nil
	}
	return strings.Split(p[1:], "/"), nil
}

// Error is a failed operation on a path. Err is the underlying error; an
// Error for a path that does not exist also matches fsproto.ErrMissing.
type Error struct {
	Op   string
	Path string
	Err  error
}

func (e *Error) Error() string { return "dotfs: " + e.Op + " " + e.Path + ": " + e.Err.Error() }

func (e *Error) Unwrap() []error {
	if errors.Is(e.Err, linux.ENOENT) {
		return []error{e.Err, fsproto.ErrMissing}
	}
	return []error{e.Err}
}

func pathErr(op, path string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Op: op, Path: path, Err: mapErrno(err)}
}

// mapErrno unwraps p9's errno errors so errors.Is works on them; other
// errors are returned as they are.
func mapErrno(err error) error {
	var errno linux.Errno
	if errors.As(err, &errno) {
		return errno
	}
	return err
}

// IsTransport reports whether err means the connection itself failed, so
// redialing may help, as opposed to a protocol outcome (a *ReplyError), a
// 9P error from the server, the end of a stream, or a canceled context.
func IsTransport(err error) bool {
	if err == nil {
		return false
	}
	var errno linux.Errno
	var re *fsproto.ReplyError
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return false
	case errors.As(err, &re), errors.As(err, &errno):
		return false
	case err == io.EOF, errors.Is(err, fsproto.ErrSyntax), errors.Is(err, fsproto.ErrUnsorted):
		return false
	}
	return true
}
