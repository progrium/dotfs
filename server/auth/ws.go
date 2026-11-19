package auth

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/coder/websocket"

	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/ninep/frame"
)

// CookieName is the cookie that may carry a token on the WebSocket
// upgrade, for browsers, which cannot set an Authorization header there.
const CookieName = "dotfs_token"

// ConnServer serves 9P on a transport for an authenticated user.
type ConnServer interface {
	ServeTransport(ctx context.Context, t frame.Transport, user string) error
}

// WebSocket is the HTTP handler for 9P over WebSocket (wss://host/9p). It
// takes a bearer token from the Authorization header or the dotfs_token
// cookie, rejects the upgrade with 401 when it is missing or unknown, and
// serves 9P with one 9P message per binary WebSocket message. The attach
// name then picks one of that user's roots.
type WebSocket struct {
	Tokens server.Tokens
	Server ConnServer
	// OriginPatterns lists other origins allowed to connect, as in
	// websocket.AcceptOptions; by default only same-origin browsers (and
	// clients that send no Origin) may.
	OriginPatterns []string
	Logf           func(format string, args ...any)
}

// TokenFromRequest returns the bearer token of r, or "".
func TokenFromRequest(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		if tok, ok := strings.CutPrefix(h, "Bearer "); ok {
			return strings.TrimSpace(tok)
		}
		return ""
	}
	if c, err := r.Cookie(CookieName); err == nil {
		return c.Value
	}
	return ""
}

func (h *WebSocket) logf(format string, args ...any) {
	if h.Logf != nil {
		h.Logf(format, args...)
	}
}

func (h *WebSocket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tok := TokenFromRequest(r)
	if tok == "" || h.Tokens == nil {
		w.Header().Set("WWW-Authenticate", `Bearer realm="dotfs"`)
		http.Error(w, "token required", http.StatusUnauthorized)
		return
	}
	user, err := h.Tokens.LookupToken(r.Context(), tok)
	if err != nil {
		w.Header().Set("WWW-Authenticate", `Bearer realm="dotfs", error="invalid_token"`)
		http.Error(w, "bad token", http.StatusUnauthorized)
		return
	}
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: h.OriginPatterns})
	if err != nil {
		h.logf("websocket accept: %v", err)
		return
	}
	c.SetReadLimit(frame.MaxSize)
	t := NewWSTransport(c)
	defer t.Close()
	if err := h.Server.ServeTransport(context.WithoutCancel(r.Context()), t, user); err != nil {
		h.logf("websocket %s: %v", user, err)
	}
}

type wsTransport struct {
	c    *websocket.Conn
	ctx  context.Context
	stop context.CancelFunc
}

// NewWSTransport returns a Transport over a WebSocket connection, one 9P
// message per binary message.
func NewWSTransport(c *websocket.Conn) frame.Transport {
	ctx, stop := context.WithCancel(context.Background())
	return &wsTransport{c: c, ctx: ctx, stop: stop}
}

func (t *wsTransport) ReadFrame() ([]byte, error) {
	typ, b, err := t.c.Read(t.ctx)
	if err != nil {
		if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
			return nil, io.EOF
		}
		return nil, err
	}
	if typ != websocket.MessageBinary {
		return nil, errors.New("auth: text WebSocket message")
	}
	if err := frame.Check(b); err != nil {
		return nil, err
	}
	return b, nil
}

func (t *wsTransport) WriteFrame(b []byte) error {
	return t.c.Write(t.ctx, websocket.MessageBinary, b)
}

func (t *wsTransport) Close() error {
	t.stop()
	return t.c.Close(websocket.StatusNormalClosure, "")
}

// Dial connects to a 9P-over-WebSocket URL (ws:// or wss://) with a bearer
// token and returns the connection as a byte stream, which 9P client
// libraries such as hugelgupf/p9 expect: written bytes are regrouped into
// one WebSocket message per 9P message, and received messages are read
// back to back.
func Dial(ctx context.Context, url, token string) (io.ReadWriteCloser, error) {
	hdr := http.Header{}
	if token != "" {
		hdr.Set("Authorization", "Bearer "+token)
	}
	c, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: hdr})
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusUnauthorized {
			return nil, ErrUnauthorized
		}
		return nil, err
	}
	c.SetReadLimit(frame.MaxSize)
	return newStreamConn(NewWSTransport(c)), nil
}

// ErrUnauthorized is returned by Dial when the server rejects the token.
var ErrUnauthorized = errors.New("auth: unauthorized")

// streamConn adapts a Transport to a byte stream.
type streamConn struct {
	t   frame.Transport
	asm frame.Assembler
	rmu sync.Mutex
	rb  bytes.Reader
}

func newStreamConn(t frame.Transport) *streamConn {
	s := &streamConn{t: t}
	s.asm.Emit = t.WriteFrame
	return s
}

func (s *streamConn) Read(p []byte) (int, error) {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	for s.rb.Len() == 0 {
		b, err := s.t.ReadFrame()
		if err != nil {
			return 0, err
		}
		s.rb.Reset(b)
	}
	return s.rb.Read(p)
}

func (s *streamConn) Write(p []byte) (int, error) { return s.asm.Write(p) }
func (s *streamConn) Close() error                { return s.t.Close() }
