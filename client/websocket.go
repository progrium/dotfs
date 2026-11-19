package client

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/coder/websocket"
)

// DialWebSocket connects to a dotfs server's WebSocket endpoint (such as
// wss://host/9p) and attaches to opts.Root. opts.Token, when set, is sent
// as "Authorization: Bearer <token>" on the upgrade.
func DialWebSocket(ctx context.Context, url string, opts Options) (*Client, error) {
	h := http.Header{}
	if opts.Token != "" {
		h.Set("Authorization", "Bearer "+opts.Token)
	}
	ws, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: h})
	if err != nil {
		return nil, err
	}
	return New(ctx, WebSocketConn(ws, opts.msize()), opts)
}

// WebSocketConn adapts a WebSocket to the byte stream a 9P client or
// server reads and writes, framing one 9P message per binary WebSocket
// message as the protocol expects. Reads accept any framing. msize bounds
// the size of one message.
func WebSocketConn(ws *websocket.Conn, msize uint32) io.ReadWriteCloser {
	ctx, cancel := context.WithCancel(context.Background())
	ws.SetReadLimit(int64(msize) + 4096)
	return &wsConn{ws: ws, ctx: ctx, cancel: cancel, max: msize + 4096}
}

type wsConn struct {
	ws     *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	max    uint32

	rmu sync.Mutex
	r   io.Reader

	wmu  sync.Mutex
	wbuf []byte
}

func (c *wsConn) Read(p []byte) (int, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	for {
		if c.r == nil {
			typ, r, err := c.ws.Reader(c.ctx)
			if err != nil {
				if websocket.CloseStatus(err) == websocket.StatusNormalClosure || errors.Is(err, context.Canceled) {
					return 0, io.EOF
				}
				return 0, err
			}
			if typ != websocket.MessageBinary {
				return 0, fmt.Errorf("dotfs: unexpected WebSocket %v message", typ)
			}
			c.r = r
		}
		n, err := c.r.Read(p)
		if err == io.EOF {
			c.r = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

// Write collects bytes until it holds whole 9P messages (size[4] at the
// start of each), then sends each as one WebSocket message.
func (c *wsConn) Write(p []byte) (int, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.wbuf = append(c.wbuf, p...)
	for len(c.wbuf) >= 4 {
		size := binary.LittleEndian.Uint32(c.wbuf)
		if size < 7 || size > c.max {
			return 0, fmt.Errorf("dotfs: bad 9P message size %d", size)
		}
		if uint32(len(c.wbuf)) < size {
			break
		}
		if err := c.ws.Write(c.ctx, websocket.MessageBinary, c.wbuf[:size]); err != nil {
			return 0, err
		}
		c.wbuf = append(c.wbuf[:0], c.wbuf[size:]...)
	}
	return len(p), nil
}

func (c *wsConn) Close() error {
	err := c.ws.Close(websocket.StatusNormalClosure, "")
	c.cancel()
	return err
}
