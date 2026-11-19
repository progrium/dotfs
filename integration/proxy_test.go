package integration

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Proxy is a TCP proxy in front of the server's WebSocket listener. It
// can cut every open connection, refuse new ones (offline), delay data,
// and cut connections after a number of upstream bytes (mid-upload).
type Proxy struct {
	t      testing.TB
	target string
	l      net.Listener

	mu      sync.Mutex
	conns   map[net.Conn]struct{}
	offline bool
	delay   time.Duration
	cutAt   int64 // cut when upstream bytes reach this; 0 for never

	Up   atomic.Int64 // bytes client to server
	Down atomic.Int64 // bytes server to client
	Cuts atomic.Int64
}

// NewProxy listens on a loopback port and forwards to target.
func NewProxy(t testing.TB, target string) *Proxy {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &Proxy{t: t, target: target, l: l, conns: map[net.Conn]struct{}{}}
	go p.accept()
	t.Cleanup(func() { l.Close(); p.CutAll() })
	return p
}

// Addr is the address clients dial.
func (p *Proxy) Addr() string { return p.l.Addr().String() }

// SetOffline refuses new connections and cuts open ones when on.
func (p *Proxy) SetOffline(on bool) {
	p.mu.Lock()
	p.offline = on
	p.mu.Unlock()
	if on {
		p.CutAll()
	}
}

// SetDelay delays every chunk of data in both directions by d.
func (p *Proxy) SetDelay(d time.Duration) {
	p.mu.Lock()
	p.delay = d
	p.mu.Unlock()
}

// CutAfter cuts every connection once n more bytes have gone upstream.
func (p *Proxy) CutAfter(n int64) {
	p.mu.Lock()
	p.cutAt = p.Up.Load() + n
	p.mu.Unlock()
}

// CutAll closes every open connection.
func (p *Proxy) CutAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		c.Close()
	}
	if len(p.conns) > 0 {
		p.Cuts.Add(1)
	}
	clear(p.conns)
}

func (p *Proxy) accept() {
	for {
		c, err := p.l.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if p.offline {
			p.mu.Unlock()
			c.Close()
			continue
		}
		p.mu.Unlock()
		go p.handle(c)
	}
}

func (p *Proxy) handle(c net.Conn) {
	s, err := net.Dial("tcp", p.target)
	if err != nil {
		c.Close()
		return
	}
	p.mu.Lock()
	if p.offline {
		p.mu.Unlock()
		c.Close()
		s.Close()
		return
	}
	p.conns[c] = struct{}{}
	p.conns[s] = struct{}{}
	p.mu.Unlock()
	done := make(chan struct{}, 2)
	go func() { p.pipe(s, c, &p.Up, true); done <- struct{}{} }()
	go func() { p.pipe(c, s, &p.Down, false); done <- struct{}{} }()
	<-done
	c.Close()
	s.Close()
	<-done
	p.mu.Lock()
	delete(p.conns, c)
	delete(p.conns, s)
	p.mu.Unlock()
}

func (p *Proxy) pipe(dst io.Writer, src io.Reader, n *atomic.Int64, up bool) {
	buf := make([]byte, 32<<10)
	for {
		k, err := src.Read(buf)
		if k > 0 {
			p.mu.Lock()
			d, cutAt := p.delay, p.cutAt
			p.mu.Unlock()
			if d > 0 {
				time.Sleep(d)
			}
			if up && cutAt > 0 && n.Load()+int64(k) >= cutAt {
				p.mu.Lock()
				p.cutAt = 0
				p.mu.Unlock()
				p.CutAll()
				return
			}
			if _, werr := dst.Write(buf[:k]); werr != nil {
				return
			}
			n.Add(int64(k))
		}
		if err != nil {
			return
		}
	}
}
