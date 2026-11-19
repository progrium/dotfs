package ninep_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/progrium/dotfs/client"
	"github.com/progrium/dotfs/client/clienttest"
	"github.com/progrium/dotfs/server/auth"
	"github.com/progrium/dotfs/server/fake"
	"github.com/progrium/dotfs/server/ninep"
)

// The reference client's behavior tests, run against this server over TCP
// and over WebSocket.

func newServer(t *testing.T) (*ninep.Server, *fake.Server) {
	fs := fake.NewServer()
	srv := ninep.New(ninep.Config{Meta: fs.Meta, Store: fs.Store, Hub: fs.Hub,
		Auth: auth.Policy{Tokens: fs.Tokens}, CreateRoots: true})
	t.Cleanup(func() { srv.Close() })
	return srv, fs
}

func dialClient(t *testing.T, addr string, opts client.Options) client.Conn {
	t.Helper()
	c, err := client.Dial(context.Background(), addr, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestClientConnTCP(t *testing.T) {
	clienttest.TestConn(t, func(t *testing.T) clienttest.Dial {
		srv, _ := newServer(t)
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go srv.Serve(ctx, l)
		return func(t *testing.T) client.Conn {
			return dialClient(t, l.Addr().String(), client.Options{Root: "alice/home"})
		}
	})
}

func TestClientConnWebSocket(t *testing.T) {
	clienttest.TestConn(t, func(t *testing.T) clienttest.Dial {
		srv, fs := newServer(t)
		tok, _ := fs.Tokens.CreateToken(context.Background(), "alice")
		mux := http.NewServeMux()
		mux.Handle("/9p", &auth.WebSocket{Tokens: fs.Tokens, Server: srv})
		hs := httptest.NewServer(mux)
		t.Cleanup(hs.Close)
		url := "ws" + strings.TrimPrefix(hs.URL, "http") + "/9p"
		return func(t *testing.T) client.Conn {
			return dialClient(t, url, client.Options{Root: "home", Token: tok})
		}
	})
}
