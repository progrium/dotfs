package client_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/progrium/dotfs/client"
	"github.com/progrium/dotfs/client/clienttest"
	"github.com/progrium/dotfs/fsproto"
	sfake "github.com/progrium/dotfs/server/fake"
	"github.com/progrium/dotfs/server/httpd"
)

// startHTTP serves http-dotfs for a fake server at /r/ and returns its URL
// and a token for user alice.
func startHTTP(t *testing.T) (string, string) {
	t.Helper()
	fs := sfake.NewServer()
	tok, err := fs.Tokens.CreateToken(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	h := httpd.New(httpd.Config{Meta: fs.Meta, Store: fs.Store, Hub: fs.Hub, Tokens: fs.Tokens, CreateRoots: true})
	mux := http.NewServeMux()
	mux.Handle("/r/", h)
	hs := httptest.NewServer(mux)
	t.Cleanup(hs.Close)
	return hs.URL, tok
}

func TestConnHTTP(t *testing.T) {
	clienttest.TestConn(t, func(t *testing.T) clienttest.Dial {
		url, tok := startHTTP(t)
		return func(t *testing.T) client.Conn {
			c, err := client.DialConn(ctx, url, client.Options{Root: "home", Token: tok})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { c.Close() })
			return c
		}
	})
}

func TestDialConnHTTP(t *testing.T) {
	url, tok := startHTTP(t)
	// The server's URL plus a root, and the root's base URL, both work.
	for _, addr := range []string{url, url + "/r"} {
		c, err := client.DialConn(ctx, addr, client.Options{Root: "home", Token: tok})
		if err != nil {
			t.Fatalf("DialConn %s: %v", addr, err)
		}
		c.Close()
	}
	c, err := client.DialConn(ctx, url+"/r/home/", client.Options{Token: tok})
	if err != nil {
		t.Fatalf("DialConn base URL: %v", err)
	}
	c.Close()
	_, err = client.DialConn(ctx, url, client.Options{Root: "home", Token: "wrong"})
	if !errors.Is(err, fsproto.ErrDenied) || client.IsTransport(err) {
		t.Fatalf("bad token: %v", err)
	}
}
