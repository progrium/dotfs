package auth_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hugelgupf/p9/linux"
	"github.com/hugelgupf/p9/p9"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/auth"
	"github.com/progrium/dotfs/server/fake"
	"github.com/progrium/dotfs/server/ninep"
)

var ctx = context.Background()

func TestParseAttachName(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want auth.AttachName
		bad  bool
	}{
		{in: "home", want: auth.AttachName{Root: "home"}},
		{in: "/alice/home", want: auth.AttachName{User: "alice", Root: "home"}},
		{in: "home?token=abc", want: auth.AttachName{Root: "home", Token: "abc"}},
		{in: "", bad: true},
		{in: "a/b/c", bad: true},
		{in: "home?x=1", bad: true},
		{in: "alice/..", bad: true},
	} {
		got, err := auth.ParseAttachName(tc.in)
		if (err != nil) != tc.bad || (!tc.bad && got != tc.want) {
			t.Errorf("ParseAttachName(%q) = %+v, %v", tc.in, got, err)
		}
	}
}

func TestPolicy(t *testing.T) {
	tk := fake.NewTokens()
	tok, _ := tk.CreateToken(ctx, "alice")
	p := auth.Policy{Tokens: tk}
	for _, tc := range []struct {
		conn, uname, aname string
		want               server.Root
	}{
		{"", "", "alice/home", "alice/home"},
		{"", "alice", "home", "alice/home"},
		{"", "alice", "bob/home", ""},
		{"", "", "home", ""},
		{"alice", "", "home", "alice/home"},
		{"alice", "bob", "home", "alice/home"},
		{"alice", "", "bob/home", ""},
		{"", "", "home?token=" + tok, "alice/home"},
		{"", "bob", "home?token=" + tok, "alice/home"},
		{"bob", "", "home?token=" + tok, ""},
		{"", "", "home?token=nope", ""},
	} {
		got, err := p.Attach(ctx, tc.conn, tc.uname, tc.aname)
		if got != tc.want || (tc.want == "") != errors.Is(err, fsproto.ErrDenied) {
			t.Errorf("Attach(%q, %q, %q) = %q, %v; want %q", tc.conn, tc.uname, tc.aname, got, err, tc.want)
		}
	}
	p.RequireToken = true
	if _, err := p.Attach(ctx, "", "alice", "home"); !errors.Is(err, fsproto.ErrDenied) {
		t.Errorf("RequireToken without token: %v", err)
	}
	if _, err := p.Attach(ctx, "alice", "", "home"); err != nil {
		t.Errorf("RequireToken with transport user: %v", err)
	}
}

func TestWebSocket(t *testing.T) {
	fs := fake.NewServer()
	fs.CreateRoot("alice", "home")
	fs.CreateRoot("bob", "home")
	srv := ninep.New(ninep.Config{Meta: fs.Meta, Store: fs.Store, Hub: fs.Hub, Auth: auth.Policy{Tokens: fs.Tokens}})
	defer srv.Close()
	mux := http.NewServeMux()
	mux.Handle("/9p", &auth.WebSocket{Tokens: fs.Tokens, Server: srv, Logf: t.Logf})
	hs := httptest.NewServer(mux)
	defer hs.Close()
	url := "ws" + strings.TrimPrefix(hs.URL, "http") + "/9p"

	if _, err := auth.Dial(ctx, url, ""); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("dial without token: %v", err)
	}
	if _, err := auth.Dial(ctx, url, "bogus"); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("dial with bad token: %v", err)
	}

	tok, _ := fs.Tokens.CreateToken(ctx, "alice")
	conn, err := auth.Dial(ctx, url, tok)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := p9.NewClient(conn)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := cl.Attach("bob/home"); !errors.Is(err, linux.EACCES) {
		t.Fatalf("attach another user's root: %v", err)
	}
	// Even a local-style token in the attach name cannot switch users.
	bobTok, _ := fs.Tokens.CreateToken(ctx, "bob")
	if _, err := cl.Attach("home?token=" + bobTok); !errors.Is(err, linux.EACCES) {
		t.Fatalf("attach with another user's token: %v", err)
	}
	root, err := cl.Attach("home")
	if err != nil {
		t.Fatal(err)
	}
	_, ctl, err := root.Walk([]string{".fs", "ctl"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := ctl.Open(p9.ReadWrite); err != nil {
		t.Fatal(err)
	}
	ctl.WriteAt([]byte("mkdir /over-ws"), 0)
	buf := make([]byte, 100)
	n, _ := ctl.ReadAt(buf, 0)
	if string(buf[:n]) != "ok 1\n" {
		t.Fatalf("ctl over websocket: %q", buf[:n])
	}

	// The cookie works too, for browsers.
	req, _ := http.NewRequest("GET", hs.URL+"/9p", nil)
	req.AddCookie(&http.Cookie{Name: auth.CookieName, Value: tok})
	if got := auth.TokenFromRequest(req); got != tok {
		t.Fatalf("cookie token %q", got)
	}
}
