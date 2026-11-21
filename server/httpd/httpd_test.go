package httpd_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/events"
	"github.com/progrium/dotfs/server/fake"
	"github.com/progrium/dotfs/server/httpd"
	"github.com/progrium/dotfs/server/meta"
	"github.com/progrium/dotfs/server/store"
)

var bg = context.Background()

type env struct {
	t    *testing.T
	url  string // the root's base URL, ending in "/"
	tok  string
	hub  server.Hub
	home server.Scope
}

// newEnv serves alice's root "home" from the real store and metadata, or
// from the fakes.
func newEnv(t *testing.T, real bool) *env {
	t.Helper()
	var m server.Meta
	var st server.Store
	var hub server.Hub
	var tk server.Tokens
	if real {
		dir := t.TempDir()
		s, err := store.New(filepath.Join(dir, "data"), &store.Options{NoSync: true})
		if err != nil {
			t.Fatal(err)
		}
		mm, err := meta.Open(filepath.Join(dir, "meta.db"), s, nil, &meta.Options{NoSync: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { mm.Close() })
		h := events.New(mm, events.Options{})
		mm.SetPublisher(h)
		m, st, hub, tk = mm, s, h, mm
	} else {
		fs := fake.NewServer()
		m, st, hub, tk = fs.Meta, fs.Store, fs.Hub, fs.Tokens
	}
	tok, err := tk.CreateToken(bg, "alice")
	if err != nil {
		t.Fatal(err)
	}
	h := httpd.New(httpd.Config{Meta: m, Store: st, Hub: hub, Tokens: tk, CreateRoots: true,
		Keepalive: 50 * time.Millisecond})
	mux := http.NewServeMux()
	mux.Handle("/r/", h)
	hs := httptest.NewServer(mux)
	t.Cleanup(hs.Close)
	return &env{t: t, url: hs.URL + "/r/home/", tok: tok, hub: hub,
		home: server.RootScope(server.NewRoot("alice", "home"))}
}

func both(t *testing.T, fn func(t *testing.T, e *env)) {
	t.Run("real", func(t *testing.T) { fn(t, newEnv(t, true)) })
	t.Run("fake", func(t *testing.T) { fn(t, newEnv(t, false)) })
}

type resp struct {
	code int
	h    http.Header
	body string
}

func (e *env) do(method, path, body string, hdr ...string) resp {
	e.t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, e.url+path, rd)
	if err != nil {
		e.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+e.tok)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		e.t.Fatal(err)
	}
	return resp{res.StatusCode, res.Header, string(b)}
}

func (r resp) want(t *testing.T, code int, body string) {
	t.Helper()
	if r.code != code || (body != "*" && r.body != body) {
		t.Fatalf("got %d %q, want %d %q", r.code, r.body, code, body)
	}
}

func TestAuth(t *testing.T) {
	e := newEnv(t, false)
	for _, tok := range []string{"", "wrong"} {
		e.tok = tok
		for _, p := range []string{"", ".fs/index", "a.txt", ".fs/ctl"} {
			if r := e.do("GET", p, ""); r.code != http.StatusUnauthorized {
				t.Errorf("token %q, GET %s: %d", tok, p, r.code)
			}
		}
	}
}

func TestDiscovery(t *testing.T) {
	both(t, func(t *testing.T, e *env) {
		e.do("GET", ".fs/", "").want(t, 200, "index\nevents\nctl\nstage/\n")
		e.do("GET", ".fs/nope", "").want(t, 404, "*")
		r := e.do("POST", ".fs/ctl", "caps")
		if r.code != 200 || !strings.HasPrefix(r.body, "ok caps hello mkdir") {
			t.Fatalf("caps: %d %q", r.code, r.body)
		}
	})
}

func TestPutGetDelete(t *testing.T) {
	both(t, func(t *testing.T, e *env) {
		r := e.do("PUT", "a%20b.txt", "hello", "If-None-Match", "*", "Dotfs-Mtime", "1763491250", "Dotfs-Mode", "0600")
		r.want(t, 201, "*")
		v := r.h.Get("ETag")
		ent, err := fsproto.ParseEntry(r.h.Get("Dotfs-Entry"))
		if err != nil || ent.Path != "/a b.txt" || ent.Mode != 0o600 || ent.Mtime != 1763491250 || ent.Length != 5 {
			t.Fatalf("entry %+v, %v", ent, err)
		}
		if v != fmt.Sprintf("%q", fmt.Sprint(ent.Version)) || r.h.Get("Dotfs-Seq") != fmt.Sprint(ent.Version) {
			t.Fatalf("ETag %s, seq %s, entry version %d", v, r.h.Get("Dotfs-Seq"), ent.Version)
		}
		if !strings.HasPrefix(r.body, fmt.Sprintf("ok %d b3:", ent.Version)) {
			t.Fatalf("body %q", r.body)
		}

		g := e.do("GET", "a%20b.txt", "")
		g.want(t, 200, "hello")
		if g.h.Get("ETag") != v {
			t.Fatalf("GET ETag %s, want %s", g.h.Get("ETag"), v)
		}
		e.do("GET", "a%20b.txt", "", "If-None-Match", v).want(t, 304, "")
		e.do("GET", "a%20b.txt", "", "Range", "bytes=1-3").want(t, 206, "ell")
		h := e.do("HEAD", "a%20b.txt", "")
		h.want(t, 200, "")
		if h.h.Get("Dotfs-Entry") == "" {
			t.Fatal("HEAD without Dotfs-Entry")
		}

		// Creating again conflicts, and so does a stale base.
		c := e.do("PUT", "a%20b.txt", "x", "If-None-Match", "*")
		c.want(t, 409, fmt.Sprintf("conflict %d\n", ent.Version))
		if c.h.Get("ETag") != v {
			t.Fatalf("conflict ETag %s", c.h.Get("ETag"))
		}
		e.do("PUT", "a%20b.txt", "x", "If-Match", `"12345"`).want(t, 409, "*")

		// A matching base replaces it and keeps the mode.
		u := e.do("PUT", "a%20b.txt", "bye", "If-Match", v)
		u.want(t, 200, "*")
		ent2, _ := fsproto.ParseEntry(u.h.Get("Dotfs-Entry"))
		if ent2.Mode != 0o600 || ent2.Version <= ent.Version {
			t.Fatalf("after update %+v", ent2)
		}
		// An unconditional PUT is a plain write.
		e.do("PUT", "a%20b.txt", "plain", "").want(t, 200, "*")
		e.do("GET", "a%20b.txt", "").want(t, 200, "plain")

		// A wrong hash publishes nothing.
		e.do("PUT", "h.txt", "abc", "Dotfs-Hash", fsproto.HashBytes([]byte("xyz"))).want(t, 422, "*")
		e.do("GET", "h.txt", "").want(t, 404, "*")
		e.do("PUT", "h.txt", "abc", "Dotfs-Hash", fsproto.HashBytes([]byte("abc"))).want(t, 201, "*")

		// No implicit parents, and .fs is reserved.
		e.do("PUT", "no/such.txt", "x").want(t, 404, "*")
		e.do("PUT", ".fs/foo", "x").want(t, 404, "*")

		cur := e.do("HEAD", "a%20b.txt", "").h.Get("ETag")
		e.do("DELETE", "a%20b.txt", "", "If-Match", `"12345"`).want(t, 409, "*")
		d := e.do("DELETE", "a%20b.txt", "", "If-Match", cur)
		d.want(t, 204, "")
		if d.h.Get("Dotfs-Seq") == "" {
			t.Fatal("DELETE without Dotfs-Seq")
		}
		e.do("GET", "a%20b.txt", "").want(t, 404, "*")
		e.do("DELETE", "a%20b.txt", "").want(t, 404, "*")
	})
}

func TestDirectories(t *testing.T) {
	both(t, func(t *testing.T, e *env) {
		e.do("POST", ".fs/ctl", "mkdir /d\nmkdir /d/e").want(t, 200, "*")
		e.do("PUT", "d/e/f.txt", "f").want(t, 201, "*")
		e.do("PUT", "d/g.txt", "g").want(t, 201, "*")
		r := e.do("GET", "d/", "")
		r.want(t, 200, "*")
		lines := strings.Split(strings.TrimSuffix(r.body, "\n"), "\n")
		if len(lines) != 4 || !strings.HasPrefix(lines[0], "fsindex 1 seq ") ||
			!strings.HasPrefix(lines[1], "/d d ") || !strings.HasPrefix(lines[2], "/d/e d ") || !strings.HasPrefix(lines[3], "/d/g.txt f ") {
			t.Fatalf("listing:\n%s", r.body)
		}
		r = e.do("GET", "d?depth=infinity", "")
		if n := strings.Count(r.body, "\n"); r.code != 200 || n != 5 || !strings.Contains(r.body, "/d/e/f.txt f ") {
			t.Fatalf("depth=infinity: %d\n%s", r.code, r.body)
		}
		e.do("DELETE", "d", "").want(t, 400, "*")
		e.do("PUT", "d", "x").want(t, 400, "*")
	})
}

func TestIndex(t *testing.T) {
	both(t, func(t *testing.T, e *env) {
		e.do("PUT", "a.txt", "a").want(t, 201, "*")
		r := e.do("GET", ".fs/index", "")
		r.want(t, 200, "*")
		seq := r.h.Get("Dotfs-Seq")
		if !strings.HasPrefix(r.body, "fsindex 1 seq "+seq+"\n/ d ") || !strings.Contains(r.body, "\n/a.txt f ") {
			t.Fatalf("index:\n%s", r.body)
		}
		e.do("GET", ".fs/index", "", "If-None-Match", `"`+seq+`"`).want(t, 304, "")

		// http.Client does not decode zstd, so the raw body arrives.
		z := e.do("GET", ".fs/index", "", "Accept-Encoding", "zstd")
		if z.code != 200 || z.h.Get("Content-Encoding") != "zstd" {
			t.Fatalf("zstd: %d %q", z.code, z.h.Get("Content-Encoding"))
		}
		d, _ := zstd.NewReader(strings.NewReader(z.body))
		plain, err := io.ReadAll(d)
		if err != nil || string(plain) != r.body {
			t.Fatalf("decoded %q, %v; want %q", plain, err, r.body)
		}
	})
}

func TestCtl(t *testing.T) {
	both(t, func(t *testing.T, e *env) {
		e.do("PUT", "a", "a").want(t, 201, "*")
		r := e.do("POST", ".fs/ctl", "mkdir /d\nmv /a /d/a - 0\nrm /nope\nstat /d/a\n", "Dotfs-Device", "dev1")
		lines := strings.Split(r.body, "\n")
		if r.code != 200 || len(lines) != 5 || !strings.HasPrefix(lines[0], "ok ") || !strings.HasPrefix(lines[1], "ok ") ||
			!strings.HasPrefix(lines[2], "missing") || !strings.HasPrefix(lines[3], "ok /d/a f ") {
			t.Fatalf("ctl: %d %q", r.code, r.body)
		}
		e.do("POST", ".fs/ctl", "").want(t, 400, "*")
		e.do("GET", ".fs/ctl", "").want(t, 405, "*")
		e.do("POST", ".fs/ctl", "bogus").want(t, 200, "*")
	})
}

func TestStage(t *testing.T) {
	both(t, func(t *testing.T, e *env) {
		e.do("HEAD", ".fs/stage/up1", "").want(t, 404, "")
		e.do("PUT", ".fs/stage/up1", "hello ").want(t, 204, "")
		h := e.do("HEAD", ".fs/stage/up1", "")
		if h.code != 200 || h.h.Get("Content-Length") != "6" {
			t.Fatalf("HEAD stage: %d %v", h.code, h.h)
		}
		r := e.do("PUT", ".fs/stage/up1", "world", "Content-Range", "bytes 6-10/*")
		if r.code != 204 || r.h.Get("Dotfs-Stage-Size") != "11" {
			t.Fatalf("PUT range: %d %v", r.code, r.h)
		}
		c := e.do("POST", ".fs/ctl", "commit up1 /hw.txt 0 1763491250 0644 hash="+fsproto.HashBytes([]byte("hello world")))
		if c.code != 200 || !strings.HasPrefix(c.body, "ok ") {
			t.Fatalf("commit: %d %q", c.code, c.body)
		}
		e.do("GET", "hw.txt", "").want(t, 200, "hello world")
		e.do("HEAD", ".fs/stage/up1", "").want(t, 404, "")
		e.do("PUT", ".fs/stage/up2", "x").want(t, 204, "")
		e.do("DELETE", ".fs/stage/up2", "").want(t, 204, "")
		e.do("PUT", ".fs/stage/.dotfs-plain-x", "x").want(t, 400, "*")
		e.do("PUT", ".fs/stage/up3", "x", "Content-Range", "bytes x-y/*").want(t, 400, "*")
	})
}

// events opens a stream and returns a function that reads its next line.
func (e *env) events(query string, hdr ...string) (func() string, func()) {
	e.t.Helper()
	ctx, cancel := context.WithCancel(bg)
	req, _ := http.NewRequestWithContext(ctx, "GET", e.url+".fs/events"+query, nil)
	req.Header.Set("Authorization", "Bearer "+e.tok)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	if res.StatusCode != 200 {
		e.t.Fatalf("events: %d", res.StatusCode)
	}
	br := bufio.NewReader(res.Body)
	lines := make(chan string, 100)
	go func() {
		defer close(lines)
		for {
			l, err := br.ReadString('\n')
			if err != nil {
				return
			}
			lines <- strings.TrimSuffix(l, "\n")
		}
	}()
	next := func() string {
		e.t.Helper()
		select {
		case l, ok := <-lines:
			if !ok {
				return "EOF"
			}
			return l
		case <-time.After(5 * time.Second):
			e.t.Fatal("no event")
			return ""
		}
	}
	return next, func() { cancel(); res.Body.Close() }
}

// skip returns the next line that is not a keepalive.
func skip(next func() string, keep string) string {
	for {
		if l := next(); l != keep {
			return l
		}
	}
}

func TestEvents(t *testing.T) {
	both(t, func(t *testing.T, e *env) {
		next, stop := e.events("")
		defer stop()
		first := skip(next, "")
		if !strings.HasPrefix(first, "seq ") {
			t.Fatalf("first record %q", first)
		}
		r := e.do("PUT", "a.txt", "a", "Dotfs-Device", "laptop")
		v := strings.Trim(r.h.Get("ETag"), `"`)
		got := skip(next, "")
		if !strings.HasPrefix(got, v+" put /a.txt f ") || !strings.HasSuffix(got, " dev=laptop") {
			t.Fatalf("put record %q, version %s", got, v)
		}

		// since replays, path filters, and SSE carries ids.
		next2, stop2 := e.events("?since=0&path=/a.txt", "Accept", "text/event-stream")
		defer stop2()
		if l := skip(next2, ":"); l != "id: "+v {
			t.Fatalf("sse id %q", l)
		}
		if l := next2(); !strings.HasPrefix(l, "data: "+v+" put /a.txt") {
			t.Fatalf("sse data %q", l)
		}

		// A long poll with nothing new returns empty after wait; with
		// something waiting it returns at once.
		start := time.Now()
		e.do("GET", ".fs/events?since="+v+"&wait=1", "").want(t, 200, "")
		if time.Since(start) < 900*time.Millisecond {
			t.Fatal("long poll returned early")
		}
		lp := e.do("GET", ".fs/events?since=0&wait=10", "")
		if lp.code != 200 || !strings.HasPrefix(lp.body, v+" put /a.txt") {
			t.Fatalf("long poll: %d %q", lp.code, lp.body)
		}

		// A since after the current seq gets reset, and the stream ends.
		next3, stop3 := e.events("?since=999999")
		defer stop3()
		if l := skip(next3, ""); !strings.HasSuffix(l, " reset") {
			t.Fatalf("reset %q", l)
		}
		if l := skip(next3, ""); l != "EOF" {
			t.Fatalf("after reset %q", l)
		}
	})
}

func TestNestedScope(t *testing.T) {
	e := newEnv(t, true)
	e.do("POST", ".fs/ctl", "mkdir /n").want(t, 200, "*")
	e.do("PUT", "n/x.txt", "x").want(t, 201, "*")
	e.do("POST", ".fs/ctl", "flags /n i").want(t, 200, "*")
	idx := e.do("GET", "n/.fs/index", "")
	if idx.code != 200 || !strings.Contains(idx.body, "\n/x.txt f ") {
		t.Fatalf("nested index: %d\n%s", idx.code, idx.body)
	}
	e.do("GET", "n/x.txt", "").want(t, 200, "x")
	e.do("PUT", "n/y.txt", "y", "If-None-Match", "*").want(t, 201, "*")
	r := e.do("POST", "n/.fs/ctl", "stat /y.txt")
	if !strings.HasPrefix(r.body, "ok /y.txt f ") {
		t.Fatalf("nested stat %q", r.body)
	}
	e.do("GET", "x.txt/.fs/index", "").want(t, 404, "*")
	top := e.do("GET", ".fs/index", "")
	if strings.Contains(top.body, "x.txt") {
		t.Fatalf("parent index lists nested children:\n%s", top.body)
	}
}
