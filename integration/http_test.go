package integration

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// These run one dotfs serve with a sync client over 9P (WebSocket) and one
// over http-dotfs, so every change crosses from one form to the other.

func mixedPair(t *testing.T) (*Server, *Client, *Client) {
	t.Helper()
	srv := NewServer(t)
	a := NewClient(t, srv, "dev-a", "home").Start()
	b := NewClient(t, srv, "dev-b", "home")
	b.HTTP = true
	b.Start()
	return srv, a, b
}

func TestHTTPSyncBothWays(t *testing.T) {
	t.Parallel()
	_, a, b := mixedPair(t)
	a.Write("/docs/readme.md", "hello")
	a.Write("/a b/c%d.txt", "odd name")
	b.Write("/from-http.txt", "b")
	b.Write("/deep/er/file.txt", "deep")
	got := Converge(t, convergeTimeout, a, b)
	expect(t, got, map[string]string{
		"/docs/readme.md": "hello", "/a b/c%d.txt": "odd name",
		"/from-http.txt": "b", "/deep/er/file.txt": "deep",
	})

	b.Write("/docs/readme.md", "edited over http")
	a.Rename("/from-http.txt", "/moved.txt")
	b.Remove("/deep")
	got = Converge(t, convergeTimeout, a, b)
	expect(t, got, map[string]string{
		"/docs/readme.md": "edited over http", "/a b/c%d.txt": "odd name", "/moved.txt": "b",
	})
}

func TestHTTPConflict(t *testing.T) {
	t.Parallel()
	_, a, b := mixedPair(t)
	a.Write("/notes.txt", "original")
	Converge(t, convergeTimeout, a, b)

	b.Proxy.SetOffline(true)
	a.Write("/notes.txt", "a edit")
	time.Sleep(time.Second)
	b.Write("/notes.txt", "b edit")
	time.Sleep(500 * time.Millisecond)
	b.Proxy.SetOffline(false)

	got := Converge(t, convergeTimeout, a, b)
	cc := conflictCopies(got)
	if len(cc) != 1 || got["/notes.txt"] != "a edit" || got[cc[0]] != "b edit" {
		t.Fatalf("want a's edit at the path and b's in one conflict copy:\n%s", show(got))
	}
}

// TestHTTPCurl checks the example session in docs/http-dotfs.md against a
// real dotfs serve, with a plain HTTP client.
func TestHTTPCurl(t *testing.T) {
	t.Parallel()
	srv := NewServer(t)
	base := fmt.Sprintf("http://%s/r/home/", srv.HTTP)
	tok := srv.Token()
	do := func(method, path, body string, hdr ...string) (*http.Response, string) {
		t.Helper()
		var rd io.Reader
		if body != "" {
			rd = strings.NewReader(body)
		}
		req, _ := http.NewRequest(method, base+path, rd)
		req.Header.Set("Authorization", "Bearer "+tok)
		for i := 0; i+1 < len(hdr); i += 2 {
			req.Header.Set(hdr[i], hdr[i+1])
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return res, string(b)
	}
	res, _ := do("PUT", "Notes/todo.md", "x", "If-None-Match", "*")
	if res.StatusCode != 404 {
		t.Fatalf("PUT without parent: %d", res.StatusCode)
	}
	if res, body := do("POST", ".fs/ctl", "mkdir /Notes\nmkdir /Archive"); res.StatusCode != 200 || strings.Count(body, "ok ") != 2 {
		t.Fatalf("mkdir: %d %q", res.StatusCode, body)
	}
	res, _ = do("PUT", "Notes/todo.md", "one", "If-None-Match", "*")
	if res.StatusCode != 201 {
		t.Fatalf("create: %d", res.StatusCode)
	}
	v := res.Header.Get("ETag")
	res, body := do("GET", "Notes/todo.md", "")
	if res.StatusCode != 200 || body != "one" || res.Header.Get("ETag") != v {
		t.Fatalf("get: %d %q %s", res.StatusCode, body, res.Header.Get("ETag"))
	}
	if res, _ = do("PUT", "Notes/todo.md", "two", "If-Match", v); res.StatusCode != 200 {
		t.Fatalf("update: %d", res.StatusCode)
	}
	if res, body = do("PUT", "Notes/todo.md", "three", "If-Match", v); res.StatusCode != 409 || !strings.HasPrefix(body, "conflict ") {
		t.Fatalf("stale update: %d %q", res.StatusCode, body)
	}
	if res, body = do("POST", ".fs/ctl", "mv /Notes/todo.md /Archive/todo.md - 0"); !strings.HasPrefix(body, "ok ") {
		t.Fatalf("mv: %d %q", res.StatusCode, body)
	}
	res, body = do("GET", ".fs/index", "")
	if res.StatusCode != 200 || !strings.Contains(body, "\n/Archive/todo.md f ") {
		t.Fatalf("index: %d\n%s", res.StatusCode, body)
	}
	res, body = do("GET", ".fs/events?since=0&wait=5", "")
	if res.StatusCode != 200 || !strings.Contains(body, " mv /Notes/todo.md /Archive/todo.md") {
		t.Fatalf("events: %d\n%s", res.StatusCode, body)
	}
	tok = "wrong"
	if res, _ = do("GET", ".fs/index", ""); res.StatusCode != 401 {
		t.Fatalf("bad token: %d", res.StatusCode)
	}
}
