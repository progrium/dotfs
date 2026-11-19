package integration

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// The sync scenarios from the implementation design, run with real dotfs
// sync processes against a real dotfs serve over WebSocket. Each client
// connects through its own proxy; "offline" means its proxy refuses
// connections while the process keeps running and queueing local changes.

const convergeTimeout = 60 * time.Second

// pair starts a server and two converged clients sharing root "home".
func pair(t *testing.T, serveArgs ...string) (*Server, *Client, *Client) {
	t.Helper()
	srv := NewServer(t, serveArgs...)
	a := NewClient(t, srv, "dev-a", "home").Start()
	b := NewClient(t, srv, "dev-b", "home").Start()
	return srv, a, b
}

func TestBootstrapThreeClients(t *testing.T) {
	srv, a, b := pair(t)
	a.Write("/docs/readme.md", "hello")
	a.Write("/docs/notes/2026.txt", "notes")
	a.Write("/a b/c%d.txt", "odd name")
	a.Write("/ünïcode/ファイル.txt", "utf-8")
	b.Write("/from-b.txt", "b")
	Converge(t, convergeTimeout, a, b)

	// A third client bootstraps into a folder that already has a file.
	c := NewClient(t, srv, "dev-c", "home")
	c.Write("/local-first.txt", "c was here")
	c.Start()
	got := Converge(t, convergeTimeout, a, b, c)
	expect(t, got, map[string]string{
		"/docs/readme.md": "hello", "/docs/notes/2026.txt": "notes", "/a b/c%d.txt": "odd name",
		"/ünïcode/ファイル.txt": "utf-8", "/from-b.txt": "b", "/local-first.txt": "c was here",
	})

	// Edits, a delete, and renames of a file and a directory flow to all.
	b.Write("/docs/readme.md", "hello again")
	c.Remove("/from-b.txt")
	a.Rename("/docs/notes", "/archive/notes")
	c.Rename("/local-first.txt", "/renamed.txt")
	got = Converge(t, convergeTimeout, a, b, c)
	expect(t, got, map[string]string{
		"/docs/readme.md": "hello again", "/archive/notes/2026.txt": "notes", "/a b/c%d.txt": "odd name",
		"/ünïcode/ファイル.txt": "utf-8", "/renamed.txt": "c was here",
	})
}

func TestConcurrentEdit(t *testing.T) {
	_, a, b := pair(t)
	a.Write("/Documents/Tax 2025.pdf", "original")
	Converge(t, convergeTimeout, a, b)

	b.Proxy.SetOffline(true)
	a.Write("/Documents/Tax 2025.pdf", "a edit")
	time.Sleep(time.Second) // a commits first
	b.Write("/Documents/Tax 2025.pdf", "b edit")
	time.Sleep(500 * time.Millisecond)
	b.Proxy.SetOffline(false)

	got := Converge(t, convergeTimeout, a, b)
	cc := conflictCopies(got)
	if len(cc) != 1 {
		t.Fatalf("conflict copies %v, want one:\n%s", cc, show(got))
	}
	// The client that committed second (b) keeps its edit in the copy.
	if !strings.HasPrefix(cc[0], "/Documents/Tax 2025.sync-conflict-") || !strings.HasSuffix(cc[0], "-dev-b.pdf") {
		t.Errorf("conflict copy named %s, want Tax 2025.sync-conflict-<time>-dev-b.pdf", cc[0])
	}
	if got["/Documents/Tax 2025.pdf"] != "a edit" || got[cc[0]] != "b edit" {
		t.Errorf("file %q and copy %q, want a's edit and b's edit", got["/Documents/Tax 2025.pdf"], got[cc[0]])
	}
}

func TestConcurrentEditBothOnline(t *testing.T) {
	// Both clients online and editing at once: whichever commits second
	// makes the one conflict copy, and nothing is lost.
	_, a, b := pair(t)
	a.Write("/race.txt", "v0")
	Converge(t, convergeTimeout, a, b)
	a.Write("/race.txt", "from a")
	b.Write("/race.txt", "from b")
	got := Converge(t, convergeTimeout, a, b)
	cc := conflictCopies(got)
	if len(cc) > 1 {
		t.Fatalf("conflict copies %v, want at most one:\n%s", cc, show(got))
	}
	vals := []string{got["/race.txt"]}
	for _, c := range cc {
		vals = append(vals, got[c])
	}
	joined := strings.Join(vals, ",")
	// Either one edit landed after the other was seen (no copy), or both
	// survive.
	if len(cc) == 1 && !(strings.Contains(joined, "from a") && strings.Contains(joined, "from b")) {
		t.Fatalf("an edit was lost: %v", vals)
	}
	if len(cc) == 0 && got["/race.txt"] != "from a" && got["/race.txt"] != "from b" {
		t.Fatalf("race.txt = %q", got["/race.txt"])
	}
}

func TestEditVersusDelete(t *testing.T) {
	_, a, b := pair(t)
	a.Write("/edit-here.txt", "v1")
	a.Write("/delete-here.txt", "v1")
	a.Write("/dir/kept.txt", "v1")
	a.Write("/dir/other.txt", "v1")
	Converge(t, convergeTimeout, a, b)

	b.Proxy.SetOffline(true)
	// a deletes what b edits, and edits what b deletes.
	a.Remove("/edit-here.txt")
	a.Write("/delete-here.txt", "a edit")
	a.Remove("/dir")
	time.Sleep(time.Second)
	b.Write("/edit-here.txt", "b edit")
	b.Remove("/delete-here.txt")
	b.Write("/dir/kept.txt", "b edit")
	time.Sleep(500 * time.Millisecond)
	b.Proxy.SetOffline(false)

	got := Converge(t, convergeTimeout, a, b)
	expect(t, got, map[string]string{
		"/edit-here.txt": "b edit", "/delete-here.txt": "a edit", "/dir/kept.txt": "b edit",
	})
}

func TestRenameVersusEdit(t *testing.T) {
	_, a, b := pair(t)
	a.Write("/report.txt", "draft")
	a.Write("/proj/src/main.go", "v1")
	a.Write("/proj/src/util.go", "v1")
	Converge(t, convergeTimeout, a, b)

	b.Proxy.SetOffline(true)
	a.Rename("/report.txt", "/final/report.txt")
	a.Rename("/proj", "/project")
	time.Sleep(time.Second)
	b.Write("/report.txt", "edited")
	b.Write("/proj/src/main.go", "v2")
	time.Sleep(500 * time.Millisecond)
	b.Proxy.SetOffline(false)

	got := Converge(t, convergeTimeout, a, b)
	expect(t, got, map[string]string{
		"/final/report.txt": "edited", "/project/src/main.go": "v2", "/project/src/util.go": "v1",
	})
}

func TestLongOffline(t *testing.T) {
	_, a, b := pair(t)
	for i := range 20 {
		a.Write(fmt.Sprintf("/shared/%02d.txt", i), "v1")
	}
	Converge(t, convergeTimeout, a, b)

	b.Proxy.SetOffline(true)
	for i := range 300 {
		a.Write(fmt.Sprintf("/from-a/%03d.txt", i), fmt.Sprint("a", i))
		b.Write(fmt.Sprintf("/from-b/%03d.txt", i), fmt.Sprint("b", i))
	}
	for i := range 10 {
		a.Write(fmt.Sprintf("/shared/%02d.txt", i), "a edit")
	}
	time.Sleep(2 * time.Second) // a's edits commit before b's
	for i := 5; i < 15; i++ {
		b.Write(fmt.Sprintf("/shared/%02d.txt", i), "b edit")
	}
	b.Remove("/shared/19.txt")
	b.Rename("/shared/18.txt", "/moved/18.txt")
	// b restarts while offline; its queue survives.
	time.Sleep(time.Second)
	b.Stop()
	b.Start()
	time.Sleep(time.Second)
	b.Proxy.SetOffline(false)

	got := Converge(t, 2*convergeTimeout, a, b)
	if n := len(conflictCopies(got)); n != 5 {
		t.Errorf("got %d conflict copies, want 5 (files 05 to 09)", n)
	}
	for i := range 20 {
		p := fmt.Sprintf("/shared/%02d.txt", i)
		want := "v1"
		switch {
		case i < 10:
			want = "a edit"
		case i < 15:
			want = "b edit"
		case i >= 18:
			want = ""
		}
		if got[p] != want {
			t.Errorf("%s = %q, want %q", p, got[p], want)
		}
	}
	for i := range 300 {
		if got[fmt.Sprintf("/from-a/%03d.txt", i)] != fmt.Sprint("a", i) || got[fmt.Sprintf("/from-b/%03d.txt", i)] != fmt.Sprint("b", i) {
			t.Fatalf("change %d missing", i)
		}
	}
	if got["/moved/18.txt"] != "v1" {
		t.Errorf("/moved/18.txt = %q", got["/moved/18.txt"])
	}
}

func TestResetBehindRetention(t *testing.T) {
	// The server keeps only 50 changes; b misses 200.
	_, a, b := pair(t, "-keep-log", "50")
	a.Write("/stay.txt", "s")
	a.Write("/gone.txt", "g")
	a.Write("/edit.txt", "v1")
	Converge(t, convergeTimeout, a, b)

	b.Proxy.SetOffline(true)
	a.Write("/edit.txt", "v2")
	a.Remove("/gone.txt")
	a.Rename("/stay.txt", "/moved.txt")
	for i := range 200 {
		a.Write(fmt.Sprintf("/bulk/%03d", i), fmt.Sprint(i))
	}
	b.Write("/local.txt", "while offline")
	time.Sleep(3 * time.Second)
	b.Proxy.SetOffline(false)

	got := Converge(t, convergeTimeout, a, b)
	want := map[string]string{"/edit.txt": "v2", "/moved.txt": "s", "/local.txt": "while offline"}
	for i := range 200 {
		want[fmt.Sprintf("/bulk/%03d", i)] = fmt.Sprint(i)
	}
	expect(t, got, want)
	if !strings.Contains(b.Log(), "events reset") {
		t.Errorf("b recovered without an events reset:\n%s", tail(b.Log(), 20))
	}
}
