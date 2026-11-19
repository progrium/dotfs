// Package integration runs the real dotfs binary as separate processes: one
// dotfs serve, several dotfs sync clients connected over WebSocket through
// fault-injecting proxies, and dotfs conform against the server.
//
// The tests build ./cmd/dotfs once in TestMain, with the race detector
// when DOTFS_RACE is set. They are skipped with -short.
package integration

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// bin is the dotfs binary built for this test run.
var bin string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	dir, err := os.MkdirTemp("", "dotfs-integration-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	bin = filepath.Join(dir, "dotfs")
	args := []string{"build", "-o", bin}
	if os.Getenv("DOTFS_RACE") != "" {
		args = append(args, "-race")
	}
	cmd := exec.Command("go", append(args, "github.com/progrium/dotfs/cmd/dotfs")...)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build dotfs:", err)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func needBinary(t testing.TB) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration tests run the dotfs binary; skipped with -short")
	}
}

// nextPort hands out ports below Linux's ephemeral range (32768 and up),
// so a port given to one test's server is never also picked by the kernel
// for another test's port-0 listener while the tests run in parallel. The
// start is spread by process ID in case two test binaries run at once.
var (
	portMu   sync.Mutex
	nextPort = 20000 + os.Getpid()%500*20
)

// freeAddr returns a loopback address with a port that is free and that no
// other test in this process has been given.
func freeAddr(t testing.TB) string {
	t.Helper()
	portMu.Lock()
	defer portMu.Unlock()
	for range 1000 {
		nextPort++
		if nextPort >= 32000 {
			nextPort = 20000
		}
		addr := fmt.Sprintf("127.0.0.1:%d", nextPort)
		if l, err := net.Listen("tcp", addr); err == nil {
			l.Close()
			return addr
		}
	}
	t.Fatal("no free port")
	return ""
}

// logBuf collects a process's output and keeps it for the test log.
type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// proc is one running dotfs process.
type proc struct {
	cmd  *exec.Cmd
	out  *logBuf
	done chan struct{}
}

func start(t testing.TB, out *logBuf, env []string, args ...string) *proc {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Stdout, cmd.Stderr = out, out
	cmd.Env = append(os.Environ(), env...)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &proc{cmd: cmd, out: out, done: make(chan struct{})}
	go func() { cmd.Wait(); close(p.done) }()
	return p
}

// stop ends the process with sig and waits for it.
func (p *proc) stop(sig os.Signal) {
	if p == nil {
		return
	}
	select {
	case <-p.done:
		return
	default:
	}
	p.cmd.Process.Signal(sig)
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		p.cmd.Process.Kill()
		<-p.done
	}
}

// Server is a dotfs serve process with its data dir.
type Server struct {
	t     testing.TB
	Data  string
	TCP   string // 9P over TCP
	HTTP  string // 9P over WebSocket at /9p
	Args  []string
	out   *logBuf
	p     *proc
	token string
}

// NewServer starts dotfs serve on a fresh data dir with extra flags.
func NewServer(t testing.TB, args ...string) *Server {
	t.Helper()
	needBinary(t)
	s := &Server{t: t, Data: t.TempDir(), TCP: freeAddr(t), HTTP: freeAddr(t), Args: args, out: &logBuf{}}
	s.Start()
	t.Cleanup(func() {
		s.Stop()
		checkRaces(t, "serve", s.out.String())
		if t.Failed() {
			t.Logf("serve log:\n%s", tail(s.out.String(), 60))
		}
	})
	return s
}

// Start starts (or restarts) the server on the same data dir and addresses.
func (s *Server) Start() {
	s.t.Helper()
	args := append([]string{"serve", "-data", s.Data, "-tcp", s.TCP, "-http", s.HTTP}, s.Args...)
	s.p = start(s.t, s.out, nil, args...)
	deadline := time.Now().Add(20 * time.Second)
	for {
		c, err := net.Dial("tcp", s.HTTP)
		if err == nil {
			c.Close()
			return
		}
		select {
		case <-s.p.done:
			s.t.Fatalf("dotfs serve exited:\n%s", tail(s.out.String(), 30))
		default:
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("dotfs serve did not listen on %s:\n%s", s.HTTP, tail(s.out.String(), 30))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Stop shuts the server down cleanly.
func (s *Server) Stop() { s.p.stop(syscall.SIGTERM) }

// Kill kills the server with SIGKILL, as a crash would.
func (s *Server) Kill() { s.p.stop(syscall.SIGKILL) }

// Log returns the server's output so far.
func (s *Server) Log() string { return s.out.String() }

// Token returns a WebSocket token for user alice, creating it once. The
// server must be running, since the token table lives in its database.
func (s *Server) Token() string {
	s.t.Helper()
	if s.token != "" {
		return s.token
	}
	out, err := exec.Command(bin, "token", "create", "-data", s.Data, "alice").CombinedOutput()
	if err != nil {
		s.t.Fatalf("token create: %v\n%s", err, out)
	}
	s.token = strings.TrimSpace(string(out))
	return s.token
}

// Client is a dotfs sync process for one folder, connected through its own
// proxy so a test can cut, delay, or block its connection.
type Client struct {
	t      testing.TB
	srv    *Server
	Name   string
	Dir    string
	Root   string
	Proxy  *Proxy
	Flags  []string
	out    *logBuf
	p      *proc
	starts int
}

// NewClient makes a client for root on srv. It is not started.
func NewClient(t testing.TB, srv *Server, name, root string) *Client {
	t.Helper()
	c := &Client{t: t, srv: srv, Name: name, Dir: t.TempDir(), Root: root, out: &logBuf{}}
	c.Proxy = NewProxy(t, srv.HTTP)
	t.Cleanup(func() {
		c.Stop()
		checkRaces(t, name, c.out.String())
		if t.Failed() {
			t.Logf("%s log:\n%s", name, tail(c.out.String(), 60))
		}
	})
	return c
}

// Start runs dotfs sync in the background.
func (c *Client) Start() *Client {
	c.t.Helper()
	c.starts++
	url := fmt.Sprintf("ws://%s/9p/%s", c.Proxy.Addr(), c.Root)
	fmt.Fprintf(c.out, "--- start %d\n", c.starts)
	args := []string{"sync", "-device", c.Name, "-debounce", "100ms", "-scan", "2s"}
	args = append(args, c.Flags...)
	args = append(args, url, c.Dir)
	c.p = start(c.t, c.out, []string{"DOTFS_TOKEN=" + c.srv.Token()}, args...)
	return c
}

// Once runs dotfs sync -once in the foreground and returns its error.
func (c *Client) Once() error {
	c.t.Helper()
	url := fmt.Sprintf("ws://%s/9p/%s", c.Proxy.Addr(), c.Root)
	cmd := exec.Command(bin, "sync", "-once", "-device", c.Name, url, c.Dir)
	cmd.Env = append(os.Environ(), "DOTFS_TOKEN="+c.srv.Token())
	cmd.Stdout, cmd.Stderr = c.out, c.out
	return cmd.Run()
}

// Goroutines ends a running sync process with SIGQUIT and returns the
// goroutine dump the Go runtime prints, for diagnosing a stuck client.
func (c *Client) Goroutines() string {
	if c.p == nil {
		return ""
	}
	select {
	case <-c.p.done:
		return ""
	default:
	}
	before := len(c.out.String())
	c.p.stop(syscall.SIGQUIT)
	out := c.out.String()
	if before > len(out) {
		return ""
	}
	return out[before:]
}

// Stop stops the sync process cleanly.
func (c *Client) Stop() { c.p.stop(syscall.SIGTERM) }

// Kill kills the sync process.
func (c *Client) Kill() { c.p.stop(syscall.SIGKILL) }

// Log returns the client's output so far.
func (c *Client) Log() string { return c.out.String() }

func (c *Client) path(p string) string {
	return filepath.Join(c.Dir, filepath.FromSlash(strings.TrimPrefix(p, "/")))
}

// Write writes a file, making parent directories.
func (c *Client) Write(p, content string) {
	c.t.Helper()
	full := c.path(p)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		c.t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		c.t.Fatal(err)
	}
}

// Remove removes a file or tree.
func (c *Client) Remove(p string) {
	c.t.Helper()
	if err := os.RemoveAll(c.path(p)); err != nil {
		c.t.Fatal(err)
	}
}

// Rename renames, making the destination's parent.
func (c *Client) Rename(from, to string) {
	c.t.Helper()
	if err := os.MkdirAll(filepath.Dir(c.path(to)), 0o755); err != nil {
		c.t.Fatal(err)
	}
	if err := os.Rename(c.path(from), c.path(to)); err != nil {
		c.t.Fatal(err)
	}
}

// Tree returns the folder as a map from slash path to content, with
// directories ending in "/" mapped to "". The state dir is left out.
func (c *Client) Tree() map[string]string { return readTree(c.t, c.Dir) }

func readTree(t testing.TB, dir string) map[string]string {
	t.Helper()
	m := map[string]string{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil // changed under us; the next poll sees it
			}
			return err
		}
		if p == dir {
			return nil
		}
		rel := "/" + filepath.ToSlash(strings.TrimPrefix(p, dir+string(filepath.Separator)))
		if rel == "/.dotfs" {
			return filepath.SkipDir
		}
		switch {
		case d.IsDir():
			m[rel+"/"] = ""
		case d.Type()&fs.ModeSymlink != 0:
			tgt, _ := os.Readlink(p)
			m[rel] = "-> " + tgt
		default:
			b, err := os.ReadFile(p)
			if err != nil && !os.IsNotExist(err) {
				return err
			}
			if err == nil {
				m[rel] = string(b)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// Converge waits until every client's folder has the same tree and the
// trees stay the same for settle, then returns the tree.
func Converge(t testing.TB, timeout time.Duration, cs ...*Client) map[string]string {
	t.Helper()
	const settle = 1500 * time.Millisecond
	deadline := time.Now().Add(timeout)
	var last string
	var since time.Time
	for {
		trees := make([]map[string]string, len(cs))
		for i, c := range cs {
			trees[i] = c.Tree()
		}
		same := true
		for _, tr := range trees[1:] {
			if !equal(trees[0], tr) {
				same = false
			}
		}
		key := show(trees[0])
		if same {
			if key != last {
				last, since = key, time.Now()
			} else if time.Since(since) >= settle {
				return trees[0]
			}
		} else {
			last = ""
		}
		if time.Now().After(deadline) {
			var b strings.Builder
			for i, c := range cs {
				fmt.Fprintf(&b, "%s:\n%s\n", c.Name, show(trees[i]))
			}
			for _, c := range cs {
				if st := c.Goroutines(); st != "" {
					t.Logf("%s goroutines:\n%s", c.Name, st)
				}
			}
			t.Fatalf("clients did not converge in %s:\n%s", timeout, b.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// WaitFor polls until cond holds.
func WaitFor(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func equal(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

func show(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		v := m[k]
		if len(v) > 40 {
			v = fmt.Sprintf("%s... (%d bytes)", v[:40], len(v))
		}
		fmt.Fprintf(&b, "  %s %q\n", k, v)
	}
	return b.String()
}

// conflictCopies lists the paths that are conflict copies.
func conflictCopies(m map[string]string) []string {
	var out []string
	for k := range m {
		if strings.Contains(k, ".sync-conflict-") {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func tail(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = append([]string{fmt.Sprintf("... (%d lines before)", len(lines)-n)}, lines[len(lines)-n:]...)
	}
	return strings.Join(lines, "\n")
}

// expect checks that got has exactly the entries in want, ignoring
// directories want does not mention.
func expect(t testing.TB, got, want map[string]string) {
	t.Helper()
	for k, v := range want {
		if g, ok := got[k]; !ok {
			t.Errorf("missing %s", k)
		} else if g != v {
			t.Errorf("%s = %q, want %q", k, g, v)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok && !strings.HasSuffix(k, "/") {
			t.Errorf("unexpected %s", k)
		}
	}
	if t.Failed() {
		t.Logf("tree:\n%s", show(got))
	}
}

// countLines counts lines of s that contain sub.
func countLines(s, sub string) int {
	n := 0
	sc := bufio.NewScanner(strings.NewReader(s))
	for sc.Scan() {
		if strings.Contains(sc.Text(), sub) {
			n++
		}
	}
	return n
}

// get fetches an HTTP URL, for checking the server is up.
func get(ctx context.Context, url string) (int, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, nil
}

// checkRaces fails the test if a race-enabled dotfs reported a data race.
func checkRaces(t testing.TB, who, out string) {
	t.Helper()
	if i := strings.Index(out, "WARNING: DATA RACE"); i >= 0 {
		t.Errorf("%s reported a data race:\n%s", who, tail(out[i:], 80))
	}
}
