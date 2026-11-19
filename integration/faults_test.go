package integration

import (
	"crypto/rand"
	"fmt"
	mrand "math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// rootDir is where srv keeps alice's root on disk.
func rootDir(srv *Server, root string) string {
	return filepath.Join(srv.Data, "roots", "alice", root)
}

// checkServerTree checks the server's files on disk match a converged tree.
func checkServerTree(t *testing.T, srv *Server, root string, want map[string]string) {
	t.Helper()
	got := readTree(t, rootDir(srv, root))
	if !equal(got, want) {
		t.Errorf("server's files differ from the clients':\nserver:\n%s\nclients:\n%s", show(got), show(want))
	}
}

func TestServerCrashDuringCommits(t *testing.T) {
	srv, a, b := pair(t)
	a.Write("/seed.txt", "seed")
	Converge(t, convergeTimeout, a, b)

	// a and b write steadily while the server is killed and restarted
	// at random points.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for _, c := range []*Client{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				case <-time.After(15 * time.Millisecond):
				}
				c.Write(fmt.Sprintf("/%s/%04d.txt", c.Name, i), strings.Repeat(c.Name, i%50+1))
				if i%7 == 6 {
					c.Write(fmt.Sprintf("/%s/%04d.txt", c.Name, i-3), "rewritten")
				}
			}
		}()
	}
	for range 5 {
		time.Sleep(time.Duration(300+mrand.IntN(900)) * time.Millisecond)
		srv.Kill()
		time.Sleep(time.Duration(mrand.IntN(300)) * time.Millisecond)
		srv.Start()
	}
	close(stop)
	wg.Wait()

	got := Converge(t, 2*convergeTimeout, a, b)
	if cc := conflictCopies(got); len(cc) > 0 {
		t.Errorf("crashes made conflict copies: %v", cc)
	}
	// A fresh client bootstraps the same tree, and the server's files
	// match it.
	c := NewClient(t, srv, "dev-c", "home").Start()
	Converge(t, convergeTimeout, a, c)
	checkServerTree(t, srv, "home", got)
	// The restarted server still conforms.
	checkConform(t, conform(t, srv, "conform-after-crash"))
	t.Logf("server restarts: 5; files: %d", len(got))
}

func TestFlakyNetwork(t *testing.T) {
	_, a, b := pair(t)
	a.Proxy.SetDelay(20 * time.Millisecond)
	b.Proxy.SetDelay(5 * time.Millisecond)
	a.Write("/start.txt", "x")
	Converge(t, convergeTimeout, a, b)

	// Cut a's and b's connections at random while both write.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 200 {
			a.Write(fmt.Sprintf("/a/%03d.txt", i), fmt.Sprint(i))
			if i%3 == 0 {
				b.Write(fmt.Sprintf("/b/%03d.txt", i), fmt.Sprint(i))
			}
			if i%30 == 15 {
				b.Rename(fmt.Sprintf("/b/%03d.txt", i-15), fmt.Sprintf("/b/moved-%03d.txt", i-15))
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	for {
		select {
		case <-done:
		case <-time.After(time.Duration(100+mrand.IntN(400)) * time.Millisecond):
			if mrand.IntN(2) == 0 {
				a.Proxy.CutAll()
			} else {
				b.Proxy.CutAll()
			}
			continue
		}
		break
	}
	a.Proxy.SetDelay(0)
	b.Proxy.SetDelay(0)
	got := Converge(t, 2*convergeTimeout, a, b)
	if cc := conflictCopies(got); len(cc) > 0 {
		t.Errorf("dropped connections made conflict copies: %v", cc)
	}
	for i := range 200 {
		if got[fmt.Sprintf("/a/%03d.txt", i)] != fmt.Sprint(i) {
			t.Fatalf("/a/%03d.txt missing or wrong", i)
		}
	}
	t.Logf("cuts: a %d, b %d", a.Proxy.Cuts.Load(), b.Proxy.Cuts.Load())
}

func TestUploadResumesAfterCut(t *testing.T) {
	_, a, b := pair(t)
	Converge(t, convergeTimeout, a, b)
	const size = 48 << 20
	big := make([]byte, size)
	rand.Read(big)
	before := a.Proxy.Up.Load()
	// Cut a's connection twice during the upload: after 12 MiB and again
	// 12 MiB later.
	a.Proxy.CutAfter(12 << 20)
	go func() {
		WaitFor(t, 60*time.Second, "first cut", func() bool { return a.Proxy.Cuts.Load() >= 1 })
		a.Proxy.CutAfter(12 << 20)
	}()
	if err := os.WriteFile(filepath.Join(a.Dir, "big.bin"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	WaitFor(t, 2*convergeTimeout, "big.bin on b", func() bool {
		got, err := os.ReadFile(filepath.Join(b.Dir, "big.bin"))
		return err == nil && string(got) == string(big)
	})
	up := a.Proxy.Up.Load() - before
	t.Logf("uploaded %d MiB for a %d MiB file across %d cuts", up>>20, size>>20, a.Proxy.Cuts.Load())
	if a.Proxy.Cuts.Load() < 2 {
		t.Errorf("only %d cuts happened", a.Proxy.Cuts.Load())
	}
	// Resuming sends each range once, plus framing; a restart from zero
	// would send at least size+24 MiB.
	if up > size+size/8 {
		t.Errorf("uploaded %d bytes for a %d byte file: finished ranges were resent", up, size)
	}
}

// tmpfs mounts a size-limited tmpfs at dir, or skips the test.
func tmpfs(t *testing.T, dir, size string) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("needs root to mount a small tmpfs")
	}
	if out, err := exec.Command("mount", "-t", "tmpfs", "-o", "size="+size, "tmpfs", dir).CombinedOutput(); err != nil {
		t.Skipf("cannot mount tmpfs: %v %s", err, out)
	}
	t.Cleanup(func() { exec.Command("umount", "-l", dir).Run() })
}

func resize(t *testing.T, dir, size string) {
	t.Helper()
	if out, err := exec.Command("mount", "-o", "remount,size="+size, dir).CombinedOutput(); err != nil {
		t.Fatalf("remount: %v %s", err, out)
	}
}

func TestDiskFull(t *testing.T) {
	needBinary(t)
	data := t.TempDir()
	tmpfs(t, data, "32m")
	srv := &Server{t: t, Data: data, TCP: freeAddr(t), HTTP: freeAddr(t), out: &logBuf{}}
	srv.Start()
	t.Cleanup(func() {
		srv.Stop()
		checkRaces(t, "serve", srv.Log())
		if t.Failed() {
			t.Logf("serve log:\n%s", tail(srv.Log(), 60))
		}
	})
	a := NewClient(t, srv, "dev-a", "home").Start()
	b := NewClient(t, srv, "dev-b", "home").Start()
	a.Write("/before.txt", "ok")
	Converge(t, convergeTimeout, a, b)

	// Staging: a file bigger than the disk cannot be uploaded.
	big := make([]byte, 40<<20)
	rand.Read(big)
	if err := os.WriteFile(filepath.Join(a.Dir, "big.bin"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	a.Write("/small-1.txt", "small")
	time.Sleep(5 * time.Second)
	if _, err := os.Stat(filepath.Join(b.Dir, "big.bin")); err == nil {
		t.Fatal("big.bin reached b through a full disk")
	}
	select {
	case <-srv.p.done:
		t.Fatalf("serve exited on a full disk")
	default:
	}

	// Publish: fill what is left, so the commit's SQLite write and
	// rename have no room.
	filler := filepath.Join(data, "filler")
	f, _ := os.Create(filler)
	f.ReadFrom(zeroReader{})
	f.Close()
	for i := range 5 {
		a.Write(fmt.Sprintf("/small-%d.txt", i+2), "small")
	}
	b.Write("/from-b.txt", "b")
	time.Sleep(5 * time.Second)
	select {
	case <-srv.p.done:
		t.Fatalf("serve exited on a full disk")
	default:
	}

	// Free the space: everything converges, and the server's files are
	// whole.
	os.Remove(filler)
	resize(t, data, "256m")
	got := Converge(t, 3*convergeTimeout, a, b)
	if string(got["/big.bin"]) != string(big) {
		t.Errorf("big.bin did not arrive whole")
	}
	for i := range 6 {
		if got[fmt.Sprintf("/small-%d.txt", i+1)] != "small" {
			t.Errorf("small-%d.txt missing", i+1)
		}
	}
	if got["/from-b.txt"] != "b" {
		t.Errorf("from-b.txt missing")
	}
	if cc := conflictCopies(got); len(cc) > 0 {
		t.Errorf("a full disk made conflict copies: %v", cc)
	}
	checkServerTree(t, srv, "home", got)
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}
