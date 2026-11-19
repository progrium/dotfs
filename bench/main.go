// Command bench measures dotfs serve and dotfs sync against Syncthing on
// one machine, with every process on loopback.
//
//	go run ./bench -work /tmp/dotfs-bench -syncthing /path/to/syncthing
//
// It generates a deterministic corpus shaped like a home folder, then for
// each system: seeds device A with the corpus, bootstraps an empty device
// B, measures change-to-visible latency from A to B, syncs a burst of
// small files, and reconnects B after offline changes on both sides. It
// prints a table and appends one JSON line per system to -o.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hugelgupf/p9/p9"
)

var (
	work      = flag.String("work", "dotfs-bench", "work `dir` for the corpus, folders, and server data")
	nfiles    = flag.Int("files", 100_000, "files in the corpus")
	nbytes    = flag.Int64("bytes", 2_500_000_000, "approximate corpus size in bytes")
	seed      = flag.Uint64("seed", 1, "corpus seed")
	systems   = flag.String("systems", "dotfs,syncthing", "comma-separated systems to run")
	dotfsBin  = flag.String("dotfs", "", "dotfs binary (default: build ./cmd/dotfs)")
	stBin     = flag.String("syncthing", "syncthing", "syncthing binary")
	stVersion = flag.String("syncthing-version", "", "Syncthing version to report, for a binary built without version stamping")
	latencyN  = flag.Int("latency", 20, "single-file changes timed from A to B")
	smallN    = flag.Int("small", 10_000, "small files in the burst")
	offlineN  = flag.Int("offline", 1000, "changes per side while B is offline")
	debounce  = flag.Duration("debounce", time.Second, "dotfs -debounce and Syncthing fsWatcherDelayS")
	out       = flag.String("o", "", "append results as JSON lines to `file`")
	keep      = flag.Bool("keep", false, "keep each system's folders after its run")
	pollEvery = 250 * time.Millisecond
)

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime)
	if err := os.MkdirAll(*work, 0o755); err != nil {
		log.Fatal(err)
	}
	abs, err := filepath.Abs(*work)
	if err != nil {
		log.Fatal(err)
	}
	*work = abs
	corpus := filepath.Join(*work, "corpus")
	if err := makeCorpus(corpus, *nfiles, *nbytes, *seed); err != nil {
		log.Fatal(err)
	}
	want, err := listing(corpus)
	if err != nil {
		log.Fatal(err)
	}
	var total int64
	for _, e := range want {
		total += e.size
	}
	log.Printf("corpus: %d entries, %.2f GB", len(want), float64(total)/1e9)

	var results []*Result
	for _, name := range strings.Split(*systems, ",") {
		var s System
		switch name {
		case "dotfs":
			s = &dotfsSystem{}
		case "syncthing":
			s = &stSystem{}
		default:
			log.Fatalf("unknown system %q", name)
		}
		r, err := run(s, corpus, want)
		if err != nil {
			log.Fatalf("%s: %v", name, err)
		}
		r.CorpusFiles, r.CorpusBytes = len(want), total
		results = append(results, r)
		if *out != "" {
			f, err := os.OpenFile(*out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
			if err != nil {
				log.Fatal(err)
			}
			json.NewEncoder(f).Encode(r)
			f.Close()
		}
	}
	printTable(results)
}

// Result is one system's numbers. Durations are in seconds.
type Result struct {
	System      string            `json:"system"`
	Version     string            `json:"version"`
	Date        string            `json:"date"`
	Machine     string            `json:"machine"`
	CorpusFiles int               `json:"corpus_files"`
	CorpusBytes int64             `json:"corpus_bytes"`
	Seed        float64           `json:"seed_s"`      // A takes in the corpus
	Bootstrap   float64           `json:"bootstrap_s"` // empty B gets everything
	LatencyP50  float64           `json:"latency_p50_s"`
	LatencyP95  float64           `json:"latency_p95_s"`
	LatencyMax  float64           `json:"latency_max_s"`
	SmallN      int               `json:"small_n"`
	Small       float64           `json:"small_s"` // burst written on A until all on B
	OfflineN    int               `json:"offline_n"`
	Reconnect   float64           `json:"reconnect_s"` // B restarts until A and B match
	IndexPlain  int64             `json:"index_plain_bytes,omitempty"`
	IndexZstd   int64             `json:"index_zstd_bytes,omitempty"`
	IndexRead   float64           `json:"index_read_s,omitempty"`
	MetaBytes   map[string]int64  `json:"meta_bytes"`  // metadata on disk
	PeakRSS     map[string]int64  `json:"peak_rss_kb"` // per process
	Notes       map[string]string `json:"notes,omitempty"`
}

// System is one sync system under test, with devices A and B.
type System interface {
	Name() string
	Version() string
	// Setup prepares a fresh run in dir, with A's folder already holding
	// the corpus and B's folder empty.
	Setup(dir, a, b string) error
	Start(dev string) error // "a" or "b"
	Stop(dev string) error
	// Measure adds system-specific numbers once everything has synced.
	Measure(r *Result) error
	Close() error
}

func run(s System, corpus string, want map[string]entry) (*Result, error) {
	r := &Result{
		System: s.Name(), Version: s.Version(), Date: time.Now().UTC().Format(time.RFC3339),
		Machine: machine(), MetaBytes: map[string]int64{}, PeakRSS: map[string]int64{}, Notes: map[string]string{},
	}
	dir := filepath.Join(*work, s.Name())
	os.RemoveAll(dir)
	a, b := filepath.Join(dir, "a"), filepath.Join(dir, "b")
	if err := os.MkdirAll(b, 0o755); err != nil {
		return nil, err
	}
	log.Printf("%s: copying the corpus into A", s.Name())
	if out, err := exec.Command("cp", "-a", corpus, a).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("cp: %v %s", err, out)
	}
	if err := s.Setup(dir, a, b); err != nil {
		return nil, err
	}
	defer func() {
		s.Close()
		if !*keep {
			os.RemoveAll(dir)
		}
	}()
	dropCaches()

	// Seed: A takes in the corpus (dotfs uploads it to the server;
	// Syncthing scans and hashes it).
	t0 := time.Now()
	if err := s.Start("a"); err != nil {
		return nil, err
	}
	if seeded, ok := s.(interface{ WaitSeeded(map[string]entry) error }); ok {
		if err := seeded.WaitSeeded(want); err != nil {
			return nil, err
		}
	}
	r.Seed = time.Since(t0).Seconds()
	log.Printf("%s: seed %.1fs", s.Name(), r.Seed)

	// Bootstrap: an empty B gets the whole corpus.
	dropCaches()
	t0 = time.Now()
	if err := s.Start("b"); err != nil {
		return nil, err
	}
	if err := waitListing(b, want, 2*time.Hour); err != nil {
		return nil, fmt.Errorf("bootstrap: %w", err)
	}
	r.Bootstrap = time.Since(t0).Seconds()
	log.Printf("%s: bootstrap %.1fs", s.Name(), r.Bootstrap)
	if err := sameContent(corpus, b, want, 2000); err != nil {
		return nil, fmt.Errorf("bootstrap: %w", err)
	}
	time.Sleep(5 * time.Second) // let both sides settle

	// Latency: one small file at a time from A to B.
	var lat []float64
	for i := range *latencyN {
		p := fmt.Sprintf("bench/latency/%03d.txt", i)
		content := fmt.Sprintf("change %d at %s\n", i, time.Now())
		if err := writeFile(filepath.Join(a, p), []byte(content)); err != nil {
			return nil, err
		}
		t0 := time.Now()
		for {
			got, err := os.ReadFile(filepath.Join(b, p))
			if err == nil && string(got) == content {
				break
			}
			if time.Since(t0) > 2*time.Minute {
				return nil, fmt.Errorf("latency: %s never reached B", p)
			}
			time.Sleep(2 * time.Millisecond)
		}
		lat = append(lat, time.Since(t0).Seconds())
		time.Sleep(time.Duration(500+mrand.IntN(1000)) * time.Millisecond)
	}
	slices.Sort(lat)
	r.LatencyP50, r.LatencyP95, r.LatencyMax = pct(lat, 50), pct(lat, 95), lat[len(lat)-1]
	log.Printf("%s: latency p50 %.3fs p95 %.3fs max %.3fs", s.Name(), r.LatencyP50, r.LatencyP95, r.LatencyMax)

	// Small files: a burst of 4 KiB files on A, timed until all are on B.
	r.SmallN = *smallN
	rnd := mrand.New(mrand.NewPCG(*seed, 99))
	t0 = time.Now()
	for i := range *smallN {
		buf := make([]byte, 4096)
		fillText(rnd, buf)
		if err := writeFile(filepath.Join(a, "bench/small", fmt.Sprintf("%02d/%05d.txt", i/500, i)), buf); err != nil {
			return nil, err
		}
	}
	if sd, ok := s.(interface{ ServerDir() string }); ok {
		// Also note when the server has them all.
		go func() {
			for countFiles(filepath.Join(sd.ServerDir(), "bench/small"), 4096) < *smallN {
				if time.Since(t0) > time.Hour {
					return
				}
				time.Sleep(pollEvery)
			}
			log.Printf("%s: %d small files on the server after %.1fs", s.Name(), *smallN, time.Since(t0).Seconds())
		}()
	}
	for {
		n := countFiles(filepath.Join(b, "bench/small"), 4096)
		if n == *smallN {
			break
		}
		if time.Since(t0) > time.Hour {
			return nil, fmt.Errorf("small files: %d of %d reached B", n, *smallN)
		}
		time.Sleep(pollEvery)
	}
	r.Small = time.Since(t0).Seconds()
	log.Printf("%s: %d small files %.1fs", s.Name(), *smallN, r.Small)
	time.Sleep(5 * time.Second)

	// Offline: stop B, change both sides, restart B.
	r.OfflineN = *offlineN
	if err := s.Stop("b"); err != nil {
		return nil, err
	}
	if err := offlineChanges(a, b, *offlineN, rnd); err != nil {
		return nil, err
	}
	time.Sleep(3 * time.Second) // A's watcher commits its side first
	t0 = time.Now()
	if err := s.Start("b"); err != nil {
		return nil, err
	}
	if err := waitMatch(a, b, time.Hour); err != nil {
		return nil, fmt.Errorf("reconnect: %w", err)
	}
	r.Reconnect = time.Since(t0).Seconds()
	log.Printf("%s: reconnect after %d changes per side %.1fs", s.Name(), *offlineN, r.Reconnect)
	if la, err := listing(a); err == nil {
		if n := conflicts(la); n > 0 {
			r.Notes["conflicts"] = strconv.Itoa(n)
		}
	}

	if err := s.Measure(r); err != nil {
		return nil, err
	}
	return r, nil
}

// offlineChanges makes n changes in each folder on disjoint paths: new
// files, appends to existing small files, deletes, and renames.
func offlineChanges(a, b string, n int, rnd *mrand.Rand) error {
	la, err := listing(a)
	if err != nil {
		return err
	}
	var smalls []string
	for p, e := range la {
		if !e.dir && e.size < 64<<10 && !strings.HasPrefix(p, "bench/") {
			smalls = append(smalls, p)
		}
	}
	slices.Sort(smalls)
	rnd.Shuffle(len(smalls), func(i, j int) { smalls[i], smalls[j] = smalls[j], smalls[i] })
	if len(smalls) < 2*n {
		return fmt.Errorf("corpus too small for %d offline changes", n)
	}
	for side, dir := range []string{a, b} {
		mine := smalls[side*n : (side+1)*n]
		for i := range n {
			var err error
			switch i % 4 {
			case 0: // new file
				buf := make([]byte, 1000+rnd.IntN(20000))
				fillText(rnd, buf)
				err = writeFile(filepath.Join(dir, fmt.Sprintf("bench/offline-%d/%04d.txt", side, i)), buf)
			case 1: // edit
				var f *os.File
				f, err = os.OpenFile(filepath.Join(dir, mine[i]), os.O_APPEND|os.O_WRONLY, 0)
				if err == nil {
					fmt.Fprintf(f, "\nedited offline on side %d\n", side)
					err = f.Close()
				}
			case 2: // delete
				err = os.Remove(filepath.Join(dir, mine[i]))
			case 3: // rename within its directory
				p := filepath.Join(dir, mine[i])
				err = os.Rename(p, p+".renamed")
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// --- dotfs ---

type dotfsSystem struct {
	bin, data, tcp, http, token string
	dirs                        map[string]string
	procs                       map[string]*exec.Cmd
	logs                        map[string]*os.File
}

func (d *dotfsSystem) Name() string { return "dotfs" }

func (d *dotfsSystem) Version() string {
	out, _ := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	return "dev " + strings.TrimSpace(string(out))
}

func (d *dotfsSystem) Setup(dir, a, b string) error {
	d.bin = *dotfsBin
	if d.bin == "" {
		d.bin = filepath.Join(dir, "dotfs")
		if out, err := exec.Command("go", "build", "-o", d.bin, "./cmd/dotfs").CombinedOutput(); err != nil {
			return fmt.Errorf("build dotfs: %v %s", err, out)
		}
	}
	d.data = filepath.Join(dir, "server")
	d.tcp, d.http = freeAddr(), freeAddr()
	d.dirs = map[string]string{"a": a, "b": b}
	d.procs = map[string]*exec.Cmd{}
	d.logs = map[string]*os.File{}
	if err := d.startProc("serve", "serve", "-data", d.data, "-tcp", d.tcp, "-http", d.http); err != nil {
		return err
	}
	if err := waitDial(d.http); err != nil {
		return err
	}
	tok, err := exec.Command(d.bin, "token", "create", "-data", d.data, "bench").Output()
	if err != nil {
		return fmt.Errorf("token: %v", err)
	}
	d.token = strings.TrimSpace(string(tok))
	return nil
}

func (d *dotfsSystem) startProc(name string, args ...string) error {
	lf, err := os.OpenFile(filepath.Join(filepath.Dir(d.data), name+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(d.bin, args...)
	cmd.Stdout, cmd.Stderr = lf, lf
	cmd.Env = append(os.Environ(), "DOTFS_TOKEN="+d.token)
	if err := cmd.Start(); err != nil {
		return err
	}
	d.procs[name], d.logs[name] = cmd, lf
	return nil
}

func (d *dotfsSystem) Start(dev string) error {
	url := fmt.Sprintf("ws://%s/9p/home", d.http)
	return d.startProc(dev, "sync", "-q", "-device", dev, "-debounce", debounce.String(), url, d.dirs[dev])
}

func (d *dotfsSystem) Stop(dev string) error { return d.stopProc(dev) }

func (d *dotfsSystem) stopProc(name string) error {
	cmd := d.procs[name]
	if cmd == nil {
		return nil
	}
	cmd.Process.Signal(syscall.SIGTERM)
	err := cmd.Wait()
	d.logs[name].Close()
	delete(d.procs, name)
	var ee *exec.ExitError
	if errors.As(err, &ee) && !ee.Exited() {
		err = nil // killed by our signal
	}
	return err
}

// ServerDir is the server's copy of the root.
func (d *dotfsSystem) ServerDir() string { return filepath.Join(d.data, "roots", "bench", "home") }

// WaitSeeded waits until the server's copy of the root holds the corpus.
func (d *dotfsSystem) WaitSeeded(want map[string]entry) error {
	return waitListing(filepath.Join(d.data, "roots", "bench", "home"), want, 2*time.Hour)
}

func (d *dotfsSystem) Measure(r *Result) error {
	for _, name := range []string{"serve", "a", "b"} {
		if cmd := d.procs[name]; cmd != nil {
			r.PeakRSS[name] = peakRSS(cmd.Process.Pid)
		}
	}
	// Index size and read time, plain and zstd, over TCP.
	for _, enc := range []string{"", "zstd"} {
		t0 := time.Now()
		n, err := readIndex(d.tcp, "bench", "home", enc)
		if err != nil {
			return fmt.Errorf("index %s: %w", enc, err)
		}
		if enc == "" {
			r.IndexPlain = n
		} else {
			r.IndexZstd = n
			r.IndexRead = time.Since(t0).Seconds()
		}
	}
	r.MetaBytes["server meta.db"] = duBytes(filepath.Join(d.data, "meta.db"), filepath.Join(d.data, "meta.db-wal"))
	r.MetaBytes["client state"] = duBytes(filepath.Join(d.dirs["b"], ".dotfs", "state.db"), filepath.Join(d.dirs["b"], ".dotfs", "state.db-wal"))
	r.Notes["transport"] = "WebSocket on loopback"
	return nil
}

func (d *dotfsSystem) Close() error {
	for name := range d.procs {
		d.stopProc(name)
	}
	return nil
}

// readIndex reads .fs/index raw and returns its size in bytes.
func readIndex(addr, user, root, enc string) (int64, error) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return 0, err
	}
	c, err := p9.NewClient(nc, p9.WithMessageSize(1<<20))
	if err != nil {
		nc.Close()
		return 0, err
	}
	defer c.Close()
	rf, err := c.Attach(user + "/" + root)
	if err != nil {
		return 0, err
	}
	defer rf.Close()
	_, f, err := rf.Walk([]string{".fs", "index"})
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if enc == "" {
		if _, _, err := f.Open(p9.ReadOnly); err != nil {
			return 0, err
		}
	} else {
		if _, _, err := f.Open(p9.ReadWrite); err != nil {
			return 0, err
		}
		if _, err := f.WriteAt([]byte("accept "+enc+"\n"), 0); err != nil {
			return 0, err
		}
	}
	buf := make([]byte, 1<<20-64)
	var off int64
	for {
		n, err := f.ReadAt(buf, off)
		off += int64(n)
		if n == 0 || err == io.EOF {
			return off, nil
		}
		if err != nil {
			return off, err
		}
	}
}

// --- Syncthing ---

type stSystem struct {
	homes, dirs, gui, listen, ids map[string]string
	procs                         map[string]*exec.Cmd
	key                           string
}

func (s *stSystem) Name() string { return "syncthing" }

func (s *stSystem) Version() string {
	if *stVersion != "" {
		return *stVersion
	}
	out, _ := exec.Command(*stBin, "--version").Output()
	return strings.TrimSpace(string(out))
}

func (s *stSystem) Setup(dir, a, b string) error {
	s.key = "dotfs-bench-key"
	s.homes = map[string]string{"a": filepath.Join(dir, "st-a"), "b": filepath.Join(dir, "st-b")}
	s.dirs = map[string]string{"a": a, "b": b}
	s.gui = map[string]string{"a": freeAddr(), "b": freeAddr()}
	s.listen = map[string]string{"a": freeAddr(), "b": freeAddr()}
	s.ids = map[string]string{}
	s.procs = map[string]*exec.Cmd{}
	for _, dev := range []string{"a", "b"} {
		if out, err := exec.Command(*stBin, "generate", "--home", s.homes[dev], "--no-port-probing").CombinedOutput(); err != nil {
			return fmt.Errorf("syncthing generate: %v %s", err, out)
		}
		out, err := exec.Command(*stBin, "device-id", "--home", s.homes[dev]).Output()
		if err != nil {
			return fmt.Errorf("syncthing device-id: %v", err)
		}
		s.ids[dev] = strings.TrimSpace(string(out))
	}
	// Configure both while running, then stop them, so the folder starts
	// with the measured Start of A.
	for _, dev := range []string{"a", "b"} {
		if err := s.Start(dev); err != nil {
			return err
		}
		other := map[string]string{"a": "b", "b": "a"}[dev]
		if err := s.api(dev, "PATCH", "/rest/config/options", map[string]any{
			"listenAddresses":       []string{"tcp://" + s.listen[dev]},
			"globalAnnounceEnabled": false, "localAnnounceEnabled": false, "relaysEnabled": false,
			"natEnabled": false, "urAccepted": -1, "crashReportingEnabled": false, "autoUpgradeIntervalH": 0,
		}, nil); err != nil {
			return err
		}
		if err := s.api(dev, "PUT", "/rest/config/devices/"+s.ids[other], map[string]any{
			"deviceID": s.ids[other], "name": other, "addresses": []string{"tcp://" + s.listen[other]},
		}, nil); err != nil {
			return err
		}
		if err := s.api(dev, "PUT", "/rest/config/folders/bench", map[string]any{
			"id": "bench", "label": "bench", "path": s.dirs[dev], "type": "sendreceive",
			"devices":         []map[string]string{{"deviceID": s.ids["a"]}, {"deviceID": s.ids["b"]}},
			"rescanIntervalS": 3600, "fsWatcherEnabled": true, "fsWatcherDelayS": int(debounce.Seconds()),
			"paused": dev == "b", // B's folder starts when B is measured
		}, nil); err != nil {
			return err
		}
		if err := s.Stop(dev); err != nil {
			return err
		}
	}
	return nil
}

func (s *stSystem) Start(dev string) error {
	cmd := exec.Command(*stBin, "serve", "--home", s.homes[dev], "--no-browser",
		"--gui-address", "http://"+s.gui[dev], "--gui-apikey", s.key,
		"--log-file", filepath.Join(s.homes[dev], "syncthing.log"))
	cmd.Env = append(os.Environ(), "STNOUPGRADE=1", "STNORESTART=1")
	if err := cmd.Start(); err != nil {
		return err
	}
	s.procs[dev] = cmd
	deadline := time.Now().Add(time.Minute)
	for {
		var st map[string]any
		if err := s.api(dev, "GET", "/rest/system/status", nil, &st); err == nil {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("syncthing %s did not start", dev)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if dev == "b" {
		// Unpause B's folder if Setup paused it.
		var f map[string]any
		if err := s.api(dev, "GET", "/rest/config/folders/bench", nil, &f); err == nil && f["paused"] == true {
			return s.api(dev, "PATCH", "/rest/config/folders/bench", map[string]any{"paused": false}, nil)
		}
	}
	return nil
}

func (s *stSystem) Stop(dev string) error {
	cmd := s.procs[dev]
	if cmd == nil {
		return nil
	}
	s.api(dev, "POST", "/rest/system/shutdown", nil, nil)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(time.Minute):
		cmd.Process.Kill()
		<-done
	}
	delete(s.procs, dev)
	return nil
}

// WaitSeeded waits until A has scanned the whole corpus.
func (s *stSystem) WaitSeeded(want map[string]entry) error {
	deadline := time.Now().Add(2 * time.Hour)
	for {
		var st struct {
			State      string `json:"state"`
			LocalFiles int    `json:"localFiles"`
			LocalDirs  int    `json:"localDirectories"`
		}
		if err := s.api("a", "GET", "/rest/db/status?folder=bench", nil, &st); err == nil {
			if st.State == "idle" && st.LocalFiles+st.LocalDirs >= len(want) {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("syncthing A never finished scanning")
		}
		time.Sleep(pollEvery)
	}
}

func (s *stSystem) Measure(r *Result) error {
	for _, dev := range []string{"a", "b"} {
		if cmd := s.procs[dev]; cmd != nil {
			r.PeakRSS[dev] = peakRSS(cmd.Process.Pid)
		}
	}
	for _, dev := range []string{"a", "b"} {
		var n int64
		filepath.WalkDir(s.homes[dev], func(p string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() && strings.Contains(p, "index") {
				if fi, err := d.Info(); err == nil {
					n += fi.Size()
				}
			}
			return nil
		})
		r.MetaBytes["device "+dev+" database"] = n
	}
	r.Notes["transport"] = "TCP+TLS on loopback, no relays or discovery"
	return nil
}

func (s *stSystem) Close() error {
	for dev := range s.procs {
		s.Stop(dev)
	}
	return nil
}

func (s *stSystem) api(dev, method, path string, body, into any) error {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, "http://"+s.gui[dev]+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("X-API-Key", s.key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s %s: %s %s", method, path, resp.Status, data)
	}
	if into != nil {
		return json.Unmarshal(data, into)
	}
	return nil
}

// --- corpus ---

var words = strings.Fields(`alpha budget camera draft export family garden holiday invoice journal
kitchen letter meeting notes photos project quarterly receipt report school taxes travel
upload video wedding archive backup config design figures lecture music podcast recipe resume
scan slides spreadsheet thesis website`)

type fileKind struct {
	ext    string
	text   bool
	weight int
}

var exts = []fileKind{
	{".txt", true, 12}, {".md", true, 10}, {".go", true, 8}, {".js", true, 8}, {".json", true, 6},
	{".html", true, 4}, {".csv", true, 4}, {".pdf", false, 6}, {".docx", false, 4},
	{".jpg", false, 14}, {".png", false, 6}, {".heic", false, 4}, {".zip", false, 2},
}

// makeCorpus writes n files totalling about size bytes into dir, unless a
// marker shows the same corpus is already there. Sizes are log-normal
// (most files a few KB) with a tail of large media files; text files are
// compressible and media files are not.
func makeCorpus(dir string, n int, size int64, seed uint64) error {
	marker := filepath.Join(filepath.Dir(dir), "corpus.done")
	tag := fmt.Sprintf("files=%d bytes=%d seed=%d v2\n", n, size, seed)
	if b, err := os.ReadFile(marker); err == nil && string(b) == tag {
		return nil
	}
	os.RemoveAll(dir)
	os.Remove(marker)
	log.Printf("generating corpus: %d files, about %.1f GB", n, float64(size)/1e9)
	rnd := mrand.New(mrand.NewPCG(seed, 1))
	// Directories: a tree several levels deep.
	var dirs []string
	tops := []string{"Documents", "Pictures", "Projects", "Music", "Desktop", "Downloads", "Videos", "Notes"}
	for _, t := range tops {
		dirs = append(dirs, t)
	}
	for len(dirs) < n/25 {
		parent := dirs[rnd.IntN(len(dirs))]
		if strings.Count(parent, "/") >= 6 {
			continue
		}
		name := words[rnd.IntN(len(words))]
		if rnd.IntN(3) == 0 {
			name = fmt.Sprintf("%s %d", name, 2015+rnd.IntN(12))
		}
		dirs = append(dirs, parent+"/"+name)
	}
	// Sizes: about 40% of the bytes in large media files (2 to 64 MB),
	// the rest log-normal around 4 KB, scaled so the total lands near
	// size.
	sizes := make([]int64, n)
	nlarge := max(1, int(0.4*float64(size)/(33<<20)))
	isLarge := map[int]bool{}
	for len(isLarge) < min(nlarge, n/10+1) {
		isLarge[rnd.IntN(n)] = true
	}
	var large, small float64
	for i := range sizes {
		if isLarge[i] {
			sizes[i] = int64(2<<20 + rnd.IntN(62<<20))
			large += float64(sizes[i])
		} else {
			sizes[i] = -int64(1 + 4096*expNorm(rnd, 1.6))
			small += float64(-sizes[i])
		}
	}
	scale := (float64(size) - large) / small
	if scale < 0.1 {
		scale = 0.1
	}
	var total int64
	var tw int
	for _, e := range exts {
		tw += e.weight
	}
	buf := make([]byte, 64<<20)
	for i, sz := range sizes {
		if sz < 0 {
			sz = max(int64(float64(-sz)*scale), 1)
		}
		ext := exts[0]
		w := rnd.IntN(tw)
		for _, e := range exts {
			if w < e.weight {
				ext = e
				break
			}
			w -= e.weight
		}
		if sz > 1<<20 {
			ext = fileKind{".jpg", false, 0} // big files are photos and videos
			if rnd.IntN(3) == 0 {
				ext = fileKind{".mp4", false, 0}
			}
		}
		d := dirs[rnd.IntN(len(dirs))]
		p := filepath.Join(dir, d, fmt.Sprintf("%s-%06d%s", words[rnd.IntN(len(words))], i, ext.ext))
		b := buf[:sz]
		if ext.text {
			fillText(rnd, b)
		} else {
			fillRandom(rnd, b)
		}
		if err := writeFile(p, b); err != nil {
			return err
		}
		total += sz
	}
	// A few empty directories, as real trees have.
	for i := range 50 {
		os.MkdirAll(filepath.Join(dir, dirs[rnd.IntN(len(dirs))], fmt.Sprintf("empty-%d", i)), 0o755)
	}
	log.Printf("corpus: wrote %d files, %.2f GB", n, float64(total)/1e9)
	return os.WriteFile(marker, []byte(tag), 0o644)
}

func expNorm(rnd *mrand.Rand, sigma float64) float64 {
	return math.Exp(rnd.NormFloat64() * sigma)
}

func fillText(rnd *mrand.Rand, b []byte) {
	i := 0
	for i < len(b) {
		w := words[rnd.IntN(len(words))]
		i += copy(b[i:], w)
		if i < len(b) {
			if rnd.IntN(12) == 0 {
				b[i] = '\n'
			} else {
				b[i] = ' '
			}
			i++
		}
	}
}

func fillRandom(rnd *mrand.Rand, b []byte) {
	for i := 0; i+8 <= len(b); i += 8 {
		v := rnd.Uint64()
		for j := range 8 {
			b[i+j] = byte(v >> (8 * j))
		}
	}
}

func writeFile(p string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, b, 0o644)
}

// --- comparing folders ---

type entry struct {
	size int64
	dir  bool
}

// skip reports whether a path is a sync tool's own state or temp file.
func skip(rel string, d fs.DirEntry) bool {
	base := filepath.Base(rel)
	switch {
	case rel == ".dotfs", rel == ".stfolder", rel == ".stversions":
		return true
	case strings.HasPrefix(base, ".syncthing.") && strings.HasSuffix(base, ".tmp"):
		return true
	}
	return false
}

// listing maps each path under dir to its size, without sync state.
func listing(dir string) (map[string]entry, error) {
	m := map[string]entry{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if p == dir {
			return nil
		}
		rel, _ := filepath.Rel(dir, p)
		rel = filepath.ToSlash(rel)
		if skip(rel, d) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			m[rel] = entry{dir: true}
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return nil
		}
		m[rel] = entry{size: fi.Size()}
		return nil
	})
	return m, err
}

func sameListing(a, b map[string]entry) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func waitListing(dir string, want map[string]entry, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		got, err := listing(dir)
		if err == nil && sameListing(got, want) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s has %d of %d entries after %s", dir, len(got), len(want), timeout)
		}
		time.Sleep(pollEvery)
	}
}

func waitMatch(a, b string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		la, err1 := listing(a)
		lb, err2 := listing(b)
		if err1 == nil && err2 == nil && sameListing(la, lb) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("A has %d entries and B %d after %s", len(la), len(lb), timeout)
		}
		time.Sleep(pollEvery)
	}
}

// sameContent compares the bytes of n sampled files (all large ones).
func sameContent(a, b string, want map[string]entry, n int) error {
	var paths []string
	for p, e := range want {
		if !e.dir {
			paths = append(paths, p)
		}
	}
	slices.Sort(paths)
	rnd := mrand.New(mrand.NewPCG(7, 7))
	for i, p := range paths {
		if want[p].size < 1<<20 && rnd.IntN(len(paths)) >= n {
			continue
		}
		x, err := os.ReadFile(filepath.Join(a, p))
		if err != nil {
			return err
		}
		y, err := os.ReadFile(filepath.Join(b, p))
		if err != nil {
			return err
		}
		if !bytes.Equal(x, y) {
			return fmt.Errorf("%s differs (file %d)", p, i)
		}
	}
	return nil
}

func countFiles(dir string, size int64) int {
	n := 0
	filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() && !skip(filepath.Base(p), d) {
			if fi, err := d.Info(); err == nil && fi.Size() == size {
				n++
			}
		}
		return nil
	})
	return n
}

var conflictRE = regexp.MustCompile(`\.sync-conflict-`)

func conflicts(m map[string]entry) int {
	n := 0
	for p := range m {
		if conflictRE.MatchString(p) {
			n++
		}
	}
	return n
}

// --- helpers ---

func pct(sorted []float64, p int) float64 {
	i := (len(sorted)*p + 99) / 100
	return sorted[min(max(i-1, 0), len(sorted)-1)]
}

func freeAddr() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func waitDial(addr string) error {
	for range 400 {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("nothing listening on %s", addr)
}

// peakRSS reads the peak resident set size in KiB of a process and its
// children (Syncthing runs its work in a child of the process we start).
func peakRSS(pid int) int64 {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0
	}
	var n int64
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		if f := strings.Fields(sc.Text()); len(f) >= 2 && f[0] == "VmHWM:" {
			n, _ = strconv.ParseInt(f[1], 10, 64)
		}
	}
	// A Go program forks from whichever thread it is on, so look at the
	// children of every thread.
	tasks, _ := filepath.Glob(fmt.Sprintf("/proc/%d/task/*/children", pid))
	for _, t := range tasks {
		kids, _ := os.ReadFile(t)
		for _, k := range strings.Fields(string(kids)) {
			if c, err := strconv.Atoi(k); err == nil {
				n += peakRSS(c)
			}
		}
	}
	return n
}

func duBytes(paths ...string) int64 {
	var n int64
	for _, p := range paths {
		if fi, err := os.Stat(p); err == nil {
			n += fi.Size()
		}
	}
	return n
}

// dropCaches empties the page cache when running as root on Linux, so
// each phase starts cold; elsewhere it does nothing.
func dropCaches() {
	exec.Command("sync").Run()
	os.WriteFile("/proc/sys/vm/drop_caches", []byte("3\n"), 0)
}

func machine() string {
	cpu := runtime.GOARCH
	if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "model name") {
				cpu = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
				break
			}
		}
	}
	mem := ""
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		f := strings.Fields(string(b))
		if len(f) > 1 {
			kb, _ := strconv.ParseInt(f[1], 10, 64)
			mem = fmt.Sprintf(", %d GB RAM", (kb+(1<<19))>>20)
		}
	}
	return fmt.Sprintf("%d CPUs, %s%s, %s/%s", runtime.NumCPU(), cpu, mem, runtime.GOOS, runtime.GOARCH)
}

func printTable(rs []*Result) {
	row := func(name string, f func(r *Result) string) {
		fmt.Printf("%-34s", name)
		for _, r := range rs {
			fmt.Printf(" %18s", f(r))
		}
		fmt.Println()
	}
	row("", func(r *Result) string { return r.System })
	sec := func(v float64) string { return fmt.Sprintf("%.2fs", v) }
	row("seed A with the corpus", func(r *Result) string { return sec(r.Seed) })
	row("bootstrap empty B", func(r *Result) string { return sec(r.Bootstrap) })
	row("change to visible, p50", func(r *Result) string { return sec(r.LatencyP50) })
	row("change to visible, p95", func(r *Result) string { return sec(r.LatencyP95) })
	row(fmt.Sprintf("%d small files, A to B", *smallN), func(r *Result) string { return sec(r.Small) })
	row(fmt.Sprintf("reconnect, %d changes per side", *offlineN), func(r *Result) string { return sec(r.Reconnect) })
	row("index plain / zstd", func(r *Result) string {
		if r.IndexPlain == 0 {
			return "-"
		}
		return fmt.Sprintf("%.1f/%.1f MB", float64(r.IndexPlain)/1e6, float64(r.IndexZstd)/1e6)
	})
}
