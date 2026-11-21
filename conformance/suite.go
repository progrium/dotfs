// Package conformance checks a 9P server against the dotfs protocol in
// docs/9p-dotfs.md. It connects to any 9P2000.L address, discovers which
// extensions the server offers (index, events, path operations, and staged
// commit), and runs the tests for each.
//
// The suite speaks 9P through its own small codec (internal/wire) and uses
// fsproto only for the line formats, never the reference client, so the
// suite and the client check each other.
//
// Tests that change the tree work below one directory per run,
// /conform-<id>, and remove it at the end. Run the suite against a scope
// nothing else is writing to: several tests expect the scope's seq to move
// only by their own changes.
package conformance

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/progrium/dotfs/conformance/internal/wire"
	"github.com/progrium/dotfs/fsproto"
)

// Ext names an extension, or the discovery checks that run on every server.
type Ext string

// The extensions, in spec order.
const (
	ExtDiscovery Ext = "discovery"
	ExtIndex     Ext = "index"
	ExtEvents    Ext = "events"
	ExtCtl       Ext = "path-operations"
	ExtStage     Ext = "staged-commit"
)

// Exts lists the extensions in report order.
var Exts = []Ext{ExtDiscovery, ExtIndex, ExtEvents, ExtCtl, ExtStage}

// Title returns the extension's name as the spec writes it.
func (e Ext) Title() string {
	switch e {
	case ExtDiscovery:
		return "discovery"
	case ExtIndex:
		return "index"
	case ExtEvents:
		return "events"
	case ExtCtl:
		return "path operations"
	case ExtStage:
		return "staged commit"
	}
	return string(e)
}

// fsFile is the .fs/ file whose presence means the extension is offered.
func (e Ext) fsFile() string {
	switch e {
	case ExtIndex:
		return "index"
	case ExtEvents:
		return "events"
	case ExtCtl:
		return "ctl"
	case ExtStage:
		return "stage"
	}
	return ""
}

// Level is how strongly the spec states a requirement.
type Level string

const (
	// Must: a failure fails the run.
	Must Level = "must"
	// Should: a failure is reported as a warning.
	Should Level = "should"
)

// Test is one conformance test.
type Test struct {
	ID      string // "<ext>/<slug>"
	Ext     Ext    // the extension it belongs to
	Section string // the spec section it checks, as headings joined by " > "
	Name    string // one line on what it checks
	Level   Level
	// Requires lists other extensions the test needs for setup; it is
	// skipped when the server lacks one. Its own Ext is always required.
	Requires []Ext
	Run      func(t *T)
}

var registry []*Test

func register(ts ...*Test) {
	for _, t := range ts {
		if t.Level == "" {
			t.Level = Must
		}
		registry = append(registry, t)
	}
}

// Tests returns every test, in report order.
func Tests() []*Test {
	out := append([]*Test(nil), registry...)
	order := map[Ext]int{}
	for i, e := range Exts {
		order[e] = i
	}
	sort.SliceStable(out, func(i, j int) bool { return order[out[i].Ext] < order[out[j].Ext] })
	return out
}

// Target describes the server under test.
type Target struct {
	// Addr is shown in reports.
	Addr string
	// Dial opens a new transport connection.
	Dial func(ctx context.Context) (io.ReadWriteCloser, error)
	// User and Aname are sent in Tattach.
	User, Aname string
	// OtherUser, when set, is a second user that may attach to Aname, for
	// the test that stage files are private.
	OtherUser string
	// Trim, when set, drops the scope's log at or below seq, so the
	// trimmed-history test can run. Over the wire there is no way to do
	// this, so only in-process targets set it.
	Trim func(ctx context.Context, seq uint64) error
}

// Options tune a run.
type Options struct {
	// Run selects tests whose ID matches this regular expression.
	Run string
	// Timeout bounds each test (30s if 0).
	Timeout time.Duration
	// Quiet is how long a read must stay blocked to count as blocking
	// (300ms if 0).
	Quiet time.Duration
	// Overflow is how many changes the overflow test makes before reading
	// (3000 if 0).
	Overflow int
	// Progress, when set, is called after each test.
	Progress func(Result)
}

func (o *Options) defaults() {
	if o.Timeout == 0 {
		o.Timeout = 30 * time.Second
	}
	if o.Quiet == 0 {
		o.Quiet = 300 * time.Millisecond
	}
	if o.Overflow == 0 {
		o.Overflow = 3000
	}
}

// Status is a test's outcome.
type Status string

const (
	Pass Status = "pass"
	Fail Status = "fail"
	Warn Status = "warn" // a Should test failed
	Skip Status = "skip"
)

// Result is one test's outcome.
type Result struct {
	ID       string        `json:"id"`
	Ext      Ext           `json:"extension"`
	Section  string        `json:"section"`
	Name     string        `json:"name"`
	Level    Level         `json:"level"`
	Status   Status        `json:"status"`
	Messages []string      `json:"messages,omitempty"`
	Duration time.Duration `json:"duration_ns"`
}

// runner holds what tests in one run share.
type runner struct {
	target Target
	opts   Options
	exts   map[Ext]bool
	runDir string

	mu       sync.Mutex
	dirMade  bool
	dirErr   error
	nextName int
}

// Run discovers the server's extensions and runs the selected tests.
func Run(ctx context.Context, target Target, opts Options) (*Report, error) {
	opts.defaults()
	var sel *regexp.Regexp
	if opts.Run != "" {
		var err error
		if sel, err = regexp.Compile(opts.Run); err != nil {
			return nil, err
		}
	}
	r := &runner{target: target, opts: opts, runDir: "/conform-" + randHex(4)}
	exts, err := r.discover(ctx)
	if err != nil {
		return nil, err
	}
	r.exts = exts
	rep := &Report{Addr: target.Addr, Aname: target.Aname, Started: time.Now(), Extensions: map[Ext]bool{}}
	for _, e := range Exts[1:] {
		rep.Extensions[e] = exts[e]
	}
	for _, t := range Tests() {
		if sel != nil && !sel.MatchString(t.ID) {
			continue
		}
		res := r.runTest(ctx, t)
		rep.Results = append(rep.Results, res)
		if opts.Progress != nil {
			opts.Progress(res)
		}
	}
	r.cleanup(ctx)
	rep.Duration = time.Since(rep.Started)
	rep.finish()
	return rep, nil
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// dial opens and attaches a new connection.
func (r *runner) dial(ctx context.Context, user string) (*wire.Conn, *wire.Fid, error) {
	rwc, err := r.target.Dial(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("dial: %w", err)
	}
	c, err := wire.NewConn(rwc)
	if err != nil {
		rwc.Close()
		return nil, nil, err
	}
	root, err := c.Attach(ctx, user, r.target.Aname)
	if err != nil {
		c.Close()
		return nil, nil, fmt.Errorf("attach: %w", err)
	}
	return c, root, nil
}

// discover walks to each .fs/ file.
func (r *runner) discover(ctx context.Context) (map[Ext]bool, error) {
	ctx, cancel := context.WithTimeout(ctx, r.opts.Timeout)
	defer cancel()
	c, root, err := r.dial(ctx, r.target.User)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	exts := map[Ext]bool{ExtDiscovery: true}
	for _, e := range Exts[1:] {
		f, err := root.Walk(ctx, ".fs", e.fsFile())
		if err == nil {
			exts[e] = true
			f.Clunk(ctx)
		}
	}
	return exts, nil
}

// errStop unwinds a test goroutine after Fatalf or Skipf.
type stopped struct{}

func (r *runner) runTest(ctx context.Context, test *Test) Result {
	res := Result{ID: test.ID, Ext: test.Ext, Section: test.Section, Name: test.Name, Level: test.Level}
	for _, e := range append([]Ext{test.Ext}, test.Requires...) {
		if !r.exts[e] {
			res.Status = Skip
			res.Messages = []string{fmt.Sprintf("server does not offer %s", e.Title())}
			return res
		}
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, r.opts.Timeout)
	defer cancel()
	t := &T{ctx: ctx, r: r, test: test}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if v := recover(); v != nil {
				if _, ok := v.(stopped); !ok {
					t.fail(fmt.Sprintf("panic: %v", v))
				}
			}
		}()
		defer t.runCleanups()
		c, root, err := r.dial(ctx, r.target.User)
		if err != nil {
			t.Fatalf("%v", err)
		}
		t.conn, t.root = c, root
		t.addCleanup(func() { c.Close() })
		test.Run(t)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.fail(fmt.Sprintf("timed out after %v", r.opts.Timeout))
		// The test goroutine is stuck on the server; give it a moment to
		// see the canceled context, then report without it.
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
	res.Duration = time.Since(start)
	t.mu.Lock()
	defer t.mu.Unlock()
	res.Messages = append(res.Messages, t.msgs...)
	switch {
	case t.failed && test.Level == Should:
		res.Status = Warn
	case t.failed:
		res.Status = Fail
	case t.skipped:
		res.Status = Skip
	default:
		res.Status = Pass
	}
	return res
}

// cleanup removes the run directory.
func (r *runner) cleanup(ctx context.Context) {
	r.mu.Lock()
	made := r.dirMade
	r.mu.Unlock()
	if !made || !r.exts[ExtCtl] {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, r.opts.Timeout)
	defer cancel()
	c, root, err := r.dial(ctx, r.target.User)
	if err != nil {
		return
	}
	defer c.Close()
	if f, err := openCtl(ctx, root); err == nil {
		f.call(ctx, fsproto.RmtreeCmd{Path: r.runDir}.Encode())
		f.fid.Clunk(ctx)
	}
}

// T is a running test's handle, in the style of testing.T.
type T struct {
	ctx  context.Context
	r    *runner
	test *Test
	conn *wire.Conn
	root *wire.Fid

	mu       sync.Mutex
	msgs     []string
	failed   bool
	skipped  bool
	cleanups []func()
	dir      string
}

// Context is canceled when the test times out.
func (t *T) Context() context.Context { return t.ctx }

// Opts returns the run's options.
func (t *T) Opts() Options { return t.r.opts }

// Has reports whether the server offers e.
func (t *T) Has(e Ext) bool { return t.r.exts[e] }

func (t *T) fail(msg string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failed = true
	t.msgs = append(t.msgs, msg)
}

// Errorf records a failure and continues.
func (t *T) Errorf(format string, args ...any) { t.fail(caller() + fmt.Sprintf(format, args...)) }

// Fatalf records a failure and stops the test.
func (t *T) Fatalf(format string, args ...any) {
	t.fail(caller() + fmt.Sprintf(format, args...))
	panic(stopped{})
}

// Skipf marks the test skipped and stops it.
func (t *T) Skipf(format string, args ...any) {
	t.mu.Lock()
	t.skipped = true
	t.msgs = append(t.msgs, fmt.Sprintf(format, args...))
	t.mu.Unlock()
	panic(stopped{})
}

// Logf records a note shown with the result.
func (t *T) Logf(format string, args ...any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.msgs = append(t.msgs, fmt.Sprintf(format, args...))
}

// caller returns "file:line: " for the test code that called a T method.
func caller() string {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		return ""
	}
	if i := strings.LastIndexByte(file, '/'); i >= 0 {
		file = file[i+1:]
	}
	return fmt.Sprintf("%s:%d: ", file, line)
}

func (t *T) addCleanup(f func()) {
	t.mu.Lock()
	t.cleanups = append(t.cleanups, f)
	t.mu.Unlock()
}

// Cleanup registers f to run when the test ends, last first.
func (t *T) Cleanup(f func()) { t.addCleanup(f) }

func (t *T) runCleanups() {
	t.mu.Lock()
	cs := t.cleanups
	t.cleanups = nil
	t.mu.Unlock()
	for i := len(cs) - 1; i >= 0; i-- {
		func() {
			defer func() { recover() }()
			cs[i]()
		}()
	}
}
