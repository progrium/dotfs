package conformance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/progrium/dotfs/conformance/internal/wire"
	"github.com/progrium/dotfs/fsproto"
)

// Root returns the test connection's attach fid.
func (t *T) Root() *wire.Fid { return t.root }

// Conn returns the test's connection.
func (t *T) Conn() *wire.Conn { return t.conn }

// Dial opens another connection, attached as user ("" for the target's
// user). It is closed when the test ends.
func (t *T) Dial(user string) (*wire.Conn, *wire.Fid, error) {
	if user == "" {
		user = t.r.target.User
	}
	c, root, err := t.r.dial(t.ctx, user)
	if err != nil {
		return nil, nil, err
	}
	t.Cleanup(func() { c.Close() })
	return c, root, nil
}

// splitPath turns a protocol path into walk names.
func splitPath(p string) []string {
	p = strings.Trim(p, "/")
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}

// Walk walks from root to the protocol path p, plus any extra names.
func Walk(ctx context.Context, root *wire.Fid, p string, extra ...string) (*wire.Fid, error) {
	return root.Walk(ctx, append(splitPath(p), extra...)...)
}

// Walk walks from the test's root; the fid is clunked when the test ends.
func (t *T) Walk(p string, extra ...string) (*wire.Fid, error) {
	f, err := Walk(t.ctx, t.root, p, extra...)
	if err == nil {
		t.Cleanup(func() { f.Clunk(context.Background()) })
	}
	return f, err
}

// MustWalk is Walk that stops the test on error.
func (t *T) MustWalk(p string, extra ...string) *wire.Fid {
	f, err := t.Walk(p, extra...)
	if err != nil {
		t.Fatalf("walk %q %q: %v", p, extra, err)
	}
	return f
}

// ctlFid is one open .fs/ctl fid.
type ctlFid struct{ fid *wire.Fid }

func openCtl(ctx context.Context, root *wire.Fid, dir ...string) (*ctlFid, error) {
	f, err := root.Walk(ctx, append(append([]string(nil), dir...), ".fs", "ctl")...)
	if err != nil {
		return nil, err
	}
	if err := f.Open(ctx, wire.ORDWR); err != nil {
		f.Clunk(ctx)
		return nil, err
	}
	return &ctlFid{fid: f}, nil
}

func (c *ctlFid) send(ctx context.Context, line string) error {
	return c.fid.Write(ctx, []byte(line))
}

// recv reads the reply to the last command. The reply is one line; a
// trailing newline is allowed and removed.
func (c *ctlFid) recv(ctx context.Context) (string, error) {
	b, err := c.fid.ReadAll(ctx)
	if err != nil {
		return "", err
	}
	s := strings.TrimSuffix(string(b), "\n")
	if strings.Contains(s, "\n") {
		return s, fmt.Errorf("reply has more than one line: %q", b)
	}
	return s, nil
}

func (c *ctlFid) call(ctx context.Context, line string) (string, error) {
	if err := c.send(ctx, line); err != nil {
		return "", err
	}
	return c.recv(ctx)
}

// OpenCtl opens a new ctl fid at the root scope (or in the nested scope
// at dir names); it is clunked when the test ends.
func (t *T) OpenCtl(dir ...string) *ctlFid {
	c, err := openCtl(t.ctx, t.root, dir...)
	if err != nil {
		t.Fatalf("open .fs/ctl: %v", err)
	}
	t.Cleanup(func() { c.fid.Clunk(context.Background()) })
	return c
}

// DoLine sends a raw command line on a fresh ctl fid and returns the reply
// line.
func (t *T) DoLine(line string) string {
	c := t.OpenCtl()
	r, err := c.call(t.ctx, line)
	if err != nil {
		t.Fatalf("ctl %q: %v", line, err)
	}
	c.fid.Clunk(t.ctx)
	return r
}

// Do sends cmd and parses the reply; transport and syntax errors stop the
// test.
func (t *T) Do(cmd fsproto.Command) fsproto.Reply {
	line := cmd.Encode()
	raw := t.DoLine(line)
	r, err := fsproto.ParseReply(cmd.Name(), raw)
	if err != nil {
		t.Fatalf("ctl %q: bad reply %q: %v", line, raw, err)
	}
	return r
}

// Must sends cmd and stops the test unless the reply is ok.
func (t *T) Must(cmd fsproto.Command) fsproto.Reply {
	r := t.Do(cmd)
	if r.Status != fsproto.StatusOK {
		t.Fatalf("ctl %q: reply %q, want ok", cmd.Encode(), r.Encode(cmd.Name()))
	}
	return r
}

// Expect sends cmd and records a failure unless the reply has status want.
func (t *T) Expect(cmd fsproto.Command, want fsproto.Status) fsproto.Reply {
	r := t.Do(cmd)
	if r.Status != want {
		t.fail(caller() + fmt.Sprintf("ctl %q: reply %q, want %s", cmd.Encode(), r.Encode(cmd.Name()), want))
	}
	return r
}

// Dir returns a fresh directory for this test, creating it (and the run
// directory) with ctl mkdir.
func (t *T) Dir() string {
	if t.dir != "" {
		return t.dir
	}
	r := t.r
	r.mu.Lock()
	if !r.dirMade && r.dirErr == nil {
		rep := t.DoLine(fsproto.MkdirCmd{Path: r.runDir, Base: fsproto.At(0)}.Encode())
		if strings.HasPrefix(rep, "ok") {
			r.dirMade = true
		} else {
			r.dirErr = fmt.Errorf("mkdir %s: %s", r.runDir, rep)
		}
	}
	err := r.dirErr
	r.mu.Unlock()
	if err != nil {
		t.Fatalf("%v", err)
	}
	t.dir = r.runDir + "/" + strings.ReplaceAll(t.test.ID, "/", "-")
	t.Must(fsproto.MkdirCmd{Path: t.dir})
	return t.dir
}

// P joins names below the test directory.
func (t *T) P(names ...string) string {
	p := t.Dir()
	for _, n := range names {
		p = fsproto.Join(p, n)
	}
	return p
}

// Mkdir creates a directory and returns its version.
func (t *T) Mkdir(p string) uint64 { return t.Must(fsproto.MkdirCmd{Path: p}).Version }

// Stat returns a path's entry from ctl stat, or false if it is missing.
func (t *T) Stat(p string) (fsproto.Entry, bool) {
	r := t.Do(fsproto.StatCmd{Path: p})
	switch r.Status {
	case fsproto.StatusOK:
		if r.Entry == nil {
			t.Fatalf("stat %q: no entry in reply", p)
		}
		return *r.Entry, true
	case fsproto.StatusMissing:
		return fsproto.Entry{}, false
	}
	t.Fatalf("stat %q: reply %q", p, r.Encode("stat"))
	return fsproto.Entry{}, false
}

// testMtime is the mtime the tests give committed files.
const testMtime = 1763491200

// WriteStage creates .fs/stage/<name> and writes data to it.
func (t *T) WriteStage(name string, data []byte) {
	d := t.MustWalk("/", ".fs", "stage")
	f, err := d.Clone(t.ctx)
	if err != nil {
		t.Fatalf("walk .fs/stage: %v", err)
	}
	if err := f.Create(t.ctx, name, wire.OWRONLY, 0o600); err != nil {
		t.Fatalf("create .fs/stage/%s: %v", name, err)
	}
	defer f.Clunk(t.ctx)
	if _, err := f.WriteAt(t.ctx, data, 0); err != nil {
		t.Fatalf("write .fs/stage/%s: %v", name, err)
	}
}

// StageName returns a fresh stage file name.
func StageName() string { return "conform-" + randHex(8) }

// PutFile publishes data at p and returns its version: with staged commit
// when offered, else with a plain 9P write.
func (t *T) PutFile(p string, data []byte) uint64 {
	if t.Has(ExtStage) {
		name := StageName()
		t.WriteStage(name, data)
		e, ok := t.Stat(p)
		base := fsproto.At(0)
		if ok {
			base = fsproto.At(e.Version)
		}
		return t.Must(fsproto.CommitCmd{Stage: name, Path: p, Base: base, Mtime: testMtime, Mode: 0o644}).Version
	}
	if err := t.PlainWrite(p, data, true); err != nil {
		t.Fatalf("plain write %q: %v", p, err)
	}
	e, ok := t.Stat(p)
	if !ok {
		t.Fatalf("%q missing after a plain write", p)
	}
	return e.Version
}

// PlainWrite writes data to p with ordinary 9P: Tlcreate for a new file,
// or Tlopen with O_TRUNC, then Twrite and Tclunk.
func (t *T) PlainWrite(p string, data []byte, create bool) error {
	ctx := t.ctx
	var f *wire.Fid
	var err error
	if create {
		f, err = Walk(ctx, t.root, fsproto.Parent(p))
		if err != nil {
			return err
		}
		if err := f.Create(ctx, fsproto.Name(p), wire.OWRONLY|wire.OTRUNC, 0o644); err != nil {
			f.Clunk(ctx)
			return fmt.Errorf("create: %w", err)
		}
	} else {
		f, err = Walk(ctx, t.root, p)
		if err != nil {
			return err
		}
		if err := f.Open(ctx, wire.OWRONLY|wire.OTRUNC); err != nil {
			f.Clunk(ctx)
			return fmt.Errorf("open: %w", err)
		}
	}
	if _, err := f.WriteAt(ctx, data, 0); err != nil {
		f.Clunk(ctx)
		return fmt.Errorf("write: %w", err)
	}
	return f.Clunk(ctx)
}

// ReadFile reads a file's content with plain 9P.
func ReadFile(ctx context.Context, root *wire.Fid, p string) ([]byte, error) {
	f, err := Walk(ctx, root, p)
	if err != nil {
		return nil, err
	}
	defer f.Clunk(ctx)
	if err := f.Open(ctx, wire.ORDONLY); err != nil {
		return nil, err
	}
	return f.ReadAll(ctx)
}

// MustRead reads a file or stops the test.
func (t *T) MustRead(p string) []byte {
	b, err := ReadFile(t.ctx, t.root, p)
	if err != nil {
		t.Fatalf("read %q: %v", p, err)
	}
	return b
}

// Index is a parsed .fs/index.
type Index struct {
	HeaderLine string
	Header     fsproto.IndexHeader
	Lines      []string // body lines, decoded
	Entries    []fsproto.Entry
}

// Find returns the entry for p.
func (ix *Index) Find(p string) (fsproto.Entry, bool) {
	for _, e := range ix.Entries {
		if e.Path == p {
			return e, true
		}
	}
	return fsproto.Entry{}, false
}

// Within returns the entries at or below dir.
func (ix *Index) Within(dir string) []fsproto.Entry {
	var out []fsproto.Entry
	for _, e := range ix.Entries {
		if fsproto.Within(e.Path, dir) {
			out = append(out, e)
		}
	}
	return out
}

// readIndexRaw opens .fs/index (below dir names), writes accept if given,
// and returns the whole file.
func readIndexRaw(ctx context.Context, root *wire.Fid, accept []string, dir ...string) ([]byte, wire.Qid, error) {
	f, err := root.Walk(ctx, append(append([]string(nil), dir...), ".fs", "index")...)
	if err != nil {
		return nil, wire.Qid{}, err
	}
	defer f.Clunk(ctx)
	mode := wire.ORDONLY
	if len(accept) > 0 {
		mode = wire.ORDWR
	}
	if err := f.Open(ctx, mode); err != nil {
		return nil, wire.Qid{}, fmt.Errorf("open: %w", err)
	}
	if len(accept) > 0 {
		if err := f.Write(ctx, []byte(fsproto.EncodeAccept(accept...))); err != nil {
			return nil, wire.Qid{}, fmt.Errorf("write accept: %w", err)
		}
	}
	b, err := f.ReadAll(ctx)
	return b, f.Qid, err
}

// parseIndex splits and decodes an index file. It reports the first
// problem as an error, after parsing as much as it can.
func parseIndex(b []byte) (*Index, error) {
	nl := bytes.IndexByte(b, '\n')
	if nl < 0 {
		return nil, fmt.Errorf("no header line in %d bytes", len(b))
	}
	ix := &Index{HeaderLine: string(b[:nl])}
	h, err := fsproto.ParseIndexHeader(ix.HeaderLine)
	if err != nil {
		return ix, fmt.Errorf("header %q: %v", ix.HeaderLine, err)
	}
	ix.Header = h
	body := b[nl+1:]
	switch h.Encoding {
	case "":
	case fsproto.EncodingZstd:
		d, err := zstd.NewReader(nil)
		if err != nil {
			return ix, err
		}
		defer d.Close()
		if n := countZstdFrames(body); n != 1 {
			return ix, fmt.Errorf("zstd body has %d frames, want 1", n)
		}
		body, err = d.DecodeAll(body, nil)
		if err != nil {
			return ix, fmt.Errorf("zstd body: %v", err)
		}
	default:
		return ix, fmt.Errorf("header names encoding %q, which was not accepted", h.Encoding)
	}
	if len(body) > 0 && body[len(body)-1] != '\n' {
		return ix, fmt.Errorf("body does not end with a newline")
	}
	var firstErr error
	for _, line := range strings.SplitAfter(string(body), "\n") {
		if line == "" {
			continue
		}
		line = strings.TrimSuffix(line, "\n")
		ix.Lines = append(ix.Lines, line)
		e, err := fsproto.ParseEntry(line)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("line %q: %v", line, err)
			}
			continue
		}
		ix.Entries = append(ix.Entries, e)
	}
	return ix, firstErr
}

// countZstdFrames counts the zstd frames in b by walking frame headers. It
// returns -1 if b is not a sequence of whole frames.
func countZstdFrames(b []byte) int {
	n := 0
	for len(b) > 0 {
		var h zstd.Header
		if err := h.Decode(b); err != nil {
			return -1
		}
		if h.Skippable {
			end := h.HeaderSize + int(h.SkippableSize)
			if end > len(b) {
				return -1
			}
			b = b[end:]
			n++
			continue
		}
		// Walk the blocks to find the frame's end.
		off := h.HeaderSize
		for {
			if off+3 > len(b) {
				return -1
			}
			bh := uint32(b[off]) | uint32(b[off+1])<<8 | uint32(b[off+2])<<16
			last := bh&1 == 1
			btype := (bh >> 1) & 3
			size := int(bh >> 3)
			off += 3
			if btype == 1 { // RLE: one byte of content
				size = 1
			}
			off += size
			if last {
				break
			}
		}
		if h.HasCheckSum {
			off += 4
		}
		if off > len(b) {
			return -1
		}
		b = b[off:]
		n++
	}
	return n
}

// ReadIndex reads and parses the index, stopping the test on any error.
func (t *T) ReadIndex(accept ...string) *Index {
	b, _, err := readIndexRaw(t.ctx, t.root, accept)
	if err != nil {
		t.Fatalf("read .fs/index: %v", err)
	}
	ix, err := parseIndex(b)
	if err != nil {
		t.Fatalf(".fs/index: %v", err)
	}
	return ix
}

// Stream is one open .fs/events fid.
type Stream struct {
	t     *T
	fid   *wire.Fid
	buf   []fsproto.Event
	Ended bool // a read returned no data after a reset
}

// OpenEvents opens .fs/events (below dir names) and writes setup, if not
// empty, in one write.
func (t *T) OpenEvents(setup string, dir ...string) *Stream {
	f, err := t.root.Walk(t.ctx, append(append([]string(nil), dir...), ".fs", "events")...)
	if err != nil {
		t.Fatalf("walk .fs/events: %v", err)
	}
	t.Cleanup(func() { f.Clunk(context.Background()) })
	if err := f.Open(t.ctx, wire.ORDWR); err != nil {
		t.Fatalf("open .fs/events: %v", err)
	}
	if setup != "" {
		if err := f.Write(t.ctx, []byte(setup)); err != nil {
			t.Fatalf("write %q to .fs/events: %v", setup, err)
		}
	}
	return &Stream{t: t, fid: f}
}

// SetupSince returns a setup write for since n and any path filters.
func SetupSince(n uint64, paths ...string) string {
	return fsproto.EventsSetup{Since: n, HasSince: true, Paths: paths}.Encode()
}

// readCount is the Tread count for events.
const readCount = 64 << 10

// decodeRecords checks that one read's data is whole records and parses
// them.
func decodeRecords(b []byte) ([]fsproto.Event, error) {
	if len(b) == 0 {
		return nil, nil
	}
	if b[len(b)-1] != '\n' {
		return nil, fmt.Errorf("read does not end with a whole record: %q", tail(b))
	}
	var out []fsproto.Event
	for _, line := range strings.Split(strings.TrimSuffix(string(b), "\n"), "\n") {
		ev, err := fsproto.ParseEvent(line)
		if err != nil {
			return out, fmt.Errorf("record %q: %v", line, err)
		}
		out = append(out, ev)
	}
	return out, nil
}

func tail(b []byte) []byte {
	if len(b) > 80 {
		return b[len(b)-80:]
	}
	return b
}

// errTimeout is returned by Stream.Next when no record arrives in time.
var errTimeout = errors.New("no record before the timeout")

// read does one Tread with a timeout; on timeout the read is flushed.
func (s *Stream) read(timeout time.Duration) ([]fsproto.Event, error) {
	ctx, cancel := context.WithTimeout(s.t.ctx, timeout)
	defer cancel()
	b, err := s.fid.ReadAt(ctx, 0, readCount)
	if err != nil {
		if ctx.Err() != nil && s.t.ctx.Err() == nil {
			return nil, errTimeout
		}
		return nil, err
	}
	if len(b) == 0 {
		s.Ended = true
		return nil, io.EOF
	}
	return decodeRecords(b)
}

// Next returns the next record, waiting up to timeout. It returns
// errTimeout when none arrives and io.EOF when the stream has ended.
// Other errors stop the test.
func (s *Stream) Next(timeout time.Duration) (fsproto.Event, error) {
	for len(s.buf) == 0 {
		if s.Ended {
			return fsproto.Event{}, io.EOF
		}
		evs, err := s.read(timeout)
		if err == errTimeout || err == io.EOF {
			return fsproto.Event{}, err
		}
		if err != nil {
			s.t.Fatalf("read .fs/events: %v", err)
		}
		s.buf = evs
	}
	ev := s.buf[0]
	s.buf = s.buf[1:]
	return ev, nil
}

// MustNext returns the next record or stops the test.
func (s *Stream) MustNext() fsproto.Event {
	ev, err := s.Next(5 * time.Second)
	if err != nil {
		s.t.Fatalf("waiting for a record: %v", err)
	}
	return ev
}

// Until reads records until one with seq at or past seq arrives (and
// returns it last), stopping the test after 5 seconds without one.
func (s *Stream) Until(seq uint64) []fsproto.Event {
	var out []fsproto.Event
	for {
		ev, err := s.Next(5 * time.Second)
		if err != nil {
			s.t.Fatalf("waiting for seq %d: %v (got %d records)", seq, err, len(out))
		}
		out = append(out, ev)
		if ev.Seq >= seq {
			return out
		}
	}
}

// CurrentSeq opens an events stream without since and returns the seq
// from its first record.
func (t *T) CurrentSeq() uint64 {
	s := t.OpenEvents("")
	ev := s.MustNext()
	if ev.Op != fsproto.OpSeq {
		t.Fatalf("first record without since is %q, want seq <current>", ev.Encode())
	}
	s.fid.Clunk(t.ctx)
	return ev.Seq
}

// Marker makes a change in dir that tests read up to, so they know every
// earlier record has arrived. It returns the change's seq.
func (t *T) Marker(dir string) uint64 {
	return t.Mkdir(fsproto.Join(dir, "marker-"+randHex(4)))
}
