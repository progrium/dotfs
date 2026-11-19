package ninep

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hugelgupf/p9/linux"
	"github.com/hugelgupf/p9/p9"
	"github.com/klauspost/compress/zstd"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/auth"
	"github.com/progrium/dotfs/server/events"
	"github.com/progrium/dotfs/server/fake"
	"github.com/progrium/dotfs/server/meta"
	"github.com/progrium/dotfs/server/ninep/frame"
	"github.com/progrium/dotfs/server/store"
)

var bg = context.Background()

type env struct {
	t      *testing.T
	meta   server.Meta
	store  server.Store
	tokens server.Tokens
	srv    *Server
	home   server.Scope
}

// envMaker builds an env on one set of backends.
type envMaker func(t *testing.T, cfg Config) *env

// both runs a test against the fakes and against the real store, meta,
// and events packages.
func both(t *testing.T, f func(t *testing.T, newEnv envMaker)) {
	t.Run("fake", func(t *testing.T) { f(t, newFakeEnv) })
	t.Run("real", func(t *testing.T) { f(t, newRealEnv) })
}

func newFakeEnv(t *testing.T, cfg Config) *env {
	t.Helper()
	fs := fake.NewServer()
	return finishEnv(t, cfg, fs.Meta, fs.Store, fs.Hub, fs.Tokens)
}

func newRealEnv(t *testing.T, cfg Config) *env {
	t.Helper()
	dir := t.TempDir()
	st, err := store.New(filepath.Join(dir, "data"), &store.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	m, err := meta.Open(filepath.Join(dir, "meta.db"), st, nil, &meta.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	h := events.New(m, events.Options{})
	m.SetPublisher(h)
	return finishEnv(t, cfg, m, st, h, m)
}

func finishEnv(t *testing.T, cfg Config, m server.Meta, st server.Store, h server.Hub, tk server.Tokens) *env {
	t.Helper()
	var home server.Scope
	for _, u := range []string{"alice", "bob"} {
		r := server.NewRoot(u, "home")
		home = server.RootScope(r)
		if err := st.CreateRoot(r); err != nil {
			t.Fatal(err)
		}
		if err := m.CreateScope(bg, home); err != nil {
			t.Fatal(err)
		}
	}
	home = server.RootScope(server.NewRoot("alice", "home"))
	cfg.Meta, cfg.Store, cfg.Hub = m, st, h
	if cfg.Auth.Tokens == nil {
		cfg.Auth.Tokens = tk
	}
	srv := New(cfg)
	t.Cleanup(func() { srv.Close() })
	return &env{t: t, meta: m, store: st, tokens: tk, srv: srv, home: home}
}

// pipe returns the client end of a connection served by e.srv.
func (e *env) pipe(user string) net.Conn {
	c, s := net.Pipe()
	go e.srv.ServeTransport(bg, frame.NewStream(s), user)
	e.t.Cleanup(func() { c.Close() })
	return c
}

// attach connects a p9 client and attaches to aname.
func (e *env) attach(aname string) p9.File {
	e.t.Helper()
	cl, err := p9.NewClient(e.pipe(""))
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { cl.Close() })
	root, err := cl.Attach(aname)
	if err != nil {
		e.t.Fatalf("attach %q: %v", aname, err)
	}
	return root
}

func walk(t *testing.T, f p9.File, path string) p9.File {
	t.Helper()
	var names []string
	if path != "" {
		names = strings.Split(path, "/")
	}
	_, g, err := f.Walk(names)
	if err != nil {
		t.Fatalf("walk %q: %v", path, err)
	}
	return g
}

func readFile(t *testing.T, root p9.File, path string) string {
	t.Helper()
	f := walk(t, root, path)
	defer f.Close()
	if _, _, err := f.Open(p9.ReadOnly); err != nil {
		t.Fatalf("open %q: %v", path, err)
	}
	var out []byte
	buf := make([]byte, 4096)
	for off := int64(0); ; {
		n, err := f.ReadAt(buf, off)
		if err != nil && err != io.EOF {
			t.Fatalf("read %q: %v", path, err)
		}
		if n == 0 {
			break
		}
		out = append(out, buf[:n]...)
		off += int64(n)
	}
	return string(out)
}

func writeNew(t *testing.T, root p9.File, dir, name, data string) {
	t.Helper()
	d := walk(t, root, dir)
	f, _, _, err := d.Create(name, p9.WriteOnly, 0o640, 0, 0)
	if err != nil {
		t.Fatalf("create %s/%s: %v", dir, name, err)
	}
	if _, err := f.WriteAt([]byte(data), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func (e *env) stat(p string) fsproto.Entry {
	e.t.Helper()
	ent, err := e.meta.Stat(bg, e.home, p)
	if err != nil {
		e.t.Fatalf("stat %s: %v", p, err)
	}
	return ent
}

// ctl opens a ctl fid.
type ctlFid struct {
	t *testing.T
	f p9.File
}

func openCtl(t *testing.T, root p9.File, path string) *ctlFid {
	t.Helper()
	f := walk(t, root, path)
	if _, _, err := f.Open(p9.ReadWrite); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return &ctlFid{t: t, f: f}
}

func (c *ctlFid) do(cmd string) string {
	c.t.Helper()
	if _, err := c.f.WriteAt([]byte(cmd), 0); err != nil {
		c.t.Fatalf("write %q: %v", cmd, err)
	}
	buf := make([]byte, 8192)
	n, err := c.f.ReadAt(buf, 0)
	if err != nil && err != io.EOF {
		c.t.Fatalf("read reply to %q: %v", cmd, err)
	}
	r := string(buf[:n])
	if !strings.HasSuffix(r, "\n") {
		c.t.Fatalf("reply to %q not a line: %q", cmd, r)
	}
	return strings.TrimSuffix(r, "\n")
}

func TestDotLFiles(t *testing.T) {
	both(t, func(t *testing.T, newEnv envMaker) {
		e := newEnv(t, Config{})
		root := e.attach("alice/home")

		writeNew(t, root, "", "a.txt", "hello world")
		if got := readFile(t, root, "a.txt"); got != "hello world" {
			t.Fatalf("read = %q", got)
		}
		ent := e.stat("/a.txt")
		if ent.Mode != 0o640 || ent.Length != 11 || ent.Hash != fsproto.HashBytes([]byte("hello world")) {
			t.Fatalf("entry %+v", ent)
		}

		// Overwrite part of the file without truncating: the rest stays.
		f := walk(t, root, "a.txt")
		if _, _, err := f.Open(p9.WriteOnly); err != nil {
			t.Fatal(err)
		}
		f.WriteAt([]byte("HELLO"), 0)
		if _, _, a, err := f.GetAttr(p9.AttrMaskAll); err != nil || a.Size != 11 {
			t.Fatalf("getattr while writing: %v %+v", err, a)
		}
		// Others still see the old content until clunk.
		if got := readFile(t, root, "a.txt"); got != "hello world" {
			t.Fatalf("read before clunk = %q", got)
		}
		f.Close()
		if got := readFile(t, root, "a.txt"); got != "HELLO world" {
			t.Fatalf("read after clunk = %q", got)
		}

		// Truncating open.
		f = walk(t, root, "a.txt")
		if _, _, err := f.Open(p9.WriteOnly | dotlTrunc); err != nil {
			t.Fatal(err)
		}
		f.WriteAt([]byte("short"), 0)
		f.Close()
		if got := readFile(t, root, "a.txt"); got != "short" {
			t.Fatalf("after trunc = %q", got)
		}

		// setattr size and mtime.
		f = walk(t, root, "a.txt")
		if err := f.SetAttr(p9.SetAttrMask{Size: true, MTime: true, MTimeNotSystemTime: true, Permissions: true},
			p9.SetAttr{Size: 3, MTimeSeconds: 1000, Permissions: 0o600}); err != nil {
			t.Fatal(err)
		}
		f.Close()
		if got := readFile(t, root, "a.txt"); got != "sho" {
			t.Fatalf("after setattr = %q", got)
		}
		if ent := e.stat("/a.txt"); ent.Mtime != 1000 || ent.Mode != 0o600 {
			t.Fatalf("after setattr %+v", ent)
		}

		// Directories, rename, symlink, and unlink.
		if _, err := root.Mkdir("d", 0o755, 0, 0); err != nil {
			t.Fatal(err)
		}
		if _, err := root.Mkdir("d", 0o755, 0, 0); !errors.Is(err, linux.EEXIST) {
			t.Fatalf("mkdir existing: %v", err)
		}
		d := walk(t, root, "d")
		if err := root.RenameAt("a.txt", d, "b.txt"); err != nil {
			t.Fatal(err)
		}
		if got := readFile(t, root, "d/b.txt"); got != "sho" {
			t.Fatalf("renamed = %q", got)
		}
		if _, err := root.Symlink("d/b.txt", "link", 0, 0); err != nil {
			t.Fatal(err)
		}
		l := walk(t, root, "link")
		if target, err := l.Readlink(); err != nil || target != "d/b.txt" {
			t.Fatalf("readlink = %q, %v", target, err)
		}
		if err := root.UnlinkAt("d", 0x200); !errors.Is(err, linux.ENOTEMPTY) {
			t.Fatalf("rmdir non-empty: %v", err)
		}

		// Listing: .fs is walkable but not listed.
		if _, _, err := root.Open(p9.ReadOnly); err != nil {
			t.Fatal(err)
		}
		ds, err := root.Readdir(0, 4096)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, de := range ds {
			names = append(names, de.Name)
		}
		if strings.Join(names, " ") != "d link" {
			t.Fatalf("readdir = %v", names)
		}
		walk(t, root, ".fs/ctl")

		if err := d.UnlinkAt("b.txt", 0); err != nil {
			t.Fatal(err)
		}
		if _, _, err := root.Walk([]string{"d", "b.txt"}); !errors.Is(err, linux.ENOENT) {
			t.Fatalf("walk removed: %v", err)
		}

		// Names with ".fs" cannot be created.
		if _, err := root.Mkdir(".fs", 0o755, 0, 0); err == nil {
			t.Fatal("mkdir .fs succeeded")
		}
	})
}

func TestDotLCtl(t *testing.T) {
	both(t, func(t *testing.T, newEnv envMaker) {
		e := newEnv(t, Config{})
		root := e.attach("alice/home")
		c := openCtl(t, root, ".fs/ctl")

		if r := c.do("caps"); !strings.HasPrefix(r, "ok caps hello mkdir") || !strings.Contains(r, "commit-hash") {
			t.Fatalf("caps = %q", r)
		}
		if r := c.do("hello mac-a1\n"); r != "ok" {
			t.Fatalf("hello = %q", r)
		}
		if r := c.do("mkdir /docs 0"); r != "ok 1" {
			t.Fatalf("mkdir = %q", r)
		}
		if r := c.do("mkdir /docs"); r != "conflict 1" {
			t.Fatalf("mkdir again = %q", r)
		}

		// Staged upload, resumed, then committed with a hash.
		st := walk(t, root, ".fs/stage")
		up, _, _, err := st.Create("u1", p9.WriteOnly, 0o600, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		up.WriteAt([]byte("first "), 0)
		up.Close()
		if _, _, _, err := walk(t, root, ".fs/stage").Create("u1", p9.WriteOnly, 0o600, 0, 0); !errors.Is(err, linux.EEXIST) {
			t.Fatalf("create existing stage: %v", err)
		}
		up = walk(t, root, ".fs/stage/u1")
		_, _, a, err := up.GetAttr(p9.AttrMaskAll)
		if err != nil || a.Size != 6 {
			t.Fatalf("stage size %d, %v", a.Size, err)
		}
		if _, _, err := up.Open(p9.WriteOnly); err != nil {
			t.Fatal(err)
		}
		up.WriteAt([]byte("second"), int64(a.Size))
		up.Close()
		data := "first second"
		h := fsproto.HashBytes([]byte(data))
		if r := c.do("commit u1 /docs/f.txt 0 1700000000 0644 hash=" + h); r != "ok 2 "+h {
			t.Fatalf("commit = %q", r)
		}
		if got := readFile(t, root, "docs/f.txt"); got != data {
			t.Fatalf("committed = %q", got)
		}
		if _, _, err := root.Walk([]string{".fs", "stage", "u1"}); !errors.Is(err, linux.ENOENT) {
			t.Fatalf("stage file after commit: %v", err)
		}

		for _, tc := range []struct{ cmd, want string }{
			{"stat /docs/f.txt", "ok /docs/f.txt f 0644 12 1700000000 2 " + h + " -"},
			{"commit-hash " + h + " /copy.txt 0 1700000001 0600", "ok 3"},
			{"commit-hash b3:" + strings.Repeat("0", 64) + " /nope.txt 0 1 0644", "missing"},
			{"cp /docs/f.txt /docs/g.txt 0", "ok 4"},
			{"mv /docs/g.txt /h.txt 4 0", "ok 5"},
			{"mv /docs/f.txt /h.txt - 0", "conflict 5"},
			{"flags /docs p", "ok 6"},
			{"rm /h.txt 1", "conflict 5"},
			{"rm /h.txt 5", "ok 7"},
			{"rm /docs", "invalid"},
			{"rmtree /docs 6", "ok 8"},
			{"stat /docs", "missing"},
			{"abort nothing", "ok"},
			{"mkdir /.fs", "invalid"},
			{"frobnicate", "invalid"},
		} {
			r := c.do(tc.cmd)
			// Bare "invalid" and "missing" accept any msg=.
			loose := (tc.want == "invalid" || tc.want == "missing") && strings.HasPrefix(r, tc.want)
			if r != tc.want && !loose {
				t.Errorf("%s = %q, want %q", tc.cmd, r, tc.want)
			}
		}

		// A plain write after hello reports the device.
		evs := e.meta
		writeNew(t, root, "", "plain.txt", "x")
		var last fsproto.Event
		evs.ReadLog(bg, e.home, 0, func(ev fsproto.Event) error { last = ev; return nil })
		if last.Op != fsproto.OpPut || last.Entry.Path != "/plain.txt" || last.Dev != "mac-a1" {
			t.Fatalf("last event %+v", last)
		}
	})
}

func TestDotLIndex(t *testing.T) {
	both(t, func(t *testing.T, newEnv envMaker) {
		e := newEnv(t, Config{})
		root := e.attach("alice/home")
		writeNew(t, root, "", "a b.txt", "hi")
		root.Mkdir("dir", 0o755, 0, 0)

		plain := readFile(t, root, ".fs/index")
		r, err := fsproto.NewIndexReader(strings.NewReader(plain), nil)
		if err != nil {
			t.Fatal(err)
		}
		// Creating a file over plain 9P is two changes: the empty file at
		// create, then its content at clunk.
		if r.Header().Seq != 3 || r.Header().Encoding != "" {
			t.Fatalf("header %+v", r.Header())
		}
		var paths []string
		for {
			ent, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			paths = append(paths, ent.Path)
		}
		if strings.Join(paths, ",") != "/,/a b.txt,/dir" {
			t.Fatalf("paths %q", paths)
		}

		// qid.vers of .fs/index is the seq.
		q, _, _, err := walk(t, root, ".fs/index").GetAttr(p9.AttrMaskAll)
		if err != nil || q.Version != 3 {
			t.Fatalf("index qid %+v, %v", q, err)
		}

		// accept zstd, written to the index fid before reading.
		f := walk(t, root, ".fs/index")
		if _, _, err := f.Open(p9.ReadWrite); err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt([]byte("accept zstd"), 0); err != nil {
			t.Fatal(err)
		}
		var raw []byte
		buf := make([]byte, 1024)
		for off := int64(0); ; {
			n, _ := f.ReadAt(buf, off)
			if n == 0 {
				break
			}
			raw = append(raw, buf[:n]...)
			off += int64(n)
		}
		f.Close()
		line, body, _ := bytes.Cut(raw, []byte("\n"))
		if string(line) != "fsindex 1 seq 3 zstd" {
			t.Fatalf("zstd header %q", line)
		}
		dec, _ := zstd.NewReader(nil)
		got, err := dec.DecodeAll(body, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, wantBody, _ := strings.Cut(plain, "\n")
		if string(got) != wantBody {
			t.Fatalf("zstd body %q, want %q", got, wantBody)
		}
	})
}

// rawL is a hand-driven 9P2000.L client, for Tflush, which p9's client
// cannot send.
type rawL struct {
	t    *testing.T
	tr   frame.Transport
	msgs chan []byte
}

func (e *env) rawL(aname string) *rawL {
	r := &rawL{t: e.t, tr: frame.NewStream(e.pipe("")), msgs: make(chan []byte, 16)}
	go func() {
		for {
			f, err := r.tr.ReadFrame()
			if err != nil {
				close(r.msgs)
				return
			}
			r.msgs <- f
		}
	}()
	r.send(newEnc(tversion, 0xffff).u32(65536).str("9P2000.L").bytes())
	r.expect(rversion, 0xffff)
	r.send(newEnc(tattach, 1).u32(0).u32(noFID).str("").str(aname).u32(^uint32(0)).bytes())
	r.expect(rattach, 1)
	return r
}

func (r *rawL) send(b []byte) {
	if err := r.tr.WriteFrame(b); err != nil {
		r.t.Fatal(err)
	}
}

func (r *rawL) next(timeout time.Duration) ([]byte, bool) {
	select {
	case f, ok := <-r.msgs:
		return f, ok
	case <-time.After(timeout):
		return nil, false
	}
}

func (r *rawL) expect(typ uint8, tag uint16) *dec {
	r.t.Helper()
	f, ok := r.next(5 * time.Second)
	if !ok {
		r.t.Fatalf("no reply, want type %d tag %d", typ, tag)
	}
	if frame.Type(f) != typ || frame.Tag(f) != tag {
		r.t.Fatalf("got type %d tag %d, want type %d tag %d", frame.Type(f), frame.Tag(f), typ, tag)
	}
	return newDec(f)
}

func (r *rawL) quiet(d time.Duration) {
	r.t.Helper()
	if f, ok := r.next(d); ok {
		r.t.Fatalf("unexpected message type %d tag %d", frame.Type(f), frame.Tag(f))
	}
}

func (r *rawL) walkOpen(fid uint32, path string, tag uint16) {
	r.t.Helper()
	m := newEnc(twalk, tag).u32(0).u32(fid)
	names := strings.Split(path, "/")
	m.u16(uint16(len(names)))
	for _, n := range names {
		m.str(n)
	}
	r.send(m.bytes())
	r.expect(rwalk, tag)
	r.send(newEnc(tlopen, tag).u32(fid).u32(2).bytes())
	r.expect(rlopen, tag)
}

func (r *rawL) readRecords(fid uint32, tag uint16) string {
	r.t.Helper()
	r.send(newEnc(tread, tag).u32(fid).u64(0).u32(8192).bytes())
	d := r.expect(rread, tag)
	return string(d.take(int(d.u32())))
}

func TestDotLEventsFlush(t *testing.T) {
	both(t, func(t *testing.T, newEnv envMaker) {
		e := newEnv(t, Config{})
		root := e.attach("alice/home")
		writeNew(t, root, "", "a.txt", "1")

		r := e.rawL("alice/home")
		r.walkOpen(1, ".fs/events", 2)
		if got := r.readRecords(1, 3); got != "seq 2\n" {
			t.Fatalf("first read %q", got)
		}

		// A read with nothing to report blocks until flushed.
		r.send(newEnc(tread, 4).u32(1).u64(0).u32(8192).bytes())
		r.quiet(100 * time.Millisecond)
		r.send(newEnc(tflush, 5).u16(4).bytes())
		r.expect(rflush, 5)
		r.quiet(100 * time.Millisecond)

		// The stream is still usable, and a blocked read wakes up on a change.
		r.send(newEnc(tread, 6).u32(1).u64(0).u32(8192).bytes())
		r.quiet(50 * time.Millisecond)
		c := openCtl(t, root, ".fs/ctl")
		c.do("hello dev-x")
		c.do("mkdir /new")
		d := r.expect(rread, 6)
		if got := string(d.take(int(d.u32()))); !strings.HasPrefix(got, "3 put /new d 0755 0 ") || !strings.HasSuffix(got, " dev=dev-x\n") {
			t.Fatalf("event %q", got)
		}

		// A second stream with setup: since and a path filter.
		r.walkOpen(7, ".fs/events", 8)
		r.send(newEnc(twrite, 9).u32(7).u64(0).data([]byte("since 0\npath /new")).bytes())
		r.expect(rwrite, 9)
		if got := r.readRecords(7, 10); !strings.HasPrefix(got, "3 put /new ") || strings.Count(got, "\n") != 1 {
			t.Fatalf("filtered replay %q", got)
		}

		// Clunking an events fid ends its pending read.
		r.send(newEnc(tread, 11).u32(7).u64(0).u32(8192).bytes())
		r.quiet(50 * time.Millisecond)
		r.send(newEnc(tclunk, 12).u32(7).bytes())
		got := map[uint16]uint8{}
		for i := 0; i < 2; i++ {
			f, ok := r.next(5 * time.Second)
			if !ok {
				t.Fatal("no reply")
			}
			got[frame.Tag(f)] = frame.Type(f)
		}
		if got[11] != rlerror || got[12] != tclunk+1 {
			t.Fatalf("replies to read and clunk: %v", got)
		}
	})
}

func TestDotLEventsCloseUnblocks(t *testing.T) {
	both(t, func(t *testing.T, newEnv envMaker) {
		// A blocked events read must not keep the connection from ending.
		e := newEnv(t, Config{})
		c, s := net.Pipe()
		done := make(chan error)
		go func() { done <- e.srv.ServeTransport(bg, frame.NewStream(s), "") }()
		cl, err := p9.NewClient(c)
		if err != nil {
			t.Fatal(err)
		}
		root, err := cl.Attach("alice/home")
		if err != nil {
			t.Fatal(err)
		}
		f := walk(t, root, ".fs/events")
		if _, _, err := f.Open(p9.ReadOnly); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 4096)
		if n, err := f.ReadAt(buf, 0); err != nil || string(buf[:n]) != "seq 0\n" {
			t.Fatalf("first read %q %v", buf[:n], err)
		}
		go f.ReadAt(buf, 0)
		time.Sleep(50 * time.Millisecond)
		c.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("connection did not end")
		}
	})
}

func TestAttachPolicy(t *testing.T) {
	both(t, func(t *testing.T, newEnv envMaker) {
		e := newEnv(t, Config{})
		// Local listeners trust the attach to name its user ("bob/home" works),
		// but a missing root, no user, or a bad token does not.
		for _, aname := range []string{"alice/nope", "", "home", "alice/home?token=bad"} {
			cl, err := p9.NewClient(e.pipe(""))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := cl.Attach(aname); err == nil {
				t.Errorf("attach %q by nobody succeeded", aname)
			}
			cl.Close()
		}

		// A transport-authenticated user can only reach their own roots.
		cl, _ := p9.NewClient(e.pipe("alice"))
		if _, err := cl.Attach("bob/home"); !errors.Is(err, linux.EACCES) {
			t.Fatalf("alice attach bob/home: %v", err)
		}
		if _, err := cl.Attach("home"); err != nil {
			t.Fatalf("alice attach home: %v", err)
		}

		// RequireToken: local attaches need a token, which names the user.
		e = newEnv(t, Config{Auth: auth.Policy{RequireToken: true}})
		cl, _ = p9.NewClient(e.pipe(""))
		if _, err := cl.Attach("alice/home"); !errors.Is(err, linux.EACCES) {
			t.Fatalf("attach without token: %v", err)
		}
		tok, _ := e.tokens.CreateToken(bg, "alice")
		if _, err := cl.Attach("home?token=" + tok); err != nil {
			t.Fatalf("attach with token: %v", err)
		}
		if _, err := cl.Attach("bob/home?token=" + tok); !errors.Is(err, linux.EACCES) {
			t.Fatalf("attach bob's root with alice's token: %v", err)
		}

		// CreateRoots makes a missing root of one's own.
		e = newEnv(t, Config{CreateRoots: true})
		root := e.attach("carol/new")
		writeNew(t, root, "", "x", "y")
		if _, err := e.meta.Stat(bg, server.RootScope(server.NewRoot("carol", "new")), "/x"); err != nil {
			t.Fatal(err)
		}
	})
}

func TestDotLNestedScope(t *testing.T) {
	both(t, func(t *testing.T, newEnv envMaker) {
		e := newEnv(t, Config{})
		root := e.attach("alice/home")
		c := openCtl(t, root, ".fs/ctl")
		c.do("mkdir /big")
		if r := c.do("flags /big i"); !strings.HasPrefix(r, "ok") {
			t.Fatal(r)
		}
		nested := server.Scope{Root: e.home.Root, Dir: "/big"}
		// The real meta creates the nested scope when flag i is set.
		if err := e.meta.CreateScope(bg, nested); err != nil && !errors.Is(err, server.ErrScopeExists) {
			t.Fatal(err)
		}
		writeNew(t, root, "big", "inner.txt", "in")
		if _, err := e.meta.Stat(bg, nested, "/inner.txt"); err != nil {
			t.Fatalf("nested stat: %v", err)
		}
		nc := openCtl(t, root, "big/.fs/ctl")
		if r := nc.do("stat /inner.txt"); !strings.HasPrefix(r, "ok /inner.txt f ") {
			t.Fatalf("nested ctl stat = %q", r)
		}
		idx := readFile(t, root, "big/.fs/index")
		if !strings.Contains(idx, "\n/inner.txt f ") {
			t.Fatalf("nested index %q", idx)
		}
		if got := readFile(t, root, "big/inner.txt"); got != "in" {
			t.Fatalf("nested read %q", got)
		}
	})
}

func TestPlain9P(t *testing.T) {
	both(t, func(t *testing.T, newEnv envMaker) {
		e := newEnv(t, Config{})
		r := newRawPlain(t, e.pipe(""))
		r.rpc(&pfcall{Type: tversionP, Msize: 8192, Version: "9P2000"})
		att := r.rpc(&pfcall{Type: tattachP, Fid: 0, Afid: noFID, Uname: "alice", Aname: "home"})
		if att.Type != rattachP {
			t.Fatalf("attach: %v", att)
		}
		// create, write, clunk, then read back.
		r.rpc(&pfcall{Type: twalkP, Fid: 0, Newfid: 1})
		if f := r.rpc(&pfcall{Type: tcreateP, Fid: 1, Name: "p.txt", Perm: 0o644, Mode: 1}); f.Type != rcreateP {
			t.Fatalf("create: %v", f)
		}
		r.rpc(&pfcall{Type: twriteP, Fid: 1, Data: []byte("plain nine")})
		r.rpc(&pfcall{Type: tclunkP, Fid: 1})
		if got := e.stat("/p.txt"); got.Length != 10 || got.Mode != 0o644 {
			t.Fatalf("entry %+v", got)
		}
		r.rpc(&pfcall{Type: twalkP, Fid: 0, Newfid: 2, Wname: []string{"p.txt"}})
		r.rpc(&pfcall{Type: topenP, Fid: 2, Mode: 0})
		if f := r.rpc(&pfcall{Type: treadP, Fid: 2, Count: 100}); string(f.Data) != "plain nine" {
			t.Fatalf("read %q", f.Data)
		}
		// wstat rename within the directory.
		r.rpc(&pfcall{Type: tclunkP, Fid: 2})
		r.rpc(&pfcall{Type: twalkP, Fid: 0, Newfid: 2, Wname: []string{"p.txt"}})
		if f := r.rpc(wstatName(2, "q.txt")); f.Type != rwstatP {
			t.Fatalf("wstat: %v", f)
		}
		e.stat("/q.txt")

		// Root directory read: stat entries, no .fs.
		r.rpc(&pfcall{Type: twalkP, Fid: 0, Newfid: 3})
		r.rpc(&pfcall{Type: topenP, Fid: 3, Mode: 0})
		if f := r.rpc(&pfcall{Type: treadP, Fid: 3, Count: 4096}); !bytes.Contains(f.Data, []byte("q.txt")) || bytes.Contains(f.Data, []byte(".fs")) {
			t.Fatalf("dir read %q", f.Data)
		}

		// ctl and events with flush.
		r.rpc(&pfcall{Type: twalkP, Fid: 0, Newfid: 4, Wname: []string{".fs", "ctl"}})
		r.rpc(&pfcall{Type: topenP, Fid: 4, Mode: 2})
		r.rpc(&pfcall{Type: twriteP, Fid: 4, Data: []byte("mkdir /m")})
		if f := r.rpc(&pfcall{Type: treadP, Fid: 4, Count: 100}); string(f.Data) != "ok 4\n" {
			t.Fatalf("ctl reply %q", f.Data)
		}
		r.rpc(&pfcall{Type: twalkP, Fid: 0, Newfid: 5, Wname: []string{".fs", "events"}})
		r.rpc(&pfcall{Type: topenP, Fid: 5, Mode: 2})
		if f := r.rpc(&pfcall{Type: treadP, Fid: 5, Count: 4096}); string(f.Data) != "seq 4\n" {
			t.Fatalf("events %q", f.Data)
		}
		r.sendTag(&pfcall{Type: treadP, Fid: 5, Count: 4096}, 50)
		r.quiet(100 * time.Millisecond)
		r.sendTag(&pfcall{Type: tflushP, Oldtag: 50}, 51)
		if f := r.recv(); f.Type != rflushP || f.Tag != 51 {
			t.Fatalf("flush reply %v", f)
		}
		r.quiet(100 * time.Millisecond)
		r.sendTag(&pfcall{Type: treadP, Fid: 5, Count: 4096}, 52)
		// The event can be answered before the write that caused it.
		r.sendTag(&pfcall{Type: twriteP, Fid: 4, Data: []byte("rm /m")}, 53)
		for range 2 {
			switch f := r.recv(); f.Tag {
			case 52:
				if string(f.Data) != "5 del /m\n" {
					t.Fatalf("event after flush %v %q", f, f.Data)
				}
			case 53:
				if f.Type != rwriteP {
					t.Fatalf("rm write: %v", f)
				}
			default:
				t.Fatalf("unexpected reply %v", f)
			}
		}

		// Errors are strings.
		if f := r.rpc(&pfcall{Type: twalkP, Fid: 0, Newfid: 9, Wname: []string{"missing"}}); f.Type != rerrorP || f.Ename != "file does not exist" {
			t.Fatalf("walk missing: %v", f)
		}
	})
}
