package shim_test

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hugelgupf/p9/fsimpl/localfs"
	"github.com/hugelgupf/p9/p9"

	"github.com/progrium/dotfs/conformance/internal/wire"
	"github.com/progrium/dotfs/conformance/shim"
)

// TestP9ClientAgainstShim checks the shim's side of the codec with
// hugelgupf/p9's client: a ctl round trip, a directory listing, and a
// plain-write file read back.
func TestP9ClientAgainstShim(t *testing.T) {
	s := shim.New(shim.Options{})
	cl, err := p9.NewClient(s.Pipe())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	root, err := cl.Attach("")
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
	if _, err := ctl.WriteAt([]byte("mkdir /d"), 0); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 100)
	n, err := ctl.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "ok 1\n" {
		t.Fatalf("mkdir reply %q", got)
	}
	ctl.Close()

	_, d, err := root.Walk([]string{"d"})
	if err != nil {
		t.Fatal(err)
	}
	f, _, _, err := d.Create("hello.txt", p9.WriteOnly, 0o644, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("hello"), 0); err != nil {
		t.Fatal(err)
	}
	f.Close() // commits the plain write

	_, d2, err := root.Walk([]string{"d"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := d2.Open(p9.ReadOnly); err != nil {
		t.Fatal(err)
	}
	ents, err := d2.Readdir(0, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 1 || ents[0].Name != "hello.txt" {
		t.Fatalf("readdir = %+v", ents)
	}
	_, hf, err := root.Walk([]string{"d", "hello.txt"})
	if err != nil {
		t.Fatal(err)
	}
	_, _, attr, err := hf.GetAttr(p9.AttrMaskAll)
	if err != nil {
		t.Fatal(err)
	}
	if attr.Size != 5 || attr.Mode.FileType() != p9.ModeRegular {
		t.Fatalf("attr = %+v", attr)
	}
	if _, _, err := hf.Open(p9.ReadOnly); err != nil {
		t.Fatal(err)
	}
	n, err = hf.ReadAt(buf, 0)
	if err != nil && n == 0 {
		t.Fatal(err)
	}
	if string(buf[:n]) != "hello" {
		t.Fatalf("read %q", buf[:n])
	}
}

// TestWireClientAgainstP9 checks the client side of the codec against
// hugelgupf/p9's server over a local directory.
func TestWireClientAgainstP9(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a.txt", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go p9.NewServer(localfs.Attacher(dir)).Serve(l)

	nc, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c, err := wire.NewConn(nc)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	root, err := c.Attach(ctx, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if root.Qid.Type != wire.QTDir {
		t.Fatalf("root qid %+v", root.Qid)
	}
	f, err := root.Walk(ctx, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	a, err := f.GetAttr(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if a.Size != 7 || a.Mode&wire.SIFMT != wire.SIFREG {
		t.Fatalf("attr %+v", a)
	}
	if err := f.Open(ctx, wire.ORDONLY); err != nil {
		t.Fatal(err)
	}
	b, err := f.ReadAll(ctx)
	if err != nil || string(b) != "content" {
		t.Fatalf("read %q, %v", b, err)
	}
	if err := f.Clunk(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Walk(ctx, "nope"); !wire.IsErrno(err, wire.ENOENT) {
		t.Fatalf("walk to a missing name: %v", err)
	}
	l2, err := root.Walk(ctx, "link")
	if err != nil {
		t.Fatal(err)
	}
	if tgt, err := l2.Readlink(ctx); err != nil || tgt != "a.txt" {
		t.Fatalf("readlink %q, %v", tgt, err)
	}
	d, err := root.Clone(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Mkdir(ctx, "sub", 0o755); err != nil {
		t.Fatal(err)
	}
	nf, err := root.Walk(ctx, "sub")
	if err != nil {
		t.Fatal(err)
	}
	if err := nf.Create(ctx, "new.txt", wire.OWRONLY, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := nf.WriteAt(ctx, []byte("written"), 0); err != nil {
		t.Fatal(err)
	}
	nf.Clunk(ctx)
	got, err := os.ReadFile(filepath.Join(dir, "sub", "new.txt"))
	if err != nil || !bytes.Equal(got, []byte("written")) {
		t.Fatalf("file %q, %v", got, err)
	}
	if err := d.Open(ctx, wire.ORDONLY); err != nil {
		t.Fatal(err)
	}
	ents, err := d.Readdir(ctx)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, e := range ents {
		names[e.Name] = true
	}
	for _, n := range []string{"a.txt", "link", "sub"} {
		if !names[n] {
			t.Errorf("readdir is missing %q: %+v", n, ents)
		}
	}
}
