package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/progrium/dotfs/fsproto"
)

func TestLocalIndex(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "a b/c"), 0o755)
	os.MkdirAll(filepath.Join(dir, "sub/.fs"), 0o755)
	os.WriteFile(filepath.Join(dir, "a b/c/f.txt"), []byte("hello"), 0o644)
	os.WriteFile(filepath.Join(dir, "sub/.fs/index"), []byte("skip me"), 0o644)
	os.Symlink("a b/c/f.txt", filepath.Join(dir, "link"))

	var buf bytes.Buffer
	if err := writeLocalIndex(context.Background(), &buf, dir, true); err != nil {
		t.Fatal(err)
	}
	r, err := fsproto.NewIndexReader(&buf, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]fsproto.Entry{}
	var order []string
	for {
		e, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got[e.Path] = e
		order = append(order, e.Path)
	}
	want := []string{"/", "/a b", "/a b/c", "/a b/c/f.txt", "/link", "/sub"}
	if len(order) != len(want) {
		t.Fatalf("paths %q, want %q", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("paths %q, want %q", order, want)
		}
	}
	if f := got["/a b/c/f.txt"]; f.Length != 5 || f.Hash != fsproto.HashBytes([]byte("hello")) || f.Mode != 0o644 {
		t.Errorf("file entry %+v", f)
	}
	if l := got["/link"]; l.Type != fsproto.TypeSymlink || l.Target != "a b/c/f.txt" {
		t.Errorf("symlink entry %+v", l)
	}
}
