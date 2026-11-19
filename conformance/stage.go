package conformance

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/progrium/dotfs/conformance/internal/wire"
	"github.com/progrium/dotfs/fsproto"
)

func init() {
	register(
		&Test{
			ID: "stage/commit", Ext: ExtStage, Section: secStPublish, Requires: []Ext{ExtCtl},
			Name: "writes at any offsets, then commit publishes with mode, mtime, and hash",
			Run:  testStageCommit,
		},
		&Test{
			ID: "stage/resume", Ext: ExtStage, Section: secStStaging, Requires: []Ext{ExtCtl},
			Name: "an upload resumes on a new connection from the stage file's size",
			Run:  testStageResume,
		},
		&Test{
			ID: "stage/invisible", Ext: ExtStage, Section: secStStaging, Requires: []Ext{ExtCtl},
			Name: "stage files are listed in .fs/stage/ but not in the index",
			Run:  testStageInvisible,
		},
		&Test{
			ID: "stage/private", Ext: ExtStage, Section: secStStaging, Requires: []Ext{ExtCtl},
			Name: "another user cannot see or commit a user's stage file",
			Run:  testStagePrivate,
		},
		&Test{
			ID: "stage/hash-verify", Ext: ExtStage, Section: secStPublish, Requires: []Ext{ExtCtl},
			Name: "hash= is verified: a mismatch is invalid and changes nothing",
			Run:  testStageHash,
		},
		&Test{
			ID: "stage/base", Ext: ExtStage, Section: secStPublish, Requires: []Ext{ExtCtl},
			Name: "commit needs a base, 0 for a new path, and conflicts on a stale one",
			Run:  testStageBase,
		},
		&Test{
			ID: "stage/commit-hash", Ext: ExtStage, Section: secStPublish, Requires: []Ext{ExtCtl},
			Name: "commit-hash publishes held content, and unknown content is missing",
			Run:  testStageCommitHash,
		},
		&Test{
			ID: "stage/missing", Ext: ExtStage, Section: secNoteReply, Requires: []Ext{ExtCtl},
			Name: "commit of a missing stage file, or into a missing directory, is missing",
			Run:  testStageMissing,
		},
		&Test{
			ID: "stage/abort", Ext: ExtStage, Section: secStPublish, Requires: []Ext{ExtCtl},
			Name: "abort and commit both remove the stage file",
			Run:  testStageAbort,
		},
		&Test{
			ID: "stage/keeps-flags", Ext: ExtStage, Section: secNoteVers, Requires: []Ext{ExtCtl},
			Name: "commit keeps the path's flags",
			Run:  testStageKeepsFlags,
		},
		&Test{
			ID: "stage/own-commit", Ext: ExtStage, Section: secStOrder, Requires: []Ext{ExtCtl, ExtEvents},
			Name: "the put record for a commit carries the version the reply gave",
			Run:  testStageOwnCommit,
		},
		&Test{
			ID: "stage/atomic", Ext: ExtStage, Section: secStPublish, Requires: []Ext{ExtCtl},
			Name: "a concurrent reader sees the old content or the new, never a mix",
			Run:  testStageAtomic,
		},
		&Test{
			ID: "stage/plain-write", Ext: ExtStage, Section: secStPlain, Requires: []Ext{ExtCtl},
			Level: Should,
			Name:  "ordinary 9P create, write, and clunk publish the file, with an event",
			Run:   testStagePlainWrite,
		},
	)
}

func pattern(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i*7+i/251)
	}
	return b
}

func testStageCommit(t *T) {
	content := pattern(10000, 1)
	name := StageName()
	d := t.MustWalk("/", ".fs", "stage")
	f, err := d.Clone(t.ctx)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := f.Create(t.ctx, name, wire.OWRONLY, 0o600); err != nil {
		t.Fatalf("create stage file: %v", err)
	}
	// Out of order: the tail, then the head.
	if _, err := f.WriteAt(t.ctx, content[6000:], 6000); err != nil {
		t.Fatalf("write tail: %v", err)
	}
	if _, err := f.WriteAt(t.ctx, content[:6000], 0); err != nil {
		t.Fatalf("write head: %v", err)
	}
	f.Clunk(t.ctx)
	p := t.P("file.bin")
	r := t.Must(fsproto.CommitCmd{Stage: name, Path: p, Base: fsproto.At(0), Mtime: testMtime, Mode: 0o640})
	want := fsproto.HashBytes(content)
	if r.Hash != want {
		t.Errorf("commit replied hash %q, want %q", r.Hash, want)
	}
	e, ok := t.Stat(p)
	if !ok {
		t.Fatalf("stat %q: missing after commit", p)
	}
	if e.Type != fsproto.TypeFile || e.Length != int64(len(content)) || e.Mode != 0o640 ||
		e.Mtime != testMtime || e.Version != r.Version || e.Hash != want {
		t.Errorf("stat %q, want a %d-byte file, mode 0640, mtime %d, version %d, hash %s",
			e.Encode(), len(content), testMtime, r.Version, want)
	}
	if b := t.MustRead(p); !bytes.Equal(b, content) {
		t.Errorf("read back %d bytes, differing from the %d staged", len(b), len(content))
	}
}

func testStageResume(t *T) {
	content := pattern(9000, 3)
	name := StageName()
	half := 4096
	// First connection writes the first half, then goes away.
	c1, root1, err := t.Dial("")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	f, err := root1.Walk(t.ctx, ".fs", "stage")
	if err != nil {
		t.Fatalf("%v", err)
	}
	if err := f.Create(t.ctx, name, wire.OWRONLY, 0o600); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := f.WriteAt(t.ctx, content[:half], 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	c1.Close()
	// A new connection finds the size and writes the rest.
	g, err := t.Walk("/", ".fs", "stage", name)
	if err != nil {
		t.Fatalf("walk to the stage file on a new connection: %v", err)
	}
	a, err := g.GetAttr(t.ctx)
	if err != nil {
		t.Fatalf("getattr: %v", err)
	}
	if a.Size != uint64(half) {
		t.Fatalf("stage file size %d, want %d", a.Size, half)
	}
	if err := g.Open(t.ctx, wire.OWRONLY); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := g.WriteAt(t.ctx, content[half:], uint64(half)); err != nil {
		t.Fatalf("write rest: %v", err)
	}
	p := t.P("resumed")
	r := t.Must(fsproto.CommitCmd{Stage: name, Path: p, Base: fsproto.At(0), Mtime: testMtime, Mode: 0o644,
		Hash: fsproto.HashBytes(content)})
	if r.Hash != fsproto.HashBytes(content) {
		t.Errorf("hash %q", r.Hash)
	}
	if b := t.MustRead(p); !bytes.Equal(b, content) {
		t.Errorf("resumed upload has %d bytes, differing from the original", len(b))
	}
}

func testStageInvisible(t *T) {
	name := StageName()
	t.WriteStage(name, []byte("invisible"))
	t.Cleanup(func() { t.DoLine(fsproto.AbortCmd{Stage: name}.Encode()) })
	d := t.MustWalk("/", ".fs", "stage")
	dir, _ := d.Clone(t.ctx)
	defer dir.Clunk(t.ctx)
	if err := dir.Open(t.ctx, wire.ORDONLY); err != nil {
		t.Fatalf("open .fs/stage: %v", err)
	}
	ents, err := dir.Readdir(t.ctx)
	if err != nil {
		t.Fatalf("readdir .fs/stage: %v", err)
	}
	found := false
	for _, e := range ents {
		found = found || e.Name == name
	}
	if !found {
		t.Errorf(".fs/stage/ does not list %q", name)
	}
	if t.Has(ExtIndex) {
		for _, e := range t.ReadIndex().Entries {
			if strings.Contains(e.Path, name) {
				t.Errorf("index lists %q", e.Path)
			}
		}
	}
}

func testStagePrivate(t *T) {
	other := t.r.target.OtherUser
	if other == "" {
		t.Skipf("no second user configured (-other-user)")
	}
	name := StageName()
	t.WriteStage(name, []byte("private"))
	t.Cleanup(func() { t.DoLine(fsproto.AbortCmd{Stage: name}.Encode()) })
	_, root2, err := t.Dial(other)
	if err != nil {
		t.Skipf("user %q cannot attach: %v", other, err)
	}
	if f, err := root2.Walk(t.ctx, ".fs", "stage", name); err == nil {
		f.Clunk(t.ctx)
		t.Errorf("user %q can walk to another user's stage file", other)
	}
	c, err := openCtl(t.ctx, root2)
	if err != nil {
		t.Fatalf("open ctl as %q: %v", other, err)
	}
	defer c.fid.Clunk(t.ctx)
	rep, err := c.call(t.ctx, fsproto.CommitCmd{Stage: name, Path: t.P("stolen"), Base: fsproto.At(0), Mtime: testMtime, Mode: 0o644}.Encode())
	if err != nil {
		t.Fatalf("commit as %q: %v", other, err)
	}
	if strings.HasPrefix(rep, "ok") {
		t.Errorf("user %q committed another user's stage file: %q", other, rep)
	}
}

func testStageHash(t *T) {
	content := []byte("verified content\n")
	name := StageName()
	t.WriteStage(name, content)
	p := t.P("checked")
	wrong := fsproto.HashBytes([]byte("something else"))
	t.Expect(fsproto.CommitCmd{Stage: name, Path: p, Base: fsproto.At(0), Mtime: testMtime, Mode: 0o644, Hash: wrong}, fsproto.StatusInvalid)
	if _, ok := t.Stat(p); ok {
		t.Fatalf("a commit with a wrong hash published %q", p)
	}
	// The stage file may or may not survive a failed commit; restage.
	name2 := StageName()
	t.WriteStage(name2, content)
	r := t.Must(fsproto.CommitCmd{Stage: name2, Path: p, Base: fsproto.At(0), Mtime: testMtime, Mode: 0o644, Hash: fsproto.HashBytes(content)})
	if r.Hash != fsproto.HashBytes(content) {
		t.Errorf("reply hash %q", r.Hash)
	}
	t.DoLine(fsproto.AbortCmd{Stage: name}.Encode())
}

func testStageBase(t *T) {
	p := t.P("based")
	commit := func(base fsproto.Base, content string) fsproto.Reply {
		name := StageName()
		t.WriteStage(name, []byte(content))
		r := t.Do(fsproto.CommitCmd{Stage: name, Path: p, Base: base, Mtime: testMtime, Mode: 0o644})
		if r.Status != fsproto.StatusOK {
			t.DoLine(fsproto.AbortCmd{Stage: name}.Encode())
		}
		return r
	}
	r1 := commit(fsproto.At(0), "v1")
	if r1.Status != fsproto.StatusOK {
		t.Fatalf("commit of a new path with base 0: %q", r1.Encode("commit"))
	}
	if r := commit(fsproto.At(0), "v1b"); r.Status != fsproto.StatusConflict || r.Version != r1.Version {
		t.Errorf("base 0 on an existing path: %q, want conflict %d", r.Encode("commit"), r1.Version)
	}
	r2 := commit(fsproto.At(r1.Version), "v2")
	if r2.Status != fsproto.StatusOK || r2.Version <= r1.Version {
		t.Fatalf("commit with the current base: %q", r2.Encode("commit"))
	}
	if r := commit(fsproto.At(r1.Version), "stale"); r.Status != fsproto.StatusConflict || r.Version != r2.Version {
		t.Errorf("stale base: %q, want conflict %d", r.Encode("commit"), r2.Version)
	}
	if b := t.MustRead(p); string(b) != "v2" {
		t.Errorf("content %q after a conflict, want v2", b)
	}
	// No base at all is not a valid commit.
	name := StageName()
	t.WriteStage(name, []byte("no base"))
	line := fmt.Sprintf("commit %s %s", name, fsproto.EscapePath(p))
	if rep := t.DoLine(line); !strings.HasPrefix(rep, "invalid") {
		t.Errorf("%q replied %q, want invalid", line, rep)
	}
	line = fmt.Sprintf("commit %s %s - %d 644", name, fsproto.EscapePath(p), testMtime)
	if rep := t.DoLine(line); !strings.HasPrefix(rep, "invalid") {
		t.Errorf("%q replied %q, want invalid (no unconditional commit)", line, rep)
	}
	t.DoLine(fsproto.AbortCmd{Stage: name}.Encode())
}

func testStageCommitHash(t *T) {
	content := []byte("held content " + randHex(8))
	src := t.P("src")
	t.PutFile(src, content)
	h := fsproto.HashBytes(content)
	dst := t.P("dst")
	r := t.Must(fsproto.CommitHashCmd{Hash: h, Path: dst, Base: fsproto.At(0), Mtime: testMtime, Mode: 0o600})
	e, _ := t.Stat(dst)
	if e.Hash != h || e.Length != int64(len(content)) || e.Version != r.Version || e.Mode != 0o600 {
		t.Errorf("stat %q after commit-hash (reply %d)", e.Encode(), r.Version)
	}
	if b := t.MustRead(dst); !bytes.Equal(b, content) {
		t.Errorf("commit-hash copy has %q", b)
	}
	missing := fsproto.HashBytes([]byte("never uploaded " + randHex(16)))
	t.Expect(fsproto.CommitHashCmd{Hash: missing, Path: t.P("nope"), Base: fsproto.At(0), Mtime: testMtime, Mode: 0o644}, fsproto.StatusMissing)
	c := t.Expect(fsproto.CommitHashCmd{Hash: h, Path: dst, Base: fsproto.At(0), Mtime: testMtime, Mode: 0o644}, fsproto.StatusConflict)
	if c.Version != r.Version {
		t.Errorf("conflict carries %d, want %d", c.Version, r.Version)
	}
}

func testStageMissing(t *T) {
	t.Expect(fsproto.CommitCmd{Stage: StageName(), Path: t.P("x"), Base: fsproto.At(0), Mtime: testMtime, Mode: 0o644}, fsproto.StatusMissing)
	name := StageName()
	t.WriteStage(name, []byte("orphan"))
	t.Expect(fsproto.CommitCmd{Stage: name, Path: t.P("no", "parent"), Base: fsproto.At(0), Mtime: testMtime, Mode: 0o644}, fsproto.StatusMissing)
	if _, ok := t.Stat(t.P("no")); ok {
		t.Errorf("commit created a parent directory")
	}
	t.DoLine(fsproto.AbortCmd{Stage: name}.Encode())
}

func testStageAbort(t *T) {
	name := StageName()
	t.WriteStage(name, []byte("abort me"))
	if rep := t.DoLine(fsproto.AbortCmd{Stage: name}.Encode()); rep != "ok" {
		t.Errorf("abort replied %q", rep)
	}
	if _, err := t.Walk("/", ".fs", "stage", name); err == nil {
		t.Errorf("stage file still exists after abort")
	}
	name = StageName()
	t.WriteStage(name, []byte("commit me"))
	t.Must(fsproto.CommitCmd{Stage: name, Path: t.P("c"), Base: fsproto.At(0), Mtime: testMtime, Mode: 0o644})
	if _, err := t.Walk("/", ".fs", "stage", name); err == nil {
		t.Errorf("stage file still exists after commit")
	}
}

func testStageKeepsFlags(t *T) {
	p := t.P("flagged")
	v := t.PutFile(p, []byte("one"))
	v = t.Must(fsproto.FlagsCmd{Path: p, Flags: "p"}).Version
	name := StageName()
	t.WriteStage(name, []byte("two"))
	t.Must(fsproto.CommitCmd{Stage: name, Path: p, Base: fsproto.At(v), Mtime: testMtime, Mode: 0o644})
	if e, _ := t.Stat(p); e.Flags != "p" {
		t.Errorf("after commit, %q lost flag p", e.Encode())
	}
}

func testStageOwnCommit(t *T) {
	dir := t.Dir()
	s := t.OpenEvents(SetupSince(t.CurrentSeq(), dir))
	content := []byte("mine\n")
	name := StageName()
	t.WriteStage(name, content)
	r := t.Must(fsproto.CommitCmd{Stage: name, Path: fsproto.Join(dir, "mine"), Base: fsproto.At(0), Mtime: testMtime, Mode: 0o644})
	evs := s.Until(r.Version)
	ev := evs[len(evs)-1]
	if ev.Seq != r.Version || ev.Op != fsproto.OpPut || ev.Entry.Path != fsproto.Join(dir, "mine") {
		t.Fatalf("record %q, want put %s at %d", ev.Encode(), fsproto.Join(dir, "mine"), r.Version)
	}
	if ev.Entry.Version != r.Version || ev.Entry.Hash != r.Hash {
		t.Errorf("put line %q, reply version %d hash %s", ev.Entry.Encode(), r.Version, r.Hash)
	}
}

func testStageAtomic(t *T) {
	p := t.P("atomic")
	size := 256 << 10
	versions := [][]byte{pattern(size, 'a'), pattern(size, 'b'), pattern(size, 'c')}
	hashes := map[string]int{}
	for i, v := range versions {
		hashes[fsproto.HashBytes(v)] = i
	}
	t.PutFile(p, versions[0])
	_, root2, err := t.Dial("")
	if err != nil {
		t.Fatalf("second connection: %v", err)
	}
	ctx, stop := context.WithCancel(t.ctx)
	var wg sync.WaitGroup
	var reads, torn int
	var rerr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			b, err := ReadFile(ctx, root2, p)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				rerr = err
				return
			}
			reads++
			if _, ok := hashes[fsproto.HashBytes(b)]; !ok {
				torn++
			}
		}
	}()
	deadline := time.Now().Add(time.Second)
	for i := 1; time.Now().Before(deadline) || i < 6; i++ {
		e, _ := t.Stat(p)
		name := StageName()
		t.WriteStage(name, versions[i%len(versions)])
		t.Must(fsproto.CommitCmd{Stage: name, Path: p, Base: fsproto.At(e.Version), Mtime: testMtime, Mode: 0o644})
	}
	stop()
	wg.Wait()
	if rerr != nil {
		t.Fatalf("reader: %v", rerr)
	}
	if torn > 0 {
		t.Errorf("%d of %d concurrent reads saw content that was never committed", torn, reads)
	}
	t.Logf("%d concurrent reads", reads)
}

func testStagePlainWrite(t *T) {
	dir := t.Dir()
	s := t.OpenEvents(SetupSince(t.CurrentSeq(), dir))
	p := fsproto.Join(dir, "plain.txt")
	content := []byte("written with plain 9P\n")
	if err := t.PlainWrite(p, content, true); err != nil {
		t.Fatalf("plain create and write: %v", err)
	}
	e, ok := t.Stat(p)
	if !ok {
		t.Fatalf("%q is missing after clunk", p)
	}
	if e.Length != int64(len(content)) || e.Hash != fsproto.HashBytes(content) {
		t.Errorf("stat %q after a plain write of %d bytes", e.Encode(), len(content))
	}
	if b := t.MustRead(p); !bytes.Equal(b, content) {
		t.Errorf("read back %q", b)
	}
	evs := s.Until(e.Version)
	if ev := evs[len(evs)-1]; ev.Op != fsproto.OpPut || ev.Entry.Path != p {
		t.Errorf("record for the plain write: %q", ev.Encode())
	}
	// Overwrite an existing file with O_TRUNC: last writer wins.
	content2 := []byte("second\n")
	if err := t.PlainWrite(p, content2, false); err != nil {
		t.Fatalf("plain overwrite: %v", err)
	}
	e2, _ := t.Stat(p)
	if e2.Version <= e.Version || e2.Length != int64(len(content2)) {
		t.Errorf("stat after overwrite %q, before %q", e2.Encode(), e.Encode())
	}
	if b := t.MustRead(p); !bytes.Equal(b, content2) {
		t.Errorf("read back %q after overwrite", b)
	}
}
