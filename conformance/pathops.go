package conformance

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/progrium/dotfs/fsproto"
)

func init() {
	register(
		&Test{
			ID: "ctl/hello", Ext: ExtCtl, Section: secPOCommands,
			Name: "hello replies ok",
			Run: func(t *T) {
				if r := t.DoLine(fsproto.HelloCmd{Device: "conform"}.Encode()); r != "ok" {
					t.Errorf("hello replied %q", r)
				}
			},
		},
		&Test{
			ID: "ctl/stat", Ext: ExtCtl, Section: secPOCommands,
			Name: "stat returns the index line, or missing",
			Run:  testCtlStat,
		},
		&Test{
			ID: "ctl/mkdir", Ext: ExtCtl, Section: secPOCommands,
			Name: "mkdir with and without a base; existing paths conflict; no parents are made",
			Run:  testCtlMkdir,
		},
		&Test{
			ID: "ctl/rm", Ext: ExtCtl, Section: secPOCommands,
			Name: "rm with and without a base; missing; a non-empty directory is invalid",
			Run:  testCtlRm,
		},
		&Test{
			ID: "ctl/rmtree", Ext: ExtCtl, Section: secPOCommands,
			Name: "rmtree removes a whole subtree, conditionally",
			Run:  testCtlRmtree,
		},
		&Test{
			ID: "ctl/mv", Ext: ExtCtl, Section: secPOCond,
			Name: "mv across directories with base and tobase; conflicts change nothing",
			Run:  testCtlMv,
		},
		&Test{
			ID: "ctl/mv-invalid", Ext: ExtCtl, Section: secPOReplies,
			Name: "mv into itself and mv between a file and a directory are invalid",
			Run:  testCtlMvInvalid,
		},
		&Test{
			ID: "ctl/cp", Ext: ExtCtl, Section: secPOCommands, Requires: []Ext{ExtStage},
			Name: "cp copies content to a new version; tobase is checked; directories are invalid",
			Run:  testCtlCp,
		},
		&Test{
			ID: "ctl/flags", Ext: ExtCtl, Section: secPOCommands,
			Name: "flags sets p and i, i only on directories, and missing paths are missing",
			Run:  testCtlFlags,
		},
		&Test{
			ID: "ctl/fids-independent", Ext: ExtCtl, Section: secCtlXchg,
			Name: "two ctl fids hold separate command and reply state",
			Run:  testCtlFids,
		},
		&Test{
			ID: "ctl/bad-commands", Ext: ExtCtl, Section: secErrors,
			Name: "malformed and unknown commands get an invalid reply, not a 9P error",
			Run:  testCtlBad,
		},
		&Test{
			ID: "ctl/scope-escape", Ext: ExtCtl, Section: secSecurity,
			Name: "paths with .. or .fs never succeed, and walking .. never leaves the root",
			Run:  testCtlScope,
		},
		&Test{
			ID: "ctl/base-without-versions", Ext: ExtCtl, Section: secPOCond,
			Name: "a server without versions rejects any base with invalid",
			Run:  testCtlNoVersions,
		},
		&Test{
			ID: "ctl/versions", Ext: ExtCtl, Section: secVersions, Requires: []Ext{ExtEvents},
			Name: "each change returns the next seq, and stat reports it as the version",
			Run:  testCtlVersions,
		},
	)
}

func testCtlStat(t *T) {
	r := t.Must(fsproto.StatCmd{Path: "/"})
	if r.Entry == nil || r.Entry.Path != "/" || r.Entry.Type != fsproto.TypeDir {
		t.Errorf("stat / replied %q", r.Encode("stat"))
	}
	t.Expect(fsproto.StatCmd{Path: "/conform-no-such-path-" + randHex(4)}, fsproto.StatusMissing)
	d := t.P("x")
	v := t.Mkdir(d)
	r = t.Must(fsproto.StatCmd{Path: d})
	if r.Entry.Path != d || r.Entry.Type != fsproto.TypeDir || r.Entry.Version != v {
		t.Errorf("stat %s replied %q, want a directory at version %d", d, r.Encode("stat"), v)
	}
	if t.Has(ExtIndex) {
		ix := t.ReadIndex()
		e, _ := ix.Find(d)
		a, b := e, *r.Entry
		a.Attrs, b.Attrs = nil, nil
		if a.Encode() != b.Encode() {
			t.Errorf("stat %q, index %q", b.Encode(), a.Encode())
		}
	}
}

func testCtlMkdir(t *T) {
	dir := t.Dir()
	a := fsproto.Join(dir, "a")
	r := t.Must(fsproto.MkdirCmd{Path: a, Base: fsproto.At(0)})
	if r.Version == 0 {
		t.Errorf("mkdir replied version 0")
	}
	if e, ok := t.Stat(a); !ok || e.Type != fsproto.TypeDir {
		t.Errorf("after mkdir, stat %q: %q", a, e.Encode())
	}
	c := t.Expect(fsproto.MkdirCmd{Path: a, Base: fsproto.At(0)}, fsproto.StatusConflict)
	if c.Version != r.Version {
		t.Errorf("conflict carries %d, the path's version is %d", c.Version, r.Version)
	}
	c = t.Expect(fsproto.MkdirCmd{Path: a}, fsproto.StatusConflict)
	if c.Version != r.Version {
		t.Errorf("conflict carries %d, the path's version is %d", c.Version, r.Version)
	}
	b := fsproto.Join(dir, "b")
	c = t.Expect(fsproto.MkdirCmd{Path: b, Base: fsproto.At(r.Version)}, fsproto.StatusConflict)
	if c.Version != 0 {
		t.Errorf("mkdir of a new path with a nonzero base: conflict carries %d, want 0", c.Version)
	}
	if _, ok := t.Stat(b); ok {
		t.Errorf("a conflicting mkdir created %q", b)
	}
	t.Expect(fsproto.MkdirCmd{Path: fsproto.Join(fsproto.Join(dir, "no"), "parent")}, fsproto.StatusMissing)
	if _, ok := t.Stat(fsproto.Join(dir, "no")); ok {
		t.Errorf("mkdir created a missing parent")
	}
	t.Must(fsproto.MkdirCmd{Path: b})
}

func testCtlRm(t *T) {
	dir := t.Dir()
	a := fsproto.Join(dir, "a")
	t.Mkdir(a)
	t.Mkdir(fsproto.Join(a, "child"))
	t.Expect(fsproto.RmCmd{Path: a}, fsproto.StatusInvalid)
	if _, ok := t.Stat(a); !ok {
		t.Errorf("rm of a non-empty directory removed it")
	}
	child := fsproto.Join(a, "child")
	cv, _ := t.Stat(child)
	c := t.Expect(fsproto.RmCmd{Path: child, Base: fsproto.At(cv.Version + 1000)}, fsproto.StatusConflict)
	if c.Version != cv.Version {
		t.Errorf("conflict carries %d, want %d", c.Version, cv.Version)
	}
	if _, ok := t.Stat(child); !ok {
		t.Errorf("a conflicting rm removed the path")
	}
	r := t.Must(fsproto.RmCmd{Path: child, Base: fsproto.At(cv.Version)})
	if r.Version <= cv.Version {
		t.Errorf("rm replied seq %d, not after the path's version %d", r.Version, cv.Version)
	}
	if _, ok := t.Stat(child); ok {
		t.Errorf("rm left %q", child)
	}
	t.Expect(fsproto.RmCmd{Path: child}, fsproto.StatusMissing)
	t.Must(fsproto.RmCmd{Path: a})
	if t.Has(ExtStage) {
		f := fsproto.Join(dir, "file")
		t.PutFile(f, []byte("x"))
		t.Must(fsproto.RmCmd{Path: f})
		if _, ok := t.Stat(f); ok {
			t.Errorf("rm left file %q", f)
		}
	}
}

func testCtlRmtree(t *T) {
	dir := t.Dir()
	a := fsproto.Join(dir, "tree")
	t.Mkdir(a)
	t.Mkdir(fsproto.Join(a, "x"))
	t.Mkdir(fsproto.Join(fsproto.Join(a, "x"), "y"))
	if t.Has(ExtStage) {
		t.PutFile(fsproto.Join(fsproto.Join(a, "x"), "f"), []byte("f"))
	}
	e, _ := t.Stat(a)
	t.Expect(fsproto.RmtreeCmd{Path: a, Base: fsproto.At(e.Version + 1000)}, fsproto.StatusConflict)
	if _, ok := t.Stat(fsproto.Join(fsproto.Join(a, "x"), "y")); !ok {
		t.Errorf("a conflicting rmtree removed something")
	}
	t.Must(fsproto.RmtreeCmd{Path: a, Base: fsproto.At(e.Version)})
	for _, p := range []string{a, fsproto.Join(a, "x"), fsproto.Join(fsproto.Join(a, "x"), "y")} {
		if _, ok := t.Stat(p); ok {
			t.Errorf("rmtree left %q", p)
		}
	}
	t.Expect(fsproto.RmtreeCmd{Path: a}, fsproto.StatusMissing)
	if t.Has(ExtIndex) {
		if left := t.ReadIndex().Within(a); len(left) > 0 {
			t.Errorf("index still lists %d paths below %q", len(left), a)
		}
	}
}

func testCtlMv(t *T) {
	dir := t.Dir()
	d1, d2 := fsproto.Join(dir, "d1"), fsproto.Join(dir, "d2")
	t.Mkdir(d1)
	t.Mkdir(d2)
	src := fsproto.Join(d1, "item")
	sv := t.Mkdir(src)
	t.Mkdir(fsproto.Join(src, "inner"))
	dst := fsproto.Join(d2, "moved")
	// Wrong base: conflict, nothing moves.
	c := t.Expect(fsproto.MvCmd{From: src, To: dst, Base: fsproto.At(sv + 1000)}, fsproto.StatusConflict)
	if c.Version != sv {
		t.Errorf("conflict carries %d, want %d", c.Version, sv)
	}
	// Existing destination with tobase 0: conflict with its version.
	other := fsproto.Join(d2, "other")
	ov := t.Mkdir(other)
	c = t.Expect(fsproto.MvCmd{From: src, To: other, Base: fsproto.At(sv), ToBase: fsproto.At(0)}, fsproto.StatusConflict)
	if c.Version != ov {
		t.Errorf("tobase conflict carries %d, want the destination's version %d", c.Version, ov)
	}
	if _, ok := t.Stat(src); !ok {
		t.Fatalf("a conflicting mv moved the source")
	}
	r := t.Must(fsproto.MvCmd{From: src, To: dst, Base: fsproto.At(sv), ToBase: fsproto.At(0)})
	if _, ok := t.Stat(src); ok {
		t.Errorf("mv left the source")
	}
	e, ok := t.Stat(dst)
	if !ok {
		t.Fatalf("mv did not create the destination")
	}
	if e.Version != r.Version {
		t.Errorf("destination version %d, mv replied %d", e.Version, r.Version)
	}
	if _, ok := t.Stat(fsproto.Join(dst, "inner")); !ok {
		t.Errorf("mv did not carry the directory's children")
	}
	t.Expect(fsproto.MvCmd{From: src, To: fsproto.Join(d2, "again")}, fsproto.StatusMissing)
	// mv with an omitted base and a tobase, encoded with "-".
	line := fmt.Sprintf("mv %s %s - 0", fsproto.EscapePath(dst), fsproto.EscapePath(fsproto.Join(d1, "back")))
	if rep := t.DoLine(line); !strings.HasPrefix(rep, "ok ") {
		t.Errorf("%q replied %q", line, rep)
	}
	if t.Has(ExtStage) {
		// A file may replace a file unconditionally.
		f1, f2 := fsproto.Join(dir, "f1"), fsproto.Join(dir, "f2")
		t.PutFile(f1, []byte("one"))
		t.PutFile(f2, []byte("two"))
		t.Must(fsproto.MvCmd{From: f1, To: f2})
		if b := t.MustRead(f2); string(b) != "one" {
			t.Errorf("after mv over a file, content %q, want %q", b, "one")
		}
	}
}

func testCtlMvInvalid(t *T) {
	dir := t.Dir()
	a := fsproto.Join(dir, "a")
	t.Mkdir(a)
	t.Mkdir(fsproto.Join(a, "b"))
	t.Expect(fsproto.MvCmd{From: a, To: fsproto.Join(fsproto.Join(a, "b"), "c")}, fsproto.StatusInvalid)
	if !t.Has(ExtStage) {
		return
	}
	f := fsproto.Join(dir, "f")
	t.PutFile(f, []byte("f"))
	t.Expect(fsproto.MvCmd{From: f, To: a}, fsproto.StatusInvalid)
	t.Expect(fsproto.MvCmd{From: a, To: f}, fsproto.StatusInvalid)
}

func testCtlCp(t *T) {
	dir := t.Dir()
	src := fsproto.Join(dir, "src")
	content := []byte("copy me\n")
	sv := t.PutFile(src, content)
	dst := fsproto.Join(dir, "dst")
	r := t.Must(fsproto.CpCmd{From: src, To: dst, ToBase: fsproto.At(0)})
	if r.Version <= sv {
		t.Errorf("cp replied %d, not after the source's version %d", r.Version, sv)
	}
	if b := t.MustRead(dst); !bytes.Equal(b, content) {
		t.Errorf("copy has %q, want %q", b, content)
	}
	se, _ := t.Stat(src)
	de, _ := t.Stat(dst)
	if de.Version != r.Version || de.Hash != se.Hash || de.Length != se.Length {
		t.Errorf("stat copy %q, source %q, reply %d", de.Encode(), se.Encode(), r.Version)
	}
	if se.Version != sv {
		t.Errorf("cp changed the source's version from %d to %d", sv, se.Version)
	}
	c := t.Expect(fsproto.CpCmd{From: src, To: dst, ToBase: fsproto.At(0)}, fsproto.StatusConflict)
	if c.Version != r.Version {
		t.Errorf("conflict carries %d, want %d", c.Version, r.Version)
	}
	t.Must(fsproto.CpCmd{From: src, To: dst, ToBase: fsproto.At(r.Version)})
	t.Expect(fsproto.CpCmd{From: fsproto.Join(dir, "none"), To: fsproto.Join(dir, "x")}, fsproto.StatusMissing)
	sub := fsproto.Join(dir, "sub")
	t.Mkdir(sub)
	t.Expect(fsproto.CpCmd{From: sub, To: fsproto.Join(dir, "sub2")}, fsproto.StatusInvalid)
}

func testCtlFlags(t *T) {
	dir := t.Dir()
	d := fsproto.Join(dir, "d")
	t.Mkdir(d)
	r := t.Must(fsproto.FlagsCmd{Path: d, Flags: "p"})
	e, _ := t.Stat(d)
	if e.Flags != "p" || e.Version != r.Version {
		t.Errorf("after flags p: %q, reply %d", e.Encode(), r.Version)
	}
	t.Must(fsproto.FlagsCmd{Path: d, Flags: ""})
	if e, _ := t.Stat(d); e.Flags != "" {
		t.Errorf("after clearing flags: %q", e.Encode())
	}
	t.Expect(fsproto.FlagsCmd{Path: fsproto.Join(dir, "none"), Flags: "p"}, fsproto.StatusMissing)
	if t.Has(ExtStage) {
		f := fsproto.Join(dir, "f")
		t.PutFile(f, []byte("f"))
		t.Expect(fsproto.FlagsCmd{Path: f, Flags: "i"}, fsproto.StatusInvalid)
		t.Must(fsproto.FlagsCmd{Path: f, Flags: "p"})
	}
}

func testCtlFids(t *T) {
	dir := t.Dir()
	a, b := t.OpenCtl(), t.OpenCtl()
	p := fsproto.Join(dir, "via-a")
	if err := a.send(t.ctx, fsproto.MkdirCmd{Path: p}.Encode()); err != nil {
		t.Fatalf("write on fid a: %v", err)
	}
	rb, err := b.call(t.ctx, fsproto.StatCmd{Path: "/"}.Encode())
	if err != nil {
		t.Fatalf("fid b: %v", err)
	}
	if !strings.HasPrefix(rb, "ok / d ") {
		t.Errorf("fid b's stat / replied %q", rb)
	}
	ra, err := a.recv(t.ctx)
	if err != nil {
		t.Fatalf("read on fid a: %v", err)
	}
	r, err := fsproto.ParseReply("mkdir", ra)
	if err != nil || r.Status != fsproto.StatusOK {
		t.Errorf("fid a's mkdir replied %q", ra)
	}
	// A second command on the same fid gets its own reply.
	ra, err = a.call(t.ctx, fsproto.StatCmd{Path: p}.Encode())
	if err != nil || !strings.HasPrefix(ra, "ok ") {
		t.Errorf("second command on fid a: %q, %v", ra, err)
	}
}

func testCtlBad(t *T) {
	for _, line := range []string{
		"mkdir",
		"mkdir relative/path",
		"mkdir /a b c d e",
		"frobnicate /x",
		"stat /a//b",
		"mv /a",
		"rm /x notanumber",
		"flags /x P",
	} {
		c := t.OpenCtl()
		if err := c.send(t.ctx, line); err != nil {
			t.Errorf("%q: 9P error %v, want an invalid reply", line, err)
			continue
		}
		rep, err := c.recv(t.ctx)
		if err != nil {
			t.Errorf("%q: reading the reply: %v", line, err)
			continue
		}
		if !strings.HasPrefix(rep, "invalid") {
			t.Errorf("%q replied %q, want invalid", line, rep)
		}
	}
}

func testCtlScope(t *T) {
	dir := t.Dir()
	for _, line := range []string{
		"stat /..",
		"stat /../etc",
		"mkdir " + fsproto.EscapePath(dir) + "/../../escape",
		"mkdir /.fs/x",
		"mkdir /.fs",
		"mkdir " + fsproto.EscapePath(dir) + "/.fs",
		"mv " + fsproto.EscapePath(dir) + " /.fs/moved",
		"rm /.fs/ctl",
		"rmtree /.fs",
		"flags /.fs p",
	} {
		rep := t.DoLine(line)
		if strings.HasPrefix(rep, "ok") {
			t.Errorf("%q replied %q", line, rep)
		}
	}
	// Walking .. from the root may fail, but must not leave the root.
	if up, err := t.Walk("/", ".."); err == nil && up.Qid.Path != t.root.Qid.Path {
		t.Errorf("walk .. from the root left it (qid.path %#x, root %#x)", up.Qid.Path, t.root.Qid.Path)
	} else if err != nil {
		t.Logf("walk .. from the root: %v (allowed)", err)
	}
}

func testCtlNoVersions(t *T) {
	if t.Has(ExtEvents) || t.Has(ExtStage) {
		t.Skipf("the server has versions (events or staged commit)")
	}
	t.Expect(fsproto.MkdirCmd{Path: t.P("x"), Base: fsproto.At(0)}, fsproto.StatusInvalid)
}

func testCtlVersions(t *T) {
	dir := t.Dir()
	dirEntry, _ := t.Stat(dir)
	prev := t.CurrentSeq()
	for i, cmd := range []fsproto.Command{
		fsproto.MkdirCmd{Path: fsproto.Join(dir, "a")},
		fsproto.MkdirCmd{Path: fsproto.Join(dir, "b")},
		fsproto.FlagsCmd{Path: fsproto.Join(dir, "a"), Flags: "p"},
		fsproto.MvCmd{From: fsproto.Join(dir, "b"), To: fsproto.Join(dir, "c")},
		fsproto.RmCmd{Path: fsproto.Join(dir, "c")},
	} {
		r := t.Must(cmd)
		if r.Version != prev+1 {
			t.Errorf("change %d (%q) replied %d, want %d", i, cmd.Encode(), r.Version, prev+1)
		}
		prev = r.Version
	}
	if e, _ := t.Stat(fsproto.Join(dir, "a")); e.Version != prev-2 {
		t.Errorf("stat a: version %d, want %d (its flags change)", e.Version, prev-2)
	}
	if e, _ := t.Stat(dir); e.Version != dirEntry.Version {
		t.Logf("the parent directory's version moved from %d to %d with its children (the spec notes say it should not)", dirEntry.Version, e.Version)
	}
	// Qid.vers over 9P carries the version's low 32 bits.
	f, err := t.Walk(fsproto.Join(dir, "a"))
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if f.Qid.Version != uint32(prev-2) {
		t.Errorf("walk qid.vers %d, want %d", f.Qid.Version, uint32(prev-2))
	}
}
