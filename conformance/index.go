package conformance

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/progrium/dotfs/conformance/internal/wire"
	"github.com/progrium/dotfs/fsproto"
)

func init() {
	register(
		&Test{
			ID: "index/header", Ext: ExtIndex, Section: secIdxFormat,
			Name: "the first line is a plain fsindex 1 seq <N> header",
			Run:  testIndexHeader,
		},
		&Test{
			ID: "index/grammar", Ext: ExtIndex, Section: secIdxFormat,
			Name: "every line parses, with escaped fields and per-type rules",
			Run:  testIndexGrammar,
		},
		&Test{
			ID: "index/sorted", Ext: ExtIndex, Section: secIdxFormat,
			Name: "lines are sorted by unescaped path bytes, root first, no duplicates",
			Run:  testIndexSorted,
		},
		&Test{
			ID: "index/no-fs", Ext: ExtIndex, Section: secLocation,
			Name: ".fs/ is never listed in the index",
			Run:  testIndexNoFs,
		},
		&Test{
			ID: "index/escaping", Ext: ExtIndex, Section: secPaths, Requires: []Ext{ExtCtl},
			Name: "names with every byte class are listed, escaped, and walkable",
			Run:  testIndexEscaping,
		},
		&Test{
			ID: "index/accept-zstd", Ext: ExtIndex, Section: secIdxReading,
			Name: "accept zstd gives a plain header and one zstd frame, or a plain body",
			Run:  testIndexZstd,
		},
		&Test{
			ID: "index/accept-unknown", Ext: ExtIndex, Section: secIdxReading,
			Name: "accepting only an unknown encoding gives a plain body",
			Run:  testIndexAcceptUnknown,
		},
		&Test{
			ID: "index/qid-vers", Ext: ExtIndex, Section: secIdxReading,
			Name: "qid.vers of .fs/index is the low 32 bits of seq, and qid.path is stable",
			Run:  testIndexQid,
		},
		&Test{
			ID: "index/matches-stat", Ext: ExtIndex, Section: secVersions,
			Name: "index lines agree with 9P getattr: type, mode, length, mtime, and qid.vers",
			Run:  testIndexMatchesStat,
		},
		&Test{
			ID: "index/consistent", Ext: ExtIndex, Section: secIdxReading, Requires: []Ext{ExtEvents, ExtCtl},
			Name: "each snapshot is the scope exactly at its seq, under concurrent writes",
			Run:  testIndexConsistent,
		},
		&Test{
			ID: "index/nested", Ext: ExtIndex, Section: secIdxNested, Requires: []Ext{ExtCtl},
			Name: "flag i makes a nested scope that the parent hides, and clearing it merges back",
			Run:  testIndexNested,
		},
		&Test{
			ID: "index/prefetch-flag", Ext: ExtIndex, Section: secIdxPrefet, Requires: []Ext{ExtCtl},
			Name: "flag p set with ctl shows in the index line",
			Run:  testIndexPrefetch,
		},
	)
}

func testIndexHeader(t *T) {
	b, _, err := readIndexRaw(t.ctx, t.root, nil)
	if err != nil {
		t.Fatalf("read .fs/index: %v", err)
	}
	nl := bytes.IndexByte(b, '\n')
	if nl < 0 {
		t.Fatalf("no complete header line in %q", tail(b))
	}
	h, err := fsproto.ParseIndexHeader(string(b[:nl]))
	if err != nil {
		t.Fatalf("header %q: %v", b[:nl], err)
	}
	if h.Version != 1 {
		t.Errorf("header version %d, want 1", h.Version)
	}
	if h.Encoding != "" {
		t.Errorf("header names encoding %q without an accept", h.Encoding)
	}
	if len(b) == nl+1 {
		t.Errorf("index has no lines; the scope root / must be listed")
	}
}

// checkLine applies the per-line rules that fsproto.ParseEntry does not.
func checkLine(line string, e fsproto.Entry, seq uint64, versioned bool) []string {
	var errs []string
	for i := 0; i < len(line); i++ {
		if c := line[i]; c < 0x20 || c > 0x7e {
			errs = append(errs, fmt.Sprintf("raw byte %#x at %d (must be escaped)", c, i))
			break
		}
	}
	if len(line) > 0 && (line[0] == ' ' || line[len(line)-1] == ' ') || bytes.Contains([]byte(line), []byte("  ")) {
		errs = append(errs, "fields not separated by single spaces")
	}
	if err := e.Check(); err != nil {
		errs = append(errs, err.Error())
	}
	switch e.Type {
	case fsproto.TypeDir:
		if e.Length != 0 {
			errs = append(errs, fmt.Sprintf("directory length %d, want 0", e.Length))
		}
	case fsproto.TypeSymlink:
		if int64(len(e.Target)) != e.Length {
			errs = append(errs, fmt.Sprintf("symlink length %d, target is %d bytes", e.Length, len(e.Target)))
		}
	}
	if e.Type != fsproto.TypeSymlink && e.Target != "" {
		errs = append(errs, "target on a non-symlink")
	}
	if e.Hash != "" {
		if err := fsproto.CheckHash(e.Hash); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if e.Mode > 0o7777 {
		errs = append(errs, fmt.Sprintf("mode %o has more than permission bits", e.Mode))
	}
	if e.Version > seq {
		errs = append(errs, fmt.Sprintf("version %d is past the header's seq %d", e.Version, seq))
	}
	if e.Flags.Has(fsproto.FlagIndex) && e.Type != fsproto.TypeDir {
		errs = append(errs, "flag i on a non-directory")
	}
	return errs
}

func testIndexGrammar(t *T) {
	b, _, err := readIndexRaw(t.ctx, t.root, nil)
	if err != nil {
		t.Fatalf("read .fs/index: %v", err)
	}
	ix, err := parseIndex(b)
	if err != nil {
		t.Errorf("%v", err)
	}
	if ix == nil || ix.Header.Version == 0 {
		return
	}
	versioned := t.Has(ExtEvents) || t.Has(ExtStage)
	bad := 0
	for _, line := range ix.Lines {
		e, err := fsproto.ParseEntry(line)
		if err != nil {
			continue // reported by parseIndex
		}
		for _, msg := range checkLine(line, e, ix.Header.Seq, versioned) {
			if bad < 20 {
				t.Errorf("line %q: %s", line, msg)
			}
			bad++
		}
	}
	if bad > 20 {
		t.Errorf("... and %d more problems", bad-20)
	}
	t.Logf("%d lines at seq %d", len(ix.Lines), ix.Header.Seq)
}

func testIndexSorted(t *T) {
	if t.Has(ExtCtl) {
		// Names whose byte order differs from a per-directory walk: "/a b"
		// sorts between "/a" and "/a/c".
		dir := t.Dir()
		for _, p := range []string{"a", "a b", "a/c", "a-", "a/c/d", "B", "~"} {
			t.Mkdir(dir + "/" + p)
		}
	}
	ix := t.ReadIndex()
	if len(ix.Entries) == 0 {
		t.Fatalf("index has no lines")
	}
	if e := ix.Entries[0]; e.Path != "/" || e.Type != fsproto.TypeDir {
		t.Errorf("first line is %q, want the root directory /", e.Encode())
	}
	for i := 1; i < len(ix.Entries); i++ {
		a, b := ix.Entries[i-1].Path, ix.Entries[i].Path
		switch c := fsproto.ComparePaths(a, b); {
		case c == 0:
			t.Errorf("path %q listed twice", b)
		case c > 0:
			t.Errorf("%q is listed after %q", b, a)
		}
	}
	// Every path's parent must be listed as a directory.
	dirs := map[string]bool{}
	for _, e := range ix.Entries {
		if e.Type == fsproto.TypeDir {
			dirs[e.Path] = true
		}
	}
	for _, e := range ix.Entries {
		if e.Path != "/" && !dirs[fsproto.Parent(e.Path)] {
			t.Errorf("%q is listed but its parent directory is not", e.Path)
		}
	}
}

func hasFsComponent(p string) bool {
	for _, c := range splitPath(p) {
		if c == ".fs" {
			return true
		}
	}
	return false
}

func testIndexNoFs(t *T) {
	ix := t.ReadIndex()
	for _, e := range ix.Entries {
		if hasFsComponent(e.Path) {
			t.Errorf("index lists %q", e.Path)
		}
	}
}

// escapeNames are directory names that cover every byte class the spec
// escapes, plus printable bytes it does not.
var escapeNames = []string{
	"space name",
	"per%cent",
	"%41literal",
	"ütf-8 ünïcödé",
	"tab\tname",
	"line\nbreak",
	"ctl\x01\x1f",
	"del\x7f",
	"bad-utf8\xff\xfe",
	"-",
	"eq=sign",
	"punct!\"#$&'()*+,;<>?@[\\]^`{|}~",
}

func testIndexEscaping(t *T) {
	dir := t.Dir()
	for _, n := range escapeNames {
		t.Mkdir(fsproto.Join(dir, n))
	}
	b, _, err := readIndexRaw(t.ctx, t.root, nil)
	if err != nil {
		t.Fatalf("read .fs/index: %v", err)
	}
	ix, err := parseIndex(b)
	if err != nil {
		t.Errorf("%v", err)
	}
	byPath := map[string]string{}
	for _, line := range ix.Lines {
		e, err := fsproto.ParseEntry(line)
		if err == nil {
			byPath[e.Path] = line
		}
	}
	for _, n := range escapeNames {
		p := fsproto.Join(dir, n)
		line, ok := byPath[p]
		if !ok {
			t.Errorf("index does not list %q", p)
			continue
		}
		for i := 0; i < len(line); i++ {
			if c := line[i]; c < 0x20 || c > 0x7e {
				t.Errorf("line for %q has a raw byte %#x", p, c)
				break
			}
		}
		// Every byte the spec says to escape must be escaped.
		field := line[:bytes.IndexByte([]byte(line), ' ')]
		for _, c := range []byte(n) {
			if c <= 0x20 || c >= 0x7f || c == '%' {
				want := fmt.Sprintf("%%%02X", c)
				if !bytes.Contains(bytes.ToUpper([]byte(field)), []byte(want)) {
					t.Errorf("path field %q does not escape byte %#x as %s", field, c, want)
				}
			}
		}
		if f, err := t.Walk(p); err != nil {
			t.Errorf("walk %q: %v", p, err)
		} else if f.Qid.Type&wire.QTDir == 0 {
			t.Errorf("%q: qid is not a directory", p)
		}
		if e, ok := t.Stat(p); !ok {
			t.Errorf("ctl stat %q: missing", p)
		} else if e.Path != p {
			t.Errorf("ctl stat %q: path %q", p, e.Path)
		}
	}
	// Readdir must list the raw names.
	d := t.MustWalk(dir)
	df, err := d.Clone(t.ctx)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer df.Clunk(t.ctx)
	if err := df.Open(t.ctx, wire.ORDONLY); err != nil {
		t.Fatalf("open %q: %v", dir, err)
	}
	ents, err := df.Readdir(t.ctx)
	if err != nil {
		t.Fatalf("readdir %q: %v", dir, err)
	}
	listed := map[string]bool{}
	for _, e := range ents {
		listed[e.Name] = true
	}
	for _, n := range escapeNames {
		if !listed[n] {
			t.Errorf("readdir %q does not list %q", dir, n)
		}
	}
}

func testIndexZstd(t *T) {
	plain := t.ReadIndex()
	b, _, err := readIndexRaw(t.ctx, t.root, []string{fsproto.EncodingZstd})
	if err != nil {
		t.Fatalf("read .fs/index with accept zstd: %v", err)
	}
	nl := bytes.IndexByte(b, '\n')
	if nl < 0 {
		t.Fatalf("no plain header line")
	}
	if _, err := fsproto.ParseIndexHeader(string(b[:nl])); err != nil {
		t.Fatalf("header %q is not plain text: %v", tail(b[:nl]), err)
	}
	ix, err := parseIndex(b)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if ix.Header.Encoding == "" {
		t.Logf("server chose a plain body (allowed)")
	} else {
		t.Logf("server sent zstd: %d bytes for %d lines", len(b)-nl-1, len(ix.Lines))
	}
	if ix.Header.Seq != plain.Header.Seq {
		t.Logf("seq moved between reads (%d, %d); not comparing bodies", plain.Header.Seq, ix.Header.Seq)
		return
	}
	if len(ix.Lines) != len(plain.Lines) {
		t.Fatalf("zstd body has %d lines, plain has %d", len(ix.Lines), len(plain.Lines))
	}
	for i := range ix.Lines {
		if ix.Lines[i] != plain.Lines[i] {
			t.Fatalf("line %d differs: zstd %q, plain %q", i, ix.Lines[i], plain.Lines[i])
		}
	}
}

func testIndexAcceptUnknown(t *T) {
	b, _, err := readIndexRaw(t.ctx, t.root, []string{"x-conform-unknown"})
	if err != nil {
		t.Fatalf("read .fs/index: %v", err)
	}
	ix, err := parseIndex(b)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if ix.Header.Encoding != "" {
		t.Errorf("header names %q", ix.Header.Encoding)
	}
}

func indexQid(t *T) wire.Qid {
	f := t.MustWalk("/", ".fs", "index")
	a, err := f.GetAttr(t.ctx)
	if err != nil {
		t.Fatalf("getattr .fs/index: %v", err)
	}
	f.Clunk(t.ctx)
	return a.Qid
}

func testIndexQid(t *T) {
	q1 := indexQid(t)
	ix := t.ReadIndex()
	q2 := indexQid(t)
	if q1.Version != uint32(ix.Header.Seq) && q2.Version != uint32(ix.Header.Seq) {
		t.Errorf("qid.vers %d (then %d), index seq %d", q1.Version, q2.Version, ix.Header.Seq)
	}
	if q1.Path != q2.Path {
		t.Errorf("qid.path changed from %#x to %#x", q1.Path, q2.Path)
	}
	if !t.Has(ExtCtl) {
		return
	}
	v := t.Mkdir(t.P("changed"))
	q3 := indexQid(t)
	if q3.Version != uint32(v) {
		t.Errorf("after a change at seq %d, qid.vers is %d", v, q3.Version)
	}
	if q3.Path != q1.Path {
		t.Errorf("qid.path changed from %#x to %#x after a change", q1.Path, q3.Path)
	}
	if ix2 := t.ReadIndex(); ix2.Header.Seq != v {
		t.Logf("index seq %d after the change at %d (another writer?)", ix2.Header.Seq, v)
	}
}

func testIndexMatchesStat(t *T) {
	var entries []fsproto.Entry
	if t.Has(ExtCtl) {
		dir := t.Dir()
		t.Mkdir(fsproto.Join(dir, "sub"))
		t.Must(fsproto.FlagsCmd{Path: fsproto.Join(dir, "sub"), Flags: "p"})
		if t.Has(ExtStage) {
			t.PutFile(fsproto.Join(dir, "file"), []byte("some content\n"))
		}
		entries = t.ReadIndex().Within(dir)
	} else {
		entries = t.ReadIndex().Entries
		if len(entries) > 200 {
			entries = entries[:200]
		}
	}
	for _, e := range entries {
		f, err := t.Walk(e.Path)
		if err != nil {
			t.Errorf("walk %q: %v", e.Path, err)
			continue
		}
		a, err := f.GetAttr(t.ctx)
		f.Clunk(t.ctx)
		if err != nil {
			t.Errorf("getattr %q: %v", e.Path, err)
			continue
		}
		var wantQT uint8
		var wantFmt uint32
		switch e.Type {
		case fsproto.TypeDir:
			wantQT, wantFmt = wire.QTDir, wire.SIFDIR
		case fsproto.TypeSymlink:
			wantQT, wantFmt = wire.QTSymlink, wire.SIFLNK
		default:
			wantQT, wantFmt = wire.QTFile, wire.SIFREG
		}
		if a.Qid.Type != wantQT || a.Mode&wire.SIFMT != wantFmt {
			t.Errorf("%q: index type %c, qid type %#x, mode %o", e.Path, e.Type, a.Qid.Type, a.Mode)
		}
		if a.Mode&0o7777 != e.Mode {
			t.Errorf("%q: index mode %04o, getattr %04o", e.Path, e.Mode, a.Mode&0o7777)
		}
		if e.Type != fsproto.TypeDir && a.Size != uint64(e.Length) {
			t.Errorf("%q: index length %d, getattr size %d", e.Path, e.Length, a.Size)
		}
		if a.MtimeSec != uint64(e.Mtime) {
			t.Errorf("%q: index mtime %d, getattr %d", e.Path, e.Mtime, a.MtimeSec)
		}
		if a.Qid.Version != uint32(e.Version) {
			t.Errorf("%q: index version %d, qid.vers %d", e.Path, e.Version, a.Qid.Version)
		}
	}
}

// replay applies records to a map of entries, as a client following
// events would.
func replay(state map[string]fsproto.Entry, ev fsproto.Event) {
	switch ev.Op {
	case fsproto.OpPut:
		e := ev.Entry
		e.Attrs = nil
		state[e.Path] = e
	case fsproto.OpDel:
		for p := range state {
			if fsproto.Within(p, ev.Path) {
				delete(state, p)
			}
		}
	case fsproto.OpMv:
		moved := map[string]fsproto.Entry{}
		for p, e := range state {
			if fsproto.Within(p, ev.Path) {
				delete(state, p)
				e.Path = fsproto.Rebase(p, ev.Path, ev.To)
				if p == ev.Path {
					e.Version = ev.Seq
				}
				moved[e.Path] = e
			}
		}
		for p := range state {
			if fsproto.Within(p, ev.To) {
				delete(state, p)
			}
		}
		for p, e := range moved {
			state[p] = e
		}
	case fsproto.OpFlags:
		if e, ok := state[ev.Path]; ok {
			e.Flags = ev.Flags
			e.Version = ev.Seq
			state[ev.Path] = e
		}
	}
}

func testIndexConsistent(t *T) {
	dir := t.Dir()
	start := t.Must(fsproto.StatCmd{Path: dir}).Entry
	s := t.OpenEvents(SetupSince(start.Version, dir))

	// A writer on its own connection makes changes while the main
	// connection reads snapshots.
	_, root2, err := t.Dial("")
	if err != nil {
		t.Fatalf("second connection: %v", err)
	}
	ctl, err := openCtl(t.ctx, root2)
	if err != nil {
		t.Fatalf("open ctl: %v", err)
	}
	ctx, stop := context.WithCancel(t.ctx)
	var wg sync.WaitGroup
	var last uint64
	var werr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ctx.Err() == nil && i < 2000; i++ {
			var cmd fsproto.Command
			switch i % 4 {
			case 0, 1:
				cmd = fsproto.MkdirCmd{Path: fsproto.Join(dir, fmt.Sprintf("d%d", i))}
			case 2:
				cmd = fsproto.FlagsCmd{Path: fsproto.Join(dir, fmt.Sprintf("d%d", i-2)), Flags: "p"}
			case 3:
				cmd = fsproto.RmCmd{Path: fsproto.Join(dir, fmt.Sprintf("d%d", i-3))}
			}
			rep, err := ctl.call(t.ctx, cmd.Encode())
			if err != nil {
				werr = err
				return
			}
			r, err := fsproto.ParseReply(cmd.Name(), rep)
			if err != nil || r.Status != fsproto.StatusOK {
				werr = fmt.Errorf("%q: %q", cmd.Encode(), rep)
				return
			}
			last = r.Version
		}
	}()
	var snaps []*Index
	deadline := time.Now().Add(time.Second)
	for len(snaps) < 20 && time.Now().Before(deadline) {
		snaps = append(snaps, t.ReadIndex())
	}
	stop()
	wg.Wait()
	if werr != nil {
		t.Fatalf("writer: %v", werr)
	}
	if last == 0 {
		t.Fatalf("writer made no changes")
	}
	// Collect every record up to the writer's last change.
	evs := s.Until(last)
	root := *start
	root.Attrs = nil
	state := map[string]fsproto.Entry{dir: root}
	i := 0
	checked := 0
	for _, ix := range snaps {
		if ix.Header.Seq < start.Version {
			t.Errorf("snapshot seq %d is before the test began at %d", ix.Header.Seq, start.Version)
			continue
		}
		for i < len(evs) && evs[i].Seq <= ix.Header.Seq {
			replay(state, evs[i])
			i++
		}
		if ix.Header.Seq > last {
			continue // past what the stream was read to
		}
		got := map[string]fsproto.Entry{}
		for _, e := range ix.Within(dir) {
			e.Attrs = nil
			got[e.Path] = e
		}
		for p, want := range state {
			g, ok := got[p]
			switch {
			case !ok:
				t.Errorf("snapshot at seq %d is missing %q (version %d)", ix.Header.Seq, p, want.Version)
			case g.Version != want.Version || g.Flags != want.Flags || g.Type != want.Type:
				t.Errorf("snapshot at seq %d has %q, events say %q", ix.Header.Seq, g.Encode(), want.Encode())
			}
		}
		for p, g := range got {
			if _, ok := state[p]; !ok {
				t.Errorf("snapshot at seq %d lists %q (version %d), which events say does not exist then", ix.Header.Seq, p, g.Version)
			}
		}
		checked++
	}
	t.Logf("%d snapshots checked against %d records", checked, len(evs))
	if checked < 2 {
		t.Logf("fewer than two snapshots overlapped the writes")
	}
}

func testIndexNested(t *T) {
	dir := t.Dir()
	sub := fsproto.Join(dir, "nested")
	inner := fsproto.Join(sub, "inner")
	t.Mkdir(sub)
	t.Mkdir(inner)
	r := t.Do(fsproto.FlagsCmd{Path: sub, Flags: "i"})
	if r.Status == fsproto.StatusInvalid {
		t.Skipf("flags i replied %q: the server does not offer nested scopes", r.Encode("flags"))
	}
	if r.Status != fsproto.StatusOK {
		t.Fatalf("flags %s i replied %q", sub, r.Encode("flags"))
	}
	t.Cleanup(func() { t.DoLine(fsproto.FlagsCmd{Path: sub}.Encode()) })
	if e, _ := t.Stat(sub); !e.Flags.Has(fsproto.FlagIndex) {
		t.Errorf("stat after flags i: %q", e.Encode())
	}
	// The parent lists only the directory itself.
	if _, ok := t.Stat(inner); ok {
		t.Errorf("the parent's stat still finds %q", inner)
	}
	parent := t.ReadIndex()
	if _, ok := parent.Find(sub); !ok {
		t.Errorf("parent index does not list the nested directory itself")
	}
	if left := parent.Within(sub); len(left) > 1 {
		t.Errorf("parent index lists %d paths below the nested directory", len(left)-1)
	}
	// The parent refuses changes below it.
	t.Expect(fsproto.MkdirCmd{Path: fsproto.Join(sub, "via-parent")}, fsproto.StatusInvalid)
	t.Expect(fsproto.RmtreeCmd{Path: sub}, fsproto.StatusInvalid)
	t.Expect(fsproto.FlagsCmd{Path: "/", Flags: "i"}, fsproto.StatusInvalid)

	// The nested scope has its own index, from its own root, at seq 0.
	names := splitPath(sub)
	if _, err := t.root.Walk(t.ctx, append(names, ".fs", "index")...); err != nil {
		t.Fatalf("walk %s/.fs/index: %v", sub, err)
	}
	b, _, err := readIndexRaw(t.ctx, t.root, nil, names...)
	if err != nil {
		t.Fatalf("read nested index: %v", err)
	}
	ix, err := parseIndex(b)
	if err != nil {
		t.Fatalf("nested index: %v", err)
	}
	if ix.Header.Seq != 0 {
		t.Errorf("nested index seq %d, want 0 for a new nested scope", ix.Header.Seq)
	}
	if len(ix.Entries) == 0 || ix.Entries[0].Path != "/" {
		t.Fatalf("nested index does not start with its own root /")
	}
	if e, ok := ix.Find("/inner"); !ok {
		t.Errorf("nested index does not list /inner (paths are relative to the nested scope)")
	} else if e.Version != 0 {
		t.Errorf("moved child %q has version %d, want 0", e.Encode(), e.Version)
	}
	// Its ctl, when it has one, changes it with its own seq.
	var nestedV uint64
	if f, err := t.root.Walk(t.ctx, append(names, ".fs", "ctl")...); err == nil {
		f.Clunk(t.ctx)
		c := t.OpenCtl(names...)
		rep, err := c.call(t.ctx, fsproto.MkdirCmd{Path: "/made-inside"}.Encode())
		if err != nil {
			t.Fatalf("nested ctl: %v", err)
		}
		r, err := fsproto.ParseReply("mkdir", rep)
		if err != nil || r.Status != fsproto.StatusOK {
			t.Fatalf("nested mkdir /made-inside: %q", rep)
		}
		nestedV = r.Version
		if nestedV != 1 {
			t.Errorf("first change in the nested scope got version %d, want 1", nestedV)
		}
	}
	// Clearing i merges the children back with the flags change's version.
	cleared := t.Must(fsproto.FlagsCmd{Path: sub})
	e, ok := t.Stat(inner)
	if !ok {
		t.Fatalf("after clearing flag i, %q is missing from the parent", inner)
	}
	if e.Version != cleared.Version {
		t.Errorf("merged child %q has version %d, want %d (the flags change)", inner, e.Version, cleared.Version)
	}
	if nestedV != 0 {
		if _, ok := t.Stat(fsproto.Join(sub, "made-inside")); !ok {
			t.Errorf("a change made in the nested scope was lost when it merged back")
		}
	}
}

func testIndexPrefetch(t *T) {
	d := t.P("pre")
	t.Mkdir(d)
	v := t.Must(fsproto.FlagsCmd{Path: d, Flags: "p"}).Version
	e, ok := t.ReadIndex().Find(d)
	if !ok {
		t.Fatalf("index does not list %q", d)
	}
	if !e.Flags.Has(fsproto.FlagPrefetch) {
		t.Errorf("index line %q has no p", e.Encode())
	}
	if e.Version != v {
		t.Errorf("index version %d, flags replied %d", e.Version, v)
	}
}
