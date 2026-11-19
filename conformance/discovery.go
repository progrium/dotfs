package conformance

import (
	"sort"
	"strings"

	"github.com/progrium/dotfs/conformance/internal/wire"
	"github.com/progrium/dotfs/fsproto"
)

// Spec sections, as headings in docs/spec.md joined by " > ".
const (
	secLocation   = "Conventions > Location and scope"
	secDiscovery  = "Conventions > Discovery"
	secPaths      = "Conventions > Paths"
	secVersions   = "Conventions > Versions and sequence numbers"
	secLines      = "Conventions > Line formats"
	secCtlXchg    = "Conventions > ctl exchange"
	secErrors     = "Conventions > Errors"
	secIdxFormat  = "Extension 1: index > Format"
	secIdxReading = "Extension 1: index > Reading"
	secIdxNested  = "Extension 1: index > Nested indexes (flag i)"
	secIdxPrefet  = "Extension 1: index > Prefetch (flag p)"
	secEvOpen     = "Extension 2: events > Opening a stream"
	secEvReading  = "Extension 2: events > Reading"
	secEvRecords  = "Extension 2: events > Records"
	secEvBehind   = "Extension 2: events > Falling behind"
	secEvBoot     = "Extension 2: events > Bootstrapping with an index"
	secPOCommands = "Extension 3: path operations > Commands"
	secPOCond     = "Extension 3: path operations > Conditional operations"
	secStStaging  = "Extension 4: staged commit > Staging"
	secStPublish  = "Extension 4: staged commit > Publishing"
	secStOrder    = "Extension 4: staged commit > Ordering guarantee"
	secStPlain    = "Extension 4: staged commit > Plain writes"
	secSecurity   = "Security considerations"
	secNoteLines  = "Notes from the reference implementation > Line formats"
	secNoteReply  = "Notes from the reference implementation > Replies"
	secNoteVers   = "Notes from the reference implementation > Versions and records"
)

func init() {
	register(
		&Test{
			ID: "discovery/fs-dir", Ext: ExtDiscovery, Section: secDiscovery,
			Name: ".fs/ lists exactly the extension files that can be walked to",
			Run:  testFsDir,
		},
		&Test{
			ID: "discovery/unknown-fs-file", Ext: ExtDiscovery, Section: secLocation,
			Name: "a name in .fs/ that is not an extension is not found",
			Run:  testUnknownFsFile,
		},
		&Test{
			ID: "ctl/caps", Ext: ExtCtl, Section: secDiscovery,
			Name: "caps lists the commands ctl accepts, matching .fs/stage/",
			Run:  testCaps,
		},
	)
}

func testFsDir(t *T) {
	d, err := t.Walk("/", ".fs")
	if err != nil {
		if wire.IsErrno(err, wire.ENOENT) {
			t.Logf("no .fs/: the server offers no extensions")
			return
		}
		t.Fatalf("walk .fs: %v", err)
	}
	if d.Qid.Type&wire.QTDir == 0 {
		t.Fatalf(".fs is not a directory (qid type %#x)", d.Qid.Type)
	}
	dir, err := d.Clone(t.ctx)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer dir.Clunk(t.ctx)
	if err := dir.Open(t.ctx, wire.ORDONLY); err != nil {
		t.Fatalf("open .fs: %v", err)
	}
	ents, err := dir.Readdir(t.ctx)
	if err != nil {
		t.Fatalf("readdir .fs: %v", err)
	}
	listed := map[string]bool{}
	var names []string
	for _, e := range ents {
		if e.Name == "." || e.Name == ".." {
			continue
		}
		listed[e.Name] = true
		names = append(names, e.Name)
	}
	sort.Strings(names)
	for _, e := range Exts[1:] {
		name := e.fsFile()
		if t.Has(e) && !listed[name] {
			t.Errorf(".fs/%s can be walked to but is not listed in .fs/ (%s)", name, strings.Join(names, " "))
		}
		if !t.Has(e) && listed[name] {
			t.Errorf(".fs/%s is listed in .fs/ but cannot be walked to", name)
		}
	}
	for _, n := range names {
		switch n {
		case "index", "events", "ctl", "stage":
		default:
			t.Logf(".fs/ also lists %q, which this suite does not know", n)
		}
	}
	if t.Has(ExtStage) {
		s := t.MustWalk("/", ".fs", "stage")
		if s.Qid.Type&wire.QTDir == 0 {
			t.Errorf(".fs/stage is not a directory")
		}
	}
	for _, e := range []Ext{ExtIndex, ExtEvents, ExtCtl} {
		if t.Has(e) {
			f := t.MustWalk("/", ".fs", e.fsFile())
			if f.Qid.Type&wire.QTDir != 0 {
				t.Errorf(".fs/%s is a directory, want a file", e.fsFile())
			}
		}
	}
}

func testUnknownFsFile(t *T) {
	if _, err := t.Walk("/", ".fs"); err != nil {
		t.Skipf("no .fs/")
	}
	_, err := t.Walk("/", ".fs", "no-such-extension")
	if err == nil {
		t.Errorf("walk to .fs/no-such-extension succeeded")
	} else if !wire.IsErrno(err, wire.ENOENT) {
		t.Logf("walk to .fs/no-such-extension failed with %v (ENOENT expected, but any error is allowed)", err)
	}
}

func testCaps(t *T) {
	r := t.Must(fsproto.CapsCmd{})
	caps := map[string]bool{}
	for _, c := range r.Caps {
		caps[c] = true
	}
	if !caps[fsproto.CmdCaps] {
		t.Errorf("caps reply %v does not list caps", r.Caps)
	}
	for _, c := range fsproto.StageCommands {
		if t.Has(ExtStage) && !caps[c] {
			t.Errorf(".fs/stage/ exists but caps does not list %s", c)
		}
		if !t.Has(ExtStage) && caps[c] {
			t.Errorf("caps lists %s but there is no .fs/stage/", c)
		}
	}
	// A command the server does not list must not succeed.
	for _, c := range append(append([]string(nil), fsproto.PathCommands...), fsproto.StageCommands...) {
		if caps[c] || c == fsproto.CmdCaps {
			continue
		}
		var line string
		switch c {
		case fsproto.CmdHello:
			line = fsproto.HelloCmd{Device: "conform"}.Encode()
		case fsproto.CmdStat:
			line = fsproto.StatCmd{Path: "/"}.Encode()
		default:
			continue // would change the tree if it did run
		}
		rep := t.DoLine(line)
		if strings.HasPrefix(rep, "ok") {
			t.Errorf("%q is not in caps but replied %q", line, rep)
		}
	}
}
