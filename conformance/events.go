package conformance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/progrium/dotfs/conformance/internal/wire"
	"github.com/progrium/dotfs/fsproto"
)

func init() {
	register(
		&Test{
			ID: "events/seq-start", Ext: ExtEvents, Section: secEvOpen,
			Name: "without since, the first record is seq <current>",
			Run:  testEventsSeqStart,
		},
		&Test{
			ID: "events/since-replay", Ext: ExtEvents, Section: secEvRecords, Requires: []Ext{ExtCtl},
			Name: "since N replays every change after N in order, with no gaps",
			Run:  testEventsReplay,
		},
		&Test{
			ID: "events/put-line", Ext: ExtEvents, Section: secEvRecords, Requires: []Ext{ExtCtl},
			Name: "put carries the whole index line, and dev= names the hello device",
			Run:  testEventsPutLine,
		},
		&Test{
			ID: "events/blocking-read", Ext: ExtEvents, Section: secEvReading, Requires: []Ext{ExtCtl},
			Name: "a read blocks while there are no records and returns when one arrives",
			Run:  testEventsBlocking,
		},
		&Test{
			ID: "events/flush", Ext: ExtEvents, Section: secEvReading, Requires: []Ext{ExtCtl},
			Name: "Tflush cancels a pending read and the stream loses nothing",
			Run:  testEventsFlush,
		},
		&Test{
			ID: "events/whole-records", Ext: ExtEvents, Section: secEvReading, Requires: []Ext{ExtCtl},
			Name: "small reads still return whole records",
			Run:  testEventsWholeRecords,
		},
		&Test{
			ID: "events/path-filter", Ext: ExtEvents, Section: secEvOpen, Requires: []Ext{ExtCtl},
			Name: "path filters report only changes at or below the directories",
			Run:  testEventsPathFilter,
		},
		&Test{
			ID: "events/setup-lines", Ext: ExtEvents, Section: secNoteLines, Requires: []Ext{ExtCtl},
			Name: "several setup lines in one write all apply",
			Run:  testEventsSetupLines,
		},
		&Test{
			ID: "events/coalesce", Ext: ExtEvents, Section: secEvOpen, Requires: []Ext{ExtCtl},
			Name: "with coalesce, records stay in order and end at the latest change",
			Run:  testEventsCoalesce,
		},
		&Test{
			ID: "events/dir-mv", Ext: ExtEvents, Section: secEvRecords, Requires: []Ext{ExtCtl},
			Name: "mv of a directory is one record, with none per child",
			Run:  testEventsDirMv,
		},
		&Test{
			ID: "events/no-fs", Ext: ExtEvents, Section: secLocation, Requires: []Ext{ExtCtl},
			Name: "nothing under .fs/ is reported, including stage writes",
			Run:  testEventsNoFs,
		},
		&Test{
			ID: "events/reset-future", Ext: ExtEvents, Section: secEvBehind,
			Name: "since past the current seq gets reset with the current seq, then the stream ends",
			Run:  testEventsResetFuture,
		},
		&Test{
			ID: "events/reset-trimmed", Ext: ExtEvents, Section: secEvBehind,
			Name: "since before the retained log gets reset, then the stream ends",
			Run:  testEventsResetTrimmed,
		},
		&Test{
			ID: "events/overflow", Ext: ExtEvents, Section: secEvBehind, Requires: []Ext{ExtCtl},
			Name: "a reader that falls behind gets every record or a reset, never a gap",
			Run:  testEventsOverflow,
		},
		&Test{
			ID: "events/bootstrap", Ext: ExtEvents, Section: secEvBoot, Requires: []Ext{ExtIndex, ExtCtl},
			Name: "index at N plus events since N, and events first plus index at M, both have no gap",
			Run:  testEventsBootstrap,
		},
	)
}

func testEventsSeqStart(t *T) {
	var dir string
	if t.Has(ExtCtl) {
		dir = t.Dir()
	}
	s := t.OpenEvents("")
	ev := s.MustNext()
	if ev.Op != fsproto.OpSeq {
		t.Fatalf("first record %q, want seq <current>", ev.Encode())
	}
	if t.Has(ExtIndex) {
		ix := t.ReadIndex()
		if ix.Header.Seq < ev.Seq {
			t.Errorf("index seq %d is before the stream's current seq %d", ix.Header.Seq, ev.Seq)
		}
	}
	if !t.Has(ExtCtl) {
		return
	}
	v := t.Mkdir(fsproto.Join(dir, "after"))
	next := s.MustNext()
	if next.Seq != v || next.Op != fsproto.OpPut {
		t.Errorf("after seq %d and a mkdir at %d, next record is %q", ev.Seq, v, next.Encode())
	}
}

func testEventsReplay(t *T) {
	dir := t.Dir()
	base := t.CurrentSeq()
	var want []uint64
	want = append(want, t.Mkdir(fsproto.Join(dir, "a")))
	want = append(want, t.Mkdir(fsproto.Join(dir, "b")))
	want = append(want, t.Must(fsproto.MvCmd{From: fsproto.Join(dir, "b"), To: fsproto.Join(dir, "c")}).Version)
	want = append(want, t.Must(fsproto.FlagsCmd{Path: fsproto.Join(dir, "a"), Flags: "p"}).Version)
	want = append(want, t.Must(fsproto.RmCmd{Path: fsproto.Join(dir, "c")}).Version)
	s := t.OpenEvents(SetupSince(base))
	evs := s.Until(want[len(want)-1])
	for i, ev := range evs {
		if ev.Seq != base+uint64(i)+1 {
			t.Fatalf("record %d has seq %d, want %d (no gaps without coalesce): %q", i, ev.Seq, base+uint64(i)+1, ev.Encode())
		}
	}
	bySeq := map[uint64]fsproto.Event{}
	for _, ev := range evs {
		bySeq[ev.Seq] = ev
	}
	checks := []struct {
		op   fsproto.Op
		path string
	}{
		{fsproto.OpPut, fsproto.Join(dir, "a")},
		{fsproto.OpPut, fsproto.Join(dir, "b")},
		{fsproto.OpMv, fsproto.Join(dir, "b")},
		{fsproto.OpFlags, fsproto.Join(dir, "a")},
		{fsproto.OpDel, fsproto.Join(dir, "c")},
	}
	for i, c := range checks {
		ev, ok := bySeq[want[i]]
		if !ok {
			t.Errorf("no record for seq %d", want[i])
			continue
		}
		if ev.Op != c.op || ev.Target() != c.path {
			t.Errorf("seq %d: %q, want %s %s", want[i], ev.Encode(), c.op, c.path)
		}
	}
	if ev := bySeq[want[2]]; ev.To != fsproto.Join(dir, "c") {
		t.Errorf("mv record %q, want destination %s", ev.Encode(), fsproto.Join(dir, "c"))
	}
	if ev := bySeq[want[3]]; ev.Flags != "p" {
		t.Errorf("flags record %q, want p", ev.Encode())
	}
	// since the last seq: nothing more, so the read blocks.
	s2 := t.OpenEvents(SetupSince(want[len(want)-1], dir))
	if ev, err := s2.Next(t.Opts().Quiet); err != errTimeout {
		t.Errorf("since the latest seq: got %q, %v; want a blocked read", ev.Encode(), err)
	}
}

func testEventsPutLine(t *T) {
	dev := "conform-" + randHex(3)
	c := t.OpenCtl()
	if rep, err := c.call(t.ctx, fsproto.HelloCmd{Device: dev}.Encode()); err != nil || rep != "ok" {
		t.Fatalf("hello: %q, %v", rep, err)
	}
	dir := t.Dir()
	base := t.CurrentSeq()
	p := fsproto.Join(dir, "d")
	rep, err := c.call(t.ctx, fsproto.MkdirCmd{Path: p}.Encode())
	if err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	r, err := fsproto.ParseReply("mkdir", rep)
	if err != nil || r.Status != fsproto.StatusOK {
		t.Fatalf("mkdir: %q", rep)
	}
	s := t.OpenEvents(SetupSince(base, dir))
	ev := s.Until(r.Version)
	last := ev[len(ev)-1]
	if last.Op != fsproto.OpPut {
		t.Fatalf("record %q, want put", last.Encode())
	}
	st, _ := t.Stat(p)
	got, want := last.Entry, st
	got.Attrs, want.Attrs = nil, nil
	if got.Encode() != want.Encode() {
		t.Errorf("put line %q, stat %q", got.Encode(), want.Encode())
	}
	if last.Dev != dev {
		t.Errorf("record %q: dev %q, want %q from hello on the same ctl fid's attach", last.Encode(), last.Dev, dev)
	}
	if t.Has(ExtStage) {
		f := fsproto.Join(dir, "f")
		name := StageName()
		t.WriteStage(name, []byte("put line\n"))
		rep, err := c.call(t.ctx, fsproto.CommitCmd{Stage: name, Path: f, Base: fsproto.At(0), Mtime: testMtime, Mode: 0o600}.Encode())
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		r, err := fsproto.ParseReply("commit", rep)
		if err != nil || r.Status != fsproto.StatusOK {
			t.Fatalf("commit: %q", rep)
		}
		ev := s.Until(r.Version)
		last := ev[len(ev)-1]
		st, _ := t.Stat(f)
		got, want := last.Entry, st
		got.Attrs, want.Attrs = nil, nil
		if got.Encode() != want.Encode() {
			t.Errorf("put line %q, stat %q", got.Encode(), want.Encode())
		}
		if last.Entry.Hash != fsproto.HashBytes([]byte("put line\n")) {
			t.Errorf("put hash %q, want %q", last.Entry.Hash, fsproto.HashBytes([]byte("put line\n")))
		}
	}
}

func testEventsBlocking(t *T) {
	dir := t.Dir()
	s := t.OpenEvents(SetupSince(t.CurrentSeq(), dir))
	call := s.fid.StartRead(0, readCount)
	select {
	case <-call.Done():
		resp, err := call.Result()
		t.Fatalf("read returned with no change pending: %v %v", resp, err)
	case <-time.After(t.Opts().Quiet):
	}
	v := t.Mkdir(fsproto.Join(dir, "wake"))
	select {
	case <-call.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("pending read did not return within 5s of a change")
	}
	resp, err := call.Result()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	evs, err := decodeRecords(resp.(*wire.Rread).Data)
	if err != nil {
		t.Fatalf("%v", err)
	}
	if len(evs) == 0 || evs[0].Seq != v {
		t.Errorf("woken read returned %v, want the record for seq %d", evs, v)
	}
}

func testEventsFlush(t *T) {
	dir := t.Dir()
	s := t.OpenEvents(SetupSince(t.CurrentSeq(), dir))
	call := s.fid.StartRead(0, readCount)
	time.Sleep(t.Opts().Quiet)
	ctx, cancel := context.WithTimeout(t.ctx, 5*time.Second)
	defer cancel()
	if err := t.conn.Flush(ctx, call); err != nil {
		t.Fatalf("no Rflush within 5s: %v", err)
	}
	if _, err := call.Result(); err == nil {
		t.Errorf("flushed read was answered with data although nothing changed")
	} else if !errors.Is(err, wire.ErrFlushed) {
		t.Logf("flushed read ended with %v", err)
	}
	// The connection and the stream still work, and the stream lost
	// nothing.
	v := t.Mkdir(fsproto.Join(dir, "after-flush"))
	ev, err := s.Next(5 * time.Second)
	if err != nil {
		t.Fatalf("read after flush: %v", err)
	}
	if ev.Seq != v {
		t.Errorf("after flush, next record %q, want seq %d", ev.Encode(), v)
	}
	// A flush racing a record must not lose it either: start a read, make
	// a change, flush, and the record is either in the read or the next.
	call = s.fid.StartRead(0, readCount)
	v2 := t.Mkdir(fsproto.Join(dir, "racing"))
	t.conn.Flush(ctx, call)
	if resp, err := call.Result(); err == nil {
		evs, derr := decodeRecords(resp.(*wire.Rread).Data)
		if derr != nil {
			t.Fatalf("%v", derr)
		}
		if len(evs) == 0 || evs[0].Seq != v2 {
			t.Errorf("read answered before flush returned %v, want seq %d", evs, v2)
		}
		return
	}
	ev, err = s.Next(5 * time.Second)
	if err != nil {
		t.Fatalf("read after the racing flush: %v", err)
	}
	if ev.Seq != v2 {
		t.Errorf("record %d was lost by a flush; next is %q", v2, ev.Encode())
	}
}

func testEventsWholeRecords(t *T) {
	dir := t.Dir()
	base := t.CurrentSeq()
	var last uint64
	for i := 0; i < 20; i++ {
		last = t.Mkdir(fsproto.Join(dir, fmt.Sprintf("record-%02d-with-a-longish-name", i)))
	}
	s := t.OpenEvents(SetupSince(base, dir))
	seen := uint64(0)
	for seen < last {
		b, err := s.fid.ReadAt(t.ctx, 0, 300)
		if err != nil {
			t.Fatalf("read with count 300: %v", err)
		}
		if len(b) == 0 {
			t.Fatalf("stream ended")
		}
		if len(b) > 300 {
			t.Fatalf("read returned %d bytes for count 300", len(b))
		}
		evs, err := decodeRecords(b)
		if err != nil {
			t.Fatalf("%v", err)
		}
		seen = evs[len(evs)-1].Seq
	}
}

func testEventsPathFilter(t *T) {
	dir := t.Dir()
	a, b := fsproto.Join(dir, "a"), fsproto.Join(dir, "b")
	t.Mkdir(a)
	t.Mkdir(b)
	s := t.OpenEvents(SetupSince(t.CurrentSeq(), a))
	in1 := t.Mkdir(fsproto.Join(a, "x"))
	t.Mkdir(fsproto.Join(b, "y"))
	in2 := t.Must(fsproto.MvCmd{From: fsproto.Join(b, "y"), To: fsproto.Join(a, "y")}).Version
	t.Mkdir(fsproto.Join(b, "z"))
	in3 := t.Must(fsproto.MvCmd{From: fsproto.Join(a, "x"), To: fsproto.Join(b, "x")}).Version
	in4 := t.Must(fsproto.FlagsCmd{Path: a, Flags: "p"}).Version
	evs := s.Until(in4)
	want := []uint64{in1, in2, in3, in4}
	if len(evs) != len(want) {
		var got []string
		for _, ev := range evs {
			got = append(got, ev.Encode())
		}
		t.Fatalf("filtered stream got %q, want seqs %v", got, want)
	}
	for i, ev := range evs {
		if ev.Seq != want[i] {
			t.Errorf("record %d: %q, want seq %d", i, ev.Encode(), want[i])
		}
	}
}

func testEventsSetupLines(t *T) {
	dir := t.Dir()
	a := fsproto.Join(dir, "a")
	t.Mkdir(a)
	base := t.CurrentSeq()
	t.Mkdir(fsproto.Join(dir, "outside"))
	v := t.Mkdir(fsproto.Join(a, "inside"))
	s := t.OpenEvents(fmt.Sprintf("since %d\npath %s\n", base, fsproto.EscapePath(a)))
	ev := s.MustNext()
	if ev.Seq != v {
		t.Errorf("first record %q, want seq %d (since and path in one write)", ev.Encode(), v)
	}
}

func testEventsCoalesce(t *T) {
	dir := t.Dir()
	p := fsproto.Join(dir, "x")
	t.Mkdir(p)
	s := t.OpenEvents(SetupSince(t.CurrentSeq(), dir) + "\ncoalesce")
	var last uint64
	flags := []fsproto.Flags{"p", "", "p", "", "p"}
	for _, f := range flags {
		last = t.Must(fsproto.FlagsCmd{Path: p, Flags: f}).Version
	}
	end := t.Marker(dir)
	evs := s.Until(end)
	var prev uint64
	var lastX fsproto.Event
	for _, ev := range evs {
		if ev.Seq <= prev {
			t.Errorf("record %q is not after seq %d", ev.Encode(), prev)
		}
		prev = ev.Seq
		if ev.Target() == p {
			lastX = ev
		}
	}
	if lastX.Seq != last || lastX.Flags != flags[len(flags)-1] {
		t.Errorf("last record for %s is %q, want flags %q at seq %d", p, lastX.Encode(), flags[len(flags)-1], last)
	}
	t.Logf("%d records for %d changes", len(evs), len(flags)+1)
}

func testEventsDirMv(t *T) {
	dir := t.Dir()
	src := fsproto.Join(dir, "src")
	t.Mkdir(src)
	t.Mkdir(fsproto.Join(src, "c1"))
	t.Mkdir(fsproto.Join(fsproto.Join(src, "c1"), "deep"))
	t.Mkdir(fsproto.Join(src, "c2"))
	s := t.OpenEvents(SetupSince(t.CurrentSeq(), dir))
	v := t.Must(fsproto.MvCmd{From: src, To: fsproto.Join(dir, "dst")}).Version
	end := t.Marker(dir)
	evs := s.Until(end)
	if len(evs) != 2 {
		var got []string
		for _, ev := range evs {
			got = append(got, ev.Encode())
		}
		t.Fatalf("mv of a directory produced %q, want one mv record then the marker", got)
	}
	if evs[0].Seq != v || evs[0].Op != fsproto.OpMv || evs[0].Path != src || evs[0].To != fsproto.Join(dir, "dst") {
		t.Errorf("record %q, want mv %s %s at %d", evs[0].Encode(), src, fsproto.Join(dir, "dst"), v)
	}
}

func testEventsNoFs(t *T) {
	dir := t.Dir()
	s := t.OpenEvents(SetupSince(t.CurrentSeq()))
	// Reads of index and ctl commands that change nothing.
	t.Do(fsproto.StatCmd{Path: dir})
	t.Do(fsproto.CapsCmd{})
	if t.Has(ExtIndex) {
		t.ReadIndex()
	}
	if t.Has(ExtStage) {
		name := StageName()
		t.WriteStage(name, []byte("staged, never committed"))
		t.Must(fsproto.AbortCmd{Stage: name})
	}
	end := t.Marker(dir)
	evs := s.Until(end)
	for _, ev := range evs[:len(evs)-1] {
		t.Errorf("unexpected record %q", ev.Encode())
	}
	for _, ev := range evs {
		if hasFsComponent(ev.Target()) || hasFsComponent(ev.To) {
			t.Errorf("record %q reports .fs/", ev.Encode())
		}
	}
}

func expectReset(t *T, s *Stream, current uint64, what string) {
	ev, err := s.Next(5 * time.Second)
	if err != nil {
		t.Fatalf("%s: %v, want a reset record", what, err)
	}
	if ev.Op != fsproto.OpReset {
		t.Fatalf("%s: record %q, want reset", what, ev.Encode())
	}
	if ev.Seq != current {
		t.Errorf("%s: reset carries seq %d, current is %d (spec notes: reset carries the current seq)", what, ev.Seq, current)
	}
	if ev, err := s.Next(5 * time.Second); err != io.EOF {
		t.Errorf("%s: after reset got %q, %v; want the stream to end (an empty read)", what, ev.Encode(), err)
	}
}

func testEventsResetFuture(t *T) {
	cur := t.CurrentSeq()
	s := t.OpenEvents(SetupSince(cur + 1000))
	expectReset(t, s, cur, fmt.Sprintf("since %d at seq %d", cur+1000, cur))
}

func testEventsResetTrimmed(t *T) {
	if t.r.target.Trim == nil {
		t.Skipf("the target cannot trim its log on request (only in-process targets can)")
	}
	if !t.Has(ExtCtl) {
		t.Skipf("needs ctl to make changes")
	}
	dir := t.Dir()
	t.Mkdir(fsproto.Join(dir, "a"))
	cur := t.Mkdir(fsproto.Join(dir, "b"))
	if err := t.r.target.Trim(t.ctx, cur); err != nil {
		t.Fatalf("trim: %v", err)
	}
	s := t.OpenEvents(SetupSince(cur - 2))
	expectReset(t, s, cur, fmt.Sprintf("since %d with the log trimmed to %d", cur-2, cur))
	// since the trim point itself is still served.
	s2 := t.OpenEvents(SetupSince(cur))
	if ev, err := s2.Next(t.Opts().Quiet); err != errTimeout {
		t.Errorf("since %d (the trim point): got %q, %v; want a blocked read", cur, ev.Encode(), err)
	}
}

func testEventsOverflow(t *T) {
	dir := t.Dir()
	s := t.OpenEvents(SetupSince(t.CurrentSeq()))
	// Read one record first, so the stream is live rather than replaying
	// from the log, then fall behind.
	base := t.Mkdir(fsproto.Join(dir, "live"))
	if ev := s.MustNext(); ev.Seq != base {
		t.Fatalf("first record %q, want seq %d", ev.Encode(), base)
	}
	n := t.Opts().Overflow
	c := t.OpenCtl()
	var last uint64
	for i := 0; i < n; i++ {
		cmd := fsproto.MkdirCmd{Path: fsproto.Join(dir, fmt.Sprintf("o%05d", i))}
		rep, err := c.call(t.ctx, cmd.Encode())
		if err != nil {
			t.Fatalf("mkdir %d: %v", i, err)
		}
		r, err := fsproto.ParseReply("mkdir", rep)
		if err != nil || r.Status != fsproto.StatusOK {
			t.Fatalf("mkdir %d: %q", i, rep)
		}
		last = r.Version
	}
	next := base + 1
	for next <= last {
		ev, err := s.Next(5 * time.Second)
		if err != nil {
			t.Fatalf("after %d records: %v", next-base-1, err)
		}
		if ev.Op == fsproto.OpReset {
			if ev.Seq < next-1 {
				t.Errorf("reset carries seq %d, before the last record delivered (%d)", ev.Seq, next-1)
			}
			if _, err := s.Next(5 * time.Second); err != io.EOF {
				t.Errorf("stream did not end after reset: %v", err)
			}
			t.Logf("reset after %d of %d records", next-base-1, n)
			return
		}
		if ev.Seq != next {
			t.Fatalf("record %q, want seq %d: a gap without coalesce or reset", ev.Encode(), next)
		}
		next++
	}
	t.Logf("all %d records delivered without a reset; the server's queue holds at least that many", n)
}

func testEventsBootstrap(t *T) {
	dir := t.Dir()
	// Way one: index at N, then events since N.
	ix := t.ReadIndex()
	s := t.OpenEvents(SetupSince(ix.Header.Seq))
	v := t.Mkdir(fsproto.Join(dir, "one"))
	evs := s.Until(v)
	if evs[0].Seq != ix.Header.Seq+1 {
		t.Errorf("index at %d, then since %d: first record %q", ix.Header.Seq, ix.Header.Seq, evs[0].Encode())
	}
	// Way two: events first (seq <current>), then the index at M; records
	// after M follow without a gap.
	s2 := t.OpenEvents("")
	first := s2.MustNext()
	if first.Op != fsproto.OpSeq {
		t.Fatalf("first record %q", first.Encode())
	}
	v2 := t.Mkdir(fsproto.Join(dir, "two"))
	ix2 := t.ReadIndex()
	if ix2.Header.Seq < v2 {
		t.Errorf("index seq %d after a change at %d", ix2.Header.Seq, v2)
	}
	if _, ok := ix2.Find(fsproto.Join(dir, "two")); !ok {
		t.Errorf("index at %d does not list the change at %d", ix2.Header.Seq, v2)
	}
	v3 := t.Mkdir(fsproto.Join(dir, "three"))
	evs = s2.Until(v3)
	var kept []fsproto.Event
	for _, ev := range evs {
		if ev.Seq > ix2.Header.Seq {
			kept = append(kept, ev)
		}
	}
	for i, ev := range kept {
		if ev.Seq != ix2.Header.Seq+uint64(i)+1 {
			t.Errorf("after dropping records at or below %d, record %q leaves a gap", ix2.Header.Seq, ev.Encode())
		}
	}
	if len(evs) == 0 || evs[0].Seq != first.Seq+1 {
		t.Errorf("stream opened at seq %d: first record after it is %v", first.Seq, evs)
	}
}
