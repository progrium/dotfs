package sync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	gosync "sync"

	"github.com/progrium/dotfs/client"
	"github.com/progrium/dotfs/fsproto"
)

// maxTries bounds how often one queue entry is retried before the engine
// leaves it to the next full scan.
const maxTries = 5

// outcome of pushing one queue entry.
type outcome int

const (
	pushed   outcome = iota // done; drop the entry
	conflict                // the server moved on; catch up and retry
	later                   // keep the entry for a later cycle
)

// drain pushes the queue to the server in dependency order. A conflict
// makes it catch up on events, which settles the conflict locally, and go
// round again. Runs of new or edited small files are sent several at a
// time (see sendBatch); everything else goes one entry at a time.
func (e *Engine) drain(ctx context.Context) error {
	for round := 0; e.st.queueLen() > 0; round++ {
		items := e.ordered()
		conflicts := 0
		// handle records one entry's outcome.
		handle := func(q *qent, out outcome, err error) error {
			if err != nil {
				if isConnErr(err) {
					return err
				}
				e.logf("%s %s: %v", q.Op, q.Path, err)
				out = later
			}
			switch out {
			case pushed:
				e.st.dequeue(q)
			case conflict:
				conflicts++
				if err := e.catchUp(ctx); err != nil {
					return err
				}
				if err := e.scanHints(); err != nil {
					return err
				}
				fallthrough
			case later:
				if e.queued(q) {
					q.Tries++
					e.st.touchQueue(q)
					if q.Tries >= maxTries {
						e.logf("giving up on %s %s for now", q.Op, q.Path)
						e.st.dequeue(q)
					}
				}
			}
			return e.st.flush()
		}
		for i := 0; i < len(items); {
			if batch := e.batchable(items[i:]); len(batch) > 1 {
				i += len(batch)
				if err := e.sendRun(ctx, batch, handle); err != nil {
					return err
				}
				continue
			}
			q := items[i]
			i++
			if !e.queued(q) {
				continue // settled by catching up
			}
			out, err := e.push(ctx, q)
			if err := handle(q, out, err); err != nil {
				return err
			}
		}
		if conflicts == 0 || round >= maxTries {
			break
		}
	}
	if e.reconcile {
		return e.afterRemote(ctx)
	}
	return nil
}

// Runs of small files are sent with batchConns in flight, up to batchMax
// at a time.
const (
	batchMax   = 4096
	batchConns = 8
	batchSize  = 4 << 20 // larger files go one at a time, with resumable stage names

	// New files up to smallUpload bytes skip commit-hash (see sendFile).
	smallUpload = 64 << 10
)

// batchItem is one file in a batch: what drain knew before sending it, and
// what sending did.
type batchItem struct {
	q     *qent
	base  uint64
	flags fsproto.Flags
	row   *row
	st    lstat
	h     string
	res   sent
	redo  bool // not sent; push it on its own
}

// batchable returns the run of entries at the start of items that can be
// sent at once: syncs of regular files no bigger than batchSize, whose
// parent the server already has, that are not clean.
func (e *Engine) batchable(items []*qent) []*batchItem {
	var out []*batchItem
	for _, q := range items {
		if len(out) == batchMax || q.Op != opSync || !e.queued(q) {
			break
		}
		r := e.st.get(q.Path)
		if r != nil && r.Type != fsproto.TypeFile || e.excluded(q.Path, false) {
			break
		}
		if par := fsproto.Parent(q.Path); par != "/" {
			if pr := e.st.get(par); pr == nil || pr.Type != fsproto.TypeDir {
				break
			}
		}
		st, err := e.stat(q.Path)
		if err != nil || !st.exists || st.typ != fsproto.TypeFile || st.size > batchSize || r != nil && quickClean(r, st) {
			break
		}
		b := &batchItem{q: q, row: r, st: st}
		if r != nil {
			b.base, b.flags = r.Version, r.Flags
		}
		out = append(out, b)
	}
	return out
}

// sendRun hashes and sends the run's files with batchConns in flight,
// starting the next file as soon as one finishes. It records each result in
// the engine's state in order, as soon as every file before it has been
// recorded, and passes it to handle; the workers only read the disk and
// talk to the server. Conflicts, and files that need pushing on their own,
// wait until every file sent has been recorded, so that catching up
// recognizes the run's own commits. After an error from handle, no new
// file is started, but every file sent is still recorded.
func (e *Engine) sendRun(ctx context.Context, run []*batchItem, handle func(*qent, outcome, error) error) error {
	n := len(run)
	work := make(chan int)
	finished := make(chan int, n)
	stop := make(chan struct{})
	var wg gosync.WaitGroup
	for range min(batchConns, n) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				e.sendItem(ctx, run[i])
				finished <- i
			}
		}()
	}
	go func() {
		defer close(work)
		for i := range run {
			select {
			case work <- i:
			case <-stop:
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		close(finished)
	}()

	var (
		firstErr error
		deferred []*batchItem
		done     = make([]bool, n)
		next     int
	)
	// Files start in order, so the ones sent are always a prefix of run.
	for i := range finished {
		done[i] = true
		for next < n && done[next] {
			b := run[next]
			next++
			b.res.out, b.res.err = e.applySend(b.q, b.q.Path, b.flags, b.st, b.h, b.res)
			if firstErr != nil || !e.queued(b.q) {
				continue
			}
			if b.redo || b.res.err == nil && b.res.out == conflict {
				deferred = append(deferred, b)
				continue
			}
			if err := handle(b.q, b.res.out, b.res.err); err != nil {
				firstErr = err
				close(stop)
			}
		}
	}
	if firstErr != nil {
		return firstErr
	}
	for _, b := range deferred {
		if !e.queued(b.q) {
			continue
		}
		out, err := b.res.out, b.res.err
		if b.redo {
			out, err = e.push(ctx, b.q)
		}
		if err := handle(b.q, out, err); err != nil {
			return err
		}
	}
	return nil
}

// sendItem hashes and sends one file of a run, leaving the result in b.
// It reads the disk and talks to the server, but does not touch the
// engine's state, so several can run at once.
func (e *Engine) sendItem(ctx context.Context, b *batchItem) {
	h, err := e.hashFile(b.q.Path)
	if err != nil {
		b.res = sent{out: later, err: err}
		return
	}
	if b.row != nil && h == b.row.Hash && b.st.mode == b.row.Mode {
		b.redo = true // touched, not changed: push records the new stat
		return
	}
	if now, err := e.stat(b.q.Path); err != nil || now != b.st {
		b.res = sent{out: later, err: err} // still being written
		return
	}
	b.h = h
	b.res = e.sendFile(ctx, b.q, b.q.Path, b.base, b.st, h, false)
}

func (e *Engine) queued(q *qent) bool { return e.st.queued(q) }

// ordered returns the queue sorted so that each change finds what it needs
// on the server: new directories (shallowest first), renames, files, then
// deletes (deepest first).
func (e *Engine) ordered() []*qent {
	type item struct {
		q     *qent
		class int
		depth int
	}
	pending := e.st.pending()
	items := make([]item, 0, len(pending))
	for _, q := range pending {
		it := item{q: q, class: 2, depth: depth(q.Path)}
		if q.Op == opMv {
			it.class, it.depth = 1, depth(q.To)
		} else {
			st, _ := e.stat(q.Path)
			r := e.st.get(q.Path)
			switch {
			case st.exists && st.typ == fsproto.TypeDir && (r == nil || r.Type != fsproto.TypeDir):
				it.class = 0
			case !st.exists && r != nil:
				it.class, it.depth = 3, -it.depth
			}
		}
		items = append(items, it)
	}
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i], items[j]
		if a.class != b.class {
			return a.class < b.class
		}
		if a.depth != b.depth {
			return a.depth < b.depth
		}
		return a.q.ID < b.q.ID
	})
	out := make([]*qent, len(items))
	for i, it := range items {
		out[i] = it.q
	}
	return out
}

func (e *Engine) push(ctx context.Context, q *qent) (outcome, error) {
	if q.Op == opMv {
		return e.pushMv(ctx, q)
	}
	p := q.Path
	r := e.st.get(p)
	if e.excluded(p, r != nil && r.Type == fsproto.TypeDir) {
		return pushed, nil
	}
	ls, st, h, err := e.classify(p, r)
	if err != nil {
		return later, err
	}
	switch ls {
	case lsAbsent, lsClean:
		return pushed, nil
	case lsDeleted:
		if e.st.pendingMv(p) != nil {
			return later, nil
		}
		return e.pushDelete(ctx, p, r)
	}
	if r != nil && r.Type != st.typ {
		// The type changed: remove the old one from the server first.
		if out, err := e.pushDelete(ctx, p, r); out != pushed || err != nil {
			return out, err
		}
		r = nil
	}
	switch st.typ {
	case fsproto.TypeDir:
		return e.pushMkdir(ctx, p, st)
	case fsproto.TypeSymlink:
		if !e.warned[p] {
			e.warned[p] = true
			e.logf("not uploading symlink %s: the protocol has no command for it", p)
		}
		return pushed, nil
	}
	return e.pushFile(ctx, q, p, r, st, h)
}

// pushDelete removes p (and, for a directory, what is below it) on the
// server, each path conditional on its base.
func (e *Engine) pushDelete(ctx context.Context, p string, r *row) (outcome, error) {
	if r.Type == fsproto.TypeDir {
		kids := e.st.subtree(p)
		for i := len(kids) - 1; i > 0; i-- {
			k := kids[i]
			kr := e.st.get(k)
			if st, err := e.stat(k); err != nil {
				return later, err
			} else if st.exists {
				return later, nil // something below came back
			}
			if out, err := e.rm(ctx, k, kr); out != pushed || err != nil {
				return out, err
			}
		}
	}
	return e.rm(ctx, p, r)
}

func (e *Engine) rm(ctx context.Context, p string, r *row) (outcome, error) {
	rep, err := e.ctl(ctx, fsproto.RmCmd{Path: p, Base: fsproto.At(r.Version)})
	if err != nil {
		return later, err
	}
	switch rep.Status {
	case fsproto.StatusOK, fsproto.StatusMissing:
		e.st.del(p)
		return pushed, nil
	case fsproto.StatusConflict, fsproto.StatusInvalid:
		// Changed on the server, or a directory with new children there.
		return conflict, nil
	}
	return later, rep.Err()
}

func (e *Engine) pushMkdir(ctx context.Context, p string, st lstat) (outcome, error) {
	if out, err := e.ensureParent(ctx, p); out != pushed || err != nil {
		return out, err
	}
	rep, err := e.ctl(ctx, fsproto.MkdirCmd{Path: p, Base: fsproto.At(0)})
	if err != nil {
		return later, err
	}
	switch rep.Status {
	case fsproto.StatusOK:
		e.st.put(&row{Entry: fsproto.Entry{Path: p, Type: fsproto.TypeDir, Mode: st.mode, Mtime: st.mtime / 1e9, Version: rep.Version}, LMtime: st.mtime, LIno: st.ino})
		return pushed, nil
	case fsproto.StatusConflict:
		// Something is there already. A directory is simply adopted; any
		// thing else arrives as an event and is settled as a conflict.
		srep, err := e.ctl(ctx, fsproto.StatCmd{Path: p})
		if err != nil {
			return later, err
		}
		if srep.Status == fsproto.StatusOK && srep.Entry != nil && srep.Entry.Type == fsproto.TypeDir {
			e.st.put(&row{Entry: *srep.Entry, LMtime: st.mtime, LIno: st.ino})
			return pushed, nil
		}
		return conflict, nil
	case fsproto.StatusMissing:
		return conflict, nil // the parent went away meanwhile
	}
	return later, rep.Err()
}

// ensureParent makes sure p's parent directory exists on the server,
// creating any directories the rows do not know about.
func (e *Engine) ensureParent(ctx context.Context, p string) (outcome, error) {
	par := fsproto.Parent(p)
	if par == "/" {
		return pushed, nil
	}
	if r := e.st.get(par); r != nil && r.Type == fsproto.TypeDir {
		return pushed, nil
	}
	st, err := e.stat(par)
	if err != nil {
		return later, err
	}
	if !st.exists || st.typ != fsproto.TypeDir {
		return later, nil
	}
	return e.pushMkdir(ctx, par, st)
}

// pushFile commits the local file p: by hash when the server already holds
// the content, otherwise through a resumable stage upload.
func (e *Engine) pushFile(ctx context.Context, q *qent, p string, r *row, st lstat, h string) (outcome, error) {
	if out, err := e.ensureParent(ctx, p); out != pushed || err != nil {
		return out, err
	}
	var base uint64
	var flags fsproto.Flags
	if r != nil {
		base, flags = r.Version, r.Flags
	}
	res := e.sendFile(ctx, q, p, base, st, h, true)
	return e.applySend(q, p, flags, st, h, res)
}

// sent is what sendFile did, for applySend to record.
type sent struct {
	out     outcome
	err     error
	version uint64 // the committed version, when ok
	ok      bool   // committed
	touched bool   // q's stage fields changed
}

// sendFile commits p's content h at base, by hash or through q's stage
// file. It talks only to the server and never touches the engine's state,
// except to flush q's new stage name before an upload when persist is set,
// so several can run at once when persist is not.
func (e *Engine) sendFile(ctx context.Context, q *qent, p string, base uint64, st lstat, h string, persist bool) sent {
	mtime := st.mtime / 1e9
	// A new small file is uploaded straight away: commit-hash would
	// almost always miss, and the upload costs little more than asking.
	if base != 0 || st.size > smallUpload {
		rep, err := e.ctl(ctx, fsproto.CommitHashCmd{Hash: h, Path: p, Base: fsproto.At(base), Mtime: mtime, Mode: st.mode})
		if err != nil {
			return sent{out: later, err: err}
		}
		switch rep.Status {
		case fsproto.StatusOK:
			return sent{out: pushed, ok: true, version: rep.Version}
		case fsproto.StatusConflict, fsproto.StatusInvalid:
			return sent{out: conflict}
		}
	}

	// The server does not have the content: upload it.
	var res sent
	if q.Stage != "" && q.SHash != h {
		e.abortStage(ctx, q)
		res.touched = true
	}
	if q.Stage == "" {
		q.Stage, q.SHash = "sync-"+randomID(), h
		res.touched = true
		if persist {
			e.st.touchQueue(q)
			if err := e.st.flush(); err != nil {
				res.out, res.err = later, err
				return res
			}
		}
	}
	done := func(out outcome, err error) sent {
		res.out, res.err = out, err
		return res
	}
	f, err := os.Open(e.osPath(p))
	if err != nil {
		return done(later, err)
	}
	_, err = client.Upload(ctx, e.conn, q.Stage, f, st.size)
	f.Close()
	if err != nil {
		if localErr(err) {
			return done(later, err)
		}
		return done(later, cerr(err))
	}
	if now, err := e.stat(p); err != nil || now != st {
		// Still being written; the next scan commits the final content.
		e.abortStage(ctx, q)
		res.touched = true
		return done(later, err)
	}
	rep, err := e.ctl(ctx, fsproto.CommitCmd{Stage: q.Stage, Path: p, Base: fsproto.At(base), Mtime: mtime, Mode: st.mode, Hash: h})
	if err != nil {
		return done(later, err)
	}
	switch rep.Status {
	case fsproto.StatusOK:
		q.Stage, q.SHash = "", ""
		res.touched, res.ok, res.version = true, true, rep.Version
		return done(pushed, nil)
	case fsproto.StatusConflict:
		e.abortStage(ctx, q)
		res.touched = true
		return done(conflict, nil)
	case fsproto.StatusMissing:
		// The stage file expired, or the parent directory is gone.
		q.Stage, q.SHash = "", ""
		res.touched = true
		return done(conflict, nil)
	}
	e.abortStage(ctx, q)
	res.touched = true
	return done(later, fmt.Errorf("commit: %w", rep.Err()))
}

// applySend records what sendFile did in the engine's state.
func (e *Engine) applySend(q *qent, p string, flags fsproto.Flags, st lstat, h string, res sent) (outcome, error) {
	if res.touched {
		e.st.touchQueue(q)
	}
	if res.ok {
		e.committed(p, h, flags, res.version, st)
	}
	return res.out, res.err
}

func (e *Engine) abort(ctx context.Context, q *qent) {
	if q.Stage == "" {
		return
	}
	e.abortStage(ctx, q)
	e.st.touchQueue(q)
}

// abortStage removes q's stage file on the server and forgets it.
func (e *Engine) abortStage(ctx context.Context, q *qent) {
	if q.Stage == "" {
		return
	}
	e.ctl(ctx, fsproto.AbortCmd{Stage: q.Stage})
	q.Stage, q.SHash = "", ""
}

// committed records a successful commit of p as its new base.
func (e *Engine) committed(p, h string, flags fsproto.Flags, v uint64, st lstat) {
	e.st.put(&row{
		Entry: fsproto.Entry{Path: p, Type: fsproto.TypeFile, Mode: st.mode, Length: st.size, Mtime: st.mtime / 1e9, Version: v, Hash: h, Flags: flags},
		LSize: st.size, LMtime: st.mtime, LIno: st.ino,
	})
}

// pushMv commits a local rename.
func (e *Engine) pushMv(ctx context.Context, q *qent) (outcome, error) {
	from, to := q.Path, q.To
	rf := e.st.get(from)
	if rf == nil {
		e.st.enqueue(opSync, to, "")
		return pushed, nil
	}
	sf, err := e.stat(from)
	if err != nil {
		return later, err
	}
	stt, err := e.stat(to)
	if err != nil {
		return later, err
	}
	if sf.exists || !stt.exists || e.st.get(to) != nil {
		// No longer a plain rename: fall back to a delete and a create.
		e.st.enqueue(opSync, from, "")
		e.st.enqueue(opSync, to, "")
		return pushed, nil
	}
	if out, err := e.ensureParent(ctx, to); out != pushed || err != nil {
		return out, err
	}
	rep, err := e.ctl(ctx, fsproto.MvCmd{From: from, To: to, Base: fsproto.At(rf.Version), ToBase: fsproto.At(0)})
	if err != nil {
		return later, err
	}
	switch rep.Status {
	case fsproto.StatusOK:
		e.st.rebase(from, to, rep.Version)
		e.hint(to, true)
		return pushed, nil
	case fsproto.StatusConflict, fsproto.StatusMissing:
		return conflict, nil
	}
	// Refused (for example, into itself): fall back to delete and create.
	e.st.enqueue(opSync, from, "")
	e.st.enqueue(opSync, to, "")
	if errors.Is(rep.Err(), fsproto.ErrInvalid) {
		return pushed, nil
	}
	return later, rep.Err()
}
