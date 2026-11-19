package sync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"

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
// round again.
func (e *Engine) drain(ctx context.Context) error {
	for round := 0; len(e.st.queue) > 0; round++ {
		items := e.ordered()
		conflicts := 0
		for _, q := range items {
			if !e.queued(q) {
				continue // settled by catching up
			}
			out, err := e.push(ctx, q)
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
					e.st.touchQueue()
					if q.Tries >= maxTries {
						e.logf("giving up on %s %s for now", q.Op, q.Path)
						e.st.dequeue(q)
					}
				}
			}
			if err := e.st.flush(); err != nil {
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

func (e *Engine) queued(q *qent) bool {
	for _, x := range e.st.queue {
		if x == q {
			return true
		}
	}
	return false
}

// ordered returns the queue sorted so that each change finds what it needs
// on the server: new directories (shallowest first), renames, files, then
// deletes (deepest first).
func (e *Engine) ordered() []*qent {
	type item struct {
		q     *qent
		class int
		depth int
	}
	items := make([]item, 0, len(e.st.queue))
	for _, q := range e.st.queue {
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
	if e.ign.Ignored(p, r != nil && r.Type == fsproto.TypeDir) {
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
	mtime := st.mtime / 1e9
	rep, err := e.ctl(ctx, fsproto.CommitHashCmd{Hash: h, Path: p, Base: fsproto.At(base), Mtime: mtime, Mode: st.mode})
	if err != nil {
		return later, err
	}
	switch rep.Status {
	case fsproto.StatusOK:
		e.committed(p, h, flags, rep.Version, st)
		return pushed, nil
	case fsproto.StatusConflict, fsproto.StatusInvalid:
		return conflict, nil
	}

	// The server does not have the content: upload it.
	if q.Stage != "" && q.SHash != h {
		e.abort(ctx, q)
	}
	if q.Stage == "" {
		q.Stage, q.SHash = "sync-"+randomID(), h
		e.st.touchQueue()
		if err := e.st.flush(); err != nil {
			return later, err
		}
	}
	f, err := os.Open(e.osPath(p))
	if err != nil {
		return later, err
	}
	_, err = client.Upload(ctx, e.conn, q.Stage, f, st.size)
	f.Close()
	if err != nil {
		if localErr(err) {
			return later, err
		}
		return later, cerr(err)
	}
	if now, err := e.stat(p); err != nil || now != st {
		// Still being written; the next scan commits the final content.
		e.abort(ctx, q)
		return later, err
	}
	rep, err = e.ctl(ctx, fsproto.CommitCmd{Stage: q.Stage, Path: p, Base: fsproto.At(base), Mtime: mtime, Mode: st.mode, Hash: h})
	if err != nil {
		return later, err
	}
	switch rep.Status {
	case fsproto.StatusOK:
		q.Stage, q.SHash = "", ""
		e.st.touchQueue()
		e.committed(p, h, flags, rep.Version, st)
		return pushed, nil
	case fsproto.StatusConflict:
		e.abort(ctx, q)
		return conflict, nil
	case fsproto.StatusMissing:
		// The stage file expired, or the parent directory is gone.
		q.Stage, q.SHash = "", ""
		e.st.touchQueue()
		return conflict, nil
	}
	e.abort(ctx, q)
	return later, fmt.Errorf("commit: %w", rep.Err())
}

func (e *Engine) abort(ctx context.Context, q *qent) {
	if q.Stage == "" {
		return
	}
	e.ctl(ctx, fsproto.AbortCmd{Stage: q.Stage})
	q.Stage, q.SHash = "", ""
	e.st.touchQueue()
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
