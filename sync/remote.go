package sync

import (
	"context"
	"errors"
	"io"
	"os"
	"sort"
	gosync "sync"

	"github.com/progrium/dotfs/fsproto"
)

// handleEvent applies one record from the events stream.
func (e *Engine) handleEvent(ctx context.Context, ev fsproto.Event) error {
	switch ev.Op {
	case fsproto.OpSeq:
		return nil
	case fsproto.OpReset:
		e.logf("events reset at seq %d; re-reading the index", ev.Seq)
		if err := e.reconcileIndex(ctx); err != nil {
			return err
		}
		return e.openEvents(ctx)
	}
	if ev.Seq <= e.st.seq {
		return nil // already applied
	}
	var err error
	switch ev.Op {
	case fsproto.OpPut:
		err = e.applyPut(ctx, ev.Entry, false)
	case fsproto.OpDel:
		err = e.applyDel(ev.Path, ev.Seq)
	case fsproto.OpMv:
		err = e.applyMv(ev.Path, ev.To, ev.Seq)
	case fsproto.OpFlags:
		e.applyFlags(ev.Path, ev.Flags, ev.Seq)
	}
	if err != nil {
		if isConnErr(err) {
			return err
		}
		// A local problem with one path should not stop the stream; the
		// next reconcile or scan sees the difference again.
		e.logf("applying %s %s: %v", ev.Op, ev.Target(), err)
	}
	e.st.setSeq(ev.Seq)
	return nil
}

// applyPut makes the local path match a server entry. With force (a
// reconcile) it applies even an entry older than the row.
func (e *Engine) applyPut(ctx context.Context, ent fsproto.Entry, force bool) error {
	p := ent.Path
	if p == "/" || e.ign.Ignored(p, ent.Type == fsproto.TypeDir) {
		return nil
	}
	r := e.st.get(p)
	if r != nil && !force && ent.Version <= r.Version {
		return nil // our own change echoed back, or older than what we have
	}
	if p == "/"+IgnoreFile {
		defer func() { e.reloadIgnores() }()
	}

	// A local rename of p not yet committed: the new content belongs under
	// the new name.
	if q := e.st.pendingMv(p); q != nil && r != nil {
		moved := *r
		moved.Path = q.To
		ls, st, _, err := e.classify(q.To, &moved)
		if err != nil {
			return err
		}
		if ls == lsClean && ent.Type == fsproto.TypeFile && r.Type == fsproto.TypeFile {
			inst, hash, err := e.fetchInstall(ctx, ent, q.To, st)
			if err != nil || inst == nil {
				return err
			}
			nr := &row{Entry: ent, LSize: inst.size, LMtime: inst.mtime, LIno: inst.ino}
			nr.Hash = hash
			e.st.put(nr)
			return nil
		}
		// Renamed and changed: give up on the rename; both survive.
		e.st.dequeue(q)
		e.st.enqueue(opSync, q.To, "")
	}

	for try := 0; ; try++ {
		ls, st, h, err := e.classify(p, r)
		if err != nil {
			return err
		}
		if ls == lsDirty {
			if sameContent(ent, st, h) {
				e.st.put(&row{Entry: ent, LSize: st.size, LMtime: st.mtime, LIno: st.ino})
				return nil
			}
			if st.typ == fsproto.TypeDir && ent.Type != fsproto.TypeDir {
				err = e.clearDir(p)
			} else {
				var cp string
				cp, err = e.moveAside(p)
				if err == nil {
					e.st.enqueue(opSync, cp, "")
					e.hint(cp, true)
				}
			}
			if err != nil {
				return err
			}
			st = lstat{}
		} else if ls == lsClean && st.typ == fsproto.TypeDir && ent.Type != fsproto.TypeDir {
			if err := e.clearDir(p); err != nil {
				return err
			}
			st = lstat{}
		}
		err = e.materialize(ctx, ent, st)
		if errors.Is(err, errLocalChanged) && try < 3 {
			continue
		}
		return err
	}
}

// sameContent reports whether the local path already holds what ent
// describes, so a change on both sides is not a conflict.
func sameContent(ent fsproto.Entry, st lstat, h string) bool {
	if !st.exists || st.typ != ent.Type {
		return false
	}
	switch ent.Type {
	case fsproto.TypeDir:
		return true
	case fsproto.TypeSymlink:
		return st.target == ent.Target
	}
	return h != "" && h == ent.Hash && st.mode == ent.Mode
}

// materialize writes ent at its path, replacing st, which is clean or
// absent.
func (e *Engine) materialize(ctx context.Context, ent fsproto.Entry, st lstat) error {
	p := ent.Path
	switch ent.Type {
	case fsproto.TypeDir:
		if st.exists && st.typ != fsproto.TypeDir {
			if err := e.removeLocal(p); err != nil {
				return err
			}
		}
		if err := os.MkdirAll(e.osPath(p), 0o755); err != nil {
			return err
		}
		now, err := e.stat(p)
		if err != nil {
			return err
		}
		e.st.put(&row{Entry: ent, LMtime: now.mtime, LIno: now.ino})
		return nil
	case fsproto.TypeSymlink:
		if st.exists {
			if err := e.removeLocal(p); err != nil {
				return err
			}
		}
		if err := e.mkdirParents(p); err != nil {
			return err
		}
		if err := os.Symlink(ent.Target, e.osPath(p)); err != nil {
			return err
		}
		now, err := e.stat(p)
		if err != nil {
			return err
		}
		e.st.put(&row{Entry: ent, LSize: now.size, LMtime: now.mtime, LIno: now.ino})
		return nil
	}
	inst, hash, err := e.fetchInstall(ctx, ent, p, st)
	if err != nil || inst == nil {
		return err
	}
	r := &row{Entry: ent, LSize: inst.size, LMtime: inst.mtime, LIno: inst.ino}
	r.Hash = hash
	e.st.put(r)
	return nil
}

// fetchInstall fetches ent's content and installs it at local path p,
// which must still look like want. It returns nil if the server no longer
// has the file (a later record will say what happened to it).
func (e *Engine) fetchInstall(ctx context.Context, ent fsproto.Entry, p string, want lstat) (*lstat, string, error) {
	tmp, hash, err := e.fetch(ctx, ent)
	if errors.Is(err, fsproto.ErrMissing) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	inst, err := e.install(tmp, p, ent, want)
	if err != nil {
		os.Remove(tmp)
		return nil, "", err
	}
	if hash != ent.Hash && ent.Hash != "" {
		// The file changed on the server while we read it; the row records
		// what we hold, and the record for the newer version follows.
		e.logf("fetched %s changed while reading it", ent.Path)
	}
	return &inst, hash, nil
}

type fetched struct {
	version uint64
	tmp     string
	hash    string
	err     error
}

// fetch returns a temp file holding ent's content and its hash: a
// prefetched one, a copy of a clean local file with the same hash, or a
// read from the server.
func (e *Engine) fetch(ctx context.Context, ent fsproto.Entry) (string, string, error) {
	if f, ok := e.prefetch[ent.Path]; ok && f.version == ent.Version {
		delete(e.prefetch, ent.Path)
		return f.tmp, f.hash, f.err
	}
	if tmp, h, ok := e.copyByHash(ent.Hash); ok {
		return tmp, h, nil
	}
	return e.download(ctx, ent.Path)
}

func (e *Engine) copyByHash(hash string) (string, string, bool) {
	if hash == "" {
		return "", "", false
	}
	for _, q := range e.st.withHash(hash) {
		r := e.st.get(q)
		st, err := e.stat(q)
		if err != nil || !quickClean(r, st) {
			continue
		}
		tmp, h, err := e.copyLocal(q)
		if err != nil {
			continue
		}
		if h == hash {
			return tmp, h, true
		}
		os.Remove(tmp)
	}
	return "", "", false
}

func (e *Engine) download(ctx context.Context, p string) (string, string, error) {
	f, err := e.conn.Open(ctx, p)
	if err != nil {
		return "", "", cerr(err)
	}
	defer f.Close()
	tmp, h, err := e.writeTemp(f)
	var re readError
	if errors.As(err, &re) {
		return "", "", cerr(re.err)
	}
	return tmp, h, err
}

// clearDir empties the local directory p of everything that is clean,
// then removes it, or moves it aside as a conflict when something in it was
// changed locally.
func (e *Engine) clearDir(p string) error {
	if err := e.applyDel(p, 0); err != nil {
		return err
	}
	st, err := e.stat(p)
	if err != nil || !st.exists {
		return err
	}
	cp, err := e.moveAside(p)
	if err != nil {
		return err
	}
	e.st.enqueue(opSync, cp, "")
	e.hint(cp, true)
	return nil
}

// applyDel removes p and everything below it locally, except what was
// changed locally, which stays to be committed as new. seq 0 means a
// reconcile: no staleness check.
func (e *Engine) applyDel(p string, seq uint64) error {
	r := e.st.get(p)
	if r == nil {
		return nil
	}
	if seq != 0 && r.Version > seq {
		return nil
	}
	paths := e.st.subtree(p)
	for i := len(paths) - 1; i >= 0; i-- {
		q := paths[i]
		rq := e.st.get(q)
		if mv := e.st.pendingMv(q); mv != nil {
			e.st.dequeue(mv)
			e.st.enqueue(opSync, mv.To, "")
		}
		ls, _, _, err := e.classify(q, rq)
		if err != nil {
			return err
		}
		e.st.del(q)
		switch ls {
		case lsClean:
			if err := e.removeLocal(q); err != nil {
				if rq.Type != fsproto.TypeDir {
					return err
				}
				// Something new is inside; the directory stays and is
				// created again on the server.
				e.st.enqueue(opSync, q, "")
				e.hint(q, true)
			}
		case lsDirty:
			e.st.enqueue(opSync, q, "")
			if rq.Type == fsproto.TypeDir {
				e.hint(q, true)
			}
		}
	}
	return nil
}

// applyMv renames locally what the server renamed.
func (e *Engine) applyMv(from, to string, seq uint64) error {
	if rt := e.st.get(to); rt != nil && rt.Version >= seq {
		return nil // our own rename, or stale
	}
	rf := e.st.get(from)
	if rf == nil || rf.Version >= seq {
		if rf == nil && !e.ign.Ignored(to, true) && !e.ign.Ignored(to, false) {
			// Something we do not have moved into view; only the index
			// can say what it is.
			e.reconcile = true
		}
		return nil
	}
	dir := rf.Type == fsproto.TypeDir
	if e.ign.Ignored(to, dir) {
		return e.applyDel(from, seq)
	}
	// Make room at the destination.
	rt := e.st.get(to)
	ls, st, _, err := e.classify(to, rt)
	if err != nil {
		return err
	}
	switch ls {
	case lsDirty:
		cp, err := e.moveAside(to)
		if err != nil {
			return err
		}
		e.st.enqueue(opSync, cp, "")
		e.hint(cp, true)
	case lsClean:
		if st.typ == fsproto.TypeDir {
			if err := e.clearDir(to); err != nil {
				return err
			}
		} else if err := e.removeLocal(to); err != nil {
			return err
		}
	}
	sf, err := e.stat(from)
	if err != nil {
		return err
	}
	if sf.exists {
		if err := e.mkdirParents(to); err != nil {
			return err
		}
		if err := os.Rename(e.osPath(from), e.osPath(to)); err != nil {
			return err
		}
	}
	// A pending local rename of from now starts at to (rebase moves it).
	e.st.rebase(from, to, seq)
	return nil
}

func (e *Engine) applyFlags(p string, flags fsproto.Flags, seq uint64) {
	r := e.st.get(p)
	if r == nil || r.Version >= seq {
		return
	}
	nr := *r
	nr.Flags = flags
	nr.Version = seq
	e.st.put(&nr)
}

// reconcileIndex reads the index and makes the rows and the folder match
// it, as for a reset or the first run. Paths changed locally are handled as
// conflicts, as for events.
func (e *Engine) reconcileIndex(ctx context.Context) error {
	e.reconcile = false
	ir, err := e.conn.Index(ctx, []string{"zstd"})
	if err != nil {
		return cerr(err)
	}
	var ents []fsproto.Entry
	for {
		ent, err := ir.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			ir.Close()
			return connError{err}
		}
		ents = append(ents, ent)
	}
	ir.Close()
	seq := ir.Header().Seq

	// Fetch what is new in parallel, p files first.
	var need []fsproto.Entry
	pdirs := map[string]bool{}
	prio := map[string]bool{}
	for _, ent := range ents {
		pr := ent.Flags.Has(fsproto.FlagPrefetch) || pdirs[fsproto.Parent(ent.Path)]
		if ent.Type == fsproto.TypeDir && pr {
			pdirs[ent.Path] = true
		}
		if ent.Type != fsproto.TypeFile || e.ign.Ignored(ent.Path, false) {
			continue
		}
		if r := e.st.get(ent.Path); r != nil && r.Version == ent.Version && r.Hash == ent.Hash {
			continue
		}
		prio[ent.Path] = pr
		need = append(need, ent)
	}
	sort.SliceStable(need, func(i, j int) bool { return prio[need[i].Path] && !prio[need[j].Path] })
	e.prefetchAll(ctx, need)
	defer e.dropPrefetch()

	seen := make(map[string]bool, len(ents))
	for _, ent := range ents {
		seen[ent.Path] = true
		if ent.Path == "/" {
			continue
		}
		if r := e.st.get(ent.Path); r != nil && r.Version == ent.Version && r.Type == ent.Type && r.Hash == ent.Hash {
			if r.Flags != ent.Flags {
				nr := *r
				nr.Flags = ent.Flags
				e.st.put(&nr)
			}
			continue
		}
		if err := e.applyPut(ctx, ent, true); err != nil {
			if isConnErr(err) {
				return err
			}
			e.logf("reconcile %s: %v", ent.Path, err)
		}
	}
	var gone []string
	for p := range e.st.rows {
		if !seen[p] {
			gone = append(gone, p)
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(gone)))
	for _, p := range gone {
		if err := e.applyDel(p, 0); err != nil {
			e.logf("reconcile %s: %v", p, err)
		}
	}
	e.st.setSeq(seq)
	e.st.setMeta("bootstrapped", "1")
	return e.st.flush()
}

func (e *Engine) prefetchAll(ctx context.Context, ents []fsproto.Entry) {
	e.prefetch = make(map[string]fetched, len(ents))
	if len(ents) == 0 {
		return
	}
	// Local copies first, on this goroutine, since they read the rows.
	var remote []fsproto.Entry
	for _, ent := range ents {
		if tmp, h, ok := e.copyByHash(ent.Hash); ok {
			e.prefetch[ent.Path] = fetched{version: ent.Version, tmp: tmp, hash: h}
			continue
		}
		remote = append(remote, ent)
	}
	work := make(chan fsproto.Entry)
	var mu gosync.Mutex
	var wg gosync.WaitGroup
	for range min(e.cfg.Fetchers, len(remote)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ent := range work {
				tmp, h, err := e.download(ctx, ent.Path)
				mu.Lock()
				e.prefetch[ent.Path] = fetched{version: ent.Version, tmp: tmp, hash: h, err: err}
				mu.Unlock()
			}
		}()
	}
	for _, ent := range remote {
		work <- ent
	}
	close(work)
	wg.Wait()
}

func (e *Engine) dropPrefetch() {
	for _, f := range e.prefetch {
		if f.tmp != "" {
			os.Remove(f.tmp)
		}
	}
	e.prefetch = nil
}
