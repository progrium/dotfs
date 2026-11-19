package sync

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/progrium/dotfs/fsproto"

	_ "modernc.org/sqlite"
)

// row is what the engine knows about one synced path: the server's entry
// for it as of the last change the engine saw or made (the base for local
// edits), plus the local file's stat when it last matched that entry.
type row struct {
	fsproto.Entry
	LSize  int64  // local size when last in sync
	LMtime int64  // local mtime in nanoseconds when last in sync
	LIno   uint64 // local inode when last in sync, 0 if unknown
}

// Queue operations.
const (
	opSync = "sync" // bring the server in line with the local path
	opMv   = "mv"   // a local rename, recognized by inode or hash
)

// qent is one entry of the persistent commit queue. The base version a
// change was made against is the path's row, which only moves when the
// engine applies a remote change it has reconciled with the local one.
type qent struct {
	ID    int64
	Op    string
	Path  string
	To    string // opMv only
	Stage string // stage name of an upload in progress
	SHash string // content hash the stage file holds
	Tries int

	dead bool // dequeued; dropped from state.queue at the next compaction
}

// qkey identifies equal queue entries.
type qkey struct{ op, path, to string }

// state is the engine's local state: rows and the queue in memory, written
// through to <dir>/.dotfs/state.db by flush.
type state struct {
	db *sql.DB

	rows   map[string]*row
	kids   map[string]map[string]struct{} // parent path -> child paths with rows
	hashes hashIndex                      // content hash -> file paths with rows
	queue  []*qent                        // in ID order; may hold dead entries, so read it through pending
	seq    uint64
	meta   map[string]string

	dirtyRows map[string]struct{}
	// The queue's indexes, so enqueue, dequeue, and lookups do not scan it.
	qkeys map[qkey]*qent
	mvs   map[string]map[*qent]struct{} // opMv entries by source path
	ndead int
	qdups int // entries whose key another live entry holds in qkeys

	dirtyQ    map[*qent]struct{} // entries to write
	deletedQ  []int64            // entry IDs to delete
	dirtyMeta bool
	nextID    int64
}

const schema = `
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS files (
	path TEXT PRIMARY KEY,
	line TEXT NOT NULL,
	lsize INTEGER NOT NULL,
	lmtime INTEGER NOT NULL,
	lino INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS queue (
	id INTEGER PRIMARY KEY,
	op TEXT NOT NULL,
	path TEXT NOT NULL,
	to_path TEXT NOT NULL,
	stage TEXT NOT NULL,
	shash TEXT NOT NULL,
	tries INTEGER NOT NULL
);
`

func openState(dir string) (*state, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "state.db")+"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("state.db: %w", err)
	}
	s := &state{
		db:        db,
		rows:      map[string]*row{},
		kids:      map[string]map[string]struct{}{},
		hashes:    newHashIndex(),
		meta:      map[string]string{},
		dirtyRows: map[string]struct{}{},
		qkeys:     map[qkey]*qent{},
		mvs:       map[string]map[*qent]struct{}{},
		dirtyQ:    map[*qent]struct{}{},
	}
	if err := s.load(); err != nil {
		db.Close()
		return nil, fmt.Errorf("state.db: %w", err)
	}
	return s, nil
}

func (s *state) load() error {
	rs, err := s.db.Query(`SELECT key, value FROM meta`)
	if err != nil {
		return err
	}
	for rs.Next() {
		var k, v string
		if err := rs.Scan(&k, &v); err != nil {
			rs.Close()
			return err
		}
		s.meta[k] = v
	}
	rs.Close()
	if v := s.meta["seq"]; v != "" {
		s.seq, _ = strconv.ParseUint(v, 10, 64)
	}

	rs, err = s.db.Query(`SELECT line, lsize, lmtime, lino FROM files`)
	if err != nil {
		return err
	}
	for rs.Next() {
		var line string
		var r row
		var ino int64
		if err := rs.Scan(&line, &r.LSize, &r.LMtime, &ino); err != nil {
			rs.Close()
			return err
		}
		e, err := fsproto.ParseEntry(line)
		if err != nil {
			rs.Close()
			return err
		}
		r.Entry = e
		r.LIno = uint64(ino)
		s.put(&r)
	}
	rs.Close()
	clear(s.dirtyRows)

	rs, err = s.db.Query(`SELECT id, op, path, to_path, stage, shash, tries FROM queue ORDER BY id`)
	if err != nil {
		return err
	}
	for rs.Next() {
		q := &qent{}
		if err := rs.Scan(&q.ID, &q.Op, &q.Path, &q.To, &q.Stage, &q.SHash, &q.Tries); err != nil {
			rs.Close()
			return err
		}
		s.add(q)
		s.nextID = max(s.nextID, q.ID)
	}
	rs.Close()
	return rs.Err()
}

func (s *state) close() error { return s.db.Close() }

func (s *state) setMeta(k, v string) {
	if s.meta[k] != v {
		s.meta[k] = v
		s.dirtyMeta = true
	}
}

func (s *state) setSeq(n uint64) {
	if n != s.seq {
		s.seq = n
		s.setMeta("seq", strconv.FormatUint(n, 10))
	}
}

func (s *state) get(p string) *row { return s.rows[p] }

// put stores r under r.Path, replacing any row there.
func (s *state) put(r *row) {
	p := r.Path
	if old := s.rows[p]; old != nil {
		s.unhash(old)
	}
	s.rows[p] = r
	if r.Type == fsproto.TypeFile && r.Hash != "" {
		s.hashes.add(r.Hash, p)
	}
	s.dirtyRows[p] = struct{}{}
	if p == "/" {
		return
	}
	par := fsproto.Parent(p)
	k := s.kids[par]
	if k == nil {
		k = map[string]struct{}{}
		s.kids[par] = k
	}
	k[p] = struct{}{}
}

func (s *state) unhash(r *row) {
	if r.Type == fsproto.TypeFile && r.Hash != "" {
		s.hashes.remove(r.Hash, r.Path)
	}
}

// withHash returns the paths of file rows with content hash h, sorted.
func (s *state) withHash(h string) []string {
	var out []string
	for _, p := range s.hashes.paths(h) {
		// Keys other than b3 digests are themselves hashed, so check.
		if r := s.rows[p]; r != nil && r.Hash == h {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

// del removes the row at p only (not its descendants).
func (s *state) del(p string) {
	r, ok := s.rows[p]
	if !ok {
		return
	}
	s.unhash(r)
	delete(s.rows, p)
	s.dirtyRows[p] = struct{}{}
	if k := s.kids[fsproto.Parent(p)]; k != nil {
		delete(k, p)
		if len(k) == 0 {
			delete(s.kids, fsproto.Parent(p))
		}
	}
}

// children returns the paths of p's direct children with rows, sorted.
func (s *state) children(p string) []string {
	var out []string
	for c := range s.kids[p] {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// subtree returns p (if it has a row) and every path with a row below it,
// parents before children.
func (s *state) subtree(p string) []string {
	var out []string
	if _, ok := s.rows[p]; ok {
		out = append(out, p)
	}
	var walk func(string)
	walk = func(d string) {
		for _, c := range s.children(d) {
			out = append(out, c)
			walk(c)
		}
	}
	walk(p)
	return out
}

// rebase moves the rows at and below from to the same places below to.
// The moved path itself gets version v; paths below keep theirs.
func (s *state) rebase(from, to string, v uint64) {
	paths := s.subtree(from)
	moved := make([]*row, 0, len(paths))
	for _, p := range paths {
		r := s.rows[p]
		s.del(p)
		nr := *r
		nr.Path = fsproto.Rebase(p, from, to)
		if p == from {
			nr.Version = v
		}
		moved = append(moved, &nr)
	}
	// Anything already at the destination is replaced.
	for _, p := range s.subtree(to) {
		s.del(p)
	}
	for _, r := range moved {
		s.put(r)
	}
	for _, q := range s.pending() {
		np, nto := q.Path, q.To
		if fsproto.Within(q.Path, from) {
			np = fsproto.Rebase(q.Path, from, to)
		}
		if q.To != "" && fsproto.Within(q.To, from) {
			nto = fsproto.Rebase(q.To, from, to)
		}
		if np != q.Path || nto != q.To {
			s.unindex(q)
			q.Path, q.To = np, nto
			s.index(q)
			s.touchQueue(q)
		}
	}
}

// enqueue adds a queue entry unless an equal one is pending, and returns
// the pending entry.
func (s *state) enqueue(op, p, to string) *qent {
	if q := s.qkeys[qkey{op, p, to}]; q != nil {
		return q
	}
	s.nextID++
	q := &qent{ID: s.nextID, Op: op, Path: p, To: to}
	s.add(q)
	s.touchQueue(q)
	return q
}

// add appends q, which has the highest ID so far, and indexes it.
func (s *state) add(q *qent) {
	s.queue = append(s.queue, q)
	s.index(q)
}

func (s *state) index(q *qent) {
	k := qkey{q.Op, q.Path, q.To}
	if s.qkeys[k] == nil {
		s.qkeys[k] = q
	} else if s.qkeys[k] != q {
		s.qdups++
	}
	if q.Op == opMv {
		m := s.mvs[q.Path]
		if m == nil {
			m = map[*qent]struct{}{}
			s.mvs[q.Path] = m
		}
		m[q] = struct{}{}
	}
}

func (s *state) unindex(q *qent) {
	k := qkey{q.Op, q.Path, q.To}
	if s.qkeys[k] == q {
		delete(s.qkeys, k)
		if s.qdups == 0 {
			s.unindexMv(q)
			return
		}
		// Another live entry with the same key (possible after a rebase)
		// takes its place.
		for _, x := range s.queue {
			if x != q && !x.dead && x.Op == q.Op && x.Path == q.Path && x.To == q.To {
				s.qkeys[k] = x
				s.qdups--
				break
			}
		}
	} else if s.qkeys[k] != nil {
		s.qdups--
	}
	s.unindexMv(q)
}

func (s *state) unindexMv(q *qent) {
	if q.Op == opMv {
		if m := s.mvs[q.Path]; m != nil {
			delete(m, q)
			if len(m) == 0 {
				delete(s.mvs, q.Path)
			}
		}
	}
}

func (s *state) dequeue(q *qent) {
	if q.dead {
		return
	}
	s.unindex(q)
	q.dead = true
	s.ndead++
	delete(s.dirtyQ, q)
	s.deletedQ = append(s.deletedQ, q.ID)
}

// queued reports whether q is still pending. Entries are only ever made
// by add, so one that is not dead is in the queue.
func (s *state) queued(q *qent) bool { return !q.dead }

// pending returns the live queue entries in ID order. The slice is the
// state's own; dequeueing while ranging over it is safe, since entries are
// only marked dead until the next call.
func (s *state) pending() []*qent {
	if s.ndead > 0 {
		live := make([]*qent, 0, len(s.queue)-s.ndead)
		for _, q := range s.queue {
			if !q.dead {
				live = append(live, q)
			}
		}
		s.queue, s.ndead = live, 0
	}
	return s.queue
}

// queueLen is the number of live queue entries.
func (s *state) queueLen() int { return len(s.queue) - s.ndead }

// pendingMv returns the queued local rename of p, if any, the oldest first.
func (s *state) pendingMv(p string) *qent {
	var best *qent
	for q := range s.mvs[p] {
		if best == nil || q.ID < best.ID {
			best = q
		}
	}
	return best
}

// touchQueue marks q to be written at the next flush.
func (s *state) touchQueue(q *qent) {
	if !q.dead {
		s.dirtyQ[q] = struct{}{}
	}
}

// flush writes every change since the last flush in one transaction.
func (s *state) flush() error {
	if len(s.dirtyRows) == 0 && len(s.dirtyQ) == 0 && len(s.deletedQ) == 0 && !s.dirtyMeta {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if len(s.dirtyRows) > 0 {
		up, err := tx.Prepare(`INSERT OR REPLACE INTO files (path, line, lsize, lmtime, lino) VALUES (?, ?, ?, ?, ?)`)
		if err != nil {
			return err
		}
		dl, err := tx.Prepare(`DELETE FROM files WHERE path = ?`)
		if err != nil {
			return err
		}
		for p := range s.dirtyRows {
			if r := s.rows[p]; r != nil {
				_, err = up.Exec(p, r.Entry.Encode(), r.LSize, r.LMtime, int64(r.LIno))
			} else {
				_, err = dl.Exec(p)
			}
			if err != nil {
				return err
			}
		}
	}
	for _, id := range s.deletedQ {
		if _, err := tx.Exec(`DELETE FROM queue WHERE id = ?`, id); err != nil {
			return err
		}
	}
	for q := range s.dirtyQ {
		if _, err := tx.Exec(`INSERT OR REPLACE INTO queue (id, op, path, to_path, stage, shash, tries) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			q.ID, q.Op, q.Path, q.To, q.Stage, q.SHash, q.Tries); err != nil {
			return err
		}
	}
	if s.dirtyMeta {
		for k, v := range s.meta {
			if _, err := tx.Exec(`INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)`, k, v); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	clear(s.dirtyRows)
	clear(s.dirtyQ)
	s.deletedQ = s.deletedQ[:0]
	s.dirtyMeta = false
	return nil
}

var errOtherRoot = errors.New("this folder is already synced with a different root")
