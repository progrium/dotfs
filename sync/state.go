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
}

// state is the engine's local state: rows and the queue in memory, written
// through to <dir>/.dotfs/state.db by flush.
type state struct {
	db *sql.DB

	rows   map[string]*row
	kids   map[string]map[string]struct{} // parent path -> child paths with rows
	hashes map[string]map[string]struct{} // content hash -> file paths with rows
	queue  []*qent
	seq    uint64
	meta   map[string]string

	dirtyRows  map[string]struct{}
	dirtyQueue bool
	dirtyMeta  bool
	nextID     int64
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
		hashes:    map[string]map[string]struct{}{},
		meta:      map[string]string{},
		dirtyRows: map[string]struct{}{},
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
		s.queue = append(s.queue, q)
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
		h := s.hashes[r.Hash]
		if h == nil {
			h = map[string]struct{}{}
			s.hashes[r.Hash] = h
		}
		h[p] = struct{}{}
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

// del removes the row at p only (not its descendants).
func (s *state) unhash(r *row) {
	if h := s.hashes[r.Hash]; h != nil {
		delete(h, r.Path)
		if len(h) == 0 {
			delete(s.hashes, r.Hash)
		}
	}
}

// withHash returns the paths of file rows with content hash h.
func (s *state) withHash(h string) []string {
	var out []string
	for p := range s.hashes[h] {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

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
	for _, q := range s.queue {
		if fsproto.Within(q.Path, from) {
			q.Path = fsproto.Rebase(q.Path, from, to)
			s.dirtyQueue = true
		}
		if q.To != "" && fsproto.Within(q.To, from) {
			q.To = fsproto.Rebase(q.To, from, to)
			s.dirtyQueue = true
		}
	}
}

// enqueue adds a queue entry unless an equal one is pending, and returns
// the pending entry.
func (s *state) enqueue(op, p, to string) *qent {
	for _, q := range s.queue {
		if q.Op == op && q.Path == p && q.To == to {
			return q
		}
	}
	s.nextID++
	q := &qent{ID: s.nextID, Op: op, Path: p, To: to}
	s.queue = append(s.queue, q)
	s.dirtyQueue = true
	return q
}

func (s *state) dequeue(q *qent) {
	for i, x := range s.queue {
		if x == q {
			s.queue = append(s.queue[:i], s.queue[i+1:]...)
			s.dirtyQueue = true
			return
		}
	}
}

// pendingMv returns the queued local rename of p, if any.
func (s *state) pendingMv(p string) *qent {
	for _, q := range s.queue {
		if q.Op == opMv && q.Path == p {
			return q
		}
	}
	return nil
}

func (s *state) touchQueue() { s.dirtyQueue = true }

// flush writes every change since the last flush in one transaction.
func (s *state) flush() error {
	if len(s.dirtyRows) == 0 && !s.dirtyQueue && !s.dirtyMeta {
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
	if s.dirtyQueue {
		if _, err := tx.Exec(`DELETE FROM queue`); err != nil {
			return err
		}
		for _, q := range s.queue {
			if _, err := tx.Exec(`INSERT INTO queue (id, op, path, to_path, stage, shash, tries) VALUES (?, ?, ?, ?, ?, ?, ?)`,
				q.ID, q.Op, q.Path, q.To, q.Stage, q.SHash, q.Tries); err != nil {
				return err
			}
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
	s.dirtyQueue = false
	s.dirtyMeta = false
	return nil
}

var errOtherRoot = errors.New("this folder is already synced with a different root")
