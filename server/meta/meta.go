// Package meta is the server's metadata store, in SQLite: scopes, the path
// table, the per-scope change log, index snapshots, and tokens. It
// implements server.Meta (with server.Writer, server.Log, and
// server.IndexSource) and server.Tokens, and writes content through a
// server/store.Store.
//
// Each scope's changes are ordered by its own lock, so seqs are assigned
// without gaps or duplicates, and different scopes change in parallel. A
// change that touches the filesystem is crash-safe: it records its plan (an
// intent) in one transaction, makes the filesystem change, then applies the
// plan to the path table and log and drops the intent in a second
// transaction. Reads see a change only once its content is in place, and
// Open finishes any intent a crash left behind before serving.
//
// Nested scopes: setting flag i on a directory makes it a scope of its own.
// Its children move to the new scope with version 0, and the parent's index
// lists only the directory; the parent's writer rejects changes below it,
// and a move or removal of a directory that holds one. Clearing flag i
// merges the children back into the parent, with the version of that
// change.
package meta

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/store"

	_ "modernc.org/sqlite"
)

// Store is what Meta needs from the content store: server.Store plus the
// two-phase publish of server/store.
type Store interface {
	server.Store
	PrepareStage(user, name, hash string, mode uint32, mtime int64) (store.Prepared, error)
	PrepareHash(user, hash string, mode uint32, mtime int64) (store.Prepared, error)
	PrepareCopy(r server.Root, from string) (store.Prepared, error)
	PrepareSymlink(target string) (store.Prepared, error)
	VerifyStage(user, name string) error
	Place(p store.Prepared, r server.Root, path string) error
	PlaceNoSync(p store.Prepared, r server.Root, path string) error
	SyncDir(r server.Root, dir string) error
	Discard(p store.Prepared) error
	CleanTmp() error
}

var _ Store = (*store.Store)(nil)

// DefaultKeepLog is how many changes per scope the log keeps by default.
const DefaultKeepLog = 100_000

// Options configures a Meta.
type Options struct {
	// NoSync runs SQLite with synchronous=OFF, for tests and benchmarks.
	// A power loss can then lose recent changes.
	NoSync bool
	// KeepLog is how many recent changes each scope's log keeps; older ones
	// are trimmed as new ones arrive. 0 means DefaultKeepLog; use
	// Trim directly for other policies.
	KeepLog uint64
	// Now returns the time for new directories and symlinks.
	Now func() time.Time
}

// Meta is a server.Meta and server.Tokens backed by SQLite.
type Meta struct {
	st   Store
	wdb  *sql.DB // one connection: every write transaction
	rdb  *sql.DB // read-only pool
	opts Options

	pubMu sync.RWMutex
	pub   server.Publisher

	mu     sync.RWMutex
	scopes map[server.Scope]*scope
}

var (
	_ server.Meta   = (*Meta)(nil)
	_ server.Tokens = (*Meta)(nil)
)

// scope is the in-memory state of one scope.
type scope struct {
	id  int64
	key server.Scope

	// mu orders the scope's changes: it is the scope's writer.
	mu       sync.Mutex
	qmu      sync.Mutex // guards queue
	queue    []*request // changes waiting for mu, oldest first
	seq      atomic.Uint64
	logStart atomic.Uint64
	gone     bool            // merged back into its parent; guarded by mu
	broken   error           // a change failed after its filesystem step; guarded by mu
	nested   map[string]bool // scope-relative dirs that are nested scopes; guarded by mu
	w        *writer

	idxMu sync.Mutex
	idx   map[string]cachedIndex // by encoding
}

const schema = `
CREATE TABLE IF NOT EXISTS scope (
	id INTEGER PRIMARY KEY,
	root TEXT NOT NULL,
	dir BLOB NOT NULL,
	seq INTEGER NOT NULL,
	log_start INTEGER NOT NULL,
	UNIQUE (root, dir)
);
CREATE TABLE IF NOT EXISTS path (
	scope INTEGER NOT NULL,
	path BLOB NOT NULL,
	parent BLOB NOT NULL,
	type TEXT NOT NULL,
	mode INTEGER NOT NULL,
	length INTEGER NOT NULL,
	mtime INTEGER NOT NULL,
	version INTEGER NOT NULL,
	hash TEXT NOT NULL,
	flags TEXT NOT NULL,
	target BLOB NOT NULL,
	PRIMARY KEY (scope, path)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS path_parent ON path (scope, parent, path);
CREATE TABLE IF NOT EXISTS log (
	scope INTEGER NOT NULL,
	seq INTEGER NOT NULL,
	line BLOB NOT NULL,
	PRIMARY KEY (scope, seq)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS intent (
	scope INTEGER PRIMARY KEY,
	plan BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS index_cache (
	scope INTEGER NOT NULL,
	encoding TEXT NOT NULL,
	seq INTEGER NOT NULL,
	bytes BLOB NOT NULL,
	PRIMARY KEY (scope, encoding)
);
` + tokenSchema

func dsn(path string, sync string, readOnly bool) string {
	q := "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(" + sync + ")"
	if readOnly {
		q += "&_pragma=query_only(1)"
	}
	return "file:" + path + q
}

// Open opens (creating if needed) the metadata database at path, writing
// content through st and publishing each change to pub, which may be nil
// and set later with SetPublisher. It finishes any change a crash
// interrupted before returning. opts may be nil.
func Open(path string, st Store, pub server.Publisher, opts *Options) (*Meta, error) {
	m := &Meta{st: st, pub: pub, scopes: map[server.Scope]*scope{}}
	if opts != nil {
		m.opts = *opts
	}
	if m.opts.KeepLog == 0 {
		m.opts.KeepLog = DefaultKeepLog
	}
	if m.opts.Now == nil {
		m.opts.Now = time.Now
	}
	sync := "FULL"
	if m.opts.NoSync {
		sync = "OFF"
	}
	var err error
	if m.wdb, err = sql.Open("sqlite", dsn(path, sync, false)); err != nil {
		return nil, err
	}
	m.wdb.SetMaxOpenConns(1)
	if _, err := m.wdb.Exec(schema); err != nil {
		m.wdb.Close()
		return nil, fmt.Errorf("meta: schema: %w", err)
	}
	if m.rdb, err = sql.Open("sqlite", dsn(path, sync, true)); err != nil {
		m.wdb.Close()
		return nil, err
	}
	m.rdb.SetMaxOpenConns(max(4, runtime.GOMAXPROCS(0)))
	if err := m.load(); err == nil {
		err = m.recover()
	}
	if err == nil {
		err = m.load()
	}
	if err == nil {
		err = st.CleanTmp()
	}
	if err != nil {
		m.Close()
		return nil, err
	}
	return m, nil
}

// SetPublisher sets where changes are published.
func (m *Meta) SetPublisher(p server.Publisher) {
	m.pubMu.Lock()
	m.pub = p
	m.pubMu.Unlock()
}

func (m *Meta) publish(s server.Scope, ev fsproto.Event) {
	m.pubMu.RLock()
	p := m.pub
	m.pubMu.RUnlock()
	if p != nil {
		p.Publish(s, ev)
	}
}

// Close closes the database.
func (m *Meta) Close() error {
	var errs []error
	if m.rdb != nil {
		errs = append(errs, m.rdb.Close())
	}
	if m.wdb != nil {
		errs = append(errs, m.wdb.Close())
	}
	return errors.Join(errs...)
}

// load reads every scope into memory, replacing what was there.
func (m *Meta) load() error {
	rows, err := m.wdb.Query(`SELECT id, root, dir, seq, log_start FROM scope`)
	if err != nil {
		return err
	}
	defer rows.Close()
	scopes := map[server.Scope]*scope{}
	for rows.Next() {
		var (
			id         int64
			root       string
			dir        []byte
			seq, start uint64
		)
		if err := rows.Scan(&id, &root, &dir, &seq, &start); err != nil {
			return err
		}
		sc := m.newScope(id, server.Scope{Root: server.Root(root), Dir: string(dir)})
		sc.seq.Store(seq)
		sc.logStart.Store(start)
		scopes[sc.key] = sc
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, sc := range scopes {
		if p := parentOf(scopes, sc.key); p != nil {
			p.nested[fsproto.Rebase(sc.key.Dir, p.key.Dir, "/")] = true
		}
	}
	m.mu.Lock()
	m.scopes = scopes
	m.mu.Unlock()
	return nil
}

func newScope(id int64, key server.Scope) *scope {
	sc := &scope{id: id, key: key, nested: map[string]bool{}, idx: map[string]cachedIndex{}}
	return sc
}

// parentOf returns the innermost scope that strictly contains s.
func parentOf(scopes map[server.Scope]*scope, s server.Scope) *scope {
	var best *scope
	for k, sc := range scopes {
		if k.Root != s.Root || k.Dir == s.Dir || !fsproto.Within(s.Dir, k.Dir) {
			continue
		}
		if best == nil || len(k.Dir) > len(best.key.Dir) {
			best = sc
		}
	}
	return best
}

func normScope(s server.Scope) server.Scope {
	if s.Dir == "" {
		s.Dir = "/"
	}
	return s
}

func (m *Meta) scope(s server.Scope) (*scope, error) {
	m.mu.RLock()
	sc := m.scopes[normScope(s)]
	m.mu.RUnlock()
	if sc == nil {
		return nil, server.ErrNoScope
	}
	return sc, nil
}

// CreateScope creates a root scope, or makes a directory of an existing
// scope a nested scope, as setting flag i on it does.
func (m *Meta) CreateScope(ctx context.Context, s server.Scope) error {
	s = normScope(s)
	if _, err := m.scope(s); err == nil {
		return server.ErrScopeExists
	}
	if s.Dir != "/" {
		m.mu.RLock()
		parent := parentOf(m.scopes, s)
		m.mu.RUnlock()
		if parent == nil {
			return server.ErrNoScope
		}
		dir := fsproto.Rebase(s.Dir, parent.key.Dir, "/")
		e, err := m.Stat(ctx, parent.key, dir)
		if err != nil {
			return err
		}
		_, err = parent.w.SetFlags(ctx, server.Origin{}, fsproto.FlagsCmd{Path: dir, Flags: e.Flags.With(fsproto.FlagIndex)})
		if err == nil {
			if _, e2 := m.scope(s); e2 != nil {
				return server.ErrScopeExists // it was a nested scope already
			}
		}
		return err
	}
	if err := fsproto.CheckPath(s.Dir); err != nil {
		return fsproto.Invalid("bad scope")
	}
	if err := m.st.CreateRoot(s.Root); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.scopes[s] != nil {
		return server.ErrScopeExists
	}
	var id int64
	err := m.tx(func(tx *sql.Tx) error {
		if err := tx.QueryRow(`INSERT INTO scope (root, dir, seq, log_start) VALUES (?, ?, 0, 0) RETURNING id`,
			string(s.Root), []byte(s.Dir)).Scan(&id); err != nil {
			return err
		}
		return putRow(tx, id, fsproto.Entry{Path: "/", Type: fsproto.TypeDir, Mode: 0o755, Mtime: m.opts.Now().Unix()})
	})
	if err != nil {
		return err
	}
	m.scopes[s] = m.newScope(id, s)
	return nil
}

func (m *Meta) Writer(s server.Scope) (server.Writer, error) {
	sc, err := m.scope(s)
	if err != nil {
		return nil, err
	}
	return sc.w, nil
}

// tx runs fn in a write transaction.
func (m *Meta) tx(fn func(tx *sql.Tx) error) error {
	tx, err := m.wdb.Begin()
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Path table rows.

const entryCols = `path, type, mode, length, mtime, version, hash, flags, target`

type scanner interface{ Scan(dest ...any) error }

func scanEntry(r scanner) (fsproto.Entry, error) {
	var (
		e            fsproto.Entry
		path, target []byte
		typ, flags   string
	)
	if err := r.Scan(&path, &typ, &e.Mode, &e.Length, &e.Mtime, &e.Version, &e.Hash, &flags, &target); err != nil {
		return e, err
	}
	e.Path, e.Flags, e.Target = string(path), fsproto.Flags(flags), string(target)
	if len(typ) == 1 {
		e.Type = fsproto.Type(typ[0])
	}
	return e, nil
}

func parentKey(p string) []byte {
	if p == "/" {
		return []byte{}
	}
	return []byte(fsproto.Parent(p))
}

func putRow(tx *sql.Tx, id int64, e fsproto.Entry) error {
	_, err := tx.Exec(`INSERT OR REPLACE INTO path (scope, `+entryCols+`, parent) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, []byte(e.Path), string(e.Type), e.Mode, e.Length, e.Mtime, e.Version, e.Hash, string(e.Flags), []byte(e.Target), parentKey(e.Path))
	return err
}

// subtree returns a condition and its arguments matching the paths
// strictly below dir.
func subtree(dir string) (string, []any) {
	if dir == "/" {
		return `path > ?`, []any{[]byte("/")}
	}
	return `path >= ? AND path < ?`, []any{[]byte(dir + "/"), []byte(dir + "0")}
}

// queryer is a *sql.DB or *sql.Tx.
type queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func getEntry(ctx context.Context, q queryer, id int64, p string) (fsproto.Entry, bool, error) {
	e, err := scanEntry(q.QueryRowContext(ctx, `SELECT `+entryCols+` FROM path WHERE scope = ? AND path = ?`, id, []byte(p)))
	if errors.Is(err, sql.ErrNoRows) {
		return e, false, nil
	}
	return e, err == nil, err
}

func hasBelow(ctx context.Context, q queryer, id int64, dir string) (bool, error) {
	cond, args := subtree(dir)
	var n int
	err := q.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT 1 FROM path WHERE scope = ? AND `+cond+` LIMIT 1)`,
		append([]any{id}, args...)...).Scan(&n)
	return n > 0, err
}

// Reads.

func (m *Meta) Stat(ctx context.Context, s server.Scope, path string) (fsproto.Entry, error) {
	sc, err := m.scope(s)
	if err != nil {
		return fsproto.Entry{}, err
	}
	e, ok, err := getEntry(ctx, m.rdb, sc.id, path)
	if err != nil {
		return e, err
	}
	if !ok {
		return e, fsproto.Missing("")
	}
	return e, nil
}

func (m *Meta) ReadDir(ctx context.Context, s server.Scope, dir string) ([]fsproto.Entry, error) {
	sc, err := m.scope(s)
	if err != nil {
		return nil, err
	}
	tx, err := m.rdb.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	d, ok, err := getEntry(ctx, tx, sc.id, dir)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fsproto.Missing("")
	}
	if d.Type != fsproto.TypeDir {
		return nil, fsproto.Invalid("not a directory")
	}
	rows, err := tx.QueryContext(ctx, `SELECT `+entryCols+` FROM path WHERE scope = ? AND parent = ? ORDER BY path`, sc.id, []byte(dir))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []fsproto.Entry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (m *Meta) Seq(ctx context.Context, s server.Scope) (uint64, error) {
	sc, err := m.scope(s)
	if err != nil {
		return 0, err
	}
	return sc.seq.Load(), nil
}

func (m *Meta) ReadLog(ctx context.Context, s server.Scope, since uint64, fn func(fsproto.Event) error) error {
	sc, err := m.scope(s)
	if err != nil {
		return err
	}
	tx, err := m.rdb.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var seq, start uint64
	if err := tx.QueryRowContext(ctx, `SELECT seq, log_start FROM scope WHERE id = ?`, sc.id).Scan(&seq, &start); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return server.ErrNoScope
		}
		return err
	}
	if since < start || since > seq {
		return server.ErrTrimmed
	}
	rows, err := tx.QueryContext(ctx, `SELECT line FROM log WHERE scope = ? AND seq > ? AND seq <= ? ORDER BY seq`, sc.id, since, seq)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var line []byte
		if err := rows.Scan(&line); err != nil {
			return err
		}
		ev, err := fsproto.ParseEvent(string(line))
		if err != nil {
			return fmt.Errorf("meta: bad log record %q: %w", line, err)
		}
		if err := fn(ev); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (m *Meta) Trim(ctx context.Context, s server.Scope, seq uint64) error {
	sc, err := m.scope(s)
	if err != nil {
		return err
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if cur := sc.seq.Load(); seq > cur {
		seq = cur
	}
	if seq <= sc.logStart.Load() {
		return nil
	}
	if err := m.tx(func(tx *sql.Tx) error { return trimLog(tx, sc.id, seq) }); err != nil {
		return err
	}
	sc.logStart.Store(seq)
	return nil
}

func trimLog(tx *sql.Tx, id int64, seq uint64) error {
	if _, err := tx.Exec(`DELETE FROM log WHERE scope = ? AND seq <= ?`, id, seq); err != nil {
		return err
	}
	_, err := tx.Exec(`UPDATE scope SET log_start = ? WHERE id = ?`, seq, id)
	return err
}

// Entries calls fn for every path in the scope, sorted, from one
// consistent snapshot.
func (m *Meta) Entries(ctx context.Context, s server.Scope, fn func(fsproto.Entry) error) (uint64, error) {
	sc, err := m.scope(s)
	if err != nil {
		return 0, err
	}
	return m.entries(ctx, sc, fn)
}

func (m *Meta) entries(ctx context.Context, sc *scope, fn func(fsproto.Entry) error) (uint64, error) {
	tx, err := m.rdb.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var seq uint64
	if err := tx.QueryRowContext(ctx, `SELECT seq FROM scope WHERE id = ?`, sc.id).Scan(&seq); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, server.ErrNoScope
		}
		return 0, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT `+entryCols+` FROM path WHERE scope = ? ORDER BY path`, sc.id)
	if err != nil {
		return seq, err
	}
	defer rows.Close()
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return seq, err
		}
		if err := fn(e); err != nil {
			return seq, err
		}
	}
	return seq, rows.Err()
}

// Scopes lists every scope, sorted.
func (m *Meta) Scopes() []server.Scope {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]server.Scope, 0, len(m.scopes))
	for k := range m.scopes {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Root != out[j].Root {
			return out[i].Root < out[j].Root
		}
		return out[i].Dir < out[j].Dir
	})
	return out
}

// inNested reports whether p is strictly below one of sc's nested scopes.
// Callers hold sc.mu.
func (sc *scope) inNested(p string) bool {
	for d := range sc.nested {
		if p != d && fsproto.Within(p, d) {
			return true
		}
	}
	return false
}

// holdsNested reports whether p is, or is above, one of sc's nested
// scopes. Callers hold sc.mu.
func (sc *scope) holdsNested(p string) bool {
	for d := range sc.nested {
		if fsproto.Within(d, p) {
			return true
		}
	}
	return false
}
