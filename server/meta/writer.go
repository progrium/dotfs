package meta

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"fmt"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/store"
)

// plan is one change, recorded as an intent before its filesystem step so
// that the change can be finished after a crash. It is gob-encoded, which
// keeps path bytes exactly (JSON would not).
type plan struct {
	Seq   uint64
	Event string // the encoded log record
	FS    []fsOp
	Muts  []mutation
}

type fsKind uint8

const (
	fsPlace fsKind = iota + 1
	fsMkdir
	fsRemove
	fsRename
	fsSetAttr
)

// fsOp is a filesystem step. Every kind can be repeated: running it again
// after it took effect changes nothing.
type fsOp struct {
	Kind      fsKind
	Prep      store.Prepared
	Path, To  string // root-relative
	Mode      *uint32
	Mtime     *int64
	Recursive bool
}

type mutKind uint8

const (
	mutPut    mutKind = iota + 1 // upsert Entry
	mutDel                       // delete Path and everything below it
	mutMove                      // move Path and everything below it to To; Path gets Version
	mutNest                      // make directory Path a nested scope
	mutUnnest                    // merge nested scope Path back, children at Version
)

type mutation struct {
	Kind    mutKind
	Entry   fsproto.Entry
	Path    string
	To      string
	Version uint64
}

func encodePlan(p *plan) ([]byte, error) {
	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(p)
	return b.Bytes(), err
}

func decodePlan(b []byte) (*plan, error) {
	var p plan
	err := gob.NewDecoder(bytes.NewReader(b)).Decode(&p)
	return &p, err
}

// crashHook, when set (by tests), is called at each step of a change.
var crashHook func(step string)

func crash(step string) {
	if crashHook != nil {
		crashHook(step)
	}
}

// runFS runs one filesystem step. When redo is set, the step may already
// have happened, and outcomes that say so are not errors.
func (m *Meta) runFS(root server.Root, op fsOp, redo bool) error {
	var err error
	switch op.Kind {
	case fsPlace:
		return m.st.Place(op.Prep, root, op.Path)
	case fsMkdir:
		err = m.st.Mkdir(root, op.Path, *op.Mode, *op.Mtime)
		if redo && isStatus(err, fsproto.ErrConflict) {
			err = nil
		}
	case fsRemove:
		err = m.st.Remove(root, op.Path, op.Recursive)
		if redo && isStatus(err, fsproto.ErrMissing) {
			err = nil
		}
	case fsRename:
		err = m.st.Rename(root, op.Path, op.To)
		if redo && isStatus(err, fsproto.ErrMissing) {
			err = nil
		}
	case fsSetAttr:
		err = m.st.SetAttr(root, op.Path, op.Mode, op.Mtime)
	default:
		err = fmt.Errorf("meta: unknown filesystem step %d", op.Kind)
	}
	return err
}

func (m *Meta) discard(p *plan) {
	for _, op := range p.FS {
		if op.Kind == fsPlace {
			m.st.Discard(op.Prep)
		}
	}
}

// applied is what applying a plan changed in memory.
type applied struct {
	nested   []*scope // new nested scopes
	unnested []string // scope-relative dirs that are no longer nested
}

// apply applies p's metadata changes, logs it, sets the seq, and drops the
// intent, in tx.
func (m *Meta) apply(tx *sql.Tx, sc *scope, p *plan) (applied, error) {
	var out applied
	for _, mu := range p.Muts {
		switch mu.Kind {
		case mutPut:
			if err := putRow(tx, sc.id, mu.Entry); err != nil {
				return out, err
			}
		case mutDel:
			cond, args := subtree(mu.Path)
			if _, err := tx.Exec(`DELETE FROM path WHERE scope = ? AND (path = ? OR (`+cond+`))`,
				append([]any{sc.id, []byte(mu.Path)}, args...)...); err != nil {
				return out, err
			}
		case mutMove:
			if err := moveRows(tx, sc.id, mu.Path, mu.To, mu.Version); err != nil {
				return out, err
			}
		case mutNest:
			nsc, err := m.nest(tx, sc, mu.Path)
			if err != nil {
				return out, err
			}
			out.nested = append(out.nested, nsc)
		case mutUnnest:
			if err := m.unnest(tx, sc, mu.Path, mu.Version); err != nil {
				return out, err
			}
			out.unnested = append(out.unnested, mu.Path)
		default:
			return out, fmt.Errorf("meta: unknown mutation %d", mu.Kind)
		}
	}
	if _, err := tx.Exec(`INSERT INTO log (scope, seq, line) VALUES (?, ?, ?)`, sc.id, p.Seq, []byte(p.Event)); err != nil {
		return out, err
	}
	if _, err := tx.Exec(`UPDATE scope SET seq = ? WHERE id = ?`, p.Seq, sc.id); err != nil {
		return out, err
	}
	if _, err := tx.Exec(`DELETE FROM intent WHERE scope = ?`, sc.id); err != nil {
		return out, err
	}
	keep := m.opts.KeepLog
	if start := sc.logStart.Load(); p.Seq > start+keep+keep/4 {
		if err := trimLog(tx, sc.id, p.Seq-keep); err != nil {
			return out, err
		}
	}
	return out, nil
}

// moveRows renames from (which gets version v) and everything below it to
// to, replacing a file at to.
func moveRows(tx *sql.Tx, id int64, from, to string, v uint64) error {
	if _, err := tx.Exec(`DELETE FROM path WHERE scope = ? AND path = ?`, id, []byte(to)); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE path SET path = ?, parent = ?, version = ? WHERE scope = ? AND path = ?`,
		[]byte(to), parentKey(to), v, id, []byte(from)); err != nil {
		return err
	}
	cond, args := subtree(from)
	n := len(from) + 1
	_, err := tx.Exec(`UPDATE path SET path = CAST(?1 || substr(path, ?2) AS BLOB), parent = CAST(?1 || substr(parent, ?2) AS BLOB) WHERE scope = ?3 AND `+cond,
		append([]any{[]byte(to), n, id}, args...)...)
	return err
}

// nest makes dir (in sc) a nested scope and returns it.
func (m *Meta) nest(tx *sql.Tx, sc *scope, dir string) (*scope, error) {
	key := server.Scope{Root: sc.key.Root, Dir: sc.key.RootPath(dir)}
	var id int64
	if err := tx.QueryRow(`INSERT INTO scope (root, dir, seq, log_start) VALUES (?, ?, 0, 0) RETURNING id`,
		string(key.Root), []byte(key.Dir)).Scan(&id); err != nil {
		return nil, err
	}
	d, ok, err := getEntry(context.Background(), tx, sc.id, dir)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("meta: nested scope %q has no directory", dir)
	}
	cond, args := subtree(dir)
	n := len(dir) + 1
	if _, err := tx.Exec(`UPDATE path SET scope = ?1, version = 0,
		path = CAST(substr(path, ?2) AS BLOB),
		parent = CASE WHEN parent = ?3 THEN CAST('/' AS BLOB) ELSE CAST(substr(parent, ?2) AS BLOB) END
		WHERE scope = ?4 AND `+cond, append([]any{id, n, []byte(dir), sc.id}, args...)...); err != nil {
		return nil, err
	}
	d.Path, d.Version, d.Flags = "/", 0, ""
	if err := putRow(tx, id, d); err != nil {
		return nil, err
	}
	return m.newScope(id, key), nil
}

// unnest merges the nested scope at dir back into sc, giving its paths
// version v.
func (m *Meta) unnest(tx *sql.Tx, sc *scope, dir string, v uint64) error {
	key := server.Scope{Root: sc.key.Root, Dir: sc.key.RootPath(dir)}
	var id int64
	if err := tx.QueryRow(`SELECT id FROM scope WHERE root = ? AND dir = ?`, string(key.Root), []byte(key.Dir)).Scan(&id); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE path SET scope = ?1, version = ?2,
		path = CAST(?3 || path AS BLOB),
		parent = CASE WHEN parent = CAST('/' AS BLOB) THEN ?3 ELSE CAST(?3 || parent AS BLOB) END
		WHERE scope = ?4 AND path > CAST('/' AS BLOB)`, sc.id, v, []byte(dir), id); err != nil {
		return err
	}
	for _, q := range []string{
		`DELETE FROM path WHERE scope = ?`,
		`DELETE FROM log WHERE scope = ?`,
		`DELETE FROM index_cache WHERE scope = ?`,
		`DELETE FROM intent WHERE scope = ?`,
		`DELETE FROM scope WHERE id = ?`,
	} {
		if _, err := tx.Exec(q, id); err != nil {
			return err
		}
	}
	return nil
}

func (m *Meta) newScope(id int64, key server.Scope) *scope {
	sc := newScope(id, key)
	sc.w = &writer{m: m, sc: sc}
	return sc
}

// commitPlan makes p durable and visible. sc.mu is held.
func (m *Meta) commitPlan(sc *scope, p *plan) error {
	crash("prepared")
	if len(p.FS) > 0 {
		b, err := encodePlan(p)
		if err != nil {
			m.discard(p)
			return err
		}
		if err := m.tx(func(tx *sql.Tx) error {
			_, err := tx.Exec(`INSERT INTO intent (scope, plan) VALUES (?, ?)`, sc.id, b)
			return err
		}); err != nil {
			m.discard(p)
			return err
		}
		crash("intent")
		for i, op := range p.FS {
			if err := m.runFS(sc.key.Root, op, false); err != nil {
				// Nothing is visible yet. Steps are atomic, so if this was
				// the first one the change can simply be dropped.
				if i == 0 {
					if m.tx(func(tx *sql.Tx) error {
						_, err := tx.Exec(`DELETE FROM intent WHERE scope = ?`, sc.id)
						return err
					}) == nil {
						m.discard(p)
						return err
					}
				}
				sc.broken = err
				return err
			}
		}
		crash("fs")
	}
	var a applied
	err := m.tx(func(tx *sql.Tx) (err error) {
		a, err = m.apply(tx, sc, p)
		return err
	})
	if err != nil {
		if len(p.FS) > 0 {
			sc.broken = err
		}
		return err
	}
	m.applied(sc, p, a)
	crash("applied")
	return nil
}

// applied updates memory after p's transaction committed.
func (m *Meta) applied(sc *scope, p *plan, a applied) {
	sc.seq.Store(p.Seq)
	keep := m.opts.KeepLog
	if start := sc.logStart.Load(); p.Seq > start+keep+keep/4 {
		sc.logStart.Store(p.Seq - keep)
	}
	if len(a.nested) == 0 && len(a.unnested) == 0 {
		return
	}
	m.mu.Lock()
	for _, nsc := range a.nested {
		m.scopes[nsc.key] = nsc
		sc.nested[fsproto.Rebase(nsc.key.Dir, sc.key.Dir, "/")] = true
	}
	for _, d := range a.unnested {
		key := server.Scope{Root: sc.key.Root, Dir: sc.key.RootPath(d)}
		if old := m.scopes[key]; old != nil {
			old.gone = true // its lock is held by the caller
		}
		delete(m.scopes, key)
		delete(sc.nested, d)
	}
	m.mu.Unlock()
}

// recover finishes every change that was interrupted after its intent was
// recorded.
func (m *Meta) recover() error {
	rows, err := m.wdb.Query(`SELECT scope, plan FROM intent ORDER BY scope`)
	if err != nil {
		return err
	}
	type pending struct {
		id   int64
		plan []byte
	}
	var ps []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.id, &p.plan); err != nil {
			rows.Close()
			return err
		}
		ps = append(ps, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	byID := map[int64]*scope{}
	m.mu.RLock()
	for _, sc := range m.scopes {
		byID[sc.id] = sc
	}
	m.mu.RUnlock()
	for _, pd := range ps {
		sc := byID[pd.id]
		if sc == nil {
			return fmt.Errorf("meta: intent for unknown scope %d", pd.id)
		}
		if err := m.redo(sc, pd.plan); err != nil {
			return fmt.Errorf("meta: recovering scope %s%s: %w", sc.key.Root, sc.key.Dir, err)
		}
	}
	return nil
}

// redo finishes the change recorded in b.
func (m *Meta) redo(sc *scope, b []byte) error {
	p, err := decodePlan(b)
	if err != nil {
		return err
	}
	for _, op := range p.FS {
		if err := m.runFS(sc.key.Root, op, true); err != nil {
			return err
		}
	}
	var a applied
	if err := m.tx(func(tx *sql.Tx) (err error) {
		a, err = m.apply(tx, sc, p)
		return err
	}); err != nil {
		return err
	}
	m.applied(sc, p, a)
	return nil
}

// repair retries a change that failed after its filesystem step. sc.mu is
// held.
func (m *Meta) repair(sc *scope) error {
	var b []byte
	err := m.wdb.QueryRow(`SELECT plan FROM intent WHERE scope = ?`, sc.id).Scan(&b)
	if err == sql.ErrNoRows {
		sc.broken = nil
		return nil
	}
	if err != nil {
		return err
	}
	if err := m.redo(sc, b); err != nil {
		return err
	}
	sc.broken = nil
	return nil
}
