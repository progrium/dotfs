package meta

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

// Group commit.
//
// A scope's changes queue for its lock. Whoever takes the lock takes the
// whole queue and commits it in order, in as few groups as it can: each
// change is built (checked against the path table plus what earlier
// changes in the group did) and gets the next seq, and then the group is
// made durable together, with one intent transaction, the filesystem steps
// in seq order, one sync per directory placed into, and one apply
// transaction. A change whose build fails (a stale base, say) fails alone
// and takes no seq. A solo change commits as a group of its own.
//
// A change that fails at its first filesystem step makes no change, so it
// is dropped with every change after it in the group (which may depend on
// it); the ones before it commit. A failure after that leaves the scope
// broken until repair, as for a single change.

// request is one queued change.
type request struct {
	ctx   context.Context
	o     server.Origin
	solo  bool
	build func(c *change) error

	done bool // set, with res and err, under the scope's lock
	res  server.Result
	err  error
}

// overlay is what a group's changes did to single paths so far: the new
// entry, or nil for a removed one.
type overlay map[string]*fsproto.Entry

// groupPlan is a group's intent: its plans in seq order.
type groupPlan struct {
	Plans []plan
}

func encodeGroup(ps []plan) ([]byte, error) { return encodeGob(&groupPlan{Plans: ps}) }

// decodeIntent reads an intent: a group's, or one plan's as written before
// group commit.
func decodeIntent(b []byte) ([]plan, error) {
	var g groupPlan
	if err := decodeGob(b, &g); err == nil && len(g.Plans) > 0 {
		return g.Plans, nil
	}
	p, err := decodePlan(b)
	if err != nil {
		return nil, err
	}
	return []plan{*p}, nil
}

// errGroupDropped is the error of a change dropped because an earlier one
// in its group failed.
type errGroupDropped struct{ err error }

func (e errGroupDropped) Error() string {
	return fmt.Sprintf("meta: an earlier change in the same group failed: %v", e.err)
}
func (e errGroupDropped) Unwrap() error { return e.err }

// submit queues a change and waits until it has been committed, by this
// call or by the one holding the lock.
func (w *writer) submit(ctx context.Context, o server.Origin, solo bool, build func(c *change) error) (server.Result, error) {
	sc := w.sc
	r := &request{ctx: ctx, o: o, solo: solo, build: build}
	sc.qmu.Lock()
	sc.queue = append(sc.queue, r)
	sc.qmu.Unlock()
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if !r.done {
		// Nobody has taken r yet, so it is in the queue.
		sc.qmu.Lock()
		q := sc.queue
		sc.queue = nil
		sc.qmu.Unlock()
		w.m.runQueue(sc, q)
	}
	return r.res, r.err
}

// group is the changes built so far for one group commit.
type group struct {
	reqs    []*request
	changes []*change
	over    overlay
}

func finish(r *request, res server.Result, err error) {
	r.res, r.err, r.done = res, err, true
}

// runQueue commits q in order. sc.mu is held.
func (m *Meta) runQueue(sc *scope, q []*request) {
	g := &group{over: overlay{}}
	flush := func() {
		if len(g.changes) > 0 {
			m.commitGroup(sc, g)
		}
		g = &group{over: overlay{}}
	}
	for _, r := range q {
		if r.solo {
			flush()
		}
		if sc.gone {
			finish(r, server.Result{}, server.ErrNoScope)
			continue
		}
		if sc.broken != nil {
			if err := m.repair(sc); err != nil {
				finish(r, server.Result{}, err)
				continue
			}
		}
		c := &change{ctx: r.ctx, m: m, sc: sc, seq: sc.seq.Load() + 1 + uint64(len(g.changes)), over: g.over}
		if err := r.build(c); err != nil {
			m.discard(&c.plan)
			for _, f := range c.done {
				f()
			}
			finish(r, server.Result{}, err)
			continue
		}
		ev := c.ev
		ev.Seq, ev.Dev = c.seq, r.o.Dev
		c.ev = ev
		c.plan.Seq, c.plan.Event = c.seq, ev.Encode()
		for _, mu := range c.plan.Muts {
			switch mu.Kind {
			case mutPut:
				e := mu.Entry
				g.over[e.Path] = &e
			case mutDel:
				g.over[mu.Path] = nil
			}
		}
		g.reqs = append(g.reqs, r)
		g.changes = append(g.changes, c)
		if r.solo {
			flush()
		}
	}
	flush()
}

// commitGroup makes g's changes durable and visible, in order, and
// publishes them. sc.mu is held.
func (m *Meta) commitGroup(sc *scope, g *group) {
	defer func() {
		for _, c := range g.changes {
			for _, f := range c.done {
				f()
			}
		}
	}()
	plans := make([]plan, len(g.changes))
	hasFS := false
	for i, c := range g.changes {
		plans[i] = c.plan
		hasFS = hasFS || len(c.plan.FS) > 0
	}
	failAll := func(err error) {
		for _, r := range g.reqs {
			finish(r, server.Result{}, err)
		}
	}
	// dropFrom fails change i with err, and the ones after it as dropped.
	dropFrom := func(i int, err error) {
		finish(g.reqs[i], server.Result{}, err)
		for _, r := range g.reqs[i+1:] {
			finish(r, server.Result{}, errGroupDropped{err})
		}
	}
	discardFrom := func(from int) {
		for i := from; i < len(plans); i++ {
			m.discard(&plans[i])
		}
	}
	n := len(plans) // how many of the group commit
	crash("prepared")
	if hasFS {
		b, err := encodeGroup(plans)
		if err == nil {
			err = m.tx(func(tx *sql.Tx) error {
				_, err := tx.Exec(`INSERT INTO intent (scope, plan) VALUES (?, ?)`, sc.id, b)
				return err
			})
		}
		if err != nil {
			discardFrom(0)
			failAll(err)
			return
		}
		crash("intent")
		dirs := map[string]bool{}
		var order []string
	run:
		for i := range plans {
			for j, op := range plans[i].FS {
				var err error
				if op.Kind == fsPlace {
					err = m.st.PlaceNoSync(op.Prep, sc.key.Root, op.Path)
					if err == nil {
						crash("placed")
					}
					if d := fsproto.Parent(op.Path); err == nil && !dirs[d] {
						dirs[d] = true
						order = append(order, d)
					}
				} else {
					err = m.runFS(sc.key.Root, op, false)
				}
				if err == nil {
					continue
				}
				// Change i made no change if this was its first step: drop
				// it and the rest, and commit the ones before it.
				if j == 0 && m.rewriteIntent(sc, plans[:i]) == nil {
					discardFrom(i)
					dropFrom(i, err)
					n = i
					break run
				}
				sc.broken = err
				failAll(err)
				return
			}
		}
		for _, d := range order {
			if err := m.st.SyncDir(sc.key.Root, d); err != nil {
				sc.broken = err
				failAll(err)
				return
			}
		}
		if n == 0 {
			return
		}
		crash("fs")
	}
	plans = plans[:n]
	as := make([]applied, n)
	err := m.tx(func(tx *sql.Tx) error {
		for i := range plans {
			a, err := m.apply(tx, sc, &plans[i], i == n-1)
			if err != nil {
				return err
			}
			as[i] = a
		}
		return nil
	})
	if err != nil {
		if hasFS {
			sc.broken = err
		}
		failAll(err)
		return
	}
	for i := range plans {
		m.applied(sc, &plans[i], as[i])
	}
	crash("applied")
	for i := 0; i < n; i++ {
		c := g.changes[i]
		m.publish(sc.key, c.ev)
		finish(g.reqs[i], server.Result{Version: c.seq, Hash: c.hash, Event: c.ev}, nil)
	}
}

// rewriteIntent replaces the scope's intent with ps, or drops it when ps
// is empty.
func (m *Meta) rewriteIntent(sc *scope, ps []plan) error {
	var b []byte
	if len(ps) > 0 {
		var err error
		if b, err = encodeGroup(ps); err != nil {
			return err
		}
	}
	return m.tx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`DELETE FROM intent WHERE scope = ?`, sc.id); err != nil || b == nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO intent (scope, plan) VALUES (?, ?)`, sc.id, b)
		return err
	})
}
