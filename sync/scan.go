package sync

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"sort"
	"strings"

	"github.com/progrium/dotfs/fsproto"
)

// hint asks for a scan of dir (and everything below it when recursive).
type hint struct {
	dir       string
	recursive bool
}

func (e *Engine) hint(dir string, recursive bool) {
	e.addHint(hint{dir, recursive})
}

func (e *Engine) addHint(h hint) {
	e.hints[h.dir] = e.hints[h.dir] || h.recursive
}

// scanHints scans the hinted directories and queues what changed.
func (e *Engine) scanHints() error {
	if len(e.hints) == 0 {
		return nil
	}
	if err := e.checkIgnoreFile(); err != nil {
		return err
	}
	var roots []hint
	if e.hints["/"] {
		roots = []hint{{"/", true}}
	} else {
		var dirs []string
		for d := range e.hints {
			dirs = append(dirs, d)
		}
		sort.Strings(dirs)
	next:
		for _, d := range dirs {
			for _, r := range roots {
				if r.recursive && fsproto.Within(d, r.dir) {
					continue next
				}
			}
			roots = append(roots, hint{d, e.hints[d]})
		}
	}
	clear(e.hints)
	return e.scan(roots)
}

type scanSet struct {
	disk map[string]lstat
	rows map[string]bool
}

// collect lists the local directory d into s.disk and its rows into
// s.rows: everything below it when recursive, otherwise its children, plus
// whole subtrees that are new on disk or gone from it.
func (e *Engine) collect(s *scanSet, d string, recursive bool) error {
	if d != "/" {
		st, err := e.stat(d)
		if err != nil {
			return err
		}
		if st.exists && st.typ != 0 && !e.excluded(d, st.typ == fsproto.TypeDir) {
			s.disk[d] = st
		}
		if e.st.get(d) != nil {
			s.rows[d] = true
		}
		if !st.exists || st.typ != fsproto.TypeDir {
			for _, p := range e.st.subtree(d) {
				s.rows[p] = true
			}
			return nil
		}
	}
	names, err := readDirNames(e.osPath(d))
	if err != nil {
		return err
	}
	for _, name := range names {
		p := fsproto.Join(d, name)
		st, err := e.stat(p)
		if err != nil {
			return err
		}
		if !st.exists || st.typ == 0 || e.excluded(p, st.typ == fsproto.TypeDir) {
			continue
		}
		s.disk[p] = st
		if st.typ == fsproto.TypeDir {
			r := e.st.get(p)
			if recursive || r == nil || r.Type != fsproto.TypeDir {
				if err := e.collect(s, p, true); err != nil {
					return err
				}
			}
		}
	}
	for _, c := range e.st.children(d) {
		s.rows[c] = true
		r := e.st.get(c)
		st, ok := s.disk[c]
		if recursive || (r.Type == fsproto.TypeDir && (!ok || st.typ != fsproto.TypeDir)) {
			for _, p := range e.st.subtree(c) {
				s.rows[p] = true
			}
		}
	}
	return nil
}

func readDirNames(dir string) ([]string, error) {
	f, err := os.Open(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	names, err := f.Readdirnames(-1)
	sort.Strings(names)
	return names, err
}

// scan compares the given parts of the folder with the rows and queues
// local changes, pairing deletes with creates into renames.
func (e *Engine) scan(roots []hint) error {
	s := &scanSet{disk: map[string]lstat{}, rows: map[string]bool{}}
	for _, r := range roots {
		if err := e.collect(s, r.dir, r.recursive); err != nil {
			return err
		}
	}
	var deleted, created []string
	hashes := map[string]string{}
	paths := make([]string, 0, len(s.disk)+len(s.rows))
	for p := range s.disk {
		paths = append(paths, p)
	}
	for p := range s.rows {
		if _, ok := s.disk[p]; !ok {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)
	for _, p := range paths {
		r := e.st.get(p)
		if r != nil && e.excluded(p, r.Type == fsproto.TypeDir) {
			continue
		}
		st, onDisk := s.disk[p]
		switch {
		case r == nil && !onDisk:
		case r == nil:
			if st.typ == fsproto.TypeFile {
				h, err := e.hashFile(p)
				if err != nil {
					if errors.Is(err, fs.ErrNotExist) {
						continue
					}
					return err
				}
				hashes[p] = h
			}
			created = append(created, p)
		case !onDisk:
			deleted = append(deleted, p)
		default:
			ls, _, _, err := e.classify(p, r)
			if err != nil {
				return err
			}
			if ls == lsDirty || ls == lsDeleted {
				e.st.enqueue(opSync, p, "")
			}
		}
	}

	// Pair renames: by inode first (files and directories), then files by
	// hash. A paired directory takes its whole subtree with it.
	pairs := map[string]string{}
	usedTo := map[string]bool{}
	covered := func(p string, set map[string]string, byTo bool) bool {
		for from, to := range set {
			if byTo && fsproto.Within(p, to) || !byTo && fsproto.Within(p, from) {
				return true
			}
		}
		return false
	}
	byIno := map[uint64]string{}
	for _, p := range deleted {
		if r := e.st.get(p); r.LIno != 0 {
			byIno[r.LIno] = p
		}
	}
	for _, c := range created {
		st := s.disk[c]
		from, ok := byIno[st.ino]
		if st.ino == 0 || !ok || covered(c, pairs, true) || covered(from, pairs, false) {
			continue
		}
		if e.st.get(from).Type != st.typ || st.typ == fsproto.TypeSymlink {
			continue
		}
		pairs[from] = c
		usedTo[c] = true
		delete(byIno, st.ino)
	}
	byHash := map[string][]string{}
	for _, p := range deleted {
		if r := e.st.get(p); r.Type == fsproto.TypeFile && r.Hash != "" && !covered(p, pairs, false) {
			byHash[r.Hash] = append(byHash[r.Hash], p)
		}
	}
	for _, c := range created {
		h := hashes[c]
		if h == "" || usedTo[c] || covered(c, pairs, true) {
			continue
		}
		if from := byHash[h]; len(from) == 1 {
			pairs[from[0]] = c
			usedTo[c] = true
			delete(byHash, h)
		}
	}
	for from, to := range pairs {
		e.st.enqueue(opMv, from, to)
		// Content changed along with the name: commit it after the rename.
		if r := e.st.get(from); r.Type == fsproto.TypeFile && hashes[to] != r.Hash {
			e.st.enqueue(opSync, to, "")
		}
	}
	for _, p := range deleted {
		if !covered(p, pairs, false) && e.st.pendingMv(p) == nil {
			e.st.enqueue(opSync, p, "")
		}
	}
	for _, p := range created {
		if !covered(p, pairs, true) {
			e.st.enqueue(opSync, p, "")
		}
	}
	return nil
}

// loadIgnores reads the rules from the folder's ignore file.
func (e *Engine) loadIgnores() error {
	b, err := os.ReadFile(e.osPath("/" + IgnoreFile))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	ig, err := ParseIgnores(bytes.NewReader(b))
	if err != nil {
		return err
	}
	e.ign = ig
	e.watchIgn.Store(ig)
	e.ignKey = fsproto.HashBytes(b)
	return nil
}

// checkIgnoreFile reloads the rules if the ignore file changed.
func (e *Engine) checkIgnoreFile() error {
	b, err := os.ReadFile(e.osPath("/" + IgnoreFile))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if fsproto.HashBytes(b) == e.ignKey {
		return nil
	}
	return e.reloadIgnores()
}

// reloadIgnores applies new rules: rows for paths now ignored are dropped
// (their files stay), and the index is read again for paths no longer
// ignored.
func (e *Engine) reloadIgnores() error {
	old := e.ignKey
	if err := e.loadIgnores(); err != nil {
		return err
	}
	if e.ignKey == old {
		return nil
	}
	e.logf("ignore rules changed")
	var drop []string
	for p, r := range e.st.rows {
		if e.ign.Ignored(p, r.Type == fsproto.TypeDir) {
			drop = append(drop, p)
		}
	}
	for _, p := range drop {
		e.st.del(p)
	}
	for _, q := range append([]*qent(nil), e.st.queue...) {
		if e.ign.Ignored(q.Path, false) || q.To != "" && e.ign.Ignored(q.To, false) {
			e.st.dequeue(q)
		}
	}
	e.reconcile = true
	e.hints["/"] = true
	return nil
}

// excluded reports whether the engine leaves p alone: it is ignored, or
// below a nested scope (flag i), which has its own index and events that
// this engine does not follow.
func (e *Engine) excluded(p string, dir bool) bool {
	if e.ign.Ignored(p, dir) {
		return true
	}
	for q := fsproto.Parent(p); q != "/"; q = fsproto.Parent(q) {
		if r := e.st.get(q); r != nil && r.Flags.Has(fsproto.FlagIndex) {
			return true
		}
	}
	return false
}

// depth counts a path's components.
func depth(p string) int {
	if p == "/" {
		return 0
	}
	return strings.Count(p, "/")
}
