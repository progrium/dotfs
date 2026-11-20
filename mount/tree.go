package mount

import (
	"sort"
	"strings"

	"github.com/progrium/dotfs/fsproto"
)

// node is one path in the tree. Its Entry.Path is not kept up to date
// (a directory move would have to rewrite every path below it); a node's
// path is found by walking from the root.
type node struct {
	name   string
	e      fsproto.Entry
	ino    uint64
	parent *node
	kids   map[string]*node // directories only
}

func (n *node) isDir() bool { return n.e.Type == fsproto.TypeDir }

// tree is the scope's metadata: the index, kept current from events.
// It is not safe for concurrent use; FS guards it with its mutex.
type tree struct {
	root    *node
	seq     uint64
	nextIno uint64
	// inos keeps a path's inode number across a remote replace, so the
	// kernel sees new content at the same inode rather than a new file.
	inos map[string]uint64
}

func newTree() *tree {
	t := &tree{nextIno: 2, inos: map[string]uint64{}}
	t.root = &node{e: fsproto.Entry{Path: "/", Type: fsproto.TypeDir, Mode: 0o755}, ino: 1, kids: map[string]*node{}}
	return t
}

func split(p string) []string {
	if p == "/" {
		return nil
	}
	return strings.Split(p[1:], "/")
}

// get returns the node at p, or nil.
func (t *tree) get(p string) *node {
	n := t.root
	for _, c := range split(p) {
		if n.kids == nil {
			return nil
		}
		if n = n.kids[c]; n == nil {
			return nil
		}
	}
	return n
}

func (t *tree) inoFor(p string) uint64 {
	if ino, ok := t.inos[p]; ok {
		return ino
	}
	ino := t.nextIno
	t.nextIno++
	t.inos[p] = ino
	return ino
}

// put sets the entry at e.Path, creating missing parent directories, and
// returns its node. A directory keeps its children when its entry is
// replaced by another directory entry; any other type change drops them.
func (t *tree) put(e fsproto.Entry) *node {
	if e.Path == "/" {
		kids := t.root.kids
		t.root.e = e
		t.root.kids = kids
		return t.root
	}
	dir := t.mkdirs(fsproto.Parent(e.Path))
	name := fsproto.Name(e.Path)
	n := dir.kids[name]
	if n == nil {
		n = &node{name: name, parent: dir, ino: t.inoFor(e.Path)}
		dir.kids[name] = n
	}
	n.e = e
	if e.Type == fsproto.TypeDir {
		if n.kids == nil {
			n.kids = map[string]*node{}
		}
	} else {
		t.forget(n)
		n.kids = nil
	}
	return n
}

// mkdirs returns the directory at p, creating it and its parents as plain
// directories when they are missing (an index or event stream that names
// a child before its parent).
func (t *tree) mkdirs(p string) *node {
	n := t.root
	cur := "/"
	for _, c := range split(p) {
		cur = fsproto.Join(cur, c)
		k := n.kids[c]
		if k == nil || k.kids == nil {
			k = &node{name: c, parent: n, ino: t.inoFor(cur), kids: map[string]*node{},
				e: fsproto.Entry{Path: cur, Type: fsproto.TypeDir, Mode: 0o755}}
			n.kids[c] = k
		}
		n = k
	}
	return n
}

// forget drops the remembered inode numbers of everything below n.
func (t *tree) forget(n *node) {
	for _, k := range n.kids {
		delete(t.inos, t.path(k))
		t.forget(k)
	}
}

// del removes the node at p and everything below it.
func (t *tree) del(p string) *node {
	n := t.get(p)
	if n == nil || n == t.root {
		return nil
	}
	t.forget(n)
	delete(t.inos, p)
	delete(n.parent.kids, n.name)
	return n
}

// mv moves the node at from to to, replacing whatever is there, and gives
// it version v.
func (t *tree) mv(from, to string, v uint64) *node {
	n := t.get(from)
	if n == nil || n == t.root {
		return nil
	}
	t.del(to)
	moved := map[string]uint64{}
	for p, ino := range t.inos {
		if fsproto.Within(p, from) {
			moved[fsproto.Rebase(p, from, to)] = ino
			delete(t.inos, p)
		}
	}
	for p, ino := range moved {
		t.inos[p] = ino
	}
	delete(n.parent.kids, n.name)
	dir := t.mkdirs(fsproto.Parent(to))
	n.name, n.parent = fsproto.Name(to), dir
	dir.kids[n.name] = n
	n.e.Path = to
	n.e.Version = v
	return n
}

// path returns n's path.
func (t *tree) path(n *node) string {
	if n.parent == nil {
		return "/"
	}
	var parts []string
	for ; n.parent != nil; n = n.parent {
		parts = append(parts, n.name)
	}
	var b strings.Builder
	for i := len(parts) - 1; i >= 0; i-- {
		b.WriteByte('/')
		b.WriteString(parts[i])
	}
	return b.String()
}

// entry returns n's entry with its current path.
func (t *tree) entry(n *node) fsproto.Entry {
	e := n.e
	e.Path = t.path(n)
	return e
}

// walk calls fn for every node in path order, parents before children.
func (t *tree) walk(fn func(p string, n *node)) {
	var rec func(p string, n *node)
	rec = func(p string, n *node) {
		fn(p, n)
		names := make([]string, 0, len(n.kids))
		for name := range n.kids {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			rec(fsproto.Join(p, name), n.kids[name])
		}
	}
	rec("/", t.root)
}

// reset replaces the tree's contents with entries, keeping inode numbers
// of paths that still exist.
func (t *tree) reset(entries []fsproto.Entry, seq uint64) {
	old := t.inos
	t.inos = map[string]uint64{}
	for _, e := range entries {
		if ino, ok := old[e.Path]; ok {
			t.inos[e.Path] = ino
		}
	}
	t.root = &node{e: fsproto.Entry{Path: "/", Type: fsproto.TypeDir, Mode: 0o755}, ino: 1, kids: map[string]*node{}}
	for _, e := range entries {
		t.put(e)
	}
	t.seq = seq
}
