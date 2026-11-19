package fake

import (
	"bytes"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

type node struct {
	typ    fsproto.Type
	data   []byte // never modified after publish; shared by copies
	hash   string // cached content hash for files
	target string
	mode   uint32
	mtime  int64
}

type stage struct {
	data []byte
	mod  time.Time
}

// Store is an in-memory server.Store.
type Store struct {
	mu     sync.Mutex
	roots  map[server.Root]map[string]*node
	stages map[string]map[string]*stage // user -> name -> stage
	now    func() time.Time
}

var _ server.Store = (*Store)(nil)

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{
		roots:  map[server.Root]map[string]*node{},
		stages: map[string]map[string]*stage{},
		now:    time.Now,
	}
}

func (s *Store) CreateRoot(root server.Root) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.roots[root] == nil {
		s.roots[root] = map[string]*node{"/": {typ: fsproto.TypeDir, mode: 0o755, mtime: s.now().Unix()}}
	}
	return nil
}

func (s *Store) tree(root server.Root) (map[string]*node, error) {
	t := s.roots[root]
	if t == nil {
		return nil, fsproto.Missing("no such root")
	}
	return t, nil
}

func (s *Store) userStages(user string) map[string]*stage {
	m := s.stages[user]
	if m == nil {
		m = map[string]*stage{}
		s.stages[user] = m
	}
	return m
}

type stageFile struct {
	s      *Store
	st     *stage
	closed bool
}

func (f *stageFile) ReadAt(p []byte, off int64) (int, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	if off >= int64(len(f.st.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.st.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *stageFile) WriteAt(p []byte, off int64) (int, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	if end := off + int64(len(p)); end > int64(len(f.st.data)) {
		f.st.data = append(f.st.data, make([]byte, end-int64(len(f.st.data)))...)
	}
	copy(f.st.data[off:], p)
	f.st.mod = f.s.now()
	return len(p), nil
}

func (f *stageFile) Size() (int64, error) {
	f.s.mu.Lock()
	defer f.s.mu.Unlock()
	return int64(len(f.st.data)), nil
}

func (f *stageFile) Close() error { return nil }

func (s *Store) OpenStage(user, name string, create bool) (server.StageFile, error) {
	if err := fsproto.CheckStageName(name); err != nil {
		return nil, fsproto.Invalid("bad stage name")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.userStages(user)
	st := m[name]
	if st == nil {
		if !create {
			return nil, fsproto.Missing("no such stage")
		}
		st = &stage{mod: s.now()}
		m[name] = st
	}
	return &stageFile{s: s, st: st}, nil
}

func (s *Store) StatStage(user, name string) (server.StageInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.userStages(user)[name]
	if st == nil {
		return server.StageInfo{}, fsproto.Missing("no such stage")
	}
	return server.StageInfo{Name: name, Size: int64(len(st.data)), ModTime: st.mod}, nil
}

func (s *Store) ListStages(user string) ([]server.StageInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []server.StageInfo
	for name, st := range s.userStages(user) {
		out = append(out, server.StageInfo{Name: name, Size: int64(len(st.data)), ModTime: st.mod})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (s *Store) RemoveStage(user, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.userStages(user), name)
	return nil
}

func (s *Store) ExpireStages(t time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, m := range s.stages {
		for name, st := range m {
			if st.mod.Before(t) {
				delete(m, name)
				n++
			}
		}
	}
	return n, nil
}

// parentDir checks that p's parent exists and is a directory.
func parentDir(t map[string]*node, p string) error {
	n := t[fsproto.Parent(p)]
	if n == nil {
		return fsproto.Missing("no parent directory")
	}
	if n.typ != fsproto.TypeDir {
		return fsproto.Invalid("parent is not a directory")
	}
	return nil
}

// placeFile checks that a file may be put at p.
func placeFile(t map[string]*node, p string) error {
	if p == "/" {
		return fsproto.Invalid("cannot replace the root")
	}
	if err := parentDir(t, p); err != nil {
		return err
	}
	if n := t[p]; n != nil && n.typ == fsproto.TypeDir {
		return fsproto.Invalid("is a directory")
	}
	return nil
}

func (s *Store) Publish(user, stageName string, root server.Root, path string, mode uint32, mtime int64, hash string) (string, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.userStages(user)[stageName]
	if st == nil {
		return "", 0, fsproto.Missing("no such stage")
	}
	t, err := s.tree(root)
	if err != nil {
		return "", 0, err
	}
	if err := placeFile(t, path); err != nil {
		return "", 0, err
	}
	got := fsproto.HashBytes(st.data)
	if hash != "" && hash != got {
		return "", 0, fsproto.Invalid("hash mismatch")
	}
	t[path] = &node{typ: fsproto.TypeFile, data: st.data, hash: got, mode: mode, mtime: mtime}
	delete(s.userStages(user), stageName)
	return got, int64(len(st.data)), nil
}

func (s *Store) PublishHash(user, hash string, root server.Root, path string, mode uint32, mtime int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tree(root)
	if err != nil {
		return 0, err
	}
	if err := placeFile(t, path); err != nil {
		return 0, err
	}
	for r, other := range s.roots {
		if r.User() != user {
			continue
		}
		for _, n := range other {
			if n.typ == fsproto.TypeFile && s.hashOf(n) == hash {
				t[path] = &node{typ: fsproto.TypeFile, data: n.data, hash: hash, mode: mode, mtime: mtime}
				return int64(len(n.data)), nil
			}
		}
	}
	return 0, fsproto.Missing("content not held")
}

func (s *Store) hashOf(n *node) string {
	if n.hash == "" {
		n.hash = fsproto.HashBytes(n.data)
	}
	return n.hash
}

type contentFile struct{ *bytes.Reader }

func (contentFile) Close() error { return nil }

func (s *Store) Open(root server.Root, path string) (server.ContentFile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tree(root)
	if err != nil {
		return nil, err
	}
	n := t[path]
	if n == nil {
		return nil, fsproto.Missing("")
	}
	if n.typ != fsproto.TypeFile {
		return nil, fsproto.Invalid("not a file")
	}
	return contentFile{bytes.NewReader(n.data)}, nil
}

func (s *Store) Hash(root server.Root, path string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tree(root)
	if err != nil {
		return "", err
	}
	n := t[path]
	if n == nil {
		return "", fsproto.Missing("")
	}
	if n.typ != fsproto.TypeFile {
		return "", fsproto.Invalid("not a file")
	}
	return s.hashOf(n), nil
}

func (s *Store) Mkdir(root server.Root, path string, mode uint32, mtime int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tree(root)
	if err != nil {
		return err
	}
	if t[path] != nil {
		return fsproto.Conflict(0)
	}
	if err := parentDir(t, path); err != nil {
		return err
	}
	t[path] = &node{typ: fsproto.TypeDir, mode: mode, mtime: mtime}
	return nil
}

func (s *Store) Symlink(root server.Root, path, target string, mtime int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tree(root)
	if err != nil {
		return err
	}
	if err := placeFile(t, path); err != nil {
		return err
	}
	t[path] = &node{typ: fsproto.TypeSymlink, target: target, mode: 0o777, mtime: mtime}
	return nil
}

func (s *Store) Remove(root server.Root, path string, recursive bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tree(root)
	if err != nil {
		return err
	}
	if path == "/" {
		return fsproto.Invalid("cannot remove the root")
	}
	if t[path] == nil {
		return fsproto.Missing("")
	}
	var below []string
	for p := range t {
		if p != path && fsproto.Within(p, path) {
			below = append(below, p)
		}
	}
	if len(below) > 0 && !recursive {
		return fsproto.Invalid("directory not empty")
	}
	for _, p := range below {
		delete(t, p)
	}
	delete(t, path)
	return nil
}

func (s *Store) Rename(root server.Root, from, to string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tree(root)
	if err != nil {
		return err
	}
	src := t[from]
	if src == nil {
		return fsproto.Missing("")
	}
	if from == "/" || to == "/" || (to != from && fsproto.Within(to, from)) {
		return fsproto.Invalid("bad rename")
	}
	if from == to {
		return nil
	}
	if err := parentDir(t, to); err != nil {
		return err
	}
	if dst := t[to]; dst != nil && (dst.typ == fsproto.TypeDir || src.typ == fsproto.TypeDir) {
		return fsproto.Invalid("cannot replace a directory")
	}
	moved := map[string]*node{}
	for p, n := range t {
		if fsproto.Within(p, from) {
			moved[fsproto.Rebase(p, from, to)] = n
			delete(t, p)
		}
	}
	for p, n := range moved {
		t[p] = n
	}
	return nil
}

func (s *Store) Copy(root server.Root, from, to string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tree(root)
	if err != nil {
		return err
	}
	src := t[from]
	if src == nil {
		return fsproto.Missing("")
	}
	if src.typ == fsproto.TypeDir {
		return fsproto.Invalid("cannot copy a directory")
	}
	if err := placeFile(t, to); err != nil {
		return err
	}
	c := *src
	t[to] = &c
	return nil
}

func (s *Store) SetAttr(root server.Root, path string, mode *uint32, mtime *int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.tree(root)
	if err != nil {
		return err
	}
	n := t[path]
	if n == nil {
		return fsproto.Missing("")
	}
	if mode != nil {
		n.mode = *mode
	}
	if mtime != nil {
		n.mtime = *mtime
	}
	return nil
}
