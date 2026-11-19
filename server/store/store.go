// Package store is the server's content store: the plain files of every
// root, stage files for uploads in progress, and hard-link dedupe by content
// hash. It implements server.Store, plus the two-phase publish (Prepare and
// Place) that server/meta uses to make each change crash-safe.
//
// On disk, under the data directory:
//
//	roots/<user>/<root>/...  file bytes at their real paths
//	stage/<user>/<name>      uploads in progress
//	blobs/<user>/<b3-hex>    hard-link targets for dedupe
//	tmp/                     prepared content waiting to be placed
//
// All four are on one filesystem, so a publish is a rename and dedupe is a
// hard link. Blobs are kept per user, so commit-hash only ever finds content
// the attaching user can already read.
//
// Metadata (versions, flags, and the authoritative mode and mtime) lives in
// server/meta. Files deduplicated by hard links share one inode, so their
// mode and mtime on disk are the last ones set; the metadata store is what
// clients see.
package store

import (
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

// Options configures a Store.
type Options struct {
	// NoDedupe turns off hard-link dedupe. commit-hash then always misses,
	// and cp copies bytes.
	NoDedupe bool
	// NoSync skips fsync of files and directories, for tests and
	// benchmarks. A crash can then lose published content.
	NoSync bool
}

// Store is a server.Store on a local filesystem.
type Store struct {
	dir    string
	opts   Options
	prefix string // makes tmp names unique across restarts
	tmpN   atomic.Uint64

	mu     sync.Mutex
	roots  map[server.Root]*os.Root
	stages map[stageKey]*stageState
}

var _ server.Store = (*Store)(nil)

// New opens (creating if needed) a store in dir. opts may be nil.
func New(dir string, opts *Options) (*Store, error) {
	s := &Store{
		dir:    dir,
		prefix: fmt.Sprintf("%x", time.Now().UnixNano()),
		roots:  map[server.Root]*os.Root{},
		stages: map[stageKey]*stageState{},
	}
	if opts != nil {
		s.opts = *opts
	}
	for _, d := range []string{"roots", "stage", "blobs", "tmp"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o700); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Dir returns the data directory.
func (s *Store) Dir() string { return s.dir }

// Close releases open directory handles.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for r, fr := range s.roots {
		fr.Close()
		delete(s.roots, r)
	}
	return nil
}

// checkName checks a user or root name, which becomes one directory name.
func checkName(n string) error {
	if n == "" || n == "." || n == ".." || strings.ContainsAny(n, "/\x00") {
		return fsproto.Invalid("bad name")
	}
	return nil
}

func checkRoot(r server.Root) error {
	if err := checkName(r.User()); err != nil {
		return err
	}
	return checkName(r.Name())
}

// RootDir returns the directory holding root's files.
func (s *Store) RootDir(r server.Root) string {
	return filepath.Join(s.dir, "roots", r.User(), r.Name())
}

func (s *Store) CreateRoot(r server.Root) error {
	if err := checkRoot(r); err != nil {
		return err
	}
	return os.MkdirAll(s.RootDir(r), 0o755)
}

// root returns a handle that keeps lookups inside root's directory.
func (s *Store) root(r server.Root) (*os.Root, error) {
	if err := checkRoot(r); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if fr := s.roots[r]; fr != nil {
		return fr, nil
	}
	fr, err := os.OpenRoot(s.RootDir(r))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fsproto.Missing("no such root")
	}
	if err != nil {
		return nil, err
	}
	s.roots[r] = fr
	return fr, nil
}

// rel converts a protocol path to one relative to a root directory.
func rel(p string) (string, error) {
	if err := fsproto.CheckPath(p); err != nil {
		return "", fsproto.Invalid("bad path")
	}
	if p == "/" {
		return ".", nil
	}
	return filepath.FromSlash(p[1:]), nil
}

// full returns the absolute on-disk path of p in root r.
func (s *Store) full(r server.Root, p string) string {
	if p == "/" {
		return s.RootDir(r)
	}
	return filepath.Join(s.RootDir(r), filepath.FromSlash(p[1:]))
}

// mapErr turns filesystem errors into protocol outcomes where one fits.
func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fs.ErrNotExist):
		return fsproto.Missing("")
	case errors.Is(err, fs.ErrExist):
		return fsproto.Conflict(0)
	}
	return err
}

func fileMode(m uint32) fs.FileMode { return fs.FileMode(m&0o777 | 0o600) }
func dirMode(m uint32) fs.FileMode  { return fs.FileMode(m&0o777 | 0o700) }

func (s *Store) syncDir(dir string) error {
	if s.opts.NoSync {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (s *Store) syncFile(f *os.File) error {
	if s.opts.NoSync {
		return nil
	}
	return f.Sync()
}

// checkPlace checks that a file may be put at p: its parent is a
// directory and p is not one.
func checkPlace(fr *os.Root, p string) error {
	if p == "/" {
		return fsproto.Invalid("cannot replace the root")
	}
	if err := checkParent(fr, p); err != nil {
		return err
	}
	r, _ := rel(p)
	if fi, err := fr.Lstat(r); err == nil && fi.IsDir() {
		return fsproto.Invalid("is a directory")
	}
	return nil
}

func checkParent(fr *os.Root, p string) error {
	r, err := rel(fsproto.Parent(p))
	if err != nil {
		return err
	}
	fi, err := fr.Lstat(r)
	if errors.Is(err, fs.ErrNotExist) {
		return fsproto.Missing("no parent directory")
	}
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fsproto.Invalid("parent is not a directory")
	}
	return nil
}

// Stage files.

type stageKey struct{ user, name string }

// stageState is shared by every open handle of one stage file. It hashes
// writes that arrive in order, so a commit usually needs no second read,
// and it seals the file once a commit has taken it.
type stageState struct {
	mu     sync.Mutex
	sealed bool
	h      hash.Hash // nil once out-of-order writes make it useless
	hashed int64
	writes uint64   // writes so far, so a verification can tell it is current
	ver    verified // the last VerifyStage, if any
}

// verified is what VerifyStage found: the content's hash and size, synced
// to disk, as of the stage's writes-th write.
type verified struct {
	ok     bool
	file   fs.FileInfo // which file, in case the name was reused
	writes uint64
	size   int64
	hash   string
}

// ErrSealed is returned by writes to a stage file that has been committed.
var ErrSealed = errors.New("store: stage file already committed")

func (s *Store) stagePath(user, name string) string {
	return filepath.Join(s.dir, "stage", user, name)
}

// stageState returns the state for a stage file; size is its current
// length, used when the state is new.
func (s *Store) stageState(k stageKey, size int64) *stageState {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stages[k]
	if st == nil {
		st = &stageState{}
		if size == 0 {
			st.h = fsproto.NewB3()
		}
		s.stages[k] = st
	}
	return st
}

func (s *Store) dropStage(k stageKey) {
	s.mu.Lock()
	delete(s.stages, k)
	s.mu.Unlock()
}

type stageFile struct {
	f  *os.File
	st *stageState
}

func (f *stageFile) ReadAt(p []byte, off int64) (int, error) { return f.f.ReadAt(p, off) }

func (f *stageFile) WriteAt(p []byte, off int64) (int, error) {
	st := f.st
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.sealed {
		return 0, ErrSealed
	}
	n, err := f.f.WriteAt(p, off)
	st.writes++
	if st.h != nil {
		if off == st.hashed {
			st.h.Write(p[:n])
			st.hashed += int64(n)
		} else {
			st.h = nil
		}
	}
	return n, err
}

func (f *stageFile) Size() (int64, error) {
	fi, err := f.f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (f *stageFile) Close() error { return f.f.Close() }

func (s *Store) OpenStage(user, name string, create bool) (server.StageFile, error) {
	if err := checkName(user); err != nil {
		return nil, err
	}
	if err := fsproto.CheckStageName(name); err != nil {
		return nil, fsproto.Invalid("bad stage name")
	}
	p := s.stagePath(user, name)
	flag := os.O_RDWR
	if create {
		flag |= os.O_CREATE
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(p, flag, 0o600)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fsproto.Missing("no such stage")
	}
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		f.Close()
		return nil, fsproto.Invalid("not a stage file")
	}
	return &stageFile{f: f, st: s.stageState(stageKey{user, name}, fi.Size())}, nil
}

func stageInfo(fi fs.FileInfo) server.StageInfo {
	return server.StageInfo{Name: fi.Name(), Size: fi.Size(), ModTime: fi.ModTime()}
}

func (s *Store) StatStage(user, name string) (server.StageInfo, error) {
	if checkName(user) != nil || fsproto.CheckStageName(name) != nil {
		return server.StageInfo{}, fsproto.Missing("no such stage")
	}
	fi, err := os.Lstat(s.stagePath(user, name))
	if err != nil {
		return server.StageInfo{}, mapErr(err)
	}
	return stageInfo(fi), nil
}

func (s *Store) ListStages(user string) ([]server.StageInfo, error) {
	if err := checkName(user); err != nil {
		return nil, nil
	}
	des, err := os.ReadDir(filepath.Join(s.dir, "stage", user))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []server.StageInfo
	for _, de := range des {
		fi, err := de.Info()
		if err != nil {
			continue
		}
		out = append(out, stageInfo(fi))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (s *Store) RemoveStage(user, name string) error {
	if checkName(user) != nil || fsproto.CheckStageName(name) != nil {
		return nil
	}
	s.dropStage(stageKey{user, name})
	err := os.Remove(s.stagePath(user, name))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func (s *Store) ExpireStages(t time.Time) (int, error) {
	users, err := os.ReadDir(filepath.Join(s.dir, "stage"))
	if err != nil {
		return 0, err
	}
	n := 0
	for _, u := range users {
		des, err := os.ReadDir(filepath.Join(s.dir, "stage", u.Name()))
		if err != nil {
			continue
		}
		for _, de := range des {
			fi, err := de.Info()
			if err != nil || !fi.ModTime().Before(t) {
				continue
			}
			s.dropStage(stageKey{u.Name(), de.Name()})
			if os.Remove(filepath.Join(s.dir, "stage", u.Name(), de.Name())) == nil {
				n++
			}
		}
	}
	return n, nil
}

// Two-phase publish.

// Prepared is content waiting in the store's tmp directory to be placed at
// a path. It is plain data, so a caller can record it and finish the
// placement after a crash.
type Prepared struct {
	Tmp  string // name in tmp/
	Hash string // content hash, when known
	Size int64
}

func (s *Store) tmpPath(name string) string { return filepath.Join(s.dir, "tmp", name) }

func (s *Store) newTmp() string {
	return fmt.Sprintf("%s-%d", s.prefix, s.tmpN.Add(1))
}

func (s *Store) blobPath(user, hash string) (string, bool) {
	algo, hex, err := fsproto.SplitHash(hash)
	if err != nil || algo != fsproto.HashB3 {
		return "", false
	}
	return filepath.Join(s.dir, "blobs", user, hex), true
}

// dedupe makes tmp share an inode with the user's blob of the same
// content, adding the blob if there is none. Failures leave tmp as it is.
func (s *Store) dedupe(user, hash, tmp string) {
	if s.opts.NoDedupe {
		return
	}
	bp, ok := s.blobPath(user, hash)
	if !ok {
		return
	}
	if err := os.MkdirAll(filepath.Dir(bp), 0o700); err != nil {
		return
	}
	err := os.Link(tmp, bp)
	if err == nil || !errors.Is(err, fs.ErrExist) {
		return
	}
	// The blob exists: link it in place of tmp.
	alt := tmp + ".d"
	if os.Link(bp, alt) == nil {
		if os.Rename(alt, tmp) != nil {
			os.Remove(alt)
		}
	}
}

func (s *Store) setAttrs(p string, mode uint32, mtime int64) error {
	if err := os.Chmod(p, fileMode(mode)); err != nil {
		return err
	}
	t := time.Unix(mtime, 0)
	return os.Chtimes(p, t, t)
}

// PrepareStage takes user's stage file for a commit: it checks the content
// against hash (when not empty), moves it to tmp, dedupes it, and sets its
// mode and mtime. Later writes through open handles fail with ErrSealed. A
// mismatch is fsproto.ErrInvalid and leaves the stage file as it was.
func (s *Store) PrepareStage(user, name, hash string, mode uint32, mtime int64) (Prepared, error) {
	if checkName(user) != nil || fsproto.CheckStageName(name) != nil {
		return Prepared{}, fsproto.Missing("no such stage")
	}
	k := stageKey{user, name}
	sp := s.stagePath(user, name)
	f, err := os.Open(sp)
	if errors.Is(err, fs.ErrNotExist) {
		return Prepared{}, fsproto.Missing("no such stage")
	}
	if err != nil {
		return Prepared{}, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return Prepared{}, err
	}
	st := s.stageState(k, fi.Size())
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.sealed {
		return Prepared{}, fsproto.Missing("stage already committed")
	}
	// Writes cannot happen now; take the size again in case one landed
	// between Stat and the lock.
	if fi, err = f.Stat(); err != nil {
		return Prepared{}, err
	}
	size := fi.Size()
	got, err := s.verifyLocked(f, fi, st)
	if err != nil {
		return Prepared{}, err
	}
	if hash != "" && hash != got {
		return Prepared{}, fsproto.Invalid("hash mismatch")
	}
	tmp := s.newTmp()
	tp := s.tmpPath(tmp)
	if err := os.Rename(sp, tp); err != nil {
		return Prepared{}, mapErr(err)
	}
	st.sealed = true
	s.dropStage(k)
	s.dedupe(user, got, tp)
	if err := s.setAttrs(tp, mode, mtime); err != nil {
		os.Remove(tp)
		return Prepared{}, err
	}
	return Prepared{Tmp: tmp, Hash: got, Size: size}, nil
}

// verifyLocked returns the hash of the stage file f, whose size is size,
// with its content synced to disk. It reuses a VerifyStage done since the
// last write. st.mu is held.
func (s *Store) verifyLocked(f *os.File, fi fs.FileInfo, st *stageState) (string, error) {
	size := fi.Size()
	if v := st.ver; v.ok && v.writes == st.writes && v.size == size && os.SameFile(v.file, fi) {
		return v.hash, nil
	}
	var got string
	if st.h != nil && st.hashed == size {
		got = fsproto.FormatHash(fsproto.HashB3, st.h.Sum(nil))
	} else {
		h, n, err := fsproto.HashReader(io.NewSectionReader(f, 0, size))
		if err != nil {
			return "", err
		}
		if n != size {
			return "", io.ErrUnexpectedEOF
		}
		got = h
	}
	if err := s.syncFile(f); err != nil {
		return "", err
	}
	st.ver = verified{ok: true, file: fi, writes: st.writes, size: size, hash: got}
	return got, nil
}

// VerifyStage hashes user's stage file and syncs it to disk ahead of a
// commit, so that PrepareStage, which runs under the scope's lock, can skip
// both when no write has landed since. It changes nothing a client can
// see, and its errors can be ignored: PrepareStage then does the work
// itself and reports them.
func (s *Store) VerifyStage(user, name string) error {
	if checkName(user) != nil || fsproto.CheckStageName(name) != nil {
		return fsproto.Missing("no such stage")
	}
	k := stageKey{user, name}
	sp := s.stagePath(user, name)
	f, err := os.Open(sp)
	if err != nil {
		return mapErr(err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	st := s.stageState(k, fi.Size())
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.sealed {
		return fsproto.Missing("stage already committed")
	}
	// A commit may have taken the file between Open and the lock, leaving
	// st new; then there is nothing to verify, and st must not linger.
	if now, err := os.Lstat(sp); err != nil || !os.SameFile(now, fi) {
		s.mu.Lock()
		if s.stages[k] == st {
			delete(s.stages, k)
		}
		s.mu.Unlock()
		return fsproto.Missing("no such stage")
	}
	if fi, err = f.Stat(); err != nil {
		return err
	}
	_, err = s.verifyLocked(f, fi, st)
	return err
}

// PrepareHash links content user already holds into tmp, with mode and
// mtime set. It returns fsproto.ErrMissing when no published file of user
// has that content.
func (s *Store) PrepareHash(user, hash string, mode uint32, mtime int64) (Prepared, error) {
	if s.opts.NoDedupe || checkName(user) != nil {
		return Prepared{}, fsproto.Missing("content not held")
	}
	bp, ok := s.blobPath(user, hash)
	if !ok {
		return Prepared{}, fsproto.Missing("content not held")
	}
	fi, err := os.Lstat(bp)
	// A blob with no other link is content the user no longer has.
	if err != nil || !fi.Mode().IsRegular() || nlink(fi) < 2 {
		return Prepared{}, fsproto.Missing("content not held")
	}
	tmp := s.newTmp()
	tp := s.tmpPath(tmp)
	if err := os.Link(bp, tp); err != nil {
		return Prepared{}, mapErr(err)
	}
	if err := s.setAttrs(tp, mode, mtime); err != nil {
		os.Remove(tp)
		return Prepared{}, err
	}
	return Prepared{Tmp: tmp, Hash: hash, Size: fi.Size()}, nil
}

// PrepareCopy puts a copy of the file or symlink at from in tmp: a hard
// link when dedupe is on, otherwise a byte copy. A directory is
// fsproto.ErrInvalid.
func (s *Store) PrepareCopy(r server.Root, from string) (Prepared, error) {
	fr, err := s.root(r)
	if err != nil {
		return Prepared{}, err
	}
	rf, err := rel(from)
	if err != nil {
		return Prepared{}, err
	}
	fi, err := fr.Lstat(rf)
	if err != nil {
		return Prepared{}, mapErr(err)
	}
	tmp := s.newTmp()
	tp := s.tmpPath(tmp)
	switch {
	case fi.IsDir():
		return Prepared{}, fsproto.Invalid("cannot copy a directory")
	case fi.Mode()&fs.ModeSymlink != 0:
		target, err := fr.Readlink(rf)
		if err != nil {
			return Prepared{}, err
		}
		if err := os.Symlink(target, tp); err != nil {
			return Prepared{}, err
		}
		return Prepared{Tmp: tmp, Size: int64(len(target))}, nil
	case !fi.Mode().IsRegular():
		return Prepared{}, fsproto.Invalid("not a regular file")
	}
	if !s.opts.NoDedupe {
		if err := os.Link(s.full(r, from), tp); err == nil {
			return Prepared{Tmp: tmp, Size: fi.Size()}, nil
		}
	}
	src, err := fr.Open(rf)
	if err != nil {
		return Prepared{}, mapErr(err)
	}
	defer src.Close()
	dst, err := os.OpenFile(tp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fi.Mode().Perm())
	if err != nil {
		return Prepared{}, err
	}
	_, err = io.Copy(dst, src)
	if err == nil {
		err = s.syncFile(dst)
	}
	if cerr := dst.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Chtimes(tp, fi.ModTime(), fi.ModTime())
	}
	if err != nil {
		os.Remove(tp)
		return Prepared{}, err
	}
	return Prepared{Tmp: tmp, Size: fi.Size()}, nil
}

// PrepareSymlink puts a new symlink in tmp.
func (s *Store) PrepareSymlink(target string) (Prepared, error) {
	tmp := s.newTmp()
	if err := os.Symlink(target, s.tmpPath(tmp)); err != nil {
		return Prepared{}, err
	}
	return Prepared{Tmp: tmp, Size: int64(len(target))}, nil
}

// Place atomically moves prepared content to path, replacing a file
// there, and syncs the directory. If the content is no longer in tmp, Place
// assumes an earlier call already moved it and returns nil, so it can be
// repeated after a crash.
func (s *Store) Place(p Prepared, r server.Root, path string) error {
	if err := s.PlaceNoSync(p, r, path); err != nil {
		return err
	}
	return s.syncDir(s.full(r, fsproto.Parent(path)))
}

// PlaceNoSync is Place without the sync of path's directory, so that
// several placements can share one; the caller then calls SyncDir.
func (s *Store) PlaceNoSync(p Prepared, r server.Root, path string) error {
	tp := s.tmpPath(p.Tmp)
	if _, err := os.Lstat(tp); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	fr, err := s.root(r)
	if err != nil {
		return err
	}
	if err := checkPlace(fr, path); err != nil {
		return err
	}
	return os.Rename(tp, s.full(r, path))
}

// SyncDir syncs the directory dir of root r to disk.
func (s *Store) SyncDir(r server.Root, dir string) error {
	return s.syncDir(s.full(r, dir))
}

// Discard removes prepared content that will not be placed.
func (s *Store) Discard(p Prepared) error {
	if p.Tmp == "" {
		return nil
	}
	err := os.RemoveAll(s.tmpPath(p.Tmp))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// CleanTmp removes everything in tmp. Call it at startup, after any
// placements recorded before a crash have been finished.
func (s *Store) CleanTmp() error {
	des, err := os.ReadDir(filepath.Join(s.dir, "tmp"))
	if err != nil {
		return err
	}
	for _, de := range des {
		if err := os.RemoveAll(s.tmpPath(de.Name())); err != nil {
			return err
		}
	}
	return nil
}

// GC removes blobs that no published file links to any more, and returns
// how many it removed.
func (s *Store) GC() (int, error) {
	users, err := os.ReadDir(filepath.Join(s.dir, "blobs"))
	if err != nil {
		return 0, err
	}
	n := 0
	for _, u := range users {
		dir := filepath.Join(s.dir, "blobs", u.Name())
		des, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, de := range des {
			fi, err := de.Info()
			if err == nil && nlink(fi) < 2 && os.Remove(filepath.Join(dir, de.Name())) == nil {
				n++
			}
		}
	}
	return n, nil
}

// server.Store.

func (s *Store) placeChecked(r server.Root, path string) error {
	fr, err := s.root(r)
	if err != nil {
		return err
	}
	return checkPlace(fr, path)
}

func (s *Store) place(p Prepared, r server.Root, path string) error {
	if err := s.Place(p, r, path); err != nil {
		s.Discard(p)
		return err
	}
	return nil
}

func (s *Store) Publish(user, stage string, r server.Root, path string, mode uint32, mtime int64, hash string) (string, int64, error) {
	if err := s.placeChecked(r, path); err != nil {
		return "", 0, err
	}
	p, err := s.PrepareStage(user, stage, hash, mode, mtime)
	if err != nil {
		return "", 0, err
	}
	return p.Hash, p.Size, s.place(p, r, path)
}

func (s *Store) PublishHash(user, hash string, r server.Root, path string, mode uint32, mtime int64) (int64, error) {
	if err := s.placeChecked(r, path); err != nil {
		return 0, err
	}
	p, err := s.PrepareHash(user, hash, mode, mtime)
	if err != nil {
		return 0, err
	}
	return p.Size, s.place(p, r, path)
}

func (s *Store) Copy(r server.Root, from, to string) error {
	if err := s.placeChecked(r, to); err != nil {
		return err
	}
	p, err := s.PrepareCopy(r, from)
	if err != nil {
		return err
	}
	return s.place(p, r, to)
}

func (s *Store) Symlink(r server.Root, path, target string, mtime int64) error {
	if err := s.placeChecked(r, path); err != nil {
		return err
	}
	p, err := s.PrepareSymlink(target)
	if err != nil {
		return err
	}
	return s.place(p, r, path)
}

// regular looks up a regular file in r.
func (s *Store) regular(r server.Root, path string) (*os.Root, string, error) {
	fr, err := s.root(r)
	if err != nil {
		return nil, "", err
	}
	rp, err := rel(path)
	if err != nil {
		return nil, "", err
	}
	fi, err := fr.Lstat(rp)
	if err != nil {
		return nil, "", mapErr(err)
	}
	if !fi.Mode().IsRegular() {
		return nil, "", fsproto.Invalid("not a file")
	}
	return fr, rp, nil
}

func (s *Store) Open(r server.Root, path string) (server.ContentFile, error) {
	fr, rp, err := s.regular(r, path)
	if err != nil {
		return nil, err
	}
	f, err := fr.Open(rp)
	if err != nil {
		return nil, mapErr(err)
	}
	return f, nil
}

func (s *Store) Hash(r server.Root, path string) (string, error) {
	f, err := s.Open(r, path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h, _, err := fsproto.HashReader(f)
	return h, err
}

func (s *Store) Mkdir(r server.Root, path string, mode uint32, mtime int64) error {
	fr, err := s.root(r)
	if err != nil {
		return err
	}
	rp, err := rel(path)
	if err != nil {
		return err
	}
	if _, err := fr.Lstat(rp); err == nil {
		return fsproto.Conflict(0)
	}
	if err := checkParent(fr, path); err != nil {
		return err
	}
	if err := fr.Mkdir(rp, dirMode(mode)); err != nil {
		return mapErr(err)
	}
	// Mkdir is subject to the umask; set the mode exactly.
	fr.Chmod(rp, dirMode(mode))
	t := time.Unix(mtime, 0)
	fr.Chtimes(rp, t, t)
	return s.syncDir(s.full(r, fsproto.Parent(path)))
}

func (s *Store) Remove(r server.Root, path string, recursive bool) error {
	if path == "/" {
		return fsproto.Invalid("cannot remove the root")
	}
	fr, err := s.root(r)
	if err != nil {
		return err
	}
	rp, err := rel(path)
	if err != nil {
		return err
	}
	fi, err := fr.Lstat(rp)
	if err != nil {
		return mapErr(err)
	}
	if fi.IsDir() && recursive {
		// Move the tree out of the namespace in one step, then delete it.
		tp := s.tmpPath(s.newTmp())
		if err := os.Rename(s.full(r, path), tp); err != nil {
			return err
		}
		if err := s.syncDir(s.full(r, fsproto.Parent(path))); err != nil {
			return err
		}
		return os.RemoveAll(tp)
	}
	if err := fr.Remove(rp); err != nil {
		if fi.IsDir() {
			if des, _ := os.ReadDir(s.full(r, path)); len(des) > 0 {
				return fsproto.Invalid("directory not empty")
			}
		}
		return mapErr(err)
	}
	return s.syncDir(s.full(r, fsproto.Parent(path)))
}

func (s *Store) Rename(r server.Root, from, to string) error {
	fr, err := s.root(r)
	if err != nil {
		return err
	}
	rf, err := rel(from)
	if err != nil {
		return err
	}
	rt, err := rel(to)
	if err != nil {
		return err
	}
	src, err := fr.Lstat(rf)
	if err != nil {
		return mapErr(err)
	}
	if from == "/" || to == "/" || (to != from && fsproto.Within(to, from)) {
		return fsproto.Invalid("bad rename")
	}
	if from == to {
		return nil
	}
	if err := checkParent(fr, to); err != nil {
		return err
	}
	if dst, err := fr.Lstat(rt); err == nil && (dst.IsDir() || src.IsDir()) {
		return fsproto.Invalid("cannot replace a directory")
	}
	if err := fr.Rename(rf, rt); err != nil {
		return mapErr(err)
	}
	if err := s.syncDir(s.full(r, fsproto.Parent(to))); err != nil {
		return err
	}
	if fsproto.Parent(to) != fsproto.Parent(from) {
		return s.syncDir(s.full(r, fsproto.Parent(from)))
	}
	return nil
}

func (s *Store) SetAttr(r server.Root, path string, mode *uint32, mtime *int64) error {
	fr, err := s.root(r)
	if err != nil {
		return err
	}
	rp, err := rel(path)
	if err != nil {
		return err
	}
	fi, err := fr.Lstat(rp)
	if err != nil {
		return mapErr(err)
	}
	if fi.Mode()&fs.ModeSymlink != 0 {
		return nil // symlink attributes live only in the metadata
	}
	if mode != nil {
		m := fileMode(*mode)
		if fi.IsDir() {
			m = dirMode(*mode)
		}
		if err := fr.Chmod(rp, m); err != nil {
			return err
		}
	}
	if mtime != nil {
		t := time.Unix(*mtime, 0)
		if err := fr.Chtimes(rp, t, t); err != nil {
			return err
		}
	}
	return nil
}
