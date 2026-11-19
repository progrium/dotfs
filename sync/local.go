package sync

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/progrium/dotfs/fsproto"
)

// StateDir is the folder, at the top of a synced folder, that holds the
// engine's state. It is never synced.
const StateDir = ".dotfs"

// lstat is what the engine looks at in a local path.
type lstat struct {
	exists bool
	typ    fsproto.Type
	mode   uint32 // permission bits
	size   int64
	mtime  int64 // nanoseconds
	ino    uint64
	target string // symlinks
}

func (e *Engine) osPath(p string) string {
	if p == "/" {
		return e.dir
	}
	return filepath.Join(e.dir, filepath.FromSlash(p[1:]))
}

func (e *Engine) tmpDir() string { return filepath.Join(e.dir, StateDir, "tmp") }

func statOf(fi fs.FileInfo) lstat {
	st := lstat{exists: true, mode: uint32(fi.Mode().Perm()), mtime: fi.ModTime().UnixNano(), ino: inode(fi)}
	switch {
	case fi.Mode().IsDir():
		st.typ = fsproto.TypeDir
	case fi.Mode()&fs.ModeSymlink != 0:
		st.typ = fsproto.TypeSymlink
	case fi.Mode().IsRegular():
		st.typ = fsproto.TypeFile
		st.size = fi.Size()
	default:
		st.typ = 0 // devices, sockets, and pipes are not synced
	}
	return st
}

// stat lstats the local path for protocol path p.
func (e *Engine) stat(p string) (lstat, error) {
	fi, err := os.Lstat(e.osPath(p))
	if errors.Is(err, fs.ErrNotExist) || isNotDir(err) {
		return lstat{}, nil
	}
	if err != nil {
		return lstat{}, err
	}
	st := statOf(fi)
	if st.typ == fsproto.TypeSymlink {
		st.target, err = os.Readlink(e.osPath(p))
		if err != nil {
			return lstat{}, err
		}
		st.size = int64(len(st.target))
	}
	return st, nil
}

func isNotDir(err error) bool {
	var pe *fs.PathError
	return errors.As(err, &pe) && strings.Contains(pe.Err.Error(), "not a directory")
}

// quickClean reports whether st matches what r recorded locally, without
// reading content.
func quickClean(r *row, st lstat) bool {
	if !st.exists || st.typ != r.Type {
		return false
	}
	switch r.Type {
	case fsproto.TypeDir:
		return true
	case fsproto.TypeSymlink:
		return st.target == r.Target
	}
	return st.size == r.LSize && st.mtime == r.LMtime && (r.LIno == 0 || st.ino == 0 || st.ino == r.LIno) && st.mode == r.Mode
}

func (e *Engine) hashFile(p string) (string, error) {
	f, err := os.Open(e.osPath(p))
	if err != nil {
		return "", err
	}
	defer f.Close()
	h, _, err := hashReader(f)
	return h, err
}

// localState says how a local path relates to its row.
type localState int

const (
	lsAbsent  localState = iota // no row, nothing on disk
	lsClean                     // disk matches the row
	lsDirty                     // disk differs from the row, or exists with no row
	lsDeleted                   // a row, but nothing on disk
)

// classify compares the local path p with its row r (which may be nil).
// For a file whose stat changed but content did not, it refreshes the row's
// local stat. It returns the stat and, for dirty files, their hash.
func (e *Engine) classify(p string, r *row) (localState, lstat, string, error) {
	st, err := e.stat(p)
	if err != nil {
		return 0, st, "", err
	}
	if st.exists && st.typ == 0 {
		// Not something we sync; leave it alone as if it were absent.
		st = lstat{}
	}
	switch {
	case r == nil && !st.exists:
		return lsAbsent, st, "", nil
	case r == nil:
		h := ""
		if st.typ == fsproto.TypeFile {
			if h, err = e.hashFile(p); err != nil {
				return 0, st, "", err
			}
		}
		return lsDirty, st, h, nil
	case !st.exists:
		return lsDeleted, st, "", nil
	case quickClean(r, st):
		return lsClean, st, r.Hash, nil
	case st.typ != r.Type || st.typ != fsproto.TypeFile:
		return lsDirty, st, "", nil
	}
	h, err := e.hashFile(p)
	if err != nil {
		return 0, st, "", err
	}
	if h == r.Hash && st.mode == r.Mode {
		if e.st.get(p) == r {
			// Touched but not changed: remember the new stat.
			nr := *r
			nr.LSize, nr.LMtime, nr.LIno = st.size, st.mtime, st.ino
			e.st.put(&nr)
		}
		return lsClean, st, h, nil
	}
	return lsDirty, st, h, nil
}

// tempFile creates a file in the state folder's tmp directory, which is on
// the same filesystem as the working files, so it can be renamed into place.
func (e *Engine) tempFile() (*os.File, error) {
	if err := os.MkdirAll(e.tmpDir(), 0o700); err != nil {
		return nil, err
	}
	return os.CreateTemp(e.tmpDir(), "fetch-")
}

// install moves a fetched temp file into place at p, as described by ent,
// unless the local path changed from want since the engine decided to.
// It returns the installed file's stat.
func (e *Engine) install(tmp string, p string, ent fsproto.Entry, want lstat) (lstat, error) {
	if err := os.Chmod(tmp, os.FileMode(ent.Mode&0o777)); err != nil {
		return lstat{}, err
	}
	mt := time.Unix(ent.Mtime, 0)
	if err := os.Chtimes(tmp, mt, mt); err != nil {
		return lstat{}, err
	}
	if err := e.mkdirParents(p); err != nil {
		return lstat{}, err
	}
	now, err := e.stat(p)
	if err != nil {
		return lstat{}, err
	}
	if now != want {
		return lstat{}, errLocalChanged
	}
	if now.exists && now.typ == fsproto.TypeDir {
		if err := os.Remove(e.osPath(p)); err != nil {
			return lstat{}, err
		}
	}
	if err := os.Rename(tmp, e.osPath(p)); err != nil {
		return lstat{}, err
	}
	return e.stat(p)
}

var errLocalChanged = errors.New("local file changed while syncing it")

// mkdirParents creates p's missing parent directories locally.
func (e *Engine) mkdirParents(p string) error {
	return os.MkdirAll(filepath.Dir(e.osPath(p)), 0o755)
}

// copyLocal copies the content of local path src into a new temp file and
// returns its name and hash.
func (e *Engine) copyLocal(src string) (string, string, error) {
	in, err := os.Open(e.osPath(src))
	if err != nil {
		return "", "", err
	}
	defer in.Close()
	return e.writeTemp(in)
}

// writeTemp copies r into a new temp file and returns its name and hash.
// Errors reading r are wrapped as readError.
func (e *Engine) writeTemp(r io.Reader) (string, string, error) {
	f, err := e.tempFile()
	if err != nil {
		return "", "", err
	}
	h := getHasher()
	defer putHasher(h)
	if _, err := copyBuffered(io.MultiWriter(f, h), readErrs{r}); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return "", "", err
	}
	var sum [32]byte
	return f.Name(), fsproto.FormatHash(fsproto.HashB3, h.Sum(sum[:0])), nil
}

// readError marks a failure reading the source of a copy, as opposed to
// writing the local temp file.
type readError struct{ err error }

func (e readError) Error() string { return e.err.Error() }
func (e readError) Unwrap() error { return e.err }

type readErrs struct{ r io.Reader }

func (r readErrs) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if err != nil && err != io.EOF {
		err = readError{err}
	}
	return n, err
}

// ConflictName returns the name a conflicting local copy of the file name
// gets: name.sync-conflict-<YYYYMMDD-HHMMSS>-<device>.ext, with the time in
// UTC. A leading dot does not start an extension, so ".bashrc" becomes
// ".bashrc.sync-conflict-...".
func ConflictName(name string, t time.Time, device string) string {
	base, ext := name, ""
	if i := strings.LastIndexByte(name, '.'); i > 0 {
		base, ext = name[:i], name[i:]
	}
	return base + ".sync-conflict-" + t.UTC().Format("20060102-150405") + "-" + device + ext
}

// moveAside renames the local path p to a free conflict name next to it and
// returns the new protocol path.
func (e *Engine) moveAside(p string) (string, error) {
	dir, name := fsproto.Parent(p), fsproto.Name(p)
	cn := ConflictName(name, e.now(), e.device)
	for i := 2; ; i++ {
		cp := fsproto.Join(dir, cn)
		st, err := e.stat(cp)
		if err != nil {
			return "", err
		}
		if !st.exists && e.st.get(cp) == nil {
			if err := os.Rename(e.osPath(p), e.osPath(cp)); err != nil {
				return "", err
			}
			e.logf("conflict: kept local %s as %s", p, cp)
			return cp, nil
		}
		cn = ConflictName(name, e.now(), e.device+"-"+strconv.Itoa(i))
	}
}

func randomID() string {
	var b [12]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// removeLocal removes the local path p if it is a file, a symlink, or an
// empty directory.
func (e *Engine) removeLocal(p string) error {
	err := os.Remove(e.osPath(p))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}
