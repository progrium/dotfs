package ninep

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"

	"github.com/hugelgupf/p9/linux"
	"github.com/klauspost/compress/zstd"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

// handle is an open file, independent of 9P dialect.
type handle interface {
	read(ctx context.Context, p []byte, off int64) (int, error)
	write(ctx context.Context, p []byte, off int64) (int, error)
	close(ctx context.Context) error
}

// openFlags says how a file is opened.
type openFlags struct {
	write bool
	trunc bool
}

// open opens n. Directories return a nil handle; they are listed with
// readdir.
func (n *node) open(ctx context.Context, fl openFlags) (handle, error) {
	srv := n.srv()
	switch n.kind {
	case kReal:
		e, err := n.stat(ctx)
		if err != nil {
			return nil, err
		}
		switch e.Type {
		case fsproto.TypeDir:
			if fl.write {
				return nil, linux.EISDIR
			}
			return nil, nil
		case fsproto.TypeSymlink:
			if fl.write {
				return nil, linux.EINVAL
			}
			return &bytesHandle{b: []byte(e.Target)}, nil
		}
		if fl.write {
			return newPlainWrite(n, fl.trunc), nil
		}
		f, err := srv.cfg.Store.Open(n.scope.Root, n.scope.RootPath(n.path))
		if err != nil {
			return nil, err
		}
		return &contentHandle{f: f}, nil
	case kIndex:
		return &indexHandle{n: n}, nil
	case kEvents:
		return newEventsHandle(srv, n.scope), nil
	case kCtl:
		return &ctlHandle{n: n}, nil
	case kStage:
		if fl.trunc {
			if err := srv.cfg.Store.RemoveStage(n.sess.user, n.path); err != nil {
				return nil, err
			}
		}
		f, err := srv.cfg.Store.OpenStage(n.sess.user, n.path, fl.trunc)
		if err != nil {
			return nil, err
		}
		return &stageHandle{f: f}, nil
	}
	if n.isDir() {
		if fl.write {
			return nil, linux.EISDIR
		}
		return nil, nil
	}
	return nil, linux.EINVAL
}

// readAtEOF turns io.EOF from a ReaderAt into a short read.
func readAtEOF(n int, err error) (int, error) {
	if errors.Is(err, io.EOF) {
		return n, nil
	}
	return n, err
}

type bytesHandle struct{ b []byte }

func (h *bytesHandle) read(_ context.Context, p []byte, off int64) (int, error) {
	if off >= int64(len(h.b)) {
		return 0, nil
	}
	return copy(p, h.b[off:]), nil
}
func (h *bytesHandle) write(context.Context, []byte, int64) (int, error) { return 0, linux.EBADF }
func (h *bytesHandle) close(context.Context) error                       { return nil }

// contentHandle reads published content.
type contentHandle struct{ f server.ContentFile }

func (h *contentHandle) read(_ context.Context, p []byte, off int64) (int, error) {
	return readAtEOF(h.f.ReadAt(p, off))
}
func (h *contentHandle) write(context.Context, []byte, int64) (int, error) { return 0, linux.EBADF }
func (h *contentHandle) close(context.Context) error                       { return h.f.Close() }

// stageHandle is an open .fs/stage/ file.
type stageHandle struct{ f server.StageFile }

func (h *stageHandle) read(_ context.Context, p []byte, off int64) (int, error) {
	return readAtEOF(h.f.ReadAt(p, off))
}
func (h *stageHandle) write(_ context.Context, p []byte, off int64) (int, error) {
	return h.f.WriteAt(p, off)
}
func (h *stageHandle) close(context.Context) error { return h.f.Close() }

// plainWrite is a real file opened for writing with plain 9P. Writes go to
// a private stage file, seeded with the current content unless the file
// was truncated, and are committed unconditionally (last writer wins) at
// clunk or fsync, so other clients see the old content or the new, and
// the change produces an event like any other.
type plainWrite struct {
	n *node

	mu    sync.Mutex
	stage string
	f     server.StageFile
	empty bool // start from empty content instead of the current file
	dirty bool
}

func newPlainWrite(n *node, trunc bool) *plainWrite {
	return &plainWrite{n: n, empty: trunc, dirty: trunc}
}

// ensure opens the stage file, seeding it. Callers hold mu.
func (w *plainWrite) ensure() error {
	if w.f != nil {
		return nil
	}
	st := w.n.srv().cfg.Store
	name := plainStageName()
	f, err := st.OpenStage(w.n.sess.user, name, true)
	if err != nil {
		return err
	}
	if !w.empty {
		src, err := st.Open(w.n.scope.Root, w.n.scope.RootPath(w.n.path))
		if err == nil {
			_, err = io.Copy(io.NewOffsetWriter(f, 0), src)
			src.Close()
		} else if errors.Is(err, fsproto.ErrMissing) {
			err = nil
		}
		if err != nil {
			f.Close()
			st.RemoveStage(w.n.sess.user, name)
			return err
		}
	}
	w.stage, w.f = name, f
	return nil
}

func (w *plainWrite) read(_ context.Context, p []byte, off int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f != nil {
		return readAtEOF(w.f.ReadAt(p, off))
	}
	if w.empty {
		return 0, nil
	}
	f, err := w.n.srv().cfg.Store.Open(w.n.scope.Root, w.n.scope.RootPath(w.n.path))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return readAtEOF(f.ReadAt(p, off))
}

func (w *plainWrite) write(_ context.Context, p []byte, off int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ensure(); err != nil {
		return 0, err
	}
	w.dirty = true
	return w.f.WriteAt(p, off)
}

// size returns the length of the pending content, or -1 when nothing is
// pending.
func (w *plainWrite) size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f != nil {
		if n, err := w.f.Size(); err == nil {
			return n
		}
	}
	if w.dirty && w.empty {
		return 0
	}
	return -1
}

// truncate sets the pending content's length.
func (w *plainWrite) truncate(size int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if size == 0 {
		w.dropLocked()
		w.empty, w.dirty = true, true
		return nil
	}
	if err := w.ensure(); err != nil {
		return err
	}
	st := w.n.srv().cfg.Store
	name := plainStageName()
	f, err := st.OpenStage(w.n.sess.user, name, true)
	if err != nil {
		return err
	}
	cur, err := w.f.Size()
	if err == nil {
		_, err = io.Copy(io.NewOffsetWriter(f, 0), io.NewSectionReader(w.f, 0, min(cur, size)))
	}
	if err == nil && size > cur {
		_, err = f.WriteAt([]byte{0}, size-1)
	}
	if err != nil {
		f.Close()
		st.RemoveStage(w.n.sess.user, name)
		return err
	}
	w.dropLocked()
	w.stage, w.f, w.dirty = name, f, true
	return nil
}

func (w *plainWrite) dropLocked() {
	if w.f != nil {
		w.f.Close()
		w.n.srv().cfg.Store.RemoveStage(w.n.sess.user, w.stage)
		w.f, w.stage = nil, ""
	}
}

// flush commits pending content.
func (w *plainWrite) flush(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.dirty {
		return nil
	}
	if w.f == nil {
		if err := w.ensure(); err != nil { // an empty stage file
			return err
		}
	}
	srv := w.n.srv()
	mode := uint32(0o644)
	if e, err := w.n.stat(ctx); err == nil {
		mode = e.Mode
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	w.f = nil
	wr, err := w.n.sess.writer(w.n.scope)
	if err == nil {
		_, err = wr.Commit(ctx, w.n.sess.origin(), fsproto.CommitCmd{
			Stage: w.stage, Path: w.n.path, Base: fsproto.Any,
			Mtime: srv.cfg.Now().Unix(), Mode: mode,
		})
	}
	if err != nil {
		srv.cfg.Store.RemoveStage(w.n.sess.user, w.stage)
	}
	w.stage, w.dirty, w.empty = "", false, false
	return err
}

func (w *plainWrite) close(ctx context.Context) error {
	err := w.flush(ctx)
	w.mu.Lock()
	w.dropLocked()
	w.mu.Unlock()
	return err
}

// indexHandle is an open .fs/index. An "accept" write before the first
// read picks encodings. The snapshot is taken at the first read and kept
// for the life of the fid, so reads at any offset see one seq.
type indexHandle struct {
	n *node

	mu     sync.Mutex
	accept []string
	data   []byte
	loaded bool
}

func (h *indexHandle) write(_ context.Context, p []byte, _ int64) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.loaded {
		return 0, linux.EINVAL
	}
	acc, err := fsproto.ParseAccept(strings.TrimSpace(string(p)))
	if err != nil {
		return 0, linux.EINVAL
	}
	h.accept = acc
	return len(p), nil
}

func (h *indexHandle) read(ctx context.Context, p []byte, off int64) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.loaded {
		b, err := loadIndex(ctx, h.n, h.accept)
		if err != nil {
			return 0, err
		}
		h.data, h.loaded = b, true
	}
	if off >= int64(len(h.data)) {
		return 0, nil
	}
	return copy(p, h.data[off:]), nil
}

func (h *indexHandle) close(context.Context) error { return nil }

var zenc, _ = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))

func loadIndex(ctx context.Context, n *node, accept []string) ([]byte, error) {
	rc, info, err := n.srv().cfg.Meta.OpenIndex(ctx, n.scope, accept)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	if info.Encoding != "" || !contains(accept, fsproto.EncodingZstd) {
		return b, nil
	}
	// The source sent plain text although zstd was accepted: compress the
	// body here, so accept zstd works whatever the source supports.
	line, body, ok := bytes.Cut(b, []byte("\n"))
	if !ok {
		return b, nil
	}
	h, err := fsproto.ParseIndexHeader(string(line))
	if err != nil {
		return nil, err
	}
	h.Encoding = fsproto.EncodingZstd
	out := append([]byte(h.Encode()), '\n')
	return zenc.EncodeAll(body, out), nil
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// ctlHandle is an open .fs/ctl: each write is one command, whose reply
// the next reads return, from offset 0.
type ctlHandle struct {
	n *node

	mu    sync.Mutex
	reply []byte
}

func (h *ctlHandle) write(ctx context.Context, p []byte, _ int64) (int, error) {
	if len(p) > h.n.srv().cfg.MaxCtlLine {
		return 0, linux.E2BIG
	}
	line := strings.TrimSuffix(string(p), "\n")
	r, err := h.n.sess.execCtl(ctx, h.n.scope, line)
	if err != nil {
		return 0, err
	}
	h.mu.Lock()
	h.reply = append([]byte(r), '\n')
	h.mu.Unlock()
	return len(p), nil
}

func (h *ctlHandle) read(_ context.Context, p []byte, off int64) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if off >= int64(len(h.reply)) {
		return 0, nil
	}
	return copy(p, h.reply[off:]), nil
}

func (h *ctlHandle) close(context.Context) error { return nil }
