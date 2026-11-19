package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/progrium/dotfs/fsproto"
)

// UploadChunk is how many bytes Upload writes per WriteAt.
var UploadChunk = 1 << 20

// Upload writes size bytes from src into the stage file name, in order,
// starting after whatever the stage file already holds. An upload
// interrupted by a dropped connection resumes, on a new connection, from
// the stage file's length without resending the bytes before it. If the
// stage file is longer than size it is aborted and written again. It
// returns how many bytes it wrote.
func Upload(ctx context.Context, c Conn, name string, src io.ReaderAt, size int64) (int64, error) {
	s, err := c.Stage(ctx, name)
	if err != nil {
		return 0, err
	}
	off, err := s.Size()
	if err != nil {
		s.Close()
		return 0, err
	}
	if off > size {
		s.Close()
		if _, err := Call(ctx, c, fsproto.AbortCmd{Stage: name}); err != nil {
			return 0, err
		}
		if s, err = c.Stage(ctx, name); err != nil {
			return 0, err
		}
		off = 0
	}
	defer s.Close()
	start := off
	buf := make([]byte, min(int64(UploadChunk), size-off))
	for off < size {
		if err := ctx.Err(); err != nil {
			return off - start, err
		}
		n := min(int64(len(buf)), size-off)
		m, err := src.ReadAt(buf[:n], off)
		if int64(m) < n {
			if err == nil || err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return off - start, fmt.Errorf("dotfs: upload source: %w", err)
		}
		if _, err := s.WriteAt(buf[:n], off); err != nil {
			return off - start, err
		}
		off += n
	}
	return off - start, s.Close()
}

// PutOptions are the optional parts of Put.
type PutOptions struct {
	Mtime int64  // 0 means now
	Mode  uint32 // 0 means 0644
	// Stage names the stage file; empty means one derived from the
	// content hash, so retrying an interrupted Put resumes its upload.
	Stage string
	// NoHashCommit skips trying commit-hash before uploading.
	NoHashCommit bool
}

// Put publishes size bytes from src at path if path is still at base: it
// tries commit-hash first, which costs no upload when the server already
// holds the content; otherwise it uploads (resuming a stage file left by
// an earlier attempt) and commits with hash= so the server checks the
// bytes. A version conflict is returned as a *fsproto.ReplyError matching
// fsproto.ErrConflict. The reply carries the new version and the hash.
func Put(ctx context.Context, c Conn, path string, base fsproto.Base, src io.ReaderAt, size int64, opts PutOptions) (fsproto.Reply, error) {
	hash, n, err := fsproto.HashReader(io.NewSectionReader(src, 0, size))
	if err != nil {
		return fsproto.Reply{}, err
	}
	if n != size {
		return fsproto.Reply{}, fmt.Errorf("dotfs: put source: %w", io.ErrUnexpectedEOF)
	}
	if opts.Mtime == 0 {
		opts.Mtime = time.Now().Unix()
	}
	if opts.Mode == 0 {
		opts.Mode = 0o644
	}
	if !opts.NoHashCommit {
		r, err := Call(ctx, c, fsproto.CommitHashCmd{Hash: hash, Path: path, Base: base, Mtime: opts.Mtime, Mode: opts.Mode})
		if err == nil {
			r.Hash = hash
			return r, nil
		}
		// missing: the server lacks the content; invalid: no commit-hash.
		if !errors.Is(err, fsproto.ErrMissing) && !errors.Is(err, fsproto.ErrInvalid) {
			return r, err
		}
	}
	name := opts.Stage
	if name == "" {
		_, digest, _ := fsproto.SplitHash(hash)
		name = "put-" + strings.ToLower(digest)
	}
	for attempt := 0; ; attempt++ {
		if _, err := Upload(ctx, c, name, src, size); err != nil {
			return fsproto.Reply{}, err
		}
		r, err := Call(ctx, c, fsproto.CommitCmd{Stage: name, Path: path, Base: base, Mtime: opts.Mtime, Mode: opts.Mode, Hash: hash})
		// missing can mean another Put of the same content committed
		// and removed the shared stage file first; upload it again.
		if errors.Is(err, fsproto.ErrMissing) && attempt == 0 {
			continue
		}
		return r, err
	}
}
