package client

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/hugelgupf/p9/linux"
	"github.com/hugelgupf/p9/p9"

	"github.com/progrium/dotfs/fsproto"
)

var stageDir = []string{".fs", "stage"}

// Stage opens .fs/stage/<name> for writing, creating it when it does not
// exist yet.
func (c *Client) Stage(ctx context.Context, name string) (Stage, error) {
	if err := fsproto.CheckStageName(name); err != nil {
		return nil, err
	}
	path := "/.fs/stage/" + name
	names := append(append([]string{}, stageDir...), name)
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		f, err := c.walk(names)
		if err == nil {
			if _, _, err := f.Open(p9.WriteOnly); err != nil {
				f.Close()
				return nil, pathErr("stage", path, err)
			}
			return &stage{f: f, path: path}, nil
		}
		if !errors.Is(err, linux.ENOENT) {
			return nil, pathErr("stage", path, err)
		}
		dir, err := c.walk(stageDir)
		if err != nil {
			return nil, pathErr("stage", path, err)
		}
		nf, _, _, err := dir.Create(name, p9.WriteOnly, 0o600, p9.NoUID, p9.NoGID)
		if err == nil {
			return &stage{f: nf, path: path}, nil
		}
		dir.Close()
		// Someone else created it between the walk and the create.
		if !errors.Is(err, linux.EEXIST) || attempt > 0 {
			return nil, pathErr("stage create", path, err)
		}
	}
}

type stage struct {
	f    p9.File
	path string
}

func (s *stage) WriteAt(p []byte, off int64) (int, error) {
	n, err := s.f.WriteAt(p, off)
	if err == nil && n < len(p) {
		err = writeFull(s.f, p[n:], off+int64(n))
		if err == nil {
			n = len(p)
		}
	}
	return n, pathErr("stage write", s.path, err)
}

func (s *stage) Size() (int64, error) {
	_, _, attr, err := s.f.GetAttr(p9.AttrMask{Size: true})
	if err != nil {
		return 0, pathErr("stage stat", s.path, err)
	}
	return int64(attr.Size), nil
}

func (s *stage) Close() error { return pathErr("stage close", s.path, s.f.Close()) }

// Open opens a file for reading. Size is the length the server reported
// when it was opened.
func (c *Client) Open(ctx context.Context, path string) (File, error) {
	names, err := splitPath(path)
	if err != nil {
		return nil, err
	}
	f, attr, err := c.walkGetAttr(names)
	if err != nil {
		return nil, pathErr("open", path, err)
	}
	if _, _, err := f.Open(p9.ReadOnly); err != nil {
		f.Close()
		return nil, pathErr("open", path, err)
	}
	return &file{f: f, path: path, size: int64(attr.Size)}, nil
}

type file struct {
	f    p9.File
	path string
	size int64

	mu  sync.Mutex
	off int64
}

func (f *file) ReadAt(p []byte, off int64) (int, error) {
	total := 0
	for total < len(p) {
		n, err := f.f.ReadAt(p[total:], off+int64(total))
		total += n
		if err != nil {
			if err == io.EOF {
				return total, io.EOF
			}
			return total, pathErr("read", f.path, err)
		}
		if n == 0 {
			return total, io.EOF
		}
	}
	return total, nil
}

func (f *file) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	n, err := f.f.ReadAt(p, f.off)
	f.off += int64(n)
	if n > 0 && err == io.EOF {
		err = nil
	}
	if err != nil && err != io.EOF {
		err = pathErr("read", f.path, err)
	}
	return n, err
}

func (f *file) Size() int64  { return f.size }
func (f *file) Close() error { return pathErr("close", f.path, f.f.Close()) }

// Read returns a file's whole content.
func (c *Client) Read(ctx context.Context, path string) ([]byte, error) {
	f, err := c.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, 0, f.Size()+1)
	r := &ctxReader{ctx: ctx, r: f}
	for {
		if len(buf) == cap(buf) {
			buf = append(buf, 0)[:len(buf)]
		}
		n, err := r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if err == io.EOF {
			return buf, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *ctxReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
