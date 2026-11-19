package client

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/hugelgupf/p9/linux"
	"github.com/hugelgupf/p9/p9"
	"github.com/klauspost/compress/zstd"

	"github.com/progrium/dotfs/fsproto"
)

var indexNames = []string{".fs", "index"}

// Index opens .fs/index and streams it. When accept is not empty it is
// written to the fid before the first read; if the server does not let the
// index be opened for writing (a read-only server), the index is read
// plain instead. zstd bodies are decoded as they stream.
func (c *Client) Index(ctx context.Context, accept []string) (IndexReader, error) {
	f, err := c.walk(indexNames)
	if err != nil {
		return nil, pathErr("index", "/.fs/index", err)
	}
	if err := c.openIndex(f, accept); err != nil {
		f.Close()
		return nil, pathErr("index", "/.fs/index", err)
	}
	r := &indexReader{f: f}
	r.src = &fileReader{ctx: ctx, f: f}
	ir, err := fsproto.NewIndexReader(r.src, r.decode)
	if err != nil {
		r.Close()
		return nil, pathErr("index", "/.fs/index", err)
	}
	r.IndexReader = ir
	return r, nil
}

func (c *Client) openIndex(f p9.File, accept []string) error {
	if len(accept) == 0 {
		_, _, err := f.Open(p9.ReadOnly)
		return err
	}
	if _, _, err := f.Open(p9.ReadWrite); err != nil {
		if !errors.Is(err, linux.EACCES) && !errors.Is(err, linux.EPERM) && !errors.Is(err, linux.EROFS) {
			return err
		}
		_, _, err = f.Open(p9.ReadOnly)
		return err
	}
	return writeFull(f, []byte(fsproto.EncodeAccept(accept...)), 0)
}

type indexReader struct {
	*fsproto.IndexReader
	f   p9.File
	src *fileReader
	dec *zstd.Decoder
}

func (r *indexReader) decode(enc string, body io.Reader) (io.Reader, error) {
	if enc != fsproto.EncodingZstd {
		return nil, fmt.Errorf("dotfs: unsupported index encoding %q", enc)
	}
	d, err := zstd.NewReader(body, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	r.dec = d
	return d, nil
}

func (r *indexReader) Close() error {
	if r.dec != nil {
		r.dec.Close()
	}
	return r.f.Close()
}

// fileReader reads a p9 file sequentially from offset 0.
type fileReader struct {
	ctx context.Context
	f   p9.File
	off int64
}

func (r *fileReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.f.ReadAt(p, r.off)
	r.off += int64(n)
	if n > 0 && err == io.EOF {
		err = nil
	}
	return n, mapErrno(err)
}
