package sync

import (
	"hash"
	"io"
	gosync "sync"

	"github.com/progrium/dotfs/fsproto"
)

// copyBufSize is how much copyBuffered reads at a time, the same as
// io.Copy. Its buffers have hashSlack more capacity than that: given a
// write without 16 KiB of capacity past the end, the BLAKE3 hasher
// allocates a padded copy of the last few chunks, which cost as much as
// the copy buffers themselves.
const (
	copyBufSize = 32 << 10
	hashSlack   = 16 << 10
)

// Copy buffers and BLAKE3 hashers are reused across files: a bootstrap or
// scan of 100,000 files would otherwise allocate one of each per file.
var (
	copyBufs = gosync.Pool{New: func() any { b := make([]byte, copyBufSize+hashSlack); return &b }}
	hashers  = gosync.Pool{New: func() any { return fsproto.NewB3() }}
)

func getHasher() hash.Hash { return hashers.Get().(hash.Hash) }

func putHasher(h hash.Hash) {
	h.Reset()
	hashers.Put(h)
}

// copyBuffered is io.Copy with a pooled buffer. Unlike io.CopyBuffer, it
// never hands off to a WriterTo or ReaderFrom (*os.File's allocate a buffer
// of their own), and it passes dst slices with capacity to spare.
func copyBuffered(dst io.Writer, src io.Reader) (int64, error) {
	bp := copyBufs.Get().(*[]byte)
	defer copyBufs.Put(bp)
	buf := *bp
	var written int64
	for {
		nr, rerr := src.Read(buf[:copyBufSize])
		if nr > 0 {
			nw, werr := dst.Write(buf[:nr])
			written += int64(nw)
			if werr != nil {
				return written, werr
			}
			if nw != nr {
				return written, io.ErrShortWrite
			}
		}
		if rerr == io.EOF {
			return written, nil
		}
		if rerr != nil {
			return written, rerr
		}
	}
}

// hashReader returns the b3 hash of everything read from r, as
// fsproto.HashReader does, with a pooled hasher and buffer.
func hashReader(r io.Reader) (string, int64, error) {
	h := getHasher()
	defer putHasher(h)
	n, err := copyBuffered(h, r)
	if err != nil {
		return "", n, err
	}
	var sum [32]byte
	return fsproto.FormatHash(fsproto.HashB3, h.Sum(sum[:0])), n, nil
}
