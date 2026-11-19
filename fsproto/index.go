package fsproto

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// IndexFormatVersion is the format version this package reads and writes.
const IndexFormatVersion = 1

// EncodingZstd is the only body encoding the spec defines: one zstd frame.
const EncodingZstd = "zstd"

// IndexHeader is the first line of .fs/index:
//
//	fsindex 1 seq <N> [<encoding>] [key=value...]
type IndexHeader struct {
	Version  int    // format version, 1
	Seq      uint64 // the snapshot is consistent at this seq
	Encoding string // body encoding, "" for plain
	Attrs    Attrs
}

// Encode returns the header line.
func (h IndexHeader) Encode() string {
	var b strings.Builder
	b.WriteString("fsindex ")
	b.WriteString(strconv.Itoa(h.Version))
	b.WriteString(" seq ")
	b.WriteString(strconv.FormatUint(h.Seq, 10))
	if h.Encoding != "" {
		b.WriteByte(' ')
		b.WriteString(h.Encoding)
	}
	appendAttrs(&b, h.Attrs)
	return b.String()
}

// ParseIndexHeader parses the header line. It accepts any format version;
// callers check Version.
func ParseIndexHeader(line string) (IndexHeader, error) {
	var h IndexHeader
	f, err := splitFields(line)
	if err != nil {
		return h, err
	}
	if len(f) < 4 || f[0] != "fsindex" || f[2] != "seq" {
		return h, syntaxErr("bad index header %q", line)
	}
	v, err := strconv.Atoi(f[1])
	if err != nil || v < 1 {
		return h, syntaxErr("bad index version %q", f[1])
	}
	h.Version = v
	if h.Seq, err = parseUint(f[3], "seq"); err != nil {
		return h, err
	}
	rest := f[4:]
	if len(rest) > 0 && !strings.Contains(rest[0], "=") {
		if !validKey(rest[0]) {
			return h, syntaxErr("bad encoding %q", rest[0])
		}
		h.Encoding = rest[0]
		rest = rest[1:]
	}
	if h.Attrs, err = parseAttrs(rest); err != nil {
		return h, err
	}
	return h, nil
}

// ParseAccept parses an "accept <encoding> [...]" write to an index fid.
func ParseAccept(line string) ([]string, error) {
	f, err := splitFields(line)
	if err != nil {
		return nil, err
	}
	if f[0] != "accept" || len(f) < 2 {
		return nil, syntaxErr("bad accept %q", line)
	}
	for _, e := range f[1:] {
		if !validKey(e) {
			return nil, syntaxErr("bad encoding %q", e)
		}
	}
	return f[1:], nil
}

// EncodeAccept returns an accept line for the given encodings.
func EncodeAccept(encodings ...string) string {
	return "accept " + strings.Join(encodings, " ")
}

// ErrUnsorted is returned by IndexReader and IndexWriter when entries are
// not in strictly increasing path order.
var ErrUnsorted = errors.New("fsproto: index entries out of order")

// Decoder wraps a body reader for a named encoding. It returns an error for
// encodings it does not support.
type Decoder func(encoding string, r io.Reader) (io.Reader, error)

// Encoder wraps a body writer for a named encoding.
type Encoder func(encoding string, w io.Writer) (io.WriteCloser, error)

// IndexReader streams entries from an index file.
type IndexReader struct {
	h    IndexHeader
	br   *bufio.Reader
	prev string
	n    int
}

// NewIndexReader reads the header from r. If the header names an encoding,
// dec is used to decode the body; a nil dec only accepts plain bodies.
func NewIndexReader(r io.Reader, dec Decoder) (*IndexReader, error) {
	br := bufio.NewReaderSize(r, 64<<10)
	line, err := readLine(br)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	h, err := ParseIndexHeader(line)
	if err != nil {
		return nil, err
	}
	if h.Version != IndexFormatVersion {
		return nil, fmt.Errorf("fsproto: unsupported index version %d", h.Version)
	}
	ir := &IndexReader{h: h, br: br}
	if h.Encoding != "" {
		if dec == nil {
			return nil, fmt.Errorf("fsproto: no decoder for index encoding %q", h.Encoding)
		}
		body, err := dec(h.Encoding, br)
		if err != nil {
			return nil, err
		}
		ir.br = bufio.NewReaderSize(body, 64<<10)
	}
	return ir, nil
}

// Header returns the index header.
func (r *IndexReader) Header() IndexHeader { return r.h }

// Next returns the next entry, or io.EOF after the last one.
func (r *IndexReader) Next() (Entry, error) {
	line, err := readLine(r.br)
	if err != nil {
		return Entry{}, err
	}
	e, err := ParseEntry(line)
	if err != nil {
		return e, err
	}
	if r.n > 0 && ComparePaths(r.prev, e.Path) >= 0 {
		return e, fmt.Errorf("%w: %q after %q", ErrUnsorted, e.Path, r.prev)
	}
	r.prev = e.Path
	r.n++
	return e, nil
}

// readLine reads one newline-terminated line, without the newline. A final
// line without a newline is accepted. It returns io.EOF at a clean end.
func readLine(br *bufio.Reader) (string, error) {
	var buf []byte
	for {
		chunk, err := br.ReadSlice('\n')
		buf = append(buf, chunk...)
		if len(buf) > MaxLineLen+1 {
			return "", syntaxErr("line longer than %d bytes", MaxLineLen)
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if err == io.EOF {
			if len(buf) == 0 {
				return "", io.EOF
			}
			return string(buf), nil
		}
		if err != nil {
			return "", err
		}
		return string(buf[:len(buf)-1]), nil
	}
}

// IndexWriter writes an index file: the plain header line, then entries in
// the header's encoding.
type IndexWriter struct {
	w    *bufio.Writer
	body io.WriteCloser
	prev string
	n    int
	err  error
}

type nopCloser struct{ io.Writer }

func (nopCloser) Close() error { return nil }

// NewIndexWriter writes h to w. If h names an encoding, enc wraps the body.
func NewIndexWriter(w io.Writer, h IndexHeader, enc Encoder) (*IndexWriter, error) {
	if h.Version == 0 {
		h.Version = IndexFormatVersion
	}
	if _, err := io.WriteString(w, h.Encode()+"\n"); err != nil {
		return nil, err
	}
	iw := &IndexWriter{}
	if h.Encoding == "" {
		iw.body = nopCloser{w}
	} else {
		if enc == nil {
			return nil, fmt.Errorf("fsproto: no encoder for index encoding %q", h.Encoding)
		}
		body, err := enc(h.Encoding, w)
		if err != nil {
			return nil, err
		}
		iw.body = body
	}
	iw.w = bufio.NewWriterSize(iw.body, 64<<10)
	return iw, nil
}

// Write appends one entry; entries must arrive in strictly increasing path
// order.
func (w *IndexWriter) Write(e Entry) error {
	if w.err != nil {
		return w.err
	}
	if w.n > 0 && ComparePaths(w.prev, e.Path) >= 0 {
		return fmt.Errorf("%w: %q after %q", ErrUnsorted, e.Path, w.prev)
	}
	if err := e.Check(); err != nil {
		return err
	}
	w.prev = e.Path
	w.n++
	var b strings.Builder
	e.encodeTo(&b)
	b.WriteByte('\n')
	_, w.err = w.w.WriteString(b.String())
	return w.err
}

// Close flushes the body and closes the encoder, if any. It does not close
// the underlying writer.
func (w *IndexWriter) Close() error {
	if w.err != nil {
		return w.err
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.body.Close()
}
