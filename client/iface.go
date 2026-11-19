// Package client defines Conn, a client's view of one dotfs scope over any
// 9P connection, and helpers on top of it. The real implementation over
// hugelgupf/p9 lives in this package; client/fake has an in-process one
// backed by server/fake, and client/clienttest has the behavior tests every
// implementation must pass.
//
// Conn is frozen after the foundation work: change it only in a small,
// separately announced commit.
package client

import (
	"context"
	"io"

	"github.com/progrium/dotfs/fsproto"
)

// Conn is a connection to one scope (an attach root, or a nested scope).
// It is safe for concurrent use: each call uses its own fids.
//
// Errors for paths that do not exist match fsproto.ErrMissing with
// errors.Is; transport failures are returned as they are.
type Conn interface {
	// Index opens .fs/index. When accept is not empty it is written first,
	// and the reader decodes whichever encoding the server picked.
	Index(ctx context.Context, accept []string) (IndexReader, error)
	// Events opens a stream on .fs/events with the given setup.
	Events(ctx context.Context, setup fsproto.EventsSetup) (EventStream, error)
	// Ctl sends one command on its own ctl fid and returns the reply. A
	// non-ok reply is not an error; use Call for that.
	Ctl(ctx context.Context, cmd fsproto.Command) (fsproto.Reply, error)
	// Stage opens (creating if needed) .fs/stage/<name> for writing.
	Stage(ctx context.Context, name string) (Stage, error)
	// Open opens a file for reading.
	Open(ctx context.Context, path string) (File, error)
	// Read returns a file's whole content.
	Read(ctx context.Context, path string) ([]byte, error)
	Close() error
}

// IndexReader streams index entries in path order.
type IndexReader interface {
	Header() fsproto.IndexHeader
	// Next returns the next entry, or io.EOF after the last.
	Next() (fsproto.Entry, error)
	Close() error
}

// EventStream is one open events fid.
type EventStream interface {
	// Next returns the next record, blocking while there is none.
	// Canceling ctx (a Tflush) returns ctx.Err() and leaves the stream
	// usable. After a reset record, Next returns io.EOF.
	Next(ctx context.Context) (fsproto.Event, error)
	Close() error
}

// Stage is a stage file open for writing.
type Stage interface {
	io.WriterAt
	// Size returns the stage file's current length, which is where an
	// interrupted upload written in order resumes.
	Size() (int64, error)
	Close() error
}

// File is a file open for reading.
type File interface {
	io.Reader
	io.ReaderAt
	// Size returns the file's length when it was opened.
	Size() int64
	Close() error
}

// Call sends cmd and turns a non-ok reply into a *fsproto.ReplyError.
func Call(ctx context.Context, c Conn, cmd fsproto.Command) (fsproto.Reply, error) {
	r, err := c.Ctl(ctx, cmd)
	if err != nil {
		return r, err
	}
	return r, r.Err()
}
