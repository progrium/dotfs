// Package server defines the interfaces between the parts of the dotfs
// server: content storage (Store), metadata and its per-scope writers
// (Meta, Writer), the change log (Log), index snapshots (IndexSource), events
// fanout (Hub), and tokens (Tokens). Implementations live in subpackages;
// server/fake has in-memory versions and server/servertest has the behavior
// tests every implementation must pass.
//
// These interfaces are frozen after the foundation work: change them only in
// a small, separately announced commit.
package server

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/progrium/dotfs/fsproto"
)

// Root names a user's root: "<user>/<root>". The user part decides
// ownership.
type Root string

// NewRoot returns the Root for a user and root name.
func NewRoot(user, name string) Root { return Root(user + "/" + name) }

// User returns the owning user.
func (r Root) User() string {
	for i := 0; i < len(r); i++ {
		if r[i] == '/' {
			return string(r[:i])
		}
	}
	return string(r)
}

// Name returns the root's name without the user.
func (r Root) Name() string {
	for i := 0; i < len(r); i++ {
		if r[i] == '/' {
			return string(r[i+1:])
		}
	}
	return ""
}

// Scope is a root, or a directory in a root that carries its own nested
// .fs/ (flag i). Paths passed with a scope are relative to Dir, as on the
// wire; Store paths are relative to the root.
type Scope struct {
	Root Root
	Dir  string // "/" for the root scope
}

// RootScope returns the scope of a whole root.
func RootScope(r Root) Scope { return Scope{Root: r, Dir: "/"} }

// RootPath converts a scope-relative path to a root-relative one.
func (s Scope) RootPath(p string) string {
	if s.Dir == "/" || s.Dir == "" {
		return p
	}
	if p == "/" {
		return s.Dir
	}
	return s.Dir + p
}

// Errors shared by implementations. Protocol outcomes (conflict, missing,
// denied, and invalid) use fsproto's *ReplyError, built with
// fsproto.Conflict, fsproto.Missing, and so on, so they map straight to ctl
// replies with fsproto.ErrorReply.
var (
	// ErrNoScope means the scope does not exist.
	ErrNoScope = errors.New("dotfs: no such scope")
	// ErrScopeExists is returned by CreateScope for an existing scope.
	ErrScopeExists = errors.New("dotfs: scope exists")
	// ErrTrimmed means the log no longer holds the changes after a seq.
	ErrTrimmed = errors.New("dotfs: log trimmed")
)

// Origin says who is making a change.
type Origin struct {
	User string // the attaching user; owns stage files
	Dev  string // device id from hello, reported as dev= in events
}

// Result is the outcome of a successful change.
type Result struct {
	// Version is the seq assigned to the change: the new version of the
	// path, or for rm and rmtree, the seq of the removal.
	Version uint64
	// Hash is the content hash, for commits.
	Hash string
	// Event is the record published for the change.
	Event fsproto.Event
}

// StageFile is an upload in progress.
type StageFile interface {
	io.ReaderAt
	io.WriterAt
	// Size returns the current length, which a client reads to resume.
	Size() (int64, error)
	Close() error
}

// StageInfo describes a stage file.
type StageInfo struct {
	Name    string
	Size    int64
	ModTime time.Time
}

// ContentFile is published content opened for reading.
type ContentFile interface {
	io.ReaderAt
	io.Reader
	io.Seeker
	io.Closer
}

// Store holds file content: stage files and the plain files of every root.
// Paths are root-relative. It does not know about versions or events; Meta's
// writers call it while holding their scope's order.
type Store interface {
	// CreateRoot makes an empty root directory. It is idempotent.
	CreateRoot(root Root) error

	// OpenStage opens the stage file name of user, creating it when create
	// is set; otherwise a missing file is fsproto.ErrMissing.
	OpenStage(user, name string, create bool) (StageFile, error)
	// StatStage describes one stage file, or returns fsproto.ErrMissing.
	StatStage(user, name string) (StageInfo, error)
	// ListStages lists a user's stage files.
	ListStages(user string) ([]StageInfo, error)
	// RemoveStage discards a stage file. A missing file is not an error.
	RemoveStage(user, name string) error
	// ExpireStages removes stage files last written before t.
	ExpireStages(t time.Time) (int, error)

	// Publish atomically moves a stage file to path, sets its mode and
	// mtime, and returns its content hash and length. If hash is not empty
	// and does not match, nothing changes and the error is
	// fsproto.ErrInvalid. A missing stage file is fsproto.ErrMissing.
	Publish(user, stage string, root Root, path string, mode uint32, mtime int64, hash string) (string, int64, error)
	// PublishHash places content the store already holds at path, looking
	// only at content user can read. It returns the content length, or
	// fsproto.ErrMissing when no such content is held.
	PublishHash(user, hash string, root Root, path string, mode uint32, mtime int64) (int64, error)

	// Open opens a file for reading.
	Open(root Root, path string) (ContentFile, error)
	// Hash returns a file's content hash.
	Hash(root Root, path string) (string, error)
	// Mkdir creates a directory.
	Mkdir(root Root, path string, mode uint32, mtime int64) error
	// Symlink creates a symlink.
	Symlink(root Root, path, target string, mtime int64) error
	// Remove removes a file, symlink, or directory, recursively when
	// recursive is set.
	Remove(root Root, path string, recursive bool) error
	// Rename moves a path, replacing a file at the destination.
	Rename(root Root, from, to string) error
	// Copy copies a file (cheaply, when content is deduplicated).
	Copy(root Root, from, to string) error
	// SetAttr changes mode and mtime; nil leaves a value unchanged.
	SetAttr(root Root, path string, mode *uint32, mtime *int64) error
}

// Log is the per-scope change log.
type Log interface {
	// Seq returns the scope's current seq.
	Seq(ctx context.Context, s Scope) (uint64, error)
	// ReadLog calls fn for every change after since, in seq order, up to the
	// current seq when it was called. It returns ErrTrimmed if the log no
	// longer holds change since+1, or if since is past the current seq.
	ReadLog(ctx context.Context, s Scope, since uint64, fn func(fsproto.Event) error) error
	// Trim drops changes at or below seq, as retention does.
	Trim(ctx context.Context, s Scope, seq uint64) error
}

// IndexInfo describes an opened index.
type IndexInfo struct {
	Seq      uint64
	Encoding string // "" for plain
	Size     int64  // total bytes, or -1 if unknown until read
}

// IndexSource produces index snapshots.
type IndexSource interface {
	// Entries calls fn for every path in the scope, sorted, at one seq,
	// which it returns. The scope root "/" is the first entry.
	Entries(ctx context.Context, s Scope, fn func(fsproto.Entry) error) (uint64, error)
	// OpenIndex returns the complete .fs/index file (header and body) at
	// the current seq. accept lists encodings the client asked for, in its
	// order of preference; the source picks one it supports or plain.
	OpenIndex(ctx context.Context, s Scope, accept []string) (io.ReadCloser, IndexInfo, error)
}

// Writer makes changes to one scope. Calls are serialized per scope, so
// seqs are assigned without gaps or duplicates, and each successful change
// is published to the Hub before the call returns. Protocol outcomes are
// returned as fsproto *ReplyError values.
type Writer interface {
	Mkdir(ctx context.Context, o Origin, c fsproto.MkdirCmd) (Result, error)
	Rm(ctx context.Context, o Origin, c fsproto.RmCmd) (Result, error)
	Rmtree(ctx context.Context, o Origin, c fsproto.RmtreeCmd) (Result, error)
	Mv(ctx context.Context, o Origin, c fsproto.MvCmd) (Result, error)
	Cp(ctx context.Context, o Origin, c fsproto.CpCmd) (Result, error)
	SetFlags(ctx context.Context, o Origin, c fsproto.FlagsCmd) (Result, error)
	// Commit publishes the stage file c.Stage of o.User. fsproto.Any as the
	// base is allowed here, for plain 9P writes.
	Commit(ctx context.Context, o Origin, c fsproto.CommitCmd) (Result, error)
	CommitHash(ctx context.Context, o Origin, c fsproto.CommitHashCmd) (Result, error)
	// Symlink creates a symlink (plain 9P only; there is no ctl command).
	Symlink(ctx context.Context, o Origin, path, target string, base fsproto.Base) (Result, error)
	// SetAttr changes mode and mtime (plain 9P only); nil leaves a value.
	SetAttr(ctx context.Context, o Origin, path string, mode *uint32, mtime *int64) (Result, error)
}

// Meta is the metadata store: scopes, the path table, the log, and index
// snapshots.
type Meta interface {
	Log
	IndexSource

	// CreateScope creates a scope with an empty root directory at seq 0.
	// For a nested scope, Dir must be a directory in the parent.
	CreateScope(ctx context.Context, s Scope) error
	// Writer returns the scope's writer, or ErrNoScope.
	Writer(s Scope) (Writer, error)
	// Stat returns a path's entry, or fsproto.ErrMissing.
	Stat(ctx context.Context, s Scope, path string) (fsproto.Entry, error)
	// ReadDir returns a directory's children, sorted.
	ReadDir(ctx context.Context, s Scope, dir string) ([]fsproto.Entry, error)
	Close() error
}

// Publisher receives each change after it is committed.
type Publisher interface {
	Publish(s Scope, ev fsproto.Event)
}

// Stream is one open .fs/events fid.
type Stream interface {
	// Next blocks until at least one record is available, then returns
	// every record it has, in seq order. Canceling ctx (Tflush) returns
	// ctx.Err() and leaves the stream usable. After a reset record the next
	// call returns io.EOF.
	Next(ctx context.Context) ([]fsproto.Event, error)
	Close() error
}

// Hub fans changes out to open event streams.
type Hub interface {
	Publisher
	// Subscribe opens a stream. Without setup.HasSince, the first record is
	// seq <current>; with it, the stream replays from the log, then goes
	// live, without gaps (unless setup.Coalesce) or duplicates. A since the
	// log no longer covers yields a reset record.
	Subscribe(ctx context.Context, s Scope, setup fsproto.EventsSetup) (Stream, error)
}

// Tokens issues and checks WebSocket bearer tokens.
type Tokens interface {
	// CreateToken returns a new token for user.
	CreateToken(ctx context.Context, user string) (string, error)
	// LookupToken returns the user a token belongs to, or fsproto.ErrDenied.
	LookupToken(ctx context.Context, token string) (string, error)
}
