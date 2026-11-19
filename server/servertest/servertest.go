// Package servertest has behavior tests that every implementation of the
// server interfaces must pass, so fakes and real implementations stay in
// step. Call them from a _test.go file in the implementing package:
//
//	func TestMeta(t *testing.T) {
//		servertest.TestMeta(t, func(t *testing.T, pub server.Publisher) (server.Store, server.Meta) {
//			st := store.New(t.TempDir())
//			return st, meta.Open(filepath.Join(t.TempDir(), "meta.db"), st, pub)
//		})
//	}
package servertest

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

// NewStore returns an empty Store for one test.
type NewStore func(t *testing.T) server.Store

// NewMeta returns an empty Meta writing through its own Store and
// publishing every change to pub.
type NewMeta func(t *testing.T, pub server.Publisher) (server.Store, server.Meta)

// NewHub returns a Hub that replays from log and bounds each stream's live
// queue at about queueLen records.
type NewHub func(t *testing.T, log server.Log, queueLen int) server.Hub

// NewTokens returns an empty Tokens.
type NewTokens func(t *testing.T) server.Tokens

// Recorder is a Publisher that records what it receives.
type Recorder struct {
	mu     sync.Mutex
	Events map[server.Scope][]fsproto.Event
}

// Publish records ev.
func (r *Recorder) Publish(s server.Scope, ev fsproto.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.Events == nil {
		r.Events = map[server.Scope][]fsproto.Event{}
	}
	r.Events[s] = append(r.Events[s], ev)
}

// Get returns a copy of the events published for s.
func (r *Recorder) Get(s server.Scope) []fsproto.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]fsproto.Event(nil), r.Events[s]...)
}

var ctx = context.Background()

func isStatus(err error, target error) bool { return errors.Is(err, target) }

func wantStatus(t *testing.T, what string, err, target error) {
	t.Helper()
	if !isStatus(err, target) {
		t.Fatalf("%s: got error %v, want %v", what, err, target)
	}
}

func wantConflict(t *testing.T, what string, err error, version uint64) {
	t.Helper()
	var re *fsproto.ReplyError
	if !errors.As(err, &re) || re.Status != fsproto.StatusConflict || re.Version != version {
		t.Fatalf("%s: got error %v, want conflict %d", what, err, version)
	}
}

func must[T any](t *testing.T, what string) func(T, error) T {
	return func(v T, err error) T {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		return v
	}
}

func check(t *testing.T, what string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

// stage writes data to a new stage file of user.
func stage(t *testing.T, st server.Store, user, name string, data []byte) {
	t.Helper()
	f, err := st.OpenStage(user, name, true)
	check(t, "OpenStage", err)
	_, err = f.WriteAt(data, 0)
	check(t, "WriteAt", err)
	check(t, "Close", f.Close())
}

func readAll(t *testing.T, st server.Store, root server.Root, p string) string {
	t.Helper()
	f, err := st.Open(root, p)
	check(t, "Open "+p, err)
	defer f.Close()
	b, err := io.ReadAll(f)
	check(t, "ReadAll", err)
	return string(b)
}
