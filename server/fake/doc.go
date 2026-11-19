// Package fake has in-memory implementations of the server interfaces for
// one process: Store, Meta (with its Writers, Log, and IndexSource), Hub,
// and Tokens. They pass the shared behavior tests in server/servertest and
// are meant for tests of the packages built on top of them.
//
// Limits: indexes are served plain only (accept is ignored), the hub never
// coalesces, and nested scopes (flag i) are independent namespaces that the
// parent scope does not hide.
package fake

import (
	"context"

	"github.com/progrium/dotfs/server"
)

// Server bundles a wired set of fakes.
type Server struct {
	Store  *Store
	Meta   *Meta
	Hub    *Hub
	Tokens *Tokens
}

// NewServer returns fakes wired together: Meta writes through Store and
// publishes to Hub, which replays from Meta's log.
func NewServer() *Server {
	st := NewStore()
	m := NewMeta(st, nil)
	h := NewHub(m, DefaultQueueLen)
	m.SetPublisher(h)
	return &Server{Store: st, Meta: m, Hub: h, Tokens: NewTokens()}
}

// CreateRoot creates a root for user and returns its scope.
func (s *Server) CreateRoot(user, name string) (server.Scope, error) {
	sc := server.RootScope(server.NewRoot(user, name))
	return sc, s.Meta.CreateScope(context.Background(), sc)
}
