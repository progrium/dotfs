package fake_test

import (
	"testing"

	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/fake"
	"github.com/progrium/dotfs/server/servertest"
)

func TestStore(t *testing.T) {
	servertest.TestStore(t, func(t *testing.T) server.Store { return fake.NewStore() })
}

func TestMeta(t *testing.T) {
	servertest.TestMeta(t, func(t *testing.T, pub server.Publisher) (server.Store, server.Meta) {
		st := fake.NewStore()
		return st, fake.NewMeta(st, pub)
	})
}

func TestHub(t *testing.T) {
	servertest.TestHub(t, func(t *testing.T, log server.Log, queueLen int) server.Hub {
		return fake.NewHub(log, queueLen)
	})
}

func TestTokens(t *testing.T) {
	servertest.TestTokens(t, func(t *testing.T) server.Tokens { return fake.NewTokens() })
}
