package meta_test

import (
	"path/filepath"
	"testing"

	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/meta"
	"github.com/progrium/dotfs/server/servertest"
	"github.com/progrium/dotfs/server/store"
)

func open(t testing.TB, dir string, pub server.Publisher, sync bool) (*store.Store, *meta.Meta) {
	t.Helper()
	st, err := store.New(filepath.Join(dir, "data"), &store.Options{NoSync: !sync})
	if err != nil {
		t.Fatal(err)
	}
	m, err := meta.Open(filepath.Join(dir, "meta.db"), st, pub, &meta.Options{NoSync: !sync})
	if err != nil {
		t.Fatal(err)
	}
	return st, m
}

func TestMeta(t *testing.T) {
	servertest.TestMeta(t, func(t *testing.T, pub server.Publisher) (server.Store, server.Meta) {
		return open(t, t.TempDir(), pub, true)
	})
}

func TestTokens(t *testing.T) {
	_, m := open(t, t.TempDir(), nil, false)
	defer m.Close()
	servertest.TestTokens(t, func(t *testing.T) server.Tokens { return m })
}
