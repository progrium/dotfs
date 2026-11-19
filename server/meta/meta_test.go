package meta_test

import (
	"context"
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

func TestOpenTokens(t *testing.T) {
	dir := t.TempDir()
	_, m := open(t, dir, nil, false)
	defer m.Close()
	// A separate opener works beside a running Meta and sees its tokens.
	tk, err := meta.OpenTokens(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer tk.Close()
	servertest.TestTokens(t, func(t *testing.T) server.Tokens { return tk })
	tok, err := tk.CreateToken(context.Background(), "bob")
	if err != nil {
		t.Fatal(err)
	}
	if u, err := m.LookupToken(context.Background(), tok); err != nil || u != "bob" {
		t.Fatalf("Meta.LookupToken = %q %v", u, err)
	}
}
