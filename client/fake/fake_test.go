package fake_test

import (
	"context"
	"errors"
	"testing"

	"github.com/progrium/dotfs/client"
	"github.com/progrium/dotfs/client/clienttest"
	"github.com/progrium/dotfs/client/fake"
	"github.com/progrium/dotfs/fsproto"
	sfake "github.com/progrium/dotfs/server/fake"
)

func TestConn(t *testing.T) {
	clienttest.TestConn(t, func(t *testing.T) clienttest.Dial {
		srv := sfake.NewServer()
		if _, err := srv.CreateRoot("alice", "home"); err != nil {
			t.Fatal(err)
		}
		return func(t *testing.T) client.Conn {
			c, err := fake.Dial(srv, "alice", "home")
			if err != nil {
				t.Fatal(err)
			}
			return c
		}
	})
}

func TestDialAndOffline(t *testing.T) {
	ctx := context.Background()
	srv := sfake.NewServer()
	srv.CreateRoot("alice", "home")
	if _, err := fake.Dial(srv, "bob", "home"); !errors.Is(err, fsproto.ErrDenied) {
		t.Fatalf("dialing someone else's root: %v", err)
	}
	c, err := fake.Dial(srv, "alice", "home")
	if err != nil {
		t.Fatal(err)
	}
	s, err := c.Events(ctx, fsproto.EventsSetup{})
	if err != nil {
		t.Fatal(err)
	}
	s.Next(ctx) // seq record
	c.SetOffline(true)
	if _, err := s.Next(ctx); !errors.Is(err, fake.ErrOffline) {
		t.Fatalf("stream after going offline: %v", err)
	}
	if _, err := c.Ctl(ctx, fsproto.CapsCmd{}); !errors.Is(err, fake.ErrOffline) {
		t.Fatalf("ctl offline: %v", err)
	}
	c.SetOffline(false)
	if _, err := client.Call(ctx, c, fsproto.MkdirCmd{Path: "/back"}); err != nil {
		t.Fatalf("ctl after reconnect: %v", err)
	}
}
