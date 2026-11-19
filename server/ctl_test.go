package server_test

import (
	"context"
	"testing"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/fake"
)

func TestCtlExecLine(t *testing.T) {
	ctx := context.Background()
	srv := fake.NewServer()
	sc, _ := srv.CreateRoot("alice", "home")
	c := server.NewCtl(srv.Meta, srv.Store, sc, "alice")
	stage, _ := srv.Store.OpenStage("alice", "s1", true)
	stage.WriteAt([]byte("hi"), 0)
	h := fsproto.HashBytes([]byte("hi"))
	for _, step := range [][2]string{
		{"hello mac%20a", "ok"},
		{"mkdir /d 0", "ok 1"},
		{"mkdir /d 0", "conflict 1"},
		{"commit s1 /d/f 0 7 0644 hash=" + h, "ok 2 " + h},
		{"commit s1 /d/g 0 7 0644", "missing msg=no%20such%20stage"},
		{"stat /d/f", "ok /d/f f 0644 2 7 2 " + h + " -"},
		{"cp /d/f /e 0", "ok 3"},
		{"commit-hash " + h + " /x 0 1 0600", "ok 4"},
		{"flags /d p", "ok 5"},
		{"mv /e /d/e - 0", "ok 6"},
		{"rm /d", "invalid msg=directory%20not%20empty"},
		{"rmtree /d 5", "ok 7"},
		{"rm /d", "missing"},
		{"abort s1", "ok"},
		{"rm /.fs/index", "invalid msg=reserved%20path"},
		{"frobnicate", "invalid"},
		{"mkdir", "invalid"},
	} {
		got, err := c.ExecLine(ctx, step[0])
		if err != nil {
			t.Fatalf("%q: %v", step[0], err)
		}
		if step[1] == "invalid" && len(got) >= 7 && got[:7] == "invalid" {
			continue
		}
		if got != step[1] {
			t.Errorf("%q -> %q, want %q", step[0], got, step[1])
		}
	}
	var devs []string
	srv.Meta.ReadLog(ctx, sc, 0, func(ev fsproto.Event) error { devs = append(devs, ev.Dev); return nil })
	if len(devs) != 7 || devs[0] != "mac a" {
		t.Errorf("dev ids in log: %q", devs)
	}
	// A Ctl offering only path operations refuses staged commit.
	c.Caps = fsproto.PathCommands
	if got, _ := c.ExecLine(ctx, "abort s1"); got != "invalid msg=unsupported%20command" {
		t.Errorf("unoffered command -> %q", got)
	}
	if got, _ := c.ExecLine(ctx, "caps"); got != "ok caps hello mkdir rm rmtree mv cp flags stat" {
		t.Errorf("caps -> %q", got)
	}
}
