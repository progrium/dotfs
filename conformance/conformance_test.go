package conformance_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/progrium/dotfs/conformance"
	"github.com/progrium/dotfs/conformance/shim"
)

func shimTarget(s *shim.Server) conformance.Target {
	return conformance.Target{
		Addr:      "shim",
		Dial:      func(context.Context) (io.ReadWriteCloser, error) { return s.Pipe(), nil },
		User:      "conform",
		OtherUser: "other",
		Trim: func(ctx context.Context, seq uint64) error {
			return s.Trim(ctx, "conform", "", seq)
		},
	}
}

// TestShim runs the whole suite against the W0 fakes behind the 9P shim.
func TestShim(t *testing.T) {
	s := shim.New(shim.Options{QueueLen: 64})
	rep := conformance.RunTest(t, shimTarget(s), conformance.Options{Overflow: 500})
	for _, p := range rep.Profiles {
		if !p.Met {
			t.Errorf("profile %s not met: %s", p.Name, p.Why)
		}
	}
	if rep.Total.Skip > 2 {
		t.Errorf("%d tests skipped against a server with every extension", rep.Total.Skip)
	}
}

// TestShimBugs checks that the suite catches deliberate protocol
// violations: each bug must fail the tests listed for it.
func TestShimBugs(t *testing.T) {
	cases := []struct {
		bug  shim.Bug
		run  string
		fail []string
	}{
		{shim.BugUnsortedIndex, "^index/", []string{"index/sorted"}},
		{shim.BugDropEvents, "^events/", []string{"events/since-replay"}},
		{shim.BugIgnoreFlush, "^events/flush$", []string{"events/flush"}},
		{shim.BugQidVers, "^(index|ctl)/", []string{"index/matches-stat", "ctl/versions"}},
	}
	for _, c := range cases {
		t.Run(string(c.bug), func(t *testing.T) {
			s := shim.New(shim.Options{Bugs: []shim.Bug{c.bug}})
			rep, err := conformance.Run(context.Background(), shimTarget(s), conformance.Options{Run: c.run, Overflow: 50, Timeout: 4 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			status := map[string]conformance.Status{}
			for _, r := range rep.Results {
				status[r.ID] = r.Status
			}
			for _, id := range c.fail {
				if status[id] != conformance.Fail {
					t.Errorf("%s: %s, want fail", id, status[id])
				}
			}
			if rep.OK {
				t.Errorf("report is OK despite the bug")
			}
		})
	}
}

// TestShimSubset runs the suite against a server with only some
// extensions, which must skip the rest and still meet its profile.
func TestShimSubset(t *testing.T) {
	s := shim.New(shim.Options{Extensions: []string{"index"}})
	rep, err := conformance.Run(context.Background(), shimTarget(s), conformance.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK {
		var b strings.Builder
		rep.WriteText(&b, false)
		t.Fatalf("index-only server failed:\n%s", b.String())
	}
	met := map[string]bool{}
	for _, p := range rep.Profiles {
		met[p.Name] = p.Met
	}
	if !met["read-only cache"] || met["watchable"] || met["sync"] {
		t.Errorf("profiles %+v, want only read-only cache", rep.Profiles)
	}

	s = shim.New(shim.Options{Extensions: []string{"events", "ctl"}})
	rep, err = conformance.Run(context.Background(), shimTarget(s), conformance.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK {
		var b strings.Builder
		rep.WriteText(&b, false)
		t.Fatalf("watchable server failed:\n%s", b.String())
	}
}
