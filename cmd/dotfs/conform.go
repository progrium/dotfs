package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/progrium/dotfs/conformance"
	"github.com/progrium/dotfs/conformance/shim"
)

// Owned by W5 (conformance).
func init() {
	c := &command{
		name:  "conform",
		usage: "conform [flags] <addr>",
		summary: "run the conformance suite against a 9P server at <addr>: host:port or tcp:host:port, " +
			"unix:<path>, or \"shim\" for the built-in fake server",
	}
	c.run = func(ctx context.Context, args []string) error { return runConform(ctx, c, args) }
	register(c)
}

// errConformFailed makes dotfs exit 1 after the report is printed.
var errConformFailed = errors.New("conformance tests failed")

func runConform(ctx context.Context, c *command, args []string) error {
	fs := newFlags(c)
	jsonOut := fs.Bool("json", false, "write the report as JSON")
	verbose := fs.Bool("v", false, "show notes on passing and skipped tests, and progress on stderr")
	run := fs.String("run", "", "run only tests whose ID matches this regular expression")
	user := fs.String("user", "conform", "user name sent in Tattach")
	aname := fs.String("aname", "", "attach name: the root (and token, where the server takes one)")
	other := fs.String("other-user", "", "a second user that may attach, to check stage files are private")
	timeout := fs.Duration("timeout", 30*time.Second, "time limit per test")
	overflow := fs.Int("overflow", 3000, "changes the overflow test makes before reading")
	list := fs.Bool("list", false, "list the tests and exit")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *list {
		for _, t := range conformance.Tests() {
			fmt.Printf("%-28s %-5s %s\n    %s\n", t.ID, t.Level, t.Name, t.Section)
		}
		return nil
	}
	if fs.NArg() != 1 {
		return errUsage
	}
	addr := fs.Arg(0)
	target := conformance.Target{Addr: addr, User: *user, Aname: *aname, OtherUser: *other}
	if addr == "shim" {
		s := shim.New(shim.Options{QueueLen: 256})
		target.Dial = func(context.Context) (io.ReadWriteCloser, error) { return s.Pipe(), nil }
		target.Trim = func(ctx context.Context, seq uint64) error { return s.Trim(ctx, *user, *aname, seq) }
		if target.OtherUser == "" {
			target.OtherUser = *user + "-other"
		}
	} else {
		network, address := parseAddr(addr)
		target.Dial = func(ctx context.Context) (io.ReadWriteCloser, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, address)
		}
	}
	opts := conformance.Options{Run: *run, Timeout: *timeout, Overflow: *overflow}
	if *verbose {
		opts.Progress = func(r conformance.Result) {
			fmt.Fprintf(os.Stderr, "%-4s %s (%v)\n", strings.ToUpper(string(r.Status)), r.ID, r.Duration.Round(time.Millisecond))
		}
	}
	rep, err := conformance.Run(ctx, target, opts)
	if err != nil {
		return err
	}
	if *jsonOut {
		if err := rep.WriteJSON(os.Stdout); err != nil {
			return err
		}
	} else {
		rep.WriteText(os.Stdout, *verbose)
	}
	if !rep.OK {
		return errConformFailed
	}
	return nil
}

// parseAddr splits "tcp:host:port", "unix:/path", or a bare "host:port".
func parseAddr(addr string) (network, address string) {
	for _, n := range []string{"tcp", "tcp4", "tcp6", "unix"} {
		if strings.HasPrefix(addr, n+":") {
			return n, addr[len(n)+1:]
		}
	}
	return "tcp", addr
}
