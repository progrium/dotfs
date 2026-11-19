package main

import (
	"context"
	"net"
	"testing"

	"github.com/progrium/dotfs/conformance/shim"
)

func TestParseAddr(t *testing.T) {
	for _, c := range []struct{ in, net, addr string }{
		{"localhost:5640", "tcp", "localhost:5640"},
		{"tcp:127.0.0.1:564", "tcp", "127.0.0.1:564"},
		{"unix:/tmp/dotfs.sock", "unix", "/tmp/dotfs.sock"},
		{"tcp6:[::1]:564", "tcp6", "[::1]:564"},
	} {
		n, a := parseAddr(c.in)
		if n != c.net || a != c.addr {
			t.Errorf("parseAddr(%q) = %q, %q", c.in, n, a)
		}
	}
}

// TestConformTCP runs the subcommand against the shim over a real TCP
// listener, as it would run against dotfs serve.
func TestConformTCP(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go shim.New(shim.Options{}).Serve(l)
	c := commands["conform"]
	if err := c.run(context.Background(), []string{"-json", "-overflow", "200", "-run", "^(discovery|ctl)/", l.Addr().String()}); err != nil {
		t.Fatal(err)
	}
}
