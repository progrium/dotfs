package shim_test

import (
	"context"
	"testing"

	"github.com/progrium/dotfs/client"
	"github.com/progrium/dotfs/client/clienttest"
	"github.com/progrium/dotfs/conformance/shim"
)

// TestReferenceClient runs the reference client's behavior tests against
// the shim, so the shim (which the conformance suite is checked against)
// and the client agree on the wire.
func TestReferenceClient(t *testing.T) {
	clienttest.TestConn(t, func(t *testing.T) clienttest.Dial {
		s := shim.New(shim.Options{})
		return func(t *testing.T) client.Conn {
			c, err := client.New(context.Background(), s.Pipe(), client.Options{Root: "home"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { c.Close() })
			return c
		}
	})
}
