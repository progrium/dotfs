package events_test

import (
	"testing"

	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/events"
	"github.com/progrium/dotfs/server/servertest"
)

func TestHub(t *testing.T) {
	servertest.TestHub(t, func(t *testing.T, log server.Log, queueLen int) server.Hub {
		return events.New(log, events.Options{QueueLen: queueLen})
	})
}

// A ring smaller than the queue makes streams switch from the log to the ring
// to live in the middle of the shared tests.
func TestHubSmallRing(t *testing.T) {
	servertest.TestHub(t, func(t *testing.T, log server.Log, queueLen int) server.Hub {
		return events.New(log, events.Options{QueueLen: queueLen, RingLen: 3})
	})
}
