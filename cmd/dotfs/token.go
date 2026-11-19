package main

import "context"

// Owned by W3 (server/auth); this is a stub until then.
func init() {
	register(&command{
		name:    "token",
		usage:   "token create <user>",
		summary: "issue a WebSocket bearer token for a user",
		run: func(ctx context.Context, args []string) error {
			return errNotImplemented
		},
	})
}
