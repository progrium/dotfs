package main

import "context"

// Owned by W3 (server/ninep, server/auth); this is a stub until then.
func init() {
	register(&command{
		name:    "serve",
		usage:   "serve [flags]",
		summary: "serve roots over 9P (TCP, Unix socket, and WebSocket)",
		run: func(ctx context.Context, args []string) error {
			return errNotImplemented
		},
	})
}
