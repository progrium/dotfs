package main

import "context"

// Owned by W6 (sync); this is a stub until then.
func init() {
	register(&command{
		name:    "sync",
		usage:   "sync <root> <dir>",
		summary: "mirror a root into a local folder and keep it in sync",
		run: func(ctx context.Context, args []string) error {
			return errNotImplemented
		},
	})
}
