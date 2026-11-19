package main

import "context"

// Owned by W5 (conformance); this is a stub until then.
func init() {
	register(&command{
		name:    "conform",
		usage:   "conform <addr>",
		summary: "run the conformance suite against a 9P server",
		run: func(ctx context.Context, args []string) error {
			return errNotImplemented
		},
	})
}
