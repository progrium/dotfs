// Command dotfs is the reference implementation of the dotfs protocol: a
// server, a sync client, a conformance suite, and tools, as subcommands.
//
// Each subcommand lives in its own file and registers itself in init, so
// workstreams add or change subcommands without touching this file.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"
)

// command is one subcommand.
type command struct {
	name    string
	usage   string // arguments, as in "sync <root> <dir>"
	summary string
	// run parses its own flags from args.
	run func(ctx context.Context, args []string) error
}

var commands = map[string]*command{}

// register adds a subcommand; call it from init.
func register(c *command) {
	if commands[c.name] != nil {
		panic("dotfs: duplicate command " + c.name)
	}
	commands[c.name] = c
}

// errNotImplemented is returned by subcommands that are still stubs.
var errNotImplemented = errors.New("not implemented yet")

// errUsage makes main print the subcommand's usage.
var errUsage = errors.New("usage")

// newFlags returns a FlagSet for a subcommand whose usage shows c's line.
func newFlags(c *command) *flag.FlagSet {
	fs := flag.NewFlagSet(c.name, flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: dotfs %s\n\n%s\n", c.usage, c.summary)
		fs.PrintDefaults()
	}
	return fs
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: dotfs <command> [arguments]\n\ncommands:\n")
	var names []string
	for n := range commands {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(os.Stderr, "  %-30s %s\n", commands[n].usage, commands[n].summary)
	}
}

func main() {
	if len(os.Args) < 2 || os.Args[1] == "-h" || os.Args[1] == "help" || os.Args[1] == "--help" {
		usage()
		os.Exit(2)
	}
	c := commands[os.Args[1]]
	if c == nil {
		fmt.Fprintf(os.Stderr, "dotfs: unknown command %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err := c.run(ctx, os.Args[2:])
	switch {
	case err == nil:
	case errors.Is(err, flag.ErrHelp):
		os.Exit(2)
	case errors.Is(err, errUsage):
		fmt.Fprintf(os.Stderr, "usage: dotfs %s\n", c.usage)
		os.Exit(2)
	default:
		fmt.Fprintf(os.Stderr, "dotfs %s: %v\n", c.name, err)
		os.Exit(1)
	}
}
