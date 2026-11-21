package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/progrium/dotfs/client"
	dsync "github.com/progrium/dotfs/sync"
)

func init() {
	c := &command{
		name:    "sync",
		usage:   "sync [flags] <root> <dir>",
		summary: "mirror a root into a local folder and keep it in sync",
	}
	c.run = func(ctx context.Context, args []string) error {
		fs := newFlags(c)
		token := fs.String("token", os.Getenv("DOTFS_TOKEN"), "bearer token for a WebSocket server (default $DOTFS_TOKEN)")
		device := fs.String("device", "", "device id for events and conflict copies (default: kept in the folder's state, or made from the host name)")
		once := fs.Bool("once", false, "sync once and exit instead of following changes")
		debounce := fs.Duration("debounce", time.Second, "how long local changes settle before they are committed")
		scan := fs.Duration("scan", time.Minute, "how often to rescan the whole folder")
		nowatch := fs.Bool("nowatch", false, "do not watch the folder; rely on the periodic scan")
		quiet := fs.Bool("q", false, "log only problems")
		memlimit := fs.Int64("memlimit", 128, "soft memory limit in MiB, past which the Go runtime collects garbage more often; 0 for none (ignored when $GOMEMLIMIT is set)")
		fs.Usage = func() {
			fmt.Fprintf(fs.Output(), `usage: dotfs %s

%s.

<root> is a server address followed by the root's name:

  tcp:host:port/<name>, or host:port/<name>
  unix:/path/to/socket/<name>
  ws://host/path/<name>, wss://host/path/<name>
  http://host/<name>, https://host/<name>   (http-dotfs, at /r/<name>/)

The folder's sync state lives in <dir>/%s/, which is never synced.

`, c.usage, c.summary, dsync.StateDir)
			fs.PrintDefaults()
		}
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 2 {
			return errUsage
		}
		if *memlimit > 0 && os.Getenv("GOMEMLIMIT") == "" {
			debug.SetMemoryLimit(*memlimit << 20)
		}
		addr, name, err := splitRoot(fs.Arg(0))
		if err != nil {
			return err
		}
		logger := log.New(os.Stderr, "", log.LstdFlags)
		logf := func(format string, args ...any) {
			if *quiet && !problem(format) {
				return
			}
			logger.Printf(format, args...)
		}
		e, err := dsync.New(dsync.Config{
			Dir:  fs.Arg(1),
			Root: fs.Arg(0),
			Dial: func(ctx context.Context) (client.Conn, error) {
				return client.DialConn(ctx, addr, client.Options{Root: name, Token: *token})
			},
			Device:       *device,
			Debounce:     *debounce,
			ScanInterval: *scan,
			NoWatch:      *nowatch,
			Logf:         logf,
		})
		if err != nil {
			return err
		}
		defer e.Close()
		logf("syncing %s with %s as %s", e.Dir(), fs.Arg(0), e.Device())
		if *once {
			return e.Sync(ctx)
		}
		return e.Run(ctx)
	}
	register(c)
}

// splitRoot splits "<addr>/<name>" at its last slash.
func splitRoot(s string) (addr, name string, err error) {
	i := strings.LastIndexByte(s, '/')
	if i <= 0 || i == len(s)-1 || strings.HasSuffix(s[:i], ":/") || strings.HasSuffix(s[:i], "/") {
		return "", "", fmt.Errorf("root %q: want <address>/<name>", s)
	}
	return s[:i], s[i+1:], nil
}

// problem reports whether a log line is worth showing with -q.
func problem(format string) bool {
	for _, w := range []string{"offline", "conflict", "giving up", "warning", "not uploading", ": %v"} {
		if strings.Contains(format, w) {
			return true
		}
	}
	return false
}
