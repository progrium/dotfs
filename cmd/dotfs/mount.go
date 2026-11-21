//go:build linux

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/progrium/dotfs/client"
	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/mount"
	"github.com/progrium/dotfs/mount/fuse"
)

func init() {
	c := &command{
		name:    "mount",
		usage:   "mount [flags] <root> <dir>",
		summary: "mount a root at a directory as on-demand files (Linux, FUSE)",
	}
	c.run = func(ctx context.Context, args []string) error {
		fs := newFlags(c)
		token := fs.String("token", os.Getenv("DOTFS_TOKEN"), "bearer token for a WebSocket or HTTP server (default $DOTFS_TOKEN)")
		device := fs.String("device", "", "device id for events and conflict copies (default: made from the host name)")
		cacheDir := fs.String("cache", "", "cache directory (default: $XDG_CACHE_HOME/dotfs/mount/<root>)")
		cacheSize := fs.Int64("cache-size", mount.DefaultCacheSize>>20, "content cache limit in MiB")
		timeout := fs.Duration("timeout", fuse.DefaultTimeout, "how long the kernel caches entries and attributes")
		pass := fs.Bool("passthrough", false, "let the kernel read cached files directly (FUSE passthrough; needs root, and opening a file for writing while it is open for reading fails)")
		noprefetch := fs.Bool("noprefetch", false, "do not fetch files flagged p after reading the index")
		allowOther := fs.Bool("allow-other", false, "let other users see the mount")
		debug := fs.Bool("debug", false, "log every FUSE request")
		quiet := fs.Bool("q", false, "log only problems")
		fs.Usage = func() {
			fmt.Fprintf(fs.Output(), `usage: dotfs %s

%s.

<root> is a server address followed by the root's name, as for dotfs sync:

  tcp:host:port/<name>, or host:port/<name>
  unix:/path/to/socket/<name>
  ws://host/path/<name>, wss://host/path/<name>
  http://host/r/<name>, https://host/r/<name>   (http-dotfs)

The whole tree is listed at once from the root's index; a file's content
is downloaded the first time it is opened and kept in the cache. Writes
are committed when a file is closed. Unmount with fusermount -u <dir>, or
stop the process.

`, c.usage, c.summary)
			fs.PrintDefaults()
		}
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 2 {
			return errUsage
		}
		dir := fs.Arg(1)
		addr, name, err := splitRoot(fs.Arg(0))
		if err != nil {
			return err
		}
		// The root without its token names the mount and its cache, so
		// the token stays out of logs and /proc/mounts, and a new token
		// keeps the cache.
		root := addr + "/" + strings.SplitN(name, "?", 2)[0]
		if *cacheDir == "" {
			base, err := os.UserCacheDir()
			if err != nil {
				return err
			}
			sum := sha256.Sum256([]byte(root))
			*cacheDir = filepath.Join(base, "dotfs", "mount", fsproto.Name(root)+"-"+hex.EncodeToString(sum[:4]))
		}
		logger := log.New(os.Stderr, "", log.LstdFlags)
		logf := func(format string, args ...any) {
			if *quiet && !problem(format) {
				return
			}
			logger.Printf(format, args...)
		}
		f, err := mount.New(ctx, mount.Config{
			Dial: func(ctx context.Context) (client.Conn, error) {
				ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
				defer cancel()
				return client.DialConn(ctx, addr, client.Options{Root: name, Token: *token, Device: *device})
			},
			Root:       root,
			CacheDir:   *cacheDir,
			CacheSize:  *cacheSize << 20,
			Device:     *device,
			NoPrefetch: *noprefetch,
			Logf:       logf,
		})
		if err != nil {
			return err
		}
		srv, err := fuse.Mount(dir, f, fuse.Options{
			Timeout:     *timeout,
			Passthrough: *pass,
			AllowOther:  *allowOther,
			Name:        root,
			Debug:       *debug,
		})
		if err != nil {
			f.Close()
			return err
		}
		logf("mounted %s at %s (cache %s, seq %d)", root, dir, *cacheDir, f.Seq())
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- f.Run(runCtx) }()
		go func() {
			<-ctx.Done()
			if err := srv.Unmount(); err != nil {
				logf("warning: unmount %s: %v; try fusermount -u %s", dir, err, dir)
			}
		}()
		srv.Wait()
		cancel()
		err = <-done
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		logf("unmounted %s", dir)
		return err
	}
	register(c)
}
