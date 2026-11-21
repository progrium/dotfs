package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/progrium/dotfs/server/auth"
	"github.com/progrium/dotfs/server/events"
	"github.com/progrium/dotfs/server/httpd"
	"github.com/progrium/dotfs/server/meta"
	"github.com/progrium/dotfs/server/ninep"
	"github.com/progrium/dotfs/server/store"
)

func init() {
	c := &command{
		name:    "serve",
		usage:   "serve [flags]",
		summary: "serve roots over 9P (TCP, Unix socket, and WebSocket) and HTTP",
	}
	c.run = func(ctx context.Context, args []string) error {
		fl := newFlags(c)
		data := fl.String("data", "dotfs-data", "data `dir`: meta.db, roots/, stage/, and blobs/")
		tcp := fl.String("tcp", "127.0.0.1:5640", "TCP `addr` for 9P; empty disables")
		unix := fl.String("unix", "", "Unix socket `path` for 9P")
		httpAddr := fl.String("http", "", "HTTP `addr` serving 9P over WebSocket at /9p and http-dotfs at /r/<root>/; empty disables")
		httpAPI := fl.Bool("http-api", true, "serve http-dotfs at /r/<root>/ on the -http listener")
		cert := fl.String("tls-cert", "", "TLS certificate `file` for -http (wss://)")
		key := fl.String("tls-key", "", "TLS key `file` for -http")
		origins := fl.String("origins", "", "comma-separated extra WebSocket origin `patterns`")
		requireToken := fl.Bool("require-token", false, "require a token in the attach name (\"<root>?token=<t>\") on TCP and Unix listeners")
		createRoots := fl.Bool("create-roots", true, "create a missing root when its owner attaches")
		stageTTL := fl.Duration("stage-ttl", 24*time.Hour, "remove stage files not written for this long (at least 1h)")
		noDedupe := fl.Bool("no-dedupe", false, "turn off hard-link dedupe")
		keepLog := fl.Uint64("keep-log", 0, "changes each scope's log keeps; older `since` values get reset (0: 100,000)")
		if err := fl.Parse(args); err != nil {
			return err
		}
		if fl.NArg() != 0 {
			return errUsage
		}
		if *stageTTL < time.Hour {
			return errors.New("-stage-ttl must be at least 1h")
		}
		if err := os.MkdirAll(*data, 0o700); err != nil {
			return err
		}
		unlock, err := lockData(*data)
		if err != nil {
			return err
		}
		defer unlock()

		st, err := store.New(*data, &store.Options{NoDedupe: *noDedupe})
		if err != nil {
			return err
		}
		defer st.Close()
		m, err := meta.Open(filepath.Join(*data, "meta.db"), st, nil, &meta.Options{KeepLog: *keepLog})
		if err != nil {
			return err
		}
		defer m.Close()
		hub := events.New(m, events.Options{})
		m.SetPublisher(hub)

		srv := ninep.New(ninep.Config{
			Meta: m, Store: st, Hub: hub,
			Auth:        auth.Policy{Tokens: m, RequireToken: *requireToken},
			CreateRoots: *createRoots,
			Logf:        log.Printf,
		})
		defer srv.Close()

		errc := make(chan error, 3)
		n := 0
		if *tcp != "" {
			l, err := net.Listen("tcp", *tcp)
			if err != nil {
				return err
			}
			log.Printf("9p on tcp %s", l.Addr())
			n++
			go func() { errc <- srv.Serve(ctx, l) }()
		}
		if *unix != "" {
			os.Remove(*unix)
			l, err := net.Listen("unix", *unix)
			if err != nil {
				return err
			}
			defer os.Remove(*unix)
			log.Printf("9p on unix %s", *unix)
			n++
			go func() { errc <- srv.Serve(ctx, l) }()
		}
		if *httpAddr != "" {
			ws := &auth.WebSocket{Tokens: m, Server: srv, Logf: log.Printf}
			if *origins != "" {
				ws.OriginPatterns = strings.Split(*origins, ",")
			}
			mux := http.NewServeMux()
			mux.Handle("/9p", ws)
			if *httpAPI {
				mux.Handle("/r/", httpd.New(httpd.Config{
					Meta: m, Store: st, Hub: hub, Tokens: m,
					CreateRoots: *createRoots, Logf: log.Printf,
				}))
			}
			hs := &http.Server{Addr: *httpAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
			go func() {
				<-ctx.Done()
				hs.Close()
			}()
			log.Printf("9p over websocket on %s/9p", *httpAddr)
			if *httpAPI {
				log.Printf("http-dotfs on %s/r/<root>/", *httpAddr)
			}
			n++
			go func() {
				var err error
				if *cert != "" {
					err = hs.ListenAndServeTLS(*cert, *key)
				} else {
					err = hs.ListenAndServe()
				}
				if errors.Is(err, http.ErrServerClosed) {
					err = nil
				}
				errc <- err
			}()
		}
		if n == 0 {
			return fmt.Errorf("no listeners: set -tcp, -unix, or -http")
		}
		go housekeep(ctx, st, *stageTTL)
		// Serve until interrupted or a listener fails.
		return <-errc
	}
	register(c)
}

// housekeep expires old stage files and collects unreferenced blobs.
func housekeep(ctx context.Context, st *store.Store, ttl time.Duration) {
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for {
		if n, err := st.ExpireStages(time.Now().Add(-ttl)); err != nil {
			log.Printf("expire stages: %v", err)
		} else if n > 0 {
			log.Printf("expired %d stage files", n)
		}
		if _, err := st.GC(); err != nil {
			log.Printf("gc: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
