// Package httpd serves dotfs roots over HTTP, as docs/http-dotfs.md
// defines: the .fs/ files (index, events, ctl, and stage/) as resources
// that carry the same text as over 9P, and plain GET, HEAD, PUT, and DELETE
// on paths, with a path's version as its ETag.
//
// A Handler is mounted at Prefix ("/r/" by default) and serves each of the
// authenticated user's roots at <Prefix><root>/. The user comes from a
// bearer token (or the dotfs_token cookie), as on the 9P WebSocket
// upgrade. Every ctl command goes through server.Ctl, so a reply over HTTP
// is the same as over 9P.
package httpd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/auth"
)

// Header names.
const (
	HeaderSeq       = "Dotfs-Seq"
	HeaderEntry     = "Dotfs-Entry"
	HeaderDevice    = "Dotfs-Device"
	HeaderMode      = "Dotfs-Mode"
	HeaderMtime     = "Dotfs-Mtime"
	HeaderHash      = "Dotfs-Hash"
	HeaderStageSize = "Dotfs-Stage-Size"
)

// Config configures a Handler.
type Config struct {
	Meta   server.Meta
	Store  server.Store
	Hub    server.Hub
	Tokens server.Tokens
	// CreateRoots makes a request to a missing root of the user create it.
	CreateRoots bool
	// Prefix is the path the Handler is mounted at; "" means "/r/".
	Prefix string
	// MaxCtlBody caps a POST .fs/ctl body; 0 means 1 MiB.
	MaxCtlBody int64
	// Keepalive is how often an idle events stream gets a blank line (or an
	// SSE comment); 0 means 30 seconds.
	Keepalive time.Duration
	// Logf logs request errors; nil discards them.
	Logf func(format string, args ...any)
	// Now returns the time used for mtimes of PUTs without Dotfs-Mtime;
	// nil is time.Now.
	Now func() time.Time
}

// Handler is an http.Handler for http-dotfs.
type Handler struct {
	cfg Config
}

// New returns a Handler.
func New(cfg Config) *Handler {
	if cfg.Prefix == "" {
		cfg.Prefix = "/r/"
	}
	if cfg.MaxCtlBody == 0 {
		cfg.MaxCtlBody = 1 << 20
	}
	if cfg.Keepalive == 0 {
		cfg.Keepalive = 30 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Handler{cfg: cfg}
}

func (h *Handler) logf(format string, args ...any) {
	if h.cfg.Logf != nil {
		h.cfg.Logf(format, args...)
	}
}

// req is one request after authentication and path resolution.
type req struct {
	h      *Handler
	w      http.ResponseWriter
	r      *http.Request
	ctx    context.Context
	user   string
	root   server.Root
	origin server.Origin
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tok := auth.TokenFromRequest(r)
	if tok == "" || h.cfg.Tokens == nil {
		w.Header().Set("WWW-Authenticate", `Bearer realm="dotfs"`)
		http.Error(w, "token required", http.StatusUnauthorized)
		return
	}
	user, err := h.cfg.Tokens.LookupToken(r.Context(), tok)
	if err != nil {
		w.Header().Set("WWW-Authenticate", `Bearer realm="dotfs", error="invalid_token"`)
		http.Error(w, "bad token", http.StatusUnauthorized)
		return
	}
	rest, ok := strings.CutPrefix(r.URL.Path, h.cfg.Prefix)
	if !ok {
		http.NotFound(w, r)
		return
	}
	rootName, rel, _ := strings.Cut(rest, "/")
	if auth.CheckName(rootName) != nil {
		http.NotFound(w, r)
		return
	}
	if rel == "" && !strings.HasSuffix(rest, "/") {
		// The root itself is <prefix><root>/; point clients there.
		http.Redirect(w, r, h.cfg.Prefix+rootName+"/", http.StatusMovedPermanently)
		return
	}
	q := &req{h: h, w: w, r: r, ctx: r.Context(), user: user, root: server.NewRoot(user, rootName)}
	q.origin = server.Origin{User: user, Dev: r.Header.Get(HeaderDevice)}
	if err := h.openRoot(q.ctx, q.root); err != nil {
		q.fail(err)
		return
	}
	comps, dirSlash, err := splitRel(rel)
	if err != nil {
		q.fail(err)
		return
	}
	for i, c := range comps {
		if c == server.ReservedName {
			q.serveFS(comps[:i], comps[i+1:], dirSlash)
			return
		}
	}
	q.servePath(comps)
}

// openRoot checks that root exists, creating it if allowed.
func (h *Handler) openRoot(ctx context.Context, root server.Root) error {
	sc := server.RootScope(root)
	if _, err := h.cfg.Meta.Seq(ctx, sc); err != nil {
		if !errors.Is(err, server.ErrNoScope) {
			return err
		}
		if !h.cfg.CreateRoots {
			return fsproto.Missing("no such root")
		}
		if err := h.cfg.Store.CreateRoot(root); err != nil {
			return err
		}
		if err := h.cfg.Meta.CreateScope(ctx, sc); err != nil && !errors.Is(err, server.ErrScopeExists) {
			return err
		}
	}
	return nil
}

// splitRel splits the decoded path below a root into components. It
// rejects ".", "..", and empty components, and reports a trailing "/".
func splitRel(rel string) ([]string, bool, error) {
	if rel == "" {
		return nil, true, nil
	}
	slash := strings.HasSuffix(rel, "/")
	rel = strings.TrimSuffix(rel, "/")
	comps := strings.Split(rel, "/")
	for _, c := range comps {
		if c == "" || c == "." || c == ".." || strings.IndexByte(c, 0) >= 0 {
			return nil, false, fsproto.Invalid("bad path")
		}
	}
	return comps, slash, nil
}

// resolve finds the scope that holds the path made of comps, following
// nested scopes (flag i), and returns the path relative to that scope.
// A component that does not exist ends the search: the rest of the path
// is then looked up, and fails, in the scope reached so far.
func (q *req) resolve(comps []string) (server.Scope, string, error) {
	m := q.h.cfg.Meta
	sc := server.RootScope(q.root)
	cur := "/"
	for i, c := range comps {
		p := fsproto.Join(cur, c)
		if i == len(comps)-1 {
			return sc, p, nil
		}
		e, err := m.Stat(q.ctx, sc, p)
		if err != nil {
			if errors.Is(err, fsproto.ErrMissing) {
				return sc, joinAll(p, comps[i+1:]), nil
			}
			return sc, "", err
		}
		if e.Type == fsproto.TypeDir && e.Flags.Has('i') {
			ns := server.Scope{Root: sc.Root, Dir: sc.RootPath(p)}
			if _, err := m.Seq(q.ctx, ns); err == nil {
				sc, cur = ns, "/"
				continue
			}
		}
		cur = p
	}
	return sc, cur, nil
}

// scopeAt returns the scope whose root is the directory made of comps:
// the attach root for none, or a nested scope.
func (q *req) scopeAt(comps []string) (server.Scope, error) {
	if len(comps) == 0 {
		return server.RootScope(q.root), nil
	}
	sc, p, err := q.resolve(comps)
	if err != nil {
		return sc, err
	}
	e, err := q.h.cfg.Meta.Stat(q.ctx, sc, p)
	if err != nil {
		return sc, err
	}
	if e.Type != fsproto.TypeDir || !e.Flags.Has('i') {
		return sc, fsproto.Missing("not a scope")
	}
	ns := server.Scope{Root: sc.Root, Dir: sc.RootPath(p)}
	if _, err := q.h.cfg.Meta.Seq(q.ctx, ns); err != nil {
		return sc, fsproto.Missing("not a scope")
	}
	return ns, nil
}

func joinAll(p string, comps []string) string {
	for _, c := range comps {
		p = fsproto.Join(p, c)
	}
	return p
}

// ctl returns a server.Ctl for scope sc, carrying the request's device id.
func (q *req) ctl(sc server.Scope) *server.Ctl {
	c := server.NewCtl(q.h.cfg.Meta, q.h.cfg.Store, sc, q.user)
	if q.origin.Dev != "" {
		c.Exec(q.ctx, fsproto.HelloCmd{Device: q.origin.Dev})
	}
	return c
}

// statusFor maps a reply status to an HTTP status code.
func statusFor(s fsproto.Status, created bool) int {
	switch s {
	case fsproto.StatusOK:
		if created {
			return http.StatusCreated
		}
		return http.StatusOK
	case fsproto.StatusConflict:
		return http.StatusConflict
	case fsproto.StatusMissing:
		return http.StatusNotFound
	case fsproto.StatusDenied:
		return http.StatusForbidden
	default:
		return http.StatusBadRequest
	}
}

// reply writes a non-ok protocol outcome: its status code, and the reply
// line as the body. A conflict also carries the current version as ETag.
func (q *req) reply(cmd string, r fsproto.Reply) {
	if r.Status == fsproto.StatusConflict && r.Version != 0 {
		q.w.Header().Set("ETag", etag(r.Version))
	}
	q.text(statusFor(r.Status, false), r.Encode(cmd)+"\n")
}

// fail reports err: a protocol outcome as reply, anything else as 500.
func (q *req) fail(err error) {
	r, rerr := fsproto.ErrorReply(err)
	if rerr != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		q.h.logf("httpd %s %s: %v", q.r.Method, q.r.URL.Path, err)
		http.Error(q.w, "internal error", http.StatusInternalServerError)
		return
	}
	if r.Status == fsproto.StatusOK {
		r.Status = fsproto.StatusInvalid
	}
	q.reply("", r)
}

func (q *req) text(code int, body string) {
	q.w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	q.w.Header().Set("X-Content-Type-Options", "nosniff")
	q.w.WriteHeader(code)
	if q.r.Method != http.MethodHead {
		fmt.Fprint(q.w, body)
	}
}

func (q *req) methodNotAllowed(allow string) {
	q.w.Header().Set("Allow", allow)
	q.text(http.StatusMethodNotAllowed, "method not allowed\n")
}

func etag(v uint64) string { return `"` + strconv.FormatUint(v, 10) + `"` }

// parseETag parses one strong entity tag holding a version.
func parseETag(s string) (uint64, bool) {
	s = strings.TrimSpace(s)
	if len(s) < 3 || s[0] != '"' || s[len(s)-1] != '"' {
		return 0, false
	}
	v, err := strconv.ParseUint(s[1:len(s)-1], 10, 64)
	return v, err == nil
}

// base returns the base version that a request's If-Match or
// If-None-Match: * sets, or fsproto.Any without either.
func (q *req) base() (fsproto.Base, error) {
	im := q.r.Header.Get("If-Match")
	inm := q.r.Header.Get("If-None-Match")
	switch {
	case im != "" && inm != "":
		return fsproto.Any, fsproto.Invalid("both If-Match and If-None-Match")
	case im != "":
		v, ok := parseETag(im)
		if !ok {
			return fsproto.Any, fsproto.Invalid("If-Match must be one version")
		}
		return fsproto.At(v), nil
	case inm != "":
		if strings.TrimSpace(inm) != "*" {
			return fsproto.Any, fsproto.Invalid("If-None-Match must be *")
		}
		return fsproto.At(0), nil
	}
	return fsproto.Any, nil
}

func (q *req) setEntry(e fsproto.Entry) {
	q.w.Header().Set(HeaderEntry, e.Encode())
	if e.Version != 0 {
		q.w.Header().Set("ETag", etag(e.Version))
	} else if e.Hash != "" {
		q.w.Header().Set("ETag", `"`+e.Hash+`"`)
	}
}

func (q *req) setSeq(seq uint64) {
	q.w.Header().Set(HeaderSeq, strconv.FormatUint(seq, 10))
}
