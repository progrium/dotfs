package httpd

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

// servePath serves plain requests on a path below a root.
func (q *req) servePath(comps []string) {
	sc, p := server.RootScope(q.root), "/"
	if len(comps) > 0 {
		var err error
		if sc, p, err = q.resolve(comps); err != nil {
			q.fail(err)
			return
		}
	}
	switch q.r.Method {
	case http.MethodGet, http.MethodHead:
		q.get(sc, p)
	case http.MethodPut:
		q.put(sc, p)
	case http.MethodDelete:
		q.delete(sc, p)
	default:
		q.methodNotAllowed("GET, HEAD, PUT, DELETE")
	}
}

func (q *req) get(sc server.Scope, p string) {
	m := q.h.cfg.Meta
	for attempt := 0; ; attempt++ {
		e, err := m.Stat(q.ctx, sc, p)
		if err != nil {
			q.fail(err)
			return
		}
		switch e.Type {
		case fsproto.TypeDir:
			q.list(sc, p, e)
			return
		case fsproto.TypeSymlink:
			q.setEntry(e)
			q.w.Header().Set("Cache-Control", "private, no-cache")
			q.w.Header().Set("Content-Length", "0")
			q.w.WriteHeader(http.StatusOK)
			return
		}
		f, err := q.h.cfg.Store.Open(sc.Root, sc.RootPath(p))
		if err != nil {
			q.fail(err)
			return
		}
		// The file may have changed between the stat and the open; serve
		// it only if the stat still names what was opened.
		if again, err := m.Stat(q.ctx, sc, p); err != nil || again.Version != e.Version {
			f.Close()
			if attempt < 3 {
				continue
			}
			q.fail(fsproto.Denied("file is changing"))
			return
		}
		q.setEntry(e)
		q.w.Header().Set("Cache-Control", "private, no-cache")
		http.ServeContent(q.w, q.r, fsproto.Name(p), time.Unix(e.Mtime, 0), f)
		f.Close()
		return
	}
}

// list serves a directory in index format: its own line and its children,
// or everything below it with ?depth=infinity.
func (q *req) list(sc server.Scope, p string, e fsproto.Entry) {
	m := q.h.cfg.Meta
	if e.Flags.Has('i') {
		ns := server.Scope{Root: sc.Root, Dir: sc.RootPath(p)}
		if _, err := m.Seq(q.ctx, ns); err == nil {
			sc, p = ns, "/"
			if e, err = m.Stat(q.ctx, sc, p); err != nil {
				q.fail(err)
				return
			}
		}
	}
	var entries []fsproto.Entry
	var seq uint64
	var err error
	switch q.r.URL.Query().Get("depth") {
	case "", "1":
		if seq, err = m.Seq(q.ctx, sc); err == nil {
			var kids []fsproto.Entry
			if kids, err = m.ReadDir(q.ctx, sc, p); err == nil {
				entries = append([]fsproto.Entry{e}, kids...)
			}
		}
	case "infinity":
		seq, err = m.Entries(q.ctx, sc, func(x fsproto.Entry) error {
			if fsproto.Within(x.Path, p) {
				entries = append(entries, x)
			}
			return nil
		})
	default:
		err = fsproto.Invalid("depth must be 1 or infinity")
	}
	if err != nil {
		q.fail(err)
		return
	}
	sort.Slice(entries, func(i, j int) bool { return fsproto.ComparePaths(entries[i].Path, entries[j].Path) < 0 })
	var b strings.Builder
	b.WriteString(fsproto.IndexHeader{Version: fsproto.IndexFormatVersion, Seq: seq}.Encode())
	b.WriteByte('\n')
	for _, x := range entries {
		b.WriteString(x.Encode())
		b.WriteByte('\n')
	}
	q.setEntry(e)
	q.setSeq(seq)
	q.w.Header().Set("Cache-Control", "private, no-cache")
	q.text(http.StatusOK, b.String())
}

// put publishes the request body at p: a commit with the base from the
// preconditions, or an unconditional plain write without them.
func (q *req) put(sc server.Scope, p string) {
	if p == "/" {
		q.fail(fsproto.Invalid("cannot replace the root"))
		return
	}
	base, err := q.base()
	if err != nil {
		q.fail(err)
		return
	}
	old, err := q.h.cfg.Meta.Stat(q.ctx, sc, p)
	exists := err == nil
	if err != nil && !errors.Is(err, fsproto.ErrMissing) {
		q.fail(err)
		return
	}
	mode := uint32(0o644)
	if exists {
		mode = old.Mode
	}
	if s := q.r.Header.Get(HeaderMode); s != "" {
		v, err := strconv.ParseUint(s, 8, 32)
		if err != nil {
			q.fail(fsproto.Invalid("bad " + HeaderMode))
			return
		}
		mode = uint32(v)
	}
	mtime := q.h.cfg.Now().Unix()
	if s := q.r.Header.Get(HeaderMtime); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			q.fail(fsproto.Invalid("bad " + HeaderMtime))
			return
		}
		mtime = v
	}
	want := q.r.Header.Get(HeaderHash)
	if want != "" {
		if _, _, err := fsproto.SplitHash(want); err != nil {
			q.fail(fsproto.Invalid("bad " + HeaderHash))
			return
		}
	}

	st := q.h.cfg.Store
	stage := plainStageName()
	f, err := st.OpenStage(q.user, stage, true)
	if err != nil {
		q.fail(err)
		return
	}
	hasher := fsproto.NewB3()
	_, werr := copyAt(f, io.TeeReader(q.r.Body, hasher), 0)
	cerr := f.Close()
	if werr == nil {
		werr = cerr
	}
	if werr != nil {
		st.RemoveStage(q.user, stage)
		q.fail(werr)
		return
	}
	got := fsproto.FormatHash(fsproto.HashB3, hasher.Sum(nil))
	if want != "" && want != got {
		st.RemoveStage(q.user, stage)
		q.text(http.StatusUnprocessableEntity, "invalid msg=hash%20mismatch\n")
		return
	}
	w, err := q.h.cfg.Meta.Writer(sc)
	if err != nil {
		st.RemoveStage(q.user, stage)
		q.fail(err)
		return
	}
	cmd := fsproto.CommitCmd{Stage: stage, Path: p, Base: base, Mtime: mtime, Mode: mode, Hash: got}
	res, err := w.Commit(q.ctx, q.origin, cmd)
	if err != nil {
		st.RemoveStage(q.user, stage)
		q.fail(err)
		return
	}
	if res.Event.Op == fsproto.OpPut {
		q.setEntry(res.Event.Entry)
	} else if e, err := q.h.cfg.Meta.Stat(q.ctx, sc, p); err == nil {
		q.setEntry(e)
	}
	q.w.Header().Set("ETag", etag(res.Version))
	q.setSeq(res.Version)
	reply := fsproto.Reply{Status: fsproto.StatusOK, Version: res.Version, Hash: res.Hash}
	q.text(statusFor(fsproto.StatusOK, !exists), reply.Encode(fsproto.CmdCommit)+"\n")
}

// delete removes p: rm with the base from If-Match.
func (q *req) delete(sc server.Scope, p string) {
	if p == "/" {
		q.fail(fsproto.Invalid("cannot remove the root"))
		return
	}
	base, err := q.base()
	if err != nil {
		q.fail(err)
		return
	}
	w, err := q.h.cfg.Meta.Writer(sc)
	if err != nil {
		q.fail(err)
		return
	}
	res, err := w.Rm(q.ctx, q.origin, fsproto.RmCmd{Path: p, Base: base})
	if err != nil {
		q.fail(err)
		return
	}
	q.setSeq(res.Version)
	q.w.WriteHeader(http.StatusNoContent)
}

func plainStageName() string {
	b := make([]byte, 12)
	rand.Read(b)
	return plainStagePrefix + hex.EncodeToString(b)
}
