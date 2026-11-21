package httpd

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

// plainStagePrefix starts the names of the private stage files that plain
// PUTs use. It is the prefix plain 9P writes use, so .fs/stage/ listings
// over 9P hide these files too, and clients may not name them.
const plainStagePrefix = ".dotfs-plain-"

// serveFS serves <scope>/.fs/<rest>.
func (q *req) serveFS(scopeComps, rest []string, dirSlash bool) {
	sc, err := q.scopeAt(scopeComps)
	if err != nil {
		q.fail(err)
		return
	}
	switch {
	case len(rest) == 0:
		if !dirSlash {
			http.Redirect(q.w, q.r, q.r.URL.Path+"/", http.StatusMovedPermanently)
			return
		}
		if q.r.Method != http.MethodGet && q.r.Method != http.MethodHead {
			q.methodNotAllowed("GET, HEAD")
			return
		}
		q.text(http.StatusOK, "index\nevents\nctl\nstage/\n")
	case len(rest) == 1 && rest[0] == "index":
		q.serveIndex(sc)
	case len(rest) == 1 && rest[0] == "events":
		q.serveEvents(sc)
	case len(rest) == 1 && rest[0] == "ctl":
		q.serveCtl(sc)
	case len(rest) == 2 && rest[0] == "stage":
		q.serveStage(rest[1])
	default:
		q.fail(fsproto.Missing("no such file"))
	}
}

var zenc, _ = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))

func acceptsZstd(r *http.Request) bool {
	for _, part := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		name, params, _ := strings.Cut(strings.TrimSpace(part), ";")
		if strings.EqualFold(strings.TrimSpace(name), "zstd") {
			qv, ok := strings.CutPrefix(strings.ReplaceAll(params, " ", ""), "q=")
			if !ok {
				return true
			}
			f, err := strconv.ParseFloat(qv, 64)
			return err == nil && f > 0
		}
	}
	return false
}

// serveIndex serves GET .fs/index: the index format, compressed as a whole
// with zstd when the client accepts it.
func (q *req) serveIndex(sc server.Scope) {
	if q.r.Method != http.MethodGet && q.r.Method != http.MethodHead {
		q.methodNotAllowed("GET, HEAD")
		return
	}
	zs := acceptsZstd(q.r)
	var accept []string
	if zs {
		accept = []string{fsproto.EncodingZstd}
	}
	rc, _, err := q.h.cfg.Meta.OpenIndex(q.ctx, sc, accept)
	if err != nil {
		q.fail(err)
		return
	}
	defer rc.Close()
	br := bufio.NewReader(rc)
	line, err := br.ReadString('\n')
	if err != nil {
		q.fail(err)
		return
	}
	hdr, err := fsproto.ParseIndexHeader(strings.TrimSuffix(line, "\n"))
	if err != nil {
		q.fail(err)
		return
	}
	h := q.w.Header()
	h.Set("Content-Type", "text/plain; charset=utf-8")
	h.Set("Cache-Control", "private, no-cache")
	h.Set("Vary", "Accept-Encoding")
	h.Set("ETag", etag(hdr.Seq))
	q.setSeq(hdr.Seq)
	if inm := q.r.Header.Get("If-None-Match"); inm != "" {
		if v, ok := parseETag(inm); ok && v == hdr.Seq {
			q.w.WriteHeader(http.StatusNotModified)
			return
		}
	}
	body := hdr
	body.Encoding = ""
	head := []byte(body.Encode() + "\n")
	if !zs {
		q.w.WriteHeader(http.StatusOK)
		if q.r.Method == http.MethodHead {
			return
		}
		q.w.Write(head)
		io.Copy(q.w, br)
		return
	}
	h.Set("Content-Encoding", "zstd")
	q.w.WriteHeader(http.StatusOK)
	if q.r.Method == http.MethodHead {
		return
	}
	if hdr.Encoding == fsproto.EncodingZstd {
		// The source already holds the body as one zstd frame: send the
		// header line as a frame of its own before it, since a zstd
		// stream may be several frames back to back.
		q.w.Write(zenc.EncodeAll(head, nil))
		io.Copy(q.w, br)
		return
	}
	zw, err := zstd.NewWriter(q.w, zstd.WithEncoderConcurrency(1))
	if err != nil {
		q.h.logf("httpd: zstd: %v", err)
		return
	}
	zw.Write(head)
	io.Copy(zw, br)
	zw.Close()
}

// serveEvents serves GET .fs/events as streamed lines, server-sent events,
// or a long poll.
func (q *req) serveEvents(sc server.Scope) {
	if q.r.Method != http.MethodGet {
		q.methodNotAllowed("GET")
		return
	}
	var setup fsproto.EventsSetup
	qs := q.r.URL.Query()
	since := qs.Get("since")
	if id := q.r.Header.Get("Last-Event-ID"); id != "" {
		since = id
	}
	if since != "" {
		v, err := strconv.ParseUint(since, 10, 64)
		if err != nil {
			q.fail(fsproto.Invalid("bad since"))
			return
		}
		setup.Since, setup.HasSince = v, true
	}
	for _, p := range qs["path"] {
		if err := fsproto.CheckPath(p); err != nil {
			q.fail(fsproto.Invalid("bad path"))
			return
		}
		setup.Paths = append(setup.Paths, p)
	}
	switch qs.Get("coalesce") {
	case "", "0", "false":
	default:
		setup.Coalesce = true
	}
	var wait time.Duration
	if ws := qs.Get("wait"); ws != "" {
		n, err := strconv.ParseUint(ws, 10, 32)
		if err != nil {
			q.fail(fsproto.Invalid("bad wait"))
			return
		}
		wait = time.Duration(n) * time.Second
	}
	sse := strings.Contains(q.r.Header.Get("Accept"), "text/event-stream")

	st, err := q.h.cfg.Hub.Subscribe(q.ctx, sc, setup)
	if err != nil {
		q.fail(err)
		return
	}
	defer st.Close()

	h := q.w.Header()
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	if sse {
		h.Set("Content-Type", "text/event-stream")
	} else {
		h.Set("Content-Type", "text/plain; charset=utf-8")
	}
	h.Set("X-Accel-Buffering", "no")
	rc := http.NewResponseController(q.w)

	if wait > 0 || qs.Has("wait") {
		q.longPoll(st, wait, sse)
		return
	}
	q.w.WriteHeader(http.StatusOK)
	rc.Flush()

	type batch struct {
		evs []fsproto.Event
		err error
	}
	ctx, cancel := context.WithCancel(q.ctx)
	defer cancel()
	next := make(chan batch)
	go func() {
		for {
			evs, err := st.Next(ctx)
			select {
			case next <- batch{evs, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	tick := time.NewTicker(q.h.cfg.Keepalive)
	defer tick.Stop()
	for {
		select {
		case <-q.ctx.Done():
			return
		case <-tick.C:
			if sse {
				io.WriteString(q.w, ":\n\n")
			} else {
				io.WriteString(q.w, "\n")
			}
			if rc.Flush() != nil {
				return
			}
		case b := <-next:
			if b.err != nil {
				if !errors.Is(b.err, io.EOF) && !errors.Is(b.err, context.Canceled) {
					q.h.logf("httpd events: %v", b.err)
				}
				return
			}
			reset := writeEvents(q.w, b.evs, sse)
			if rc.Flush() != nil || reset {
				return
			}
		}
	}
}

// longPoll answers once at least one record is available, or after wait
// with an empty body.
func (q *req) longPoll(st server.Stream, wait time.Duration, sse bool) {
	ctx, cancel := context.WithTimeout(q.ctx, wait)
	defer cancel()
	evs, err := st.Next(ctx)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, io.EOF) {
		if !errors.Is(err, context.Canceled) {
			q.h.logf("httpd events: %v", err)
		}
		return
	}
	q.w.WriteHeader(http.StatusOK)
	writeEvents(q.w, evs, sse)
}

// writeEvents writes records and reports whether one was a reset.
func writeEvents(w io.Writer, evs []fsproto.Event, sse bool) bool {
	var b bytes.Buffer
	reset := false
	for _, ev := range evs {
		line := ev.Encode()
		if sse {
			if ev.Op != fsproto.OpSeq {
				b.WriteString("id: " + strconv.FormatUint(ev.Seq, 10) + "\n")
			}
			b.WriteString("data: " + line + "\n\n")
		} else {
			b.WriteString(line + "\n")
		}
		if ev.Op == fsproto.OpReset {
			reset = true
			break
		}
	}
	w.Write(b.Bytes())
	return reset
}

// serveCtl serves POST .fs/ctl: one command per line, one reply line per
// command.
func (q *req) serveCtl(sc server.Scope) {
	if q.r.Method != http.MethodPost {
		q.methodNotAllowed("POST")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(q.w, q.r.Body, q.h.cfg.MaxCtlBody))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			q.text(http.StatusRequestEntityTooLarge, "denied msg=body%20too%20large\n")
			return
		}
		q.fail(err)
		return
	}
	c := q.ctl(sc)
	var out strings.Builder
	n := 0
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line == "" {
			continue
		}
		reply, err := c.ExecLine(q.ctx, line)
		if err != nil {
			q.fail(err)
			return
		}
		out.WriteString(reply)
		out.WriteByte('\n')
		n++
	}
	if n == 0 {
		q.text(http.StatusBadRequest, "invalid msg=no%20command\n")
		return
	}
	q.w.Header().Set("Cache-Control", "no-store")
	q.text(http.StatusOK, out.String())
}

// serveStage serves .fs/stage/<name>: PUT writes (at Content-Range's
// offset, or from 0 after truncating), HEAD returns its size, and DELETE
// aborts it.
func (q *req) serveStage(name string) {
	if fsproto.CheckStageName(name) != nil || strings.HasPrefix(name, plainStagePrefix) {
		q.fail(fsproto.Invalid("bad stage name"))
		return
	}
	st := q.h.cfg.Store
	switch q.r.Method {
	case http.MethodHead, http.MethodGet:
		if q.r.Method == http.MethodGet {
			q.methodNotAllowed("HEAD, PUT, DELETE")
			return
		}
		info, err := st.StatStage(q.user, name)
		if err != nil {
			q.fail(err)
			return
		}
		q.w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
		q.w.Header().Set(HeaderStageSize, strconv.FormatInt(info.Size, 10))
		q.w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		if err := st.RemoveStage(q.user, name); err != nil {
			q.fail(err)
			return
		}
		q.w.WriteHeader(http.StatusNoContent)
	case http.MethodPut:
		off := int64(0)
		if cr := q.r.Header.Get("Content-Range"); cr != "" {
			first, ok := parseContentRange(cr)
			if !ok {
				q.text(http.StatusBadRequest, "invalid msg=bad%20Content-Range\n")
				return
			}
			off = first
		} else if err := st.RemoveStage(q.user, name); err != nil && !errors.Is(err, fsproto.ErrMissing) {
			q.fail(err)
			return
		}
		f, err := st.OpenStage(q.user, name, true)
		if err != nil {
			q.fail(err)
			return
		}
		_, werr := copyAt(f, q.r.Body, off)
		size, serr := f.Size()
		cerr := f.Close()
		for _, err := range []error{werr, serr, cerr} {
			if err != nil {
				q.fail(err)
				return
			}
		}
		q.w.Header().Set(HeaderStageSize, strconv.FormatInt(size, 10))
		q.w.WriteHeader(http.StatusNoContent)
	default:
		q.methodNotAllowed("HEAD, PUT, DELETE")
	}
}

// parseContentRange parses "bytes <first>-<last>/<total or *>" and returns
// first. An empty write is "bytes <first>-<first-1>/*".
func parseContentRange(s string) (int64, bool) {
	rest, ok := strings.CutPrefix(strings.TrimSpace(s), "bytes ")
	if !ok {
		return 0, false
	}
	rng, _, ok := strings.Cut(rest, "/")
	if !ok {
		return 0, false
	}
	a, _, ok := strings.Cut(rng, "-")
	if !ok {
		return 0, false
	}
	first, err := strconv.ParseInt(strings.TrimSpace(a), 10, 64)
	if err != nil || first < 0 {
		return 0, false
	}
	return first, true
}

// copyAt copies r into w starting at off.
func copyAt(w io.WriterAt, r io.Reader, off int64) (int64, error) {
	buf := make([]byte, 256<<10)
	var n int64
	for {
		m, err := r.Read(buf)
		if m > 0 {
			if _, werr := w.WriteAt(buf[:m], off+n); werr != nil {
				return n, werr
			}
			n += int64(m)
		}
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
	}
}
