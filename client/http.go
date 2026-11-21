package client

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"

	"github.com/progrium/dotfs/fsproto"
)

// Header names of http-dotfs (docs/http-dotfs.md).
const (
	hdrSeq       = "Dotfs-Seq"
	hdrDevice    = "Dotfs-Device"
	hdrStageSize = "Dotfs-Stage-Size"
)

// HTTPConn is a client.Conn over http-dotfs: the dotfs extensions as HTTP
// resources under one root's base URL. Every call is its own HTTP request,
// so an HTTPConn is safe for concurrent use and has no connection to
// lose: a failed request fails only that call.
type HTTPConn struct {
	base   string // ends in "/"
	token  string
	client *http.Client

	mu     sync.Mutex
	device string
	closed bool
}

// NewHTTP returns a Conn for the root served at baseURL, such as
// https://host/r/home/. It sends opts.Token as a bearer token and
// opts.Device as Dotfs-Device on every request, and uses opts.HTTPClient
// when set. It makes no request; the first call reports a bad address or
// token.
func NewHTTP(baseURL string, opts Options) (*HTTPConn, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("dotfs: bad URL %q: %w", baseURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("dotfs: not an http or https URL: %q", baseURL)
	}
	if !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
		if u.RawPath != "" {
			u.RawPath += "/"
		}
	}
	u.RawQuery, u.Fragment = "", ""
	hc := opts.HTTPClient
	if hc == nil {
		// No overall timeout: event streams stay open.
		hc = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	}
	return &HTTPConn{base: u.String(), token: opts.Token, client: hc, device: opts.Device}, nil
}

// HTTPError is a response status other than the one a request expects.
// It matches fsproto.ErrMissing (404), fsproto.ErrDenied (401 and 403),
// fsproto.ErrConflict (409), and fsproto.ErrInvalid (400 and 422) with
// errors.Is.
type HTTPError struct {
	Status int
	Body   string // the reply line or message the server sent, trimmed
}

func (e *HTTPError) Error() string {
	if e.Body != "" {
		return fmt.Sprintf("http %d: %s", e.Status, e.Body)
	}
	return fmt.Sprintf("http %d", e.Status)
}

func (e *HTTPError) Is(target error) bool {
	switch target {
	case fsproto.ErrMissing:
		return e.Status == http.StatusNotFound
	case fsproto.ErrDenied:
		return e.Status == http.StatusUnauthorized || e.Status == http.StatusForbidden
	case fsproto.ErrConflict:
		return e.Status == http.StatusConflict
	case fsproto.ErrInvalid:
		return e.Status == http.StatusBadRequest || e.Status == http.StatusUnprocessableEntity
	}
	return false
}

func httpErr(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	return &HTTPError{Status: resp.StatusCode, Body: strings.TrimSpace(string(b))}
}

// url returns the URL of a dotfs path (absolute, unescaped) below the
// base, escaping each component as a URL path segment.
func (c *HTTPConn) url(p string) (string, error) {
	if p == "/" {
		return c.base, nil
	}
	if err := fsproto.CheckPath(p); err != nil {
		return "", err
	}
	parts := strings.Split(strings.TrimPrefix(p, "/"), "/")
	for i, s := range parts {
		parts[i] = url.PathEscape(s)
	}
	return c.base + strings.Join(parts, "/"), nil
}

func (c *HTTPConn) do(ctx context.Context, method, u string, body io.Reader, hdr http.Header) (*http.Response, error) {
	c.mu.Lock()
	closed, dev := c.closed, c.device
	c.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	for k, v := range hdr {
		req.Header[k] = v
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if dev != "" {
		req.Header.Set(hdrDevice, dev)
	}
	return c.client.Do(req)
}

// Index fetches .fs/index. When accept names zstd, it asks for
// Content-Encoding: zstd and decodes the body as it streams.
func (c *HTTPConn) Index(ctx context.Context, accept []string) (IndexReader, error) {
	hdr := http.Header{}
	for _, a := range accept {
		if a == fsproto.EncodingZstd {
			hdr.Set("Accept-Encoding", "zstd")
		}
	}
	resp, err := c.do(ctx, http.MethodGet, c.base+".fs/index", nil, hdr)
	if err != nil {
		return nil, pathErr("index", "/.fs/index", err)
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, pathErr("index", "/.fs/index", httpErr(resp))
	}
	r := &httpIndexReader{body: resp.Body}
	var src io.Reader = resp.Body
	switch enc := resp.Header.Get("Content-Encoding"); enc {
	case "", "identity":
	case "zstd":
		d, err := zstd.NewReader(resp.Body, zstd.WithDecoderConcurrency(1))
		if err != nil {
			resp.Body.Close()
			return nil, pathErr("index", "/.fs/index", err)
		}
		r.decs = append(r.decs, d)
		src = d
	default:
		resp.Body.Close()
		return nil, pathErr("index", "/.fs/index", fmt.Errorf("unsupported Content-Encoding %q", enc))
	}
	ir, err := fsproto.NewIndexReader(src, r.decode)
	if err != nil {
		r.Close()
		return nil, pathErr("index", "/.fs/index", err)
	}
	r.IndexReader = ir
	return r, nil
}

type httpIndexReader struct {
	*fsproto.IndexReader
	body io.Closer
	decs []*zstd.Decoder
}

// decode handles an encoding named in the index header, which an
// http-dotfs server does not send but a static file may.
func (r *httpIndexReader) decode(enc string, body io.Reader) (io.Reader, error) {
	if enc != fsproto.EncodingZstd {
		return nil, fmt.Errorf("dotfs: unsupported index encoding %q", enc)
	}
	d, err := zstd.NewReader(body, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	r.decs = append(r.decs, d)
	return d, nil
}

func (r *httpIndexReader) Close() error {
	for _, d := range r.decs {
		d.Close()
	}
	return r.body.Close()
}

// Events opens GET .fs/events as a stream of lines.
func (c *HTTPConn) Events(ctx context.Context, setup fsproto.EventsSetup) (EventStream, error) {
	q := url.Values{}
	if setup.HasSince {
		q.Set("since", strconv.FormatUint(setup.Since, 10))
	}
	for _, p := range setup.Paths {
		q.Add("path", p)
	}
	if setup.Coalesce {
		q.Set("coalesce", "1")
	}
	u := c.base + ".fs/events"
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	// The stream outlives ctx, which only bounds opening it.
	sctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	type opened struct {
		resp *http.Response
		err  error
	}
	ch := make(chan opened, 1)
	go func() {
		resp, err := c.do(sctx, http.MethodGet, u, nil, http.Header{"Accept": {"text/plain"}})
		ch <- opened{resp, err}
	}()
	var o opened
	select {
	case o = <-ch:
	case <-ctx.Done():
		cancel()
		if o := <-ch; o.resp != nil {
			o.resp.Body.Close()
		}
		return nil, ctx.Err()
	}
	if o.err != nil {
		cancel()
		return nil, pathErr("events", "/.fs/events", o.err)
	}
	if o.resp.StatusCode != http.StatusOK {
		defer cancel()
		defer o.resp.Body.Close()
		return nil, pathErr("events", "/.fs/events", httpErr(o.resp))
	}
	s := &httpEvents{body: o.resp.Body, cancel: cancel, lines: make(chan lineResult, 64)}
	go s.read()
	return s, nil
}

type lineResult struct {
	ev  fsproto.Event
	err error
}

type httpEvents struct {
	body   io.ReadCloser
	cancel context.CancelFunc
	lines  chan lineResult

	mu    sync.Mutex
	ended bool
	err   error

	closeOnce sync.Once
}

// read parses records off the response into lines until it ends.
func (s *httpEvents) read() {
	defer close(s.lines)
	br := bufio.NewReaderSize(s.body, 64<<10)
	for {
		line, err := br.ReadSlice('\n')
		if err == bufio.ErrBufferFull {
			// A long record: gather the rest of the line.
			buf := append([]byte(nil), line...)
			for err == bufio.ErrBufferFull && len(buf) <= fsproto.MaxLineLen {
				line, err = br.ReadSlice('\n')
				buf = append(buf, line...)
			}
			line = buf
			if err == bufio.ErrBufferFull {
				s.lines <- lineResult{err: fsproto.ErrSyntax}
				return
			}
		}
		if err != nil {
			if err == io.EOF {
				if len(bytes.TrimSpace(line)) > 0 {
					err = io.ErrUnexpectedEOF
				} else {
					err = ErrStreamEnded
				}
			} else {
				err = pathErr("events read", "/.fs/events", err)
			}
			s.lines <- lineResult{err: err}
			return
		}
		text := strings.TrimRight(string(line), "\r\n")
		if strings.TrimSpace(text) == "" {
			continue // keepalive
		}
		ev, perr := fsproto.ParseEvent(text)
		if perr != nil {
			s.lines <- lineResult{err: perr}
			return
		}
		s.lines <- lineResult{ev: ev}
		if ev.Op == fsproto.OpReset {
			return
		}
	}
}

func (s *httpEvents) Next(ctx context.Context) (fsproto.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return fsproto.Event{}, s.err
	}
	if s.ended {
		return fsproto.Event{}, io.EOF
	}
	if err := ctx.Err(); err != nil {
		return fsproto.Event{}, err
	}
	select {
	case <-ctx.Done():
		return fsproto.Event{}, ctx.Err()
	case res, ok := <-s.lines:
		switch {
		case !ok:
			s.err = ErrStreamEnded
			return fsproto.Event{}, s.err
		case res.err != nil:
			s.err = res.err
			return fsproto.Event{}, s.err
		case res.ev.Op == fsproto.OpReset:
			s.ended = true
			s.Close()
		}
		return res.ev, nil
	}
}

func (s *httpEvents) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		s.body.Close()
	})
	return nil
}

// Ctl sends one command with POST .fs/ctl and parses its reply. A hello
// also makes later requests carry the device id as Dotfs-Device, since
// HTTP has no session for it to name.
func (c *HTTPConn) Ctl(ctx context.Context, cmd fsproto.Command) (fsproto.Reply, error) {
	resp, err := c.do(ctx, http.MethodPost, c.base+".fs/ctl", strings.NewReader(cmd.Encode()),
		http.Header{"Content-Type": {"text/plain"}})
	if err != nil {
		return fsproto.Reply{}, pathErr("ctl", "/.fs/ctl", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fsproto.Reply{}, pathErr("ctl", "/.fs/ctl", httpErr(resp))
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, fsproto.MaxLineLen+1))
	if err != nil {
		return fsproto.Reply{}, pathErr("ctl read", "/.fs/ctl", err)
	}
	line, _, _ := strings.Cut(string(b), "\n")
	r, err := fsproto.ParseReply(cmd.Name(), strings.TrimSuffix(line, "\r"))
	if err != nil {
		return r, err
	}
	if h, ok := cmd.(fsproto.HelloCmd); ok && r.Status == fsproto.StatusOK {
		c.mu.Lock()
		c.device = h.Device
		c.mu.Unlock()
	}
	return r, nil
}

// Stage opens .fs/stage/<name>, creating it empty with a PUT when HEAD
// says it does not exist.
func (c *HTTPConn) Stage(ctx context.Context, name string) (Stage, error) {
	if err := fsproto.CheckStageName(name); err != nil {
		return nil, err
	}
	p := "/.fs/stage/" + name
	s := &httpStage{c: c, ctx: context.WithoutCancel(ctx), path: p, u: c.base + ".fs/stage/" + url.PathEscape(name)}
	if _, err := s.size(ctx); err == nil {
		return s, nil
	} else if !errors.Is(err, fsproto.ErrMissing) {
		return nil, err
	}
	resp, err := c.do(ctx, http.MethodPut, s.u, http.NoBody, nil)
	if err != nil {
		return nil, pathErr("stage", p, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, pathErr("stage", p, httpErr(resp))
	}
	return s, nil
}

type httpStage struct {
	c    *HTTPConn
	ctx  context.Context
	path string
	u    string
}

// WriteAt writes p at off with one PUT carrying Content-Range.
func (s *httpStage) WriteAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	hdr := http.Header{"Content-Range": {fmt.Sprintf("bytes %d-%d/*", off, off+int64(len(p))-1)}}
	resp, err := s.c.do(s.ctx, http.MethodPut, s.u, bytes.NewReader(p), hdr)
	if err != nil {
		return 0, pathErr("stage write", s.path, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode/100 != 2 {
		return 0, pathErr("stage write", s.path, httpErr(resp))
	}
	return len(p), nil
}

func (s *httpStage) Size() (int64, error) { return s.size(s.ctx) }

func (s *httpStage) size(ctx context.Context) (int64, error) {
	resp, err := s.c.do(ctx, http.MethodHead, s.u, nil, nil)
	if err != nil {
		return 0, pathErr("stage stat", s.path, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, pathErr("stage stat", s.path, &HTTPError{Status: resp.StatusCode})
	}
	if v := resp.Header.Get(hdrStageSize); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n, nil
		}
	}
	if resp.ContentLength < 0 {
		return 0, pathErr("stage stat", s.path, errors.New("no Content-Length"))
	}
	return resp.ContentLength, nil
}

func (s *httpStage) Close() error { return nil }

// Open learns a file's size with HEAD. Reads are GET requests: Read
// streams one response from the start, and ReadAt asks for a Range.
func (c *HTTPConn) Open(ctx context.Context, path string) (File, error) {
	u, err := c.url(path)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(ctx, http.MethodHead, u, nil, nil)
	if err != nil {
		return nil, pathErr("open", path, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, pathErr("open", path, &HTTPError{Status: resp.StatusCode})
	}
	size := resp.ContentLength
	if size < 0 {
		size = 0
	}
	return &httpFile{c: c, ctx: context.WithoutCancel(ctx), path: path, u: u, size: size, etag: resp.Header.Get("ETag")}, nil
}

type httpFile struct {
	c    *HTTPConn
	ctx  context.Context
	path string
	u    string
	size int64
	etag string

	mu   sync.Mutex
	body io.ReadCloser // the sequential read, opened by the first Read
}

func (f *httpFile) rangeHeader(off int64, n int) http.Header {
	hdr := http.Header{"Range": {fmt.Sprintf("bytes=%d-%d", off, off+int64(n)-1)}}
	if f.etag != "" {
		// A changed file answers 200 with the whole new content, which
		// ReadAt then refuses, rather than mixing versions.
		hdr.Set("If-Range", f.etag)
	}
	return hdr
}

func (f *httpFile) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off >= f.size {
		return 0, io.EOF
	}
	resp, err := f.c.do(f.ctx, http.MethodGet, f.u, nil, f.rangeHeader(off, len(p)))
	if err != nil {
		return 0, pathErr("read", f.path, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusPartialContent:
	case http.StatusRequestedRangeNotSatisfiable:
		return 0, io.EOF
	case http.StatusOK:
		if off != 0 {
			return 0, pathErr("read", f.path, errors.New("file changed or server ignored Range"))
		}
	default:
		return 0, pathErr("read", f.path, httpErr(resp))
	}
	n, err := io.ReadFull(resp.Body, p)
	if err == io.ErrUnexpectedEOF || (err == io.EOF && n == 0) {
		return n, io.EOF
	}
	if err != nil {
		return n, pathErr("read", f.path, err)
	}
	return n, nil
}

func (f *httpFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	if f.body == nil {
		resp, err := f.c.do(f.ctx, http.MethodGet, f.u, nil, nil)
		if err != nil {
			return 0, pathErr("read", f.path, err)
		}
		if resp.StatusCode != http.StatusOK {
			defer resp.Body.Close()
			return 0, pathErr("read", f.path, httpErr(resp))
		}
		f.body = resp.Body
	}
	n, err := f.body.Read(p)
	if err != nil && err != io.EOF {
		err = pathErr("read", f.path, err)
	}
	return n, err
}

func (f *httpFile) Size() int64 { return f.size }

func (f *httpFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.body != nil {
		f.body.Close()
		f.body = nil
	}
	return nil
}

// Read returns a file's whole content with one GET.
func (c *HTTPConn) Read(ctx context.Context, path string) ([]byte, error) {
	u, err := c.url(path)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(ctx, http.MethodGet, u, nil, nil)
	if err != nil {
		return nil, pathErr("read", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, pathErr("read", path, httpErr(resp))
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, pathErr("read", path, err)
	}
	return b, nil
}

// Close closes idle connections; later calls, including reads of files
// already open, fail with ErrClosed. Event streams already open run until
// they are closed.
func (c *HTTPConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.client.CloseIdleConnections()
	return nil
}
