package client

import (
	"context"
	"net/http"
	"net/url"
	"strings"
)

// DialConn connects to addr and returns a Conn for one root. It accepts
// every address Dial does, which it serves over 9P, plus:
//
//	http://host/r/<root>/, https://host/r/<root>/   http-dotfs (docs/http-dotfs.md)
//
// Without opts.Root, an HTTP address is the root's base URL. With it, the
// base is <addr>/<root>/, or <addr>/r/<root>/ (the layout of dotfs serve)
// when addr has no path. So https://host/r with root "home" (how dotfs
// sync splits https://host/r/home) and https://host with root "home" both
// dial https://host/r/home/. opts.Token is sent as a bearer token. DialConn checks an
// HTTP address with GET .fs/, so a wrong URL or token fails here rather
// than on the first call.
func DialConn(ctx context.Context, addr string, opts Options) (Conn, error) {
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		return Dial(ctx, addr, opts)
	}
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	if opts.Root != "" {
		p := strings.TrimSuffix(u.Path, "/")
		if p == "" {
			p = "/r"
		}
		u.Path = p + "/" + opts.Root + "/"
		u.RawPath = ""
		addr = u.String()
	}
	c, err := NewHTTP(addr, opts)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(ctx, http.MethodGet, c.base+".fs/", nil, nil)
	if err != nil {
		return nil, pathErr("dial", "/.fs/", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, pathErr("dial", "/.fs/", httpErr(resp))
	}
	return c, nil
}
