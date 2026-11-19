// Package auth decides who is attaching to which root: it parses attach
// names, checks tokens (stored by any server.Tokens, such as server/meta),
// enforces that a root belongs to the attaching user, and serves the
// WebSocket upgrade that carries 9P.
package auth

import (
	"context"
	"strings"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

// AttachName is a parsed 9P attach name:
//
//	[<user>/]<root>[?token=<token>]
//
// The user part is optional when the transport or the token already says
// who is attaching. The token form is for local listeners (TCP and Unix)
// that require one; over WebSocket the token travels on the HTTP upgrade.
type AttachName struct {
	User  string
	Root  string
	Token string
}

// ParseAttachName parses an attach name. A leading "/" is ignored.
func ParseAttachName(aname string) (AttachName, error) {
	var a AttachName
	aname = strings.TrimPrefix(aname, "/")
	if i := strings.IndexByte(aname, '?'); i >= 0 {
		q := aname[i+1:]
		aname = aname[:i]
		tok, ok := strings.CutPrefix(q, "token=")
		if !ok || tok == "" {
			return a, fsproto.Invalid("bad attach name")
		}
		a.Token = tok
	}
	if u, r, ok := strings.Cut(aname, "/"); ok {
		a.User, a.Root = u, r
		if err := CheckName(a.User); err != nil {
			return a, err
		}
	} else {
		a.Root = aname
	}
	return a, CheckName(a.Root)
}

// CheckName reports whether s is a valid user or root name: not empty,
// not "." or "..", and without "/", "?", or NUL.
func CheckName(s string) error {
	if s == "" || s == "." || s == ".." || strings.ContainsAny(s, "/?\x00") {
		return fsproto.Invalid("bad name")
	}
	return nil
}

// Policy decides attaches.
type Policy struct {
	// Tokens checks tokens given in attach names. Nil rejects them.
	Tokens server.Tokens
	// RequireToken makes connections that the transport did not
	// authenticate (TCP and Unix listeners) present a token. Without it,
	// those connections are trusted to name their user, as local
	// listeners are meant to be.
	RequireToken bool
}

// Attach returns the root that an attach may use. connUser is the user
// the transport authenticated (from a WebSocket bearer token), or "" when
// it did not; uname is the attach message's user name. Failures are
// fsproto.ErrDenied or fsproto.ErrInvalid.
//
// The attaching user is, in order: the owner of the token in the attach
// name, connUser, uname, or the user part of the attach name. A root
// belonging to someone else is denied.
func (p Policy) Attach(ctx context.Context, connUser, uname, aname string) (server.Root, error) {
	a, err := ParseAttachName(aname)
	if err != nil {
		return "", err
	}
	user := connUser
	switch {
	case a.Token != "":
		if p.Tokens == nil {
			return "", fsproto.Denied("tokens not accepted")
		}
		u, err := p.Tokens.LookupToken(ctx, a.Token)
		if err != nil {
			return "", fsproto.Denied("bad token")
		}
		if connUser != "" && u != connUser {
			return "", fsproto.Denied("token for another user")
		}
		user = u
	case connUser != "":
	case p.RequireToken:
		return "", fsproto.Denied("token required")
	default:
		user = uname
		if user == "" {
			user = a.User
		}
	}
	if user == "" || CheckName(user) != nil {
		return "", fsproto.Denied("no user")
	}
	if a.User != "" && a.User != user {
		return "", fsproto.Denied("root belongs to another user")
	}
	return server.NewRoot(user, a.Root), nil
}
