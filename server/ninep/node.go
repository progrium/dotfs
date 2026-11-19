package ninep

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"hash/fnv"
	"sort"
	"strings"
	"sync"

	"github.com/hugelgupf/p9/linux"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

// session is one attach: a user on one root. It holds the ctl state shared
// by every ctl fid of the attach, notably the device id from hello, which
// plain writes report too.
type session struct {
	srv  *Server
	user string
	root server.Root

	mu   sync.Mutex
	dev  string
	ctls map[server.Scope]*server.Ctl
}

func newSession(srv *Server, root server.Root) *session {
	return &session{srv: srv, user: root.User(), root: root, ctls: map[server.Scope]*server.Ctl{}}
}

func (s *session) origin() server.Origin {
	s.mu.Lock()
	defer s.mu.Unlock()
	return server.Origin{User: s.user, Dev: s.dev}
}

func (s *session) ctl(ctx context.Context, sc server.Scope) *server.Ctl {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.ctls[sc]
	if c == nil {
		c = server.NewCtl(s.srv.cfg.Meta, s.srv.cfg.Store, sc, s.user)
		if s.dev != "" {
			c.Exec(ctx, fsproto.HelloCmd{Device: s.dev})
		}
		s.ctls[sc] = c
	}
	return c
}

// execCtl runs one ctl line in scope sc. A successful hello applies to the
// whole attach: every scope's ctl and plain writes.
func (s *session) execCtl(ctx context.Context, sc server.Scope, line string) (string, error) {
	reply, err := s.ctl(ctx, sc).ExecLine(ctx, line)
	if err != nil {
		return "", err
	}
	if cmd, perr := fsproto.ParseCommand(line); perr == nil {
		if h, ok := cmd.(fsproto.HelloCmd); ok && strings.HasPrefix(reply, "ok") {
			s.mu.Lock()
			s.dev = h.Device
			others := make([]*server.Ctl, 0, len(s.ctls))
			for k, c := range s.ctls {
				if k != sc {
					others = append(others, c)
				}
			}
			s.mu.Unlock()
			for _, c := range others {
				c.Exec(ctx, h)
			}
		}
	}
	return reply, nil
}

func (s *session) writer(sc server.Scope) (server.Writer, error) {
	return s.srv.cfg.Meta.Writer(sc)
}

// kind is what a node is.
type kind uint8

const (
	kTop      kind = iota // 9P2000.L only: above the users
	kUser                 // 9P2000.L only: one user's roots
	kReal                 // a path in a scope
	kFS                   // a scope's .fs/
	kIndex                // .fs/index
	kEvents               // .fs/events
	kCtl                  // .fs/ctl
	kStageDir             // .fs/stage/
	kStage                // .fs/stage/<name>
)

// node is a walked-to file. Nodes are cheap values; metadata is looked up
// again when needed, since other clients change the tree.
type node struct {
	sess   *session // nil for kTop and kUser
	top    *Server  // kTop and kUser
	kind   kind
	user   string       // kUser
	scope  server.Scope // kReal and the .fs nodes
	path   string       // kReal: scope-relative path; kStage: stage name
	name   string       // the name walked to reach this node
	parent *node
}

func (n *node) srv() *Server {
	if n.sess == nil {
		return n.top
	}
	return n.sess.srv
}

// attr is a node's metadata, independent of 9P dialect.
type attr struct {
	qid    qid
	typ    fsproto.Type
	mode   uint32 // permission bits
	size   int64
	mtime  int64
	target string
}

type qid struct {
	typ  uint8
	vers uint32
	path uint64
}

const (
	qtDir     = 0x80
	qtSymlink = 0x02
	qtFile    = 0x00
)

func qidPath(parts ...string) uint64 {
	h := fnv.New64a()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return h.Sum64()
}

func qtype(t fsproto.Type) uint8 {
	switch t {
	case fsproto.TypeDir:
		return qtDir
	case fsproto.TypeSymlink:
		return qtSymlink
	}
	return qtFile
}

// entryAttr converts an entry of scope sc.
func entryAttr(sc server.Scope, e fsproto.Entry) attr {
	return attr{
		qid:    qid{typ: qtype(e.Type), vers: uint32(e.Version), path: qidPath(string(sc.Root), sc.RootPath(e.Path))},
		typ:    e.Type,
		mode:   e.Mode,
		size:   e.Length,
		mtime:  e.Mtime,
		target: e.Target,
	}
}

func synthQid(sc server.Scope, name string, typ uint8) qid {
	return qid{typ: typ, path: qidPath(string(sc.Root), sc.Dir, ".fs", name)}
}

func dirAttr(q qid, mode uint32) attr {
	return attr{qid: q, typ: fsproto.TypeDir, mode: mode}
}

func fileAttr(q qid, mode uint32) attr {
	return attr{qid: q, typ: fsproto.TypeFile, mode: mode}
}

// stat returns n's entry; n must be kReal.
func (n *node) stat(ctx context.Context) (fsproto.Entry, error) {
	return n.srv().cfg.Meta.Stat(ctx, n.scope, n.path)
}

// attr returns n's current metadata.
func (n *node) attr(ctx context.Context) (attr, error) {
	switch n.kind {
	case kTop:
		return dirAttr(qid{typ: qtDir, path: qidPath("top")}, 0o555), nil
	case kUser:
		return dirAttr(qid{typ: qtDir, path: qidPath("user", n.user)}, 0o555), nil
	case kReal:
		e, err := n.stat(ctx)
		if err != nil {
			return attr{}, err
		}
		return entryAttr(n.scope, e), nil
	case kFS:
		return dirAttr(synthQid(n.scope, "", qtDir), 0o555), nil
	case kIndex:
		seq, err := n.srv().cfg.Meta.Seq(ctx, n.scope)
		if err != nil {
			return attr{}, err
		}
		a := fileAttr(synthQid(n.scope, "index", qtFile), 0o644)
		a.qid.vers = uint32(seq)
		return a, nil
	case kEvents:
		q := synthQid(n.scope, "events", qtFile)
		n.srv().events.Store(q.path, n.scope)
		return fileAttr(q, 0o644), nil
	case kCtl:
		return fileAttr(synthQid(n.scope, "ctl", qtFile), 0o600), nil
	case kStageDir:
		return dirAttr(synthQid(n.scope, "stage", qtDir), 0o700), nil
	case kStage:
		info, err := n.srv().cfg.Store.StatStage(n.sess.user, n.path)
		if err != nil {
			return attr{}, err
		}
		a := fileAttr(qid{typ: qtFile, path: qidPath("stage", n.sess.user, n.path)}, 0o600)
		a.size = info.Size
		a.mtime = info.ModTime.Unix()
		return a, nil
	}
	return attr{}, linux.EINVAL
}

func (n *node) isDir() bool {
	switch n.kind {
	case kTop, kUser, kFS, kStageDir:
		return true
	}
	return false
}

func (n *node) child(k kind, name string) *node {
	c := *n
	c.kind, c.name, c.parent = k, name, n
	return &c
}

// walk1 walks one name from n. ".." goes to the node walked from, or stays
// at the attach root.
func (n *node) walk1(ctx context.Context, name string) (*node, attr, error) {
	if name == ".." {
		p := n.parent
		if p == nil {
			p = n
		}
		a, err := p.attr(ctx)
		return p, a, err
	}
	if name == "" || name == "." || strings.ContainsAny(name, "/\x00") {
		return nil, attr{}, linux.EINVAL
	}
	var c *node
	switch n.kind {
	case kTop:
		if err := checkName(name); err != nil {
			return nil, attr{}, err
		}
		c = &node{top: n.top, kind: kUser, user: name, name: name, parent: n}
	case kUser:
		if err := checkName(name); err != nil {
			return nil, attr{}, err
		}
		sess, err := n.top.openRoot(ctx, server.NewRoot(n.user, name))
		if err != nil {
			return nil, attr{}, err
		}
		// The attach root has no parent: ".." stays in the root.
		c = &node{sess: sess, kind: kReal, scope: server.RootScope(sess.root), path: "/", name: name}
	case kReal:
		if name == server.ReservedName {
			if n.path != "/" {
				return nil, attr{}, linux.ENOENT
			}
			c = n.child(kFS, name)
			break
		}
		p := fsproto.Join(n.path, name)
		e, err := n.srv().cfg.Meta.Stat(ctx, n.scope, p)
		if err != nil {
			return nil, attr{}, err
		}
		c = n.child(kReal, name)
		c.path = p
		if e.Type == fsproto.TypeDir && e.Flags.Has('i') {
			ns := server.Scope{Root: n.scope.Root, Dir: n.scope.RootPath(p)}
			if _, err := n.srv().cfg.Meta.Seq(ctx, ns); err == nil {
				c.scope, c.path = ns, "/"
			}
		}
		if c.scope == n.scope {
			return c, entryAttr(n.scope, e), nil
		}
	case kFS:
		switch name {
		case "index":
			c = n.child(kIndex, name)
		case "events":
			c = n.child(kEvents, name)
		case "ctl":
			c = n.child(kCtl, name)
		case "stage":
			c = n.child(kStageDir, name)
		default:
			return nil, attr{}, linux.ENOENT
		}
	case kStageDir:
		if err := fsproto.CheckStageName(name); err != nil {
			return nil, attr{}, linux.ENOENT
		}
		c = n.child(kStage, name)
		c.path = name
	default:
		return nil, attr{}, linux.ENOTDIR
	}
	a, err := c.attr(ctx)
	if err != nil {
		return nil, attr{}, err
	}
	return c, a, nil
}

func checkName(s string) error {
	if s == "" || s == "." || s == ".." || strings.ContainsAny(s, "/?\x00") {
		return linux.ENOENT
	}
	return nil
}

// dirent is one directory listing entry.
type dirent struct {
	name string
	attr attr
}

// plainStagePrefix starts the names of the stage files that plain 9P
// writes use. They are hidden from .fs/stage/ listings.
const plainStagePrefix = ".dotfs-plain-"

func plainStageName() string {
	b := make([]byte, 12)
	rand.Read(b)
	return plainStagePrefix + hex.EncodeToString(b)
}

// readdir lists a directory node. .fs/ is not listed in its scope root,
// so tools that copy a tree do not descend into it (and block on events);
// it can still be walked to.
func (n *node) readdir(ctx context.Context) ([]dirent, error) {
	switch n.kind {
	case kTop, kUser:
		return nil, nil
	case kReal:
		es, err := n.srv().cfg.Meta.ReadDir(ctx, n.scope, n.path)
		if err != nil {
			return nil, err
		}
		out := make([]dirent, 0, len(es))
		for _, e := range es {
			out = append(out, dirent{name: fsproto.Name(e.Path), attr: entryAttr(n.scope, e)})
		}
		return out, nil
	case kFS:
		var out []dirent
		for _, name := range []string{"ctl", "events", "index", "stage"} {
			_, a, err := n.walk1(ctx, name)
			if err != nil {
				return nil, err
			}
			out = append(out, dirent{name: name, attr: a})
		}
		return out, nil
	case kStageDir:
		infos, err := n.srv().cfg.Store.ListStages(n.sess.user)
		if err != nil {
			return nil, err
		}
		sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
		var out []dirent
		for _, info := range infos {
			if strings.HasPrefix(info.Name, plainStagePrefix) {
				continue
			}
			a := fileAttr(qid{typ: qtFile, path: qidPath("stage", n.sess.user, info.Name)}, 0o600)
			a.size, a.mtime = info.Size, info.ModTime.Unix()
			out = append(out, dirent{name: info.Name, attr: a})
		}
		return out, nil
	}
	return nil, linux.ENOTDIR
}

// errno maps an error from the server interfaces to a Linux errno.
func errno(err error) linux.Errno {
	var e linux.Errno
	switch {
	case err == nil:
		return 0
	case errors.As(err, &e):
		return e
	case errors.Is(err, fsproto.ErrMissing), errors.Is(err, server.ErrNoScope):
		return linux.ENOENT
	case errors.Is(err, fsproto.ErrConflict), errors.Is(err, server.ErrScopeExists):
		return linux.EEXIST
	case errors.Is(err, fsproto.ErrDenied):
		return linux.EACCES
	case errors.Is(err, fsproto.ErrInvalid):
		return linux.EINVAL
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return linux.EINTR
	}
	return linux.EIO
}
