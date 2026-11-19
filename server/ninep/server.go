// Package ninep serves dotfs roots over 9P2000.L (through hugelgupf/p9) and
// plain 9P2000 (through 9fans.net/go), with the synthetic .fs/ files
// (index, events, ctl, and stage/) mapped onto the server interfaces, and
// plain 9P writes turned into unconditional commits.
//
// Both dialects share one node layer (node.go and handles.go). A
// connection's Tversion picks the dialect. For 9P2000.L, a thin layer in
// front of p9's server (intercept.go) checks attaches and serves
// .fs/events fids itself, because p9 can neither see the attach name nor
// cancel a blocking read on Tflush.
package ninep

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/auth"
	"github.com/progrium/dotfs/server/ninep/frame"
)

// Config configures a Server.
type Config struct {
	Meta  server.Meta
	Store server.Store
	Hub   server.Hub
	// Auth decides attaches.
	Auth auth.Policy
	// CreateRoots makes an attach to a missing root of the attaching user
	// create it. Without it, such an attach fails.
	CreateRoots bool
	// UID and GID are reported as every file's owner over 9P2000.L.
	// Zero values mean the server process's own.
	UID, GID uint32
	// MaxCtlLine caps a ctl command's length; 0 means 64 KiB.
	MaxCtlLine int
	// Logf logs connection errors; nil discards them.
	Logf func(format string, args ...any)
	// Now returns the time used for plain writes' mtimes; nil is time.Now.
	Now func() time.Time
}

// Server serves 9P connections.
type Server struct {
	cfg    Config
	ctx    context.Context // canceled by Close; event streams live in it
	cancel context.CancelFunc

	// events maps the qid path of every .fs/events file handed out to its
	// scope, so the 9P2000.L layer can recognize events fids from Rwalk.
	events sync.Map // uint64 → server.Scope

	wg sync.WaitGroup
}

// New returns a Server.
func New(cfg Config) *Server {
	if cfg.UID == 0 && cfg.GID == 0 {
		cfg.UID, cfg.GID = uint32(os.Getuid()), uint32(os.Getgid())
	}
	if cfg.MaxCtlLine == 0 {
		cfg.MaxCtlLine = 64 << 10
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{cfg: cfg, ctx: ctx, cancel: cancel}
}

func (s *Server) logf(format string, args ...any) {
	if s.cfg.Logf != nil {
		s.cfg.Logf(format, args...)
	}
}

// Close ends every event stream. It does not close listeners.
func (s *Server) Close() error {
	s.cancel()
	return nil
}

// Serve accepts connections on l and serves each until it ends. The
// transport does not authenticate them: attaches follow Config.Auth for
// local listeners.
func (s *Server) Serve(ctx context.Context, l net.Listener) error {
	go func() {
		<-ctx.Done()
		l.Close()
	}()
	for {
		c, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go func() {
			if err := s.ServeTransport(ctx, frame.NewStream(c), ""); err != nil {
				s.logf("9p %s: %v", c.RemoteAddr(), err)
			}
		}()
	}
}

// ServeTransport serves one connection until it ends. user is the user
// the transport authenticated, or "" if it did not.
func (s *Server) ServeTransport(ctx context.Context, t frame.Transport, user string) error {
	defer t.Close()
	stop := context.AfterFunc(ctx, func() { t.Close() })
	defer stop()
	first, err := t.ReadFrame()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	if frame.Type(first) != tversion {
		return errors.New("ninep: first message is not Tversion")
	}
	_, version, ok := parseVersion(first)
	if !ok {
		return errors.New("ninep: bad Tversion")
	}
	var serr error
	if strings.HasPrefix(version, "9P2000.L") { // p9 also sends "9P2000.L.Google.<n>"
		serr = s.serveDotL(t, user, first)
	} else {
		serr = s.servePlain(t, user, first)
	}
	if errors.Is(serr, io.EOF) || errors.Is(serr, net.ErrClosed) {
		return nil
	}
	return serr
}

// attachRoot checks an attach and returns a session on its root,
// creating the root if allowed.
func (s *Server) attachRoot(ctx context.Context, connUser, uname, aname string) (*session, error) {
	root, err := s.cfg.Auth.Attach(ctx, connUser, uname, aname)
	if err != nil {
		return nil, err
	}
	return s.openRoot(ctx, root)
}

func (s *Server) openRoot(ctx context.Context, root server.Root) (*session, error) {
	sc := server.RootScope(root)
	if _, err := s.cfg.Meta.Seq(ctx, sc); err != nil {
		if !errors.Is(err, server.ErrNoScope) {
			return nil, err
		}
		if !s.cfg.CreateRoots {
			return nil, err
		}
		if err := s.cfg.Store.CreateRoot(root); err != nil {
			return nil, err
		}
		if err := s.cfg.Meta.CreateScope(ctx, sc); err != nil && !errors.Is(err, server.ErrScopeExists) {
			return nil, err
		}
	}
	return newSession(s, root), nil
}
