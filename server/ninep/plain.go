package ninep

import (
	"context"
	"errors"
	"sync"

	"9fans.net/go/plan9"
	"github.com/hugelgupf/p9/linux"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/ninep/frame"
)

// pconn is one plain 9P2000 connection, served directly on the node layer
// with 9fans.net/go's message codec. Every request runs in its own
// goroutine, so a blocked events read can be flushed.
type pconn struct {
	srv  *Server
	t    frame.Transport
	user string

	wmu sync.Mutex

	mu    sync.Mutex
	msize uint32
	fids  map[uint32]*pfid
	reqs  map[uint16]*preq
	wg    sync.WaitGroup
	gen   int // bumped by Tversion; replies from older generations are dropped
}

type pfid struct {
	mu   sync.Mutex
	n    *node
	h    handle
	open bool
	mode uint8
	// directory reads: the listing taken at offset 0, the next entry, and
	// the byte offset the client must read at next.
	list    []dirent
	next    int
	nextOff uint64
}

type preq struct {
	tag     uint16
	cancel  context.CancelFunc
	flushed bool
	sent    bool // the reply went out from inside the handler
}

type preqKey struct{}

func (s *Server) servePlain(t frame.Transport, user string, first []byte) error {
	c := &pconn{srv: s, t: t, user: user, fids: map[uint32]*pfid{}, reqs: map[uint16]*preq{}}
	defer c.shutdown()
	f := first
	for {
		fc, err := plan9.UnmarshalFcall(f)
		if err != nil {
			return err
		}
		c.dispatch(fc)
		if f, err = t.ReadFrame(); err != nil {
			return err
		}
	}
}

func (c *pconn) shutdown() {
	c.mu.Lock()
	for _, r := range c.reqs {
		r.flushed = true
		r.cancel()
	}
	c.mu.Unlock()
	c.wg.Wait()
	c.clunkAll()
}

func (c *pconn) clunkAll() {
	c.mu.Lock()
	fids := c.fids
	c.fids = map[uint32]*pfid{}
	c.mu.Unlock()
	for _, f := range fids {
		f.clunk(context.Background())
	}
}

func (c *pconn) send(fc *plan9.Fcall) {
	b, err := fc.Bytes()
	if err != nil {
		c.srv.logf("9p: encode %v: %v", fc, err)
		return
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.t.WriteFrame(b)
}

func perror(err error) string {
	switch errno(err) {
	case linux.ENOENT:
		return "file does not exist"
	case linux.EEXIST:
		return "file already exists"
	case linux.EACCES:
		return "permission denied"
	case linux.ENOTDIR:
		return "not a directory"
	case linux.EISDIR:
		return "is a directory"
	case linux.ENOTEMPTY:
		return "directory not empty"
	case linux.EINTR:
		return "interrupted"
	}
	return errno(err).Error()
}

func pqid(q qid) plan9.Qid { return plan9.Qid{Path: q.path, Vers: q.vers, Type: q.typ} }

func (c *pconn) dispatch(fc *plan9.Fcall) {
	switch fc.Type {
	case plan9.Tversion:
		// A new session: wait for outstanding requests, then drop fids.
		c.mu.Lock()
		for _, r := range c.reqs {
			r.flushed = true
			r.cancel()
		}
		c.mu.Unlock()
		c.wg.Wait()
		c.clunkAll()
		msize := min(fc.Msize, frame.MaxSize)
		c.mu.Lock()
		c.msize = msize
		c.gen++
		c.mu.Unlock()
		v := "9P2000"
		if len(fc.Version) < 6 || fc.Version[:6] != "9P2000" {
			v = "unknown"
		}
		c.send(&plan9.Fcall{Type: plan9.Rversion, Tag: fc.Tag, Msize: msize, Version: v})
		return
	case plan9.Tflush:
		c.mu.Lock()
		r := c.reqs[fc.Oldtag]
		if r != nil {
			r.flushed = true
			delete(c.reqs, fc.Oldtag)
		}
		c.mu.Unlock()
		if r != nil {
			r.cancel()
		}
		c.send(&plan9.Fcall{Type: plan9.Rflush, Tag: fc.Tag})
		return
	}
	ctx, cancel := context.WithCancel(c.srv.ctx)
	r := &preq{tag: fc.Tag, cancel: cancel}
	ctx = context.WithValue(ctx, preqKey{}, r)
	c.mu.Lock()
	c.reqs[fc.Tag] = r
	c.wg.Add(1)
	c.mu.Unlock()
	go func() {
		defer c.wg.Done()
		defer cancel()
		rc := c.handle(ctx, fc)
		rc.Tag = fc.Tag
		c.mu.Lock()
		defer c.mu.Unlock()
		if r.flushed || r.sent {
			return
		}
		delete(c.reqs, fc.Tag)
		c.send(rc)
	}()
}

func rerror(err error) *plan9.Fcall { return &plan9.Fcall{Type: plan9.Rerror, Ename: perror(err)} }

var (
	errBadFid   = errors.New("unknown fid")
	errFidInUse = errors.New("fid in use")
)

func (c *pconn) fid(id uint32) *pfid {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fids[id]
}

// setFid installs f as id unless id is taken (and not replace).
func (c *pconn) setFid(id uint32, f *pfid, replace bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.fids[id]; ok && !replace {
		return false
	}
	c.fids[id] = f
	return true
}

func (c *pconn) handle(ctx context.Context, fc *plan9.Fcall) *plan9.Fcall {
	switch fc.Type {
	case plan9.Tauth:
		return &plan9.Fcall{Type: plan9.Rerror, Ename: "authentication not required"}
	case plan9.Tattach:
		if fc.Afid != plan9.NOFID {
			return &plan9.Fcall{Type: plan9.Rerror, Ename: "authentication not required"}
		}
		sess, err := c.srv.attachRoot(ctx, c.user, fc.Uname, fc.Aname)
		if err != nil {
			return rerror(err)
		}
		n := &node{sess: sess, kind: kReal, scope: server.RootScope(sess.root), path: "/"}
		a, err := n.attr(ctx)
		if err != nil {
			return rerror(err)
		}
		if !c.setFid(fc.Fid, &pfid{n: n}, false) {
			return rerror(errFidInUse)
		}
		return &plan9.Fcall{Type: plan9.Rattach, Qid: pqid(a.qid)}
	}
	f := c.fid(fc.Fid)
	if f == nil {
		return &plan9.Fcall{Type: plan9.Rerror, Ename: errBadFid.Error()}
	}
	switch fc.Type {
	case plan9.Twalk:
		return c.walk(ctx, f, fc)
	case plan9.Topen:
		return c.open(ctx, f, fc)
	case plan9.Tcreate:
		return c.create(ctx, f, fc)
	case plan9.Tread:
		return c.read(ctx, f, fc)
	case plan9.Twrite:
		return c.write(ctx, f, fc)
	case plan9.Tclunk:
		c.dropFid(fc.Fid)
		if err := f.clunk(ctx); err != nil {
			return rerror(err)
		}
		return &plan9.Fcall{Type: plan9.Rclunk}
	case plan9.Tremove:
		c.dropFid(fc.Fid)
		err := f.n.remove(ctx)
		f.clunk(ctx)
		if err != nil {
			return rerror(err)
		}
		return &plan9.Fcall{Type: plan9.Rremove}
	case plan9.Tstat:
		d, err := c.stat(ctx, f)
		if err != nil {
			return rerror(err)
		}
		b, _ := d.Bytes()
		return &plan9.Fcall{Type: plan9.Rstat, Stat: b}
	case plan9.Twstat:
		return c.wstat(ctx, f, fc)
	}
	return &plan9.Fcall{Type: plan9.Rerror, Ename: "bad message type"}
}

func (c *pconn) dropFid(id uint32) {
	c.mu.Lock()
	delete(c.fids, id)
	c.mu.Unlock()
}

func (f *pfid) clunk(ctx context.Context) error {
	f.mu.Lock()
	h := f.h
	f.h, f.open = nil, false
	f.mu.Unlock()
	if h != nil {
		return h.close(ctx)
	}
	return nil
}

func (c *pconn) walk(ctx context.Context, f *pfid, fc *plan9.Fcall) *plan9.Fcall {
	f.mu.Lock()
	open := f.open
	f.mu.Unlock()
	if open {
		return &plan9.Fcall{Type: plan9.Rerror, Ename: "fid is open"}
	}
	n := f.n
	var qids []plan9.Qid
	for i, name := range fc.Wname {
		next, a, err := n.walk1(ctx, name)
		if err != nil {
			if i == 0 {
				return rerror(err)
			}
			return &plan9.Fcall{Type: plan9.Rwalk, Wqid: qids}
		}
		qids = append(qids, pqid(a.qid))
		n = next
	}
	if !c.setFid(fc.Newfid, &pfid{n: n}, fc.Newfid == fc.Fid) {
		return rerror(errFidInUse)
	}
	return &plan9.Fcall{Type: plan9.Rwalk, Wqid: qids}
}

func (c *pconn) iounit() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.msize > plan9.IOHDRSZ {
		return c.msize - plan9.IOHDRSZ
	}
	return 0
}

func (c *pconn) open(ctx context.Context, f *pfid, fc *plan9.Fcall) *plan9.Fcall {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.open {
		return &plan9.Fcall{Type: plan9.Rerror, Ename: "fid already open"}
	}
	mode := fc.Mode & 3
	fl := openFlags{write: mode == plan9.OWRITE || mode == plan9.ORDWR, trunc: fc.Mode&plan9.OTRUNC != 0}
	if f.n.kind == kIndex || f.n.kind == kCtl || f.n.kind == kEvents {
		fl.trunc = false
	}
	h, err := f.n.open(ctx, fl)
	if err != nil {
		return rerror(err)
	}
	a, err := f.n.attr(ctx)
	if err != nil {
		if h != nil {
			h.close(ctx)
		}
		return rerror(err)
	}
	f.h, f.open, f.mode = h, true, mode
	return &plan9.Fcall{Type: plan9.Ropen, Qid: pqid(a.qid), Iounit: c.iounit()}
}

func (c *pconn) create(ctx context.Context, f *pfid, fc *plan9.Fcall) *plan9.Fcall {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.open {
		return &plan9.Fcall{Type: plan9.Rerror, Ename: "fid already open"}
	}
	var (
		n   *node
		a   attr
		h   handle
		err error
	)
	if fc.Perm&plan9.DMDIR != 0 {
		n, a, err = f.n.mkdir(ctx, fc.Name)
	} else {
		n, a, h, err = f.n.create(ctx, fc.Name, uint32(fc.Perm)&0o777)
	}
	if err != nil {
		return rerror(err)
	}
	f.n, f.h, f.open, f.mode = n, h, true, fc.Mode&3
	f.list, f.next, f.nextOff = nil, 0, 0
	return &plan9.Fcall{Type: plan9.Rcreate, Qid: pqid(a.qid), Iounit: c.iounit()}
}

func (c *pconn) read(ctx context.Context, f *pfid, fc *plan9.Fcall) *plan9.Fcall {
	f.mu.Lock()
	open, h := f.open, f.h
	f.mu.Unlock()
	if !open || f.mode == plan9.OWRITE {
		return &plan9.Fcall{Type: plan9.Rerror, Ename: "fid not open for reading"}
	}
	count := int(min(fc.Count, c.iounit()))
	if h == nil {
		return c.readDir(ctx, f, fc.Offset, count)
	}
	if ev, ok := h.(*eventsHandle); ok {
		// Records are consumed only if their reply goes out, so a flush
		// that races with their arrival loses none.
		r := ctx.Value(preqKey{}).(*preq)
		err := ev.readDeliver(ctx, count, func(b []byte) bool {
			c.mu.Lock()
			defer c.mu.Unlock()
			if r.flushed {
				return false
			}
			delete(c.reqs, r.tag)
			r.sent = true
			c.send(&plan9.Fcall{Type: plan9.Rread, Tag: r.tag, Data: b})
			return true
		})
		if err != nil {
			return rerror(err)
		}
		return &plan9.Fcall{Type: plan9.Rread}
	}
	buf := make([]byte, count)
	n, err := h.read(ctx, buf, int64(fc.Offset))
	if err != nil {
		return rerror(err)
	}
	return &plan9.Fcall{Type: plan9.Rread, Data: buf[:n]}
}

func (c *pconn) readDir(ctx context.Context, f *pfid, off uint64, count int) *plan9.Fcall {
	f.mu.Lock()
	defer f.mu.Unlock()
	if off == 0 {
		list, err := f.n.readdir(ctx)
		if err != nil {
			return rerror(err)
		}
		f.list, f.next, f.nextOff = list, 0, 0
	} else if off != f.nextOff {
		return &plan9.Fcall{Type: plan9.Rerror, Ename: "bad directory offset"}
	}
	var out []byte
	for f.next < len(f.list) {
		d := c.dir(f.n, f.list[f.next].name, f.list[f.next].attr)
		b, err := d.Bytes()
		if err != nil {
			return rerror(err)
		}
		if len(out)+len(b) > count {
			break
		}
		out = append(out, b...)
		f.next++
	}
	f.nextOff += uint64(len(out))
	return &plan9.Fcall{Type: plan9.Rread, Data: out}
}

func (c *pconn) write(ctx context.Context, f *pfid, fc *plan9.Fcall) *plan9.Fcall {
	f.mu.Lock()
	open, h, mode := f.open, f.h, f.mode
	f.mu.Unlock()
	if !open || h == nil || (mode != plan9.OWRITE && mode != plan9.ORDWR) {
		return &plan9.Fcall{Type: plan9.Rerror, Ename: "fid not open for writing"}
	}
	n, err := h.write(ctx, fc.Data, int64(fc.Offset))
	if err != nil {
		return rerror(err)
	}
	return &plan9.Fcall{Type: plan9.Rwrite, Count: uint32(n)}
}

func (c *pconn) dir(n *node, name string, a attr) *plan9.Dir {
	mode := plan9.Perm(a.mode & 0o777)
	if a.qid.typ&qtDir != 0 {
		mode |= plan9.DMDIR
	}
	if name == "" {
		name = "/"
	}
	return &plan9.Dir{
		Qid: pqid(a.qid), Mode: mode, Atime: uint32(a.mtime), Mtime: uint32(a.mtime),
		Length: uint64(a.size), Name: name,
		Uid: n.owner(), Gid: n.owner(), Muid: n.owner(),
	}
}

func (n *node) owner() string {
	if n.sess != nil {
		return n.sess.user
	}
	return n.user
}

func (c *pconn) stat(ctx context.Context, f *pfid) (*plan9.Dir, error) {
	a, err := f.n.attr(ctx)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	if w, ok := f.h.(*plainWrite); ok {
		if sz := w.size(); sz >= 0 {
			a.size = sz
		}
	}
	f.mu.Unlock()
	if a.qid.typ&qtDir != 0 {
		a.size = 0
	}
	return c.dir(f.n, f.n.name, a), nil
}

func (c *pconn) wstat(ctx context.Context, f *pfid, fc *plan9.Fcall) *plan9.Fcall {
	d, err := plan9.UnmarshalDir(fc.Stat)
	if err != nil {
		return rerror(linux.EINVAL)
	}
	f.mu.Lock()
	w, _ := f.h.(*plainWrite)
	f.mu.Unlock()
	if d.Length != ^uint64(0) {
		if w != nil {
			err = w.truncate(int64(d.Length))
		} else {
			err = f.n.truncate(ctx, int64(d.Length))
		}
		if err != nil {
			return rerror(err)
		}
	}
	var mode *uint32
	var mtime *int64
	if d.Mode != ^plan9.Perm(0) {
		m := uint32(d.Mode) & 0o777
		mode = &m
	}
	if d.Mtime != ^uint32(0) {
		t := int64(d.Mtime)
		mtime = &t
	}
	if (mode != nil || mtime != nil) && f.n.kind == kReal {
		if w != nil {
			if err := w.flush(ctx); err != nil {
				return rerror(err)
			}
		}
		if err := f.n.setattr(ctx, mode, mtime); err != nil {
			return rerror(err)
		}
	}
	if d.Name != "" && d.Name != f.n.name {
		if f.n.parent == nil {
			return rerror(linux.EPERM)
		}
		if err := f.n.parent.rename(ctx, f.n.name, f.n.parent, d.Name); err != nil {
			return rerror(err)
		}
		f.mu.Lock()
		f.n = f.n.parent.child(f.n.kind, d.Name)
		f.n.path = fsproto.Join(f.n.parent.path, d.Name)
		f.mu.Unlock()
	}
	return &plan9.Fcall{Type: plan9.Rwstat}
}
