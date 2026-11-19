package wire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// DefaultMsize is the message size the client asks for.
const DefaultMsize = 1 << 20

// ErrFlushed is a call's result when the server answered its Tflush
// without answering the call.
var ErrFlushed = errors.New("wire: request flushed")

// ErrClosed is returned by calls on a closed connection.
var ErrClosed = errors.New("wire: connection closed")

// Conn is a 9P2000.L client connection. Calls may be made concurrently;
// each gets its own tag.
type Conn struct {
	rwc   io.ReadWriteCloser
	Msize uint32

	wmu sync.Mutex // serializes writes

	mu      sync.Mutex
	pending map[uint16]*Call
	nextTag uint16
	nextFid uint32
	err     error // set when the reader stops
	done    chan struct{}
}

// Call is one request in flight.
type Call struct {
	Tag  uint16
	done chan struct{}
	resp Msg
	err  error
	// flushing is set while a Tflush for this call is outstanding.
	flushing bool
	once     sync.Once
}

// complete records the answer once; later answers are dropped.
func (c *Call) complete(resp Msg, err error) {
	c.once.Do(func() {
		c.resp, c.err = resp, err
		close(c.done)
	})
}

// Done is closed when the call has an answer.
func (c *Call) Done() <-chan struct{} { return c.done }

// Result returns the answer once Done is closed.
func (c *Call) Result() (Msg, error) { <-c.done; return c.resp, c.err }

// Wait waits for the answer or for ctx; it does not flush on cancel.
func (c *Call) Wait(ctx context.Context) (Msg, error) {
	select {
	case <-c.done:
		return c.resp, c.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// NewConn negotiates the version on rwc and starts reading responses.
func NewConn(rwc io.ReadWriteCloser) (*Conn, error) {
	c := &Conn{rwc: rwc, Msize: DefaultMsize, pending: map[uint16]*Call{}, nextFid: 1, done: make(chan struct{})}
	if err := WriteMsg(rwc, NoTag, &Tversion{Msize: DefaultMsize, Version: Version}); err != nil {
		return nil, err
	}
	typ, _, m, err := ReadMsg(rwc, DefaultMsize+64)
	if err != nil {
		return nil, fmt.Errorf("version: %w", err)
	}
	rv, ok := m.(*Rversion)
	if !ok {
		if le, ok := m.(*Rlerror); ok {
			return nil, fmt.Errorf("version: %w", &Error{Errno: le.Ecode})
		}
		return nil, fmt.Errorf("version: unexpected reply type %d", typ)
	}
	if rv.Version != Version {
		return nil, fmt.Errorf("version: server speaks %q, not %q", rv.Version, Version)
	}
	if rv.Msize < 4096 || rv.Msize > DefaultMsize {
		return nil, fmt.Errorf("version: server msize %d out of range", rv.Msize)
	}
	c.Msize = rv.Msize
	go c.readLoop()
	return c, nil
}

// Close closes the connection; pending calls fail with ErrClosed.
func (c *Conn) Close() error {
	err := c.rwc.Close()
	<-c.done
	return err
}

func (c *Conn) readLoop() {
	var err error
	for {
		var tag uint16
		var m Msg
		_, tag, m, err = ReadMsg(c.rwc, c.Msize+64)
		if err != nil {
			break
		}
		c.mu.Lock()
		call := c.pending[tag]
		if call != nil && !call.flushing {
			delete(c.pending, tag)
		}
		c.mu.Unlock()
		if call == nil {
			continue // a response to a tag we gave up on
		}
		// A call being flushed keeps its tag reserved until the Rflush
		// arrives, but an answer that comes first is honored.
		finish(call, m)
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
		err = ErrClosed
	}
	c.mu.Lock()
	if c.err == nil {
		c.err = fmt.Errorf("%w: %v", ErrClosed, err)
	}
	calls := c.pending
	c.pending = map[uint16]*Call{}
	c.mu.Unlock()
	for _, call := range calls {
		call.complete(nil, c.err)
	}
	close(c.done)
}

func finish(call *Call, m Msg) {
	if le, ok := m.(*Rlerror); ok {
		call.complete(nil, &Error{Errno: le.Ecode})
	} else {
		call.complete(m, nil)
	}
}

// Start sends m and returns the call without waiting.
func (c *Conn) Start(m Msg) *Call {
	call := &Call{done: make(chan struct{})}
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		call.complete(nil, c.err)
		return call
	}
	for {
		c.nextTag++
		if c.nextTag == NoTag {
			c.nextTag = 0
		}
		if c.pending[c.nextTag] == nil {
			break
		}
	}
	call.Tag = c.nextTag
	c.pending[call.Tag] = call
	c.mu.Unlock()
	c.wmu.Lock()
	err := WriteMsg(c.rwc, call.Tag, m)
	c.wmu.Unlock()
	if err != nil {
		c.mu.Lock()
		delete(c.pending, call.Tag)
		c.mu.Unlock()
		call.complete(nil, err)
	}
	return call
}

// RPC sends m and waits for its answer. On ctx cancel it flushes the
// request and returns ctx.Err().
func (c *Conn) RPC(ctx context.Context, m Msg) (Msg, error) {
	call := c.Start(m)
	select {
	case <-call.done:
		return call.resp, call.err
	case <-ctx.Done():
		fctx, cancel := context.WithTimeout(context.Background(), flushWait)
		c.Flush(fctx, call)
		cancel()
		return nil, ctx.Err()
	}
}

// flushWait bounds how long RPC waits for an Rflush after a cancel.
var flushWait = 5 * time.Second

// Flush sends a Tflush for call and waits for the Rflush. If the server
// answers the call first, the call keeps that answer; otherwise it ends
// with ErrFlushed. It returns an error if the Rflush does not arrive.
func (c *Conn) Flush(ctx context.Context, call *Call) error {
	c.mu.Lock()
	if c.pending[call.Tag] != call {
		c.mu.Unlock()
		return nil // already answered
	}
	call.flushing = true
	c.mu.Unlock()
	f := c.Start(&Tflush{OldTag: call.Tag})
	_, err := f.Wait(ctx)
	c.mu.Lock()
	if c.pending[call.Tag] == call {
		delete(c.pending, call.Tag)
	}
	call.flushing = false
	c.mu.Unlock()
	if err == nil {
		call.complete(nil, ErrFlushed)
	} else {
		call.complete(nil, err)
	}
	return err
}

// rpc runs m and checks the reply type.
func rpc[R Msg](ctx context.Context, c *Conn, m Msg) (R, error) {
	var zero R
	resp, err := c.RPC(ctx, m)
	if err != nil {
		return zero, err
	}
	r, ok := resp.(R)
	if !ok {
		return zero, fmt.Errorf("wire: unexpected reply type %d to type %d", resp.Type(), m.Type())
	}
	return r, nil
}

func (c *Conn) newFid() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.nextFid
	c.nextFid++
	return f
}

// Fid is a client fid.
type Fid struct {
	C   *Conn
	Num uint32
	Qid Qid
	// Iounit is set by Open and Create.
	Iounit uint32
}

// Attach attaches to aname as uname.
func (c *Conn) Attach(ctx context.Context, uname, aname string) (*Fid, error) {
	f := &Fid{C: c, Num: c.newFid()}
	r, err := rpc[*Rattach](ctx, c, &Tattach{Fid: f.Num, Afid: NoFid, Uname: uname, Aname: aname, NUname: NoFid})
	if err != nil {
		return nil, err
	}
	f.Qid = r.Qid
	return f, nil
}

// Walk walks names from f to a new fid. A walk that stops early is
// reported as ENOENT, and no new fid is created.
func (f *Fid) Walk(ctx context.Context, names ...string) (*Fid, error) {
	nf := &Fid{C: f.C, Num: f.C.newFid(), Qid: f.Qid}
	// Walk in chunks of 16 names, as 9P allows at most 16 per Twalk.
	from := f.Num
	for i := 0; i == 0 || i < len(names); i += 16 {
		chunk := names[i:min(i+16, len(names))]
		r, err := rpc[*Rwalk](ctx, f.C, &Twalk{Fid: from, NewFid: nf.Num, Names: chunk})
		if err != nil {
			if from == nf.Num {
				nf.Clunk(context.Background())
			}
			return nil, err
		}
		if len(r.Qids) != len(chunk) {
			if from == nf.Num {
				nf.Clunk(context.Background())
			}
			return nil, &Error{Errno: ENOENT}
		}
		if len(r.Qids) > 0 {
			nf.Qid = r.Qids[len(r.Qids)-1]
		}
		from = nf.Num
		if len(names) == 0 {
			break
		}
	}
	return nf, nil
}

// Clone returns a new fid for the same file.
func (f *Fid) Clone(ctx context.Context) (*Fid, error) { return f.Walk(ctx) }

// Open opens f with Linux open flags.
func (f *Fid) Open(ctx context.Context, flags uint32) error {
	r, err := rpc[*Rlopen](ctx, f.C, &Tlopen{Fid: f.Num, Flags: flags})
	if err != nil {
		return err
	}
	f.Qid, f.Iounit = r.Qid, r.Iounit
	return nil
}

// Create creates name in directory f and opens it; f then refers to the
// new file.
func (f *Fid) Create(ctx context.Context, name string, flags, mode uint32) error {
	r, err := rpc[*Rlcreate](ctx, f.C, &Tlcreate{Fid: f.Num, Name: name, Flags: flags, Mode: mode, GID: NoFid})
	if err != nil {
		return err
	}
	f.Qid, f.Iounit = r.Qid, r.Iounit
	return nil
}

// Mkdir creates directory name in directory f.
func (f *Fid) Mkdir(ctx context.Context, name string, mode uint32) (Qid, error) {
	r, err := rpc[*Rmkdir](ctx, f.C, &Tmkdir{Dfid: f.Num, Name: name, Mode: mode, GID: NoFid})
	if err != nil {
		return Qid{}, err
	}
	return r.Qid, nil
}

// Symlink creates a symlink name -> target in directory f.
func (f *Fid) Symlink(ctx context.Context, name, target string) (Qid, error) {
	r, err := rpc[*Rsymlink](ctx, f.C, &Tsymlink{Fid: f.Num, Name: name, Target: target, GID: NoFid})
	if err != nil {
		return Qid{}, err
	}
	return r.Qid, nil
}

// Readlink returns a symlink's target.
func (f *Fid) Readlink(ctx context.Context) (string, error) {
	r, err := rpc[*Rreadlink](ctx, f.C, &Treadlink{Fid: f.Num})
	if err != nil {
		return "", err
	}
	return r.Target, nil
}

// GetAttr returns f's attributes.
func (f *Fid) GetAttr(ctx context.Context) (Attr, error) {
	r, err := rpc[*Rgetattr](ctx, f.C, &Tgetattr{Fid: f.Num, Mask: GetattrBasic})
	if err != nil {
		return Attr{}, err
	}
	return r.Attr, nil
}

// SetAttr sends a Tsetattr for f.
func (f *Fid) SetAttr(ctx context.Context, m Tsetattr) error {
	m.Fid = f.Num
	_, err := rpc[*Rsetattr](ctx, f.C, &m)
	return err
}

// maxIO is the most data one read or write carries.
func (f *Fid) maxIO() uint32 {
	n := f.C.Msize - 24
	if f.Iounit > 0 && f.Iounit < n {
		n = f.Iounit
	}
	return n
}

// ReadAt reads up to count bytes at off (capped by msize).
func (f *Fid) ReadAt(ctx context.Context, off uint64, count uint32) ([]byte, error) {
	r, err := rpc[*Rread](ctx, f.C, f.readMsg(off, count))
	if err != nil {
		return nil, err
	}
	return r.Data, nil
}

func (f *Fid) readMsg(off uint64, count uint32) *Tread {
	if m := f.maxIO(); count > m {
		count = m
	}
	return &Tread{Fid: f.Num, Offset: off, Count: count}
}

// StartRead sends a Tread without waiting, for blocking-read and flush tests.
func (f *Fid) StartRead(off uint64, count uint32) *Call {
	return f.C.Start(f.readMsg(off, count))
}

// ReadAll reads from offset 0 until a read returns no data.
func (f *Fid) ReadAll(ctx context.Context) ([]byte, error) {
	var out []byte
	for {
		b, err := f.ReadAt(ctx, uint64(len(out)), f.maxIO())
		if err != nil {
			return out, err
		}
		if len(b) == 0 {
			return out, nil
		}
		out = append(out, b...)
	}
}

// WriteAt writes all of b at off, in as many messages as it takes.
func (f *Fid) WriteAt(ctx context.Context, b []byte, off uint64) (int, error) {
	n := 0
	for n < len(b) || len(b) == 0 {
		chunk := b[n:min(len(b), n+int(f.maxIO()))]
		r, err := rpc[*Rwrite](ctx, f.C, &Twrite{Fid: f.Num, Offset: off + uint64(n), Data: chunk})
		if err != nil {
			return n, err
		}
		if r.Count == 0 && len(chunk) > 0 {
			return n, io.ErrShortWrite
		}
		n += int(r.Count)
		if len(b) == 0 {
			break
		}
	}
	return n, nil
}

// Write writes one message with all of b at offset 0, for ctl and setup
// writes that must arrive whole.
func (f *Fid) Write(ctx context.Context, b []byte) error {
	if len(b) > int(f.maxIO()) {
		return fmt.Errorf("wire: write of %d bytes exceeds one message", len(b))
	}
	r, err := rpc[*Rwrite](ctx, f.C, &Twrite{Fid: f.Num, Data: b})
	if err != nil {
		return err
	}
	if int(r.Count) != len(b) {
		return fmt.Errorf("wire: short write: %d of %d bytes", r.Count, len(b))
	}
	return nil
}

// Readdir reads every directory entry.
func (f *Fid) Readdir(ctx context.Context) ([]Dirent, error) {
	var out []Dirent
	var off uint64
	for {
		r, err := rpc[*Rreaddir](ctx, f.C, &Treaddir{Fid: f.Num, Offset: off, Count: f.maxIO()})
		if err != nil {
			return out, err
		}
		if len(r.Entries) == 0 {
			return out, nil
		}
		out = append(out, r.Entries...)
		off = r.Entries[len(r.Entries)-1].Offset
	}
}

// Clunk releases f.
func (f *Fid) Clunk(ctx context.Context) error {
	_, err := rpc[*Rclunk](ctx, f.C, &Tclunk{Fid: f.Num})
	return err
}

// Remove removes the file f refers to and releases f.
func (f *Fid) Remove(ctx context.Context) error {
	_, err := rpc[*Rremove](ctx, f.C, &Tremove{Fid: f.Num})
	return err
}
