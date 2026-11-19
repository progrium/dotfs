package client

import (
	"bytes"
	"context"
	"io"
	"strings"

	"github.com/hugelgupf/p9/p9"

	"github.com/progrium/dotfs/fsproto"
)

var ctlNames = []string{".fs", "ctl"}

// Ctl sends cmd on a ctl fid of its own and reads the reply. Each
// in-flight command holds one fid; finished fids are kept open for reuse,
// so a command usually costs one write and one read.
func (c *Client) Ctl(ctx context.Context, cmd fsproto.Command) (fsproto.Reply, error) {
	f, err := c.getCtl()
	if err != nil {
		return fsproto.Reply{}, pathErr("ctl", "/.fs/ctl", err)
	}
	type result struct {
		r   fsproto.Reply
		err error
	}
	done := make(chan result, 1)
	go func() {
		r, err := c.ctlOnce(f, cmd)
		done <- result{r, err}
	}()
	select {
	case res := <-done:
		if res.err != nil {
			f.Close()
			return res.r, res.err
		}
		c.putCtl(f)
		return res.r, nil
	case <-ctx.Done():
		// The command may still complete on the server; the fid is not
		// reused, and is clunked once the exchange ends.
		go func() {
			<-done
			f.Close()
		}()
		return fsproto.Reply{}, ctx.Err()
	}
}

func (c *Client) ctlOnce(f p9.File, cmd fsproto.Command) (fsproto.Reply, error) {
	line := cmd.Encode()
	if err := writeFull(f, []byte(line), 0); err != nil {
		return fsproto.Reply{}, pathErr("ctl write", "/.fs/ctl", err)
	}
	var buf bytes.Buffer
	b := make([]byte, c.readSize())
	for off := int64(0); ; {
		n, err := f.ReadAt(b, off)
		buf.Write(b[:n])
		off += int64(n)
		if bytes.IndexByte(buf.Bytes(), '\n') >= 0 || err == io.EOF || (err == nil && n == 0) {
			break
		}
		if err != nil {
			return fsproto.Reply{}, pathErr("ctl read", "/.fs/ctl", err)
		}
		if buf.Len() > fsproto.MaxLineLen {
			break
		}
	}
	reply, _, _ := strings.Cut(buf.String(), "\n")
	return fsproto.ParseReply(cmd.Name(), reply)
}

func (c *Client) getCtl() (p9.File, error) {
	c.mu.Lock()
	if n := len(c.idleCtl); n > 0 {
		f := c.idleCtl[n-1]
		c.idleCtl = c.idleCtl[:n-1]
		c.mu.Unlock()
		return f, nil
	}
	c.mu.Unlock()
	f, err := c.walk(ctlNames)
	if err != nil {
		return nil, err
	}
	if _, _, err := f.Open(p9.ReadWrite); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func (c *Client) putCtl(f p9.File) {
	c.mu.Lock()
	if !c.closed && len(c.idleCtl) < c.maxIdle {
		c.idleCtl = append(c.idleCtl, f)
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	f.Close()
}

// writeFull writes all of p at off, looping over short writes.
func writeFull(f io.WriterAt, p []byte, off int64) error {
	for {
		n, err := f.WriteAt(p, off)
		if err != nil {
			return err
		}
		if n >= len(p) {
			return nil
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p, off = p[n:], off+int64(n)
	}
}
