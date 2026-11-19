package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/hugelgupf/p9/p9"

	"github.com/progrium/dotfs/fsproto"
)

var eventsNames = []string{".fs", "events"}

// ErrStreamEnded is returned by an event stream whose server ended it
// without a reset record, which the protocol does not allow; the stream
// should be reopened.
var ErrStreamEnded = errors.New("dotfs: event stream ended without reset")

// Events opens a stream on .fs/events and writes setup, if any, as one
// write before the first read.
func (c *Client) Events(ctx context.Context, setup fsproto.EventsSetup) (EventStream, error) {
	f, err := c.walk(eventsNames)
	if err != nil {
		return nil, pathErr("events", "/.fs/events", err)
	}
	line := setup.Encode()
	mode := p9.ReadOnly
	if line != "" {
		mode = p9.ReadWrite
	}
	if _, _, err := f.Open(mode); err != nil {
		f.Close()
		return nil, pathErr("events", "/.fs/events", err)
	}
	if line != "" {
		if err := writeFull(f, []byte(line), 0); err != nil {
			f.Close()
			return nil, pathErr("events setup", "/.fs/events", err)
		}
	}
	return &eventStream{f: f, size: c.readSize()}, nil
}

type readResult struct {
	b   []byte
	err error
}

// eventStream reads records from one events fid. hugelgupf/p9's client
// cannot send Tflush, so a Next canceled by its context leaves the Tread
// pending and the next call picks up its result: no record is lost, and
// the stream stays usable.
type eventStream struct {
	f    p9.File
	size int

	mu      sync.Mutex // serializes Next
	off     int64
	partial []byte // the start of a record a server split across reads
	buf     []fsproto.Event
	pending chan readResult
	ended   bool
	err     error

	closeOnce sync.Once
}

func (s *eventStream) Next(ctx context.Context) (fsproto.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if len(s.buf) > 0 {
			ev := s.buf[0]
			s.buf = s.buf[1:]
			if ev.Op == fsproto.OpReset {
				s.ended = true
				s.buf = nil
			}
			return ev, nil
		}
		if s.err != nil {
			return fsproto.Event{}, s.err
		}
		if s.ended {
			return fsproto.Event{}, io.EOF
		}
		if err := ctx.Err(); err != nil {
			return fsproto.Event{}, err
		}
		if s.pending == nil {
			ch := make(chan readResult, 1)
			s.pending = ch
			off := s.off
			go func() {
				b := make([]byte, s.size)
				n, err := s.f.ReadAt(b, off)
				if n > 0 && err == io.EOF {
					err = nil
				}
				ch <- readResult{b[:n], err}
			}()
		}
		select {
		case <-ctx.Done():
			return fsproto.Event{}, ctx.Err()
		case res := <-s.pending:
			s.pending = nil
			if err := s.take(res); err != nil {
				s.err = err
			}
		}
	}
}

// take parses one read's records into buf.
func (s *eventStream) take(res readResult) error {
	if res.err == io.EOF || (res.err == nil && len(res.b) == 0) {
		return ErrStreamEnded
	}
	if res.err != nil {
		return pathErr("events read", "/.fs/events", res.err)
	}
	s.off += int64(len(res.b))
	data := res.b
	if len(s.partial) > 0 {
		data = append(s.partial, data...)
		s.partial = nil
	}
	for len(data) > 0 {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			if len(data) > fsproto.MaxLineLen {
				return fsproto.ErrSyntax
			}
			s.partial = append([]byte(nil), data...)
			return nil
		}
		ev, err := fsproto.ParseEvent(string(data[:i]))
		if err != nil {
			return err
		}
		s.buf = append(s.buf, ev)
		data = data[i+1:]
	}
	return nil
}

func (s *eventStream) Close() error {
	var err error
	s.closeOnce.Do(func() { err = s.f.Close() })
	return mapErrno(err)
}
