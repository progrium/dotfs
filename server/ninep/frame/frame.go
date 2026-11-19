// Package frame carries whole 9P messages over a connection. A Transport
// reads and writes one message at a time, so the server can look at each
// message before handing it on, and a message-oriented transport such as a
// WebSocket can send one 9P message per WebSocket message.
package frame

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

// MaxSize is the largest message a Transport accepts, header included.
const MaxSize = 4 << 20

// HeaderSize is the size of a 9P message header: size[4] type[1] tag[2].
const HeaderSize = 7

// ErrTooLarge is returned for a message over MaxSize or under HeaderSize.
var ErrTooLarge = errors.New("frame: bad 9P message size")

// Transport moves whole 9P messages. ReadFrame is called from one goroutine
// at a time; WriteFrame may be called from several, and must write each
// message whole.
type Transport interface {
	// ReadFrame returns the next message, header included. The caller owns
	// the returned slice.
	ReadFrame() ([]byte, error)
	// WriteFrame writes one message, header included.
	WriteFrame(b []byte) error
	Close() error
}

// Size returns the size field of a message header.
func Size(b []byte) uint32 { return binary.LittleEndian.Uint32(b) }

// Type returns the message type of a frame.
func Type(b []byte) uint8 { return b[4] }

// Tag returns the tag of a frame.
func Tag(b []byte) uint16 { return binary.LittleEndian.Uint16(b[5:]) }

// Check reports whether b is one well-formed frame by its size field.
func Check(b []byte) error {
	if len(b) < HeaderSize || len(b) > MaxSize || int(Size(b)) != len(b) {
		return fmt.Errorf("%w: %d bytes", ErrTooLarge, len(b))
	}
	return nil
}

type stream struct {
	r  *bufio.Reader
	c  io.ReadWriteCloser
	mu sync.Mutex
}

// NewStream returns a Transport over a byte stream such as a TCP or Unix
// connection.
func NewStream(c io.ReadWriteCloser) Transport {
	return &stream{r: bufio.NewReaderSize(c, 64<<10), c: c}
}

func (s *stream) ReadFrame() ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(s.r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(hdr[:])
	if n < HeaderSize || n > MaxSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrTooLarge, n)
	}
	b := make([]byte, n)
	copy(b, hdr[:])
	if _, err := io.ReadFull(s.r, b[4:]); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return b, nil
}

func (s *stream) WriteFrame(b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.c.Write(b)
	return err
}

func (s *stream) Close() error { return s.c.Close() }

// Assembler collects bytes written in pieces into whole frames, calling
// emit once per frame. 9P libraries often write one message with several
// Write calls; an Assembler turns that back into messages.
type Assembler struct {
	mu   sync.Mutex
	buf  []byte
	Emit func(frame []byte) error
}

// Write implements io.Writer.
func (a *Assembler) Write(p []byte) (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.buf = append(a.buf, p...)
	for len(a.buf) >= 4 {
		n := int(Size(a.buf))
		if n < HeaderSize || n > MaxSize {
			return 0, fmt.Errorf("%w: %d bytes", ErrTooLarge, n)
		}
		if len(a.buf) < n {
			break
		}
		f := make([]byte, n)
		copy(f, a.buf[:n])
		a.buf = append(a.buf[:0], a.buf[n:]...)
		if err := a.Emit(f); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}
