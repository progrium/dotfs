package ninep

import (
	"encoding/binary"

	"github.com/progrium/dotfs/server/ninep/frame"
)

// 9P message types the server looks at itself. Types shared by both
// dialects have the same numbers in each.
const (
	rlerror  = 7
	tlopen   = 12
	rlopen   = 13
	tversion = 100
	rversion = 101
	tattach  = 104
	rattach  = 105
	tflush   = 108
	rflush   = 109
	twalk    = 110
	rwalk    = 111
	tread    = 116
	rread    = 117
	twrite   = 118
	rwrite   = 119
	tclunk   = 120
	tremove  = 122

	noFID = ^uint32(0)
)

// dec reads fields after a frame's header. A short message sets bad.
type dec struct {
	b   []byte
	bad bool
}

func newDec(f []byte) *dec { return &dec{b: f[frame.HeaderSize:]} }

func (d *dec) take(n int) []byte {
	if d.bad || len(d.b) < n {
		d.bad = true
		return make([]byte, n)
	}
	v := d.b[:n]
	d.b = d.b[n:]
	return v
}

func (d *dec) u8() uint8   { return d.take(1)[0] }
func (d *dec) u16() uint16 { return binary.LittleEndian.Uint16(d.take(2)) }
func (d *dec) u32() uint32 { return binary.LittleEndian.Uint32(d.take(4)) }
func (d *dec) u64() uint64 { return binary.LittleEndian.Uint64(d.take(8)) }
func (d *dec) str() string { return string(d.take(int(d.u16()))) }
func (d *dec) qid() qid {
	t := d.u8()
	v := d.u32()
	return qid{typ: t, vers: v, path: d.u64()}
}

// enc builds a message.
type enc struct{ b []byte }

func newEnc(typ uint8, tag uint16) *enc {
	e := &enc{b: make([]byte, frame.HeaderSize, 64)}
	e.b[4] = typ
	binary.LittleEndian.PutUint16(e.b[5:], tag)
	return e
}

func (e *enc) u8(v uint8) *enc   { e.b = append(e.b, v); return e }
func (e *enc) u16(v uint16) *enc { e.b = binary.LittleEndian.AppendUint16(e.b, v); return e }
func (e *enc) u32(v uint32) *enc { e.b = binary.LittleEndian.AppendUint32(e.b, v); return e }
func (e *enc) u64(v uint64) *enc { e.b = binary.LittleEndian.AppendUint64(e.b, v); return e }
func (e *enc) str(s string) *enc { e.u16(uint16(len(s))); e.b = append(e.b, s...); return e }
func (e *enc) data(p []byte) *enc {
	e.u32(uint32(len(p)))
	e.b = append(e.b, p...)
	return e
}
func (e *enc) qid(q qid) *enc { return e.u8(q.typ).u32(q.vers).u64(q.path) }

func (e *enc) bytes() []byte {
	binary.LittleEndian.PutUint32(e.b, uint32(len(e.b)))
	return e.b
}

// parseVersion reads a Tversion.
func parseVersion(f []byte) (msize uint32, version string, ok bool) {
	d := newDec(f)
	msize = d.u32()
	version = d.str()
	return msize, version, !d.bad
}
