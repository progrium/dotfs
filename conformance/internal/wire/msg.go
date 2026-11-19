// Package wire is a small 9P2000.L codec: the messages the conformance
// suite and its test shim use, framed as size[4] type[1] tag[2] body.
//
// The suite carries its own codec instead of using hugelgupf/p9 for two
// reasons: p9's client cannot send Tflush, which the events tests need, and
// p9's server answers Tflush without canceling the request, which the shim
// needs to do. Tests in this package check the codec against p9 in both
// directions so the two stay compatible.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Version is the protocol version string the suite speaks.
const Version = "9P2000.L"

// NoTag is the tag of Tversion.
const NoTag uint16 = 0xFFFF

// NoFid is the fid meaning "none", as in Tattach's afid.
const NoFid uint32 = 0xFFFFFFFF

// Message types.
const (
	TypeRlerror   uint8 = 7
	TypeTstatfs   uint8 = 8
	TypeRstatfs   uint8 = 9
	TypeTlopen    uint8 = 12
	TypeRlopen    uint8 = 13
	TypeTlcreate  uint8 = 14
	TypeRlcreate  uint8 = 15
	TypeTsymlink  uint8 = 16
	TypeRsymlink  uint8 = 17
	TypeTreadlink uint8 = 22
	TypeRreadlink uint8 = 23
	TypeTgetattr  uint8 = 24
	TypeRgetattr  uint8 = 25
	TypeTsetattr  uint8 = 26
	TypeRsetattr  uint8 = 27
	TypeTreaddir  uint8 = 40
	TypeRreaddir  uint8 = 41
	TypeTmkdir    uint8 = 72
	TypeRmkdir    uint8 = 73
	TypeTrenameat uint8 = 74
	TypeRrenameat uint8 = 75
	TypeTunlinkat uint8 = 76
	TypeRunlinkat uint8 = 77
	TypeTversion  uint8 = 100
	TypeRversion  uint8 = 101
	TypeTattach   uint8 = 104
	TypeRattach   uint8 = 105
	TypeTflush    uint8 = 108
	TypeRflush    uint8 = 109
	TypeTwalk     uint8 = 110
	TypeRwalk     uint8 = 111
	TypeTread     uint8 = 116
	TypeRread     uint8 = 117
	TypeTwrite    uint8 = 118
	TypeRwrite    uint8 = 119
	TypeTclunk    uint8 = 120
	TypeRclunk    uint8 = 121
	TypeTremove   uint8 = 122
	TypeRremove   uint8 = 123
)

// Qid types.
const (
	QTDir     uint8 = 0x80
	QTSymlink uint8 = 0x02
	QTFile    uint8 = 0x00
)

// Open flags (Linux values, as 9P2000.L uses them).
const (
	ORDONLY uint32 = 0
	OWRONLY uint32 = 1
	ORDWR   uint32 = 2
	OTRUNC  uint32 = 0o1000
)

// Mode type bits, as in Rgetattr's mode.
const (
	SIFMT  uint32 = 0o170000
	SIFDIR uint32 = 0o040000
	SIFREG uint32 = 0o100000
	SIFLNK uint32 = 0o120000
)

// Getattr masks.
const (
	GetattrBasic uint64 = 0x7FF // mode through blocks
	GetattrAll   uint64 = 0x3FFF
)

// Setattr valid bits.
const (
	SetattrMode     uint32 = 0x1
	SetattrSize     uint32 = 0x8
	SetattrMtime    uint32 = 0x20
	SetattrMtimeSet uint32 = 0x100
)

// Errno values the suite and shim use.
const (
	EPERM     uint32 = 1
	ENOENT    uint32 = 2
	EIO       uint32 = 5
	EBADF     uint32 = 9
	EEXIST    uint32 = 17
	ENOTDIR   uint32 = 20
	EISDIR    uint32 = 21
	EINVAL    uint32 = 22
	ENOSPC    uint32 = 28
	ENOSYS    uint32 = 38
	ENOTEMPTY uint32 = 39
	EINTR     uint32 = 4
	EACCES    uint32 = 13
)

// Qid is a server's unique file identity.
type Qid struct {
	Type    uint8
	Version uint32
	Path    uint64
}

// Attr is Rgetattr's body.
type Attr struct {
	Valid               uint64
	Qid                 Qid
	Mode, UID, GID      uint32
	Nlink, Rdev, Size   uint64
	Blksize, Blocks     uint64
	AtimeSec, AtimeNsec uint64
	MtimeSec, MtimeNsec uint64
	CtimeSec, CtimeNsec uint64
	BtimeSec, BtimeNsec uint64
	Gen, DataVersion    uint64
}

// Dirent is one Rreaddir entry.
type Dirent struct {
	Qid    Qid
	Offset uint64
	Type   uint8
	Name   string
}

// Msg is one 9P message body. Tag and framing are handled by WriteMsg and
// ReadMsg.
type Msg interface {
	Type() uint8
	encode(*enc)
	decode(*dec)
}

// The messages. Field names follow the 9P2000.L manual pages.
type (
	Rlerror struct{ Ecode uint32 }
	Tstatfs struct{ Fid uint32 }
	Rstatfs struct {
		FsType, Bsize                       uint32
		Blocks, Bfree, Bavail, Files, Ffree uint64
		Fsid                                uint64
		Namelen                             uint32
	}
	Tlopen struct {
		Fid   uint32
		Flags uint32
	}
	Rlopen struct {
		Qid    Qid
		Iounit uint32
	}
	Tlcreate struct {
		Fid              uint32
		Name             string
		Flags, Mode, GID uint32
	}
	Rlcreate struct {
		Qid    Qid
		Iounit uint32
	}
	Tsymlink struct {
		Fid          uint32
		Name, Target string
		GID          uint32
	}
	Rsymlink  struct{ Qid Qid }
	Treadlink struct{ Fid uint32 }
	Rreadlink struct{ Target string }
	Tgetattr  struct {
		Fid  uint32
		Mask uint64
	}
	Rgetattr struct{ Attr Attr }
	Tsetattr struct {
		Fid                   uint32
		Valid, Mode, UID, GID uint32
		Size                  uint64
		AtimeSec, AtimeNsec   uint64
		MtimeSec, MtimeNsec   uint64
	}
	Rsetattr struct{}
	Treaddir struct {
		Fid    uint32
		Offset uint64
		Count  uint32
	}
	Rreaddir struct{ Entries []Dirent }
	Tmkdir   struct {
		Dfid      uint32
		Name      string
		Mode, GID uint32
	}
	Rmkdir    struct{ Qid Qid }
	Trenameat struct {
		OldDir  uint32
		OldName string
		NewDir  uint32
		NewName string
	}
	Rrenameat struct{}
	Tunlinkat struct {
		Dir   uint32
		Name  string
		Flags uint32
	}
	Runlinkat struct{}
	Tversion  struct {
		Msize   uint32
		Version string
	}
	Rversion struct {
		Msize   uint32
		Version string
	}
	Tattach struct {
		Fid, Afid    uint32
		Uname, Aname string
		NUname       uint32
	}
	Rattach struct{ Qid Qid }
	Tflush  struct{ OldTag uint16 }
	Rflush  struct{}
	Twalk   struct {
		Fid, NewFid uint32
		Names       []string
	}
	Rwalk struct{ Qids []Qid }
	Tread struct {
		Fid    uint32
		Offset uint64
		Count  uint32
	}
	Rread  struct{ Data []byte }
	Twrite struct {
		Fid    uint32
		Offset uint64
		Data   []byte
	}
	Rwrite  struct{ Count uint32 }
	Tclunk  struct{ Fid uint32 }
	Rclunk  struct{}
	Tremove struct{ Fid uint32 }
	Rremove struct{}
)

func (*Rlerror) Type() uint8   { return TypeRlerror }
func (*Tstatfs) Type() uint8   { return TypeTstatfs }
func (*Rstatfs) Type() uint8   { return TypeRstatfs }
func (*Tlopen) Type() uint8    { return TypeTlopen }
func (*Rlopen) Type() uint8    { return TypeRlopen }
func (*Tlcreate) Type() uint8  { return TypeTlcreate }
func (*Rlcreate) Type() uint8  { return TypeRlcreate }
func (*Tsymlink) Type() uint8  { return TypeTsymlink }
func (*Rsymlink) Type() uint8  { return TypeRsymlink }
func (*Treadlink) Type() uint8 { return TypeTreadlink }
func (*Rreadlink) Type() uint8 { return TypeRreadlink }
func (*Tgetattr) Type() uint8  { return TypeTgetattr }
func (*Rgetattr) Type() uint8  { return TypeRgetattr }
func (*Tsetattr) Type() uint8  { return TypeTsetattr }
func (*Rsetattr) Type() uint8  { return TypeRsetattr }
func (*Treaddir) Type() uint8  { return TypeTreaddir }
func (*Rreaddir) Type() uint8  { return TypeRreaddir }
func (*Tmkdir) Type() uint8    { return TypeTmkdir }
func (*Rmkdir) Type() uint8    { return TypeRmkdir }
func (*Trenameat) Type() uint8 { return TypeTrenameat }
func (*Rrenameat) Type() uint8 { return TypeRrenameat }
func (*Tunlinkat) Type() uint8 { return TypeTunlinkat }
func (*Runlinkat) Type() uint8 { return TypeRunlinkat }
func (*Tversion) Type() uint8  { return TypeTversion }
func (*Rversion) Type() uint8  { return TypeRversion }
func (*Tattach) Type() uint8   { return TypeTattach }
func (*Rattach) Type() uint8   { return TypeRattach }
func (*Tflush) Type() uint8    { return TypeTflush }
func (*Rflush) Type() uint8    { return TypeRflush }
func (*Twalk) Type() uint8     { return TypeTwalk }
func (*Rwalk) Type() uint8     { return TypeRwalk }
func (*Tread) Type() uint8     { return TypeTread }
func (*Rread) Type() uint8     { return TypeRread }
func (*Twrite) Type() uint8    { return TypeTwrite }
func (*Rwrite) Type() uint8    { return TypeRwrite }
func (*Tclunk) Type() uint8    { return TypeTclunk }
func (*Rclunk) Type() uint8    { return TypeRclunk }
func (*Tremove) Type() uint8   { return TypeTremove }
func (*Rremove) Type() uint8   { return TypeRremove }

func (m *Rlerror) encode(e *enc) { e.u32(m.Ecode) }
func (m *Rlerror) decode(d *dec) { m.Ecode = d.u32() }
func (m *Tstatfs) encode(e *enc) { e.u32(m.Fid) }
func (m *Tstatfs) decode(d *dec) { m.Fid = d.u32() }
func (m *Rstatfs) encode(e *enc) {
	e.u32(m.FsType)
	e.u32(m.Bsize)
	e.u64(m.Blocks)
	e.u64(m.Bfree)
	e.u64(m.Bavail)
	e.u64(m.Files)
	e.u64(m.Ffree)
	e.u64(m.Fsid)
	e.u32(m.Namelen)
}
func (m *Rstatfs) decode(d *dec) {
	m.FsType, m.Bsize = d.u32(), d.u32()
	m.Blocks, m.Bfree, m.Bavail, m.Files, m.Ffree, m.Fsid = d.u64(), d.u64(), d.u64(), d.u64(), d.u64(), d.u64()
	m.Namelen = d.u32()
}
func (m *Tlopen) encode(e *enc) { e.u32(m.Fid); e.u32(m.Flags) }
func (m *Tlopen) decode(d *dec) { m.Fid, m.Flags = d.u32(), d.u32() }
func (m *Rlopen) encode(e *enc) { e.qid(m.Qid); e.u32(m.Iounit) }
func (m *Rlopen) decode(d *dec) { m.Qid, m.Iounit = d.qid(), d.u32() }
func (m *Tlcreate) encode(e *enc) {
	e.u32(m.Fid)
	e.str(m.Name)
	e.u32(m.Flags)
	e.u32(m.Mode)
	e.u32(m.GID)
}
func (m *Tlcreate) decode(d *dec) {
	m.Fid, m.Name, m.Flags, m.Mode, m.GID = d.u32(), d.str(), d.u32(), d.u32(), d.u32()
}
func (m *Rlcreate) encode(e *enc) { e.qid(m.Qid); e.u32(m.Iounit) }
func (m *Rlcreate) decode(d *dec) { m.Qid, m.Iounit = d.qid(), d.u32() }
func (m *Tsymlink) encode(e *enc) { e.u32(m.Fid); e.str(m.Name); e.str(m.Target); e.u32(m.GID) }
func (m *Tsymlink) decode(d *dec) {
	m.Fid, m.Name, m.Target, m.GID = d.u32(), d.str(), d.str(), d.u32()
}
func (m *Rsymlink) encode(e *enc)  { e.qid(m.Qid) }
func (m *Rsymlink) decode(d *dec)  { m.Qid = d.qid() }
func (m *Treadlink) encode(e *enc) { e.u32(m.Fid) }
func (m *Treadlink) decode(d *dec) { m.Fid = d.u32() }
func (m *Rreadlink) encode(e *enc) { e.str(m.Target) }
func (m *Rreadlink) decode(d *dec) { m.Target = d.str() }
func (m *Tgetattr) encode(e *enc)  { e.u32(m.Fid); e.u64(m.Mask) }
func (m *Tgetattr) decode(d *dec)  { m.Fid, m.Mask = d.u32(), d.u64() }
func (m *Rgetattr) encode(e *enc) {
	a := &m.Attr
	e.u64(a.Valid)
	e.qid(a.Qid)
	e.u32(a.Mode)
	e.u32(a.UID)
	e.u32(a.GID)
	for _, v := range []uint64{a.Nlink, a.Rdev, a.Size, a.Blksize, a.Blocks,
		a.AtimeSec, a.AtimeNsec, a.MtimeSec, a.MtimeNsec, a.CtimeSec, a.CtimeNsec,
		a.BtimeSec, a.BtimeNsec, a.Gen, a.DataVersion} {
		e.u64(v)
	}
}
func (m *Rgetattr) decode(d *dec) {
	a := &m.Attr
	a.Valid = d.u64()
	a.Qid = d.qid()
	a.Mode, a.UID, a.GID = d.u32(), d.u32(), d.u32()
	for _, p := range []*uint64{&a.Nlink, &a.Rdev, &a.Size, &a.Blksize, &a.Blocks,
		&a.AtimeSec, &a.AtimeNsec, &a.MtimeSec, &a.MtimeNsec, &a.CtimeSec, &a.CtimeNsec,
		&a.BtimeSec, &a.BtimeNsec, &a.Gen, &a.DataVersion} {
		*p = d.u64()
	}
}
func (m *Tsetattr) encode(e *enc) {
	e.u32(m.Fid)
	e.u32(m.Valid)
	e.u32(m.Mode)
	e.u32(m.UID)
	e.u32(m.GID)
	e.u64(m.Size)
	e.u64(m.AtimeSec)
	e.u64(m.AtimeNsec)
	e.u64(m.MtimeSec)
	e.u64(m.MtimeNsec)
}
func (m *Tsetattr) decode(d *dec) {
	m.Fid, m.Valid, m.Mode, m.UID, m.GID = d.u32(), d.u32(), d.u32(), d.u32(), d.u32()
	m.Size, m.AtimeSec, m.AtimeNsec, m.MtimeSec, m.MtimeNsec = d.u64(), d.u64(), d.u64(), d.u64(), d.u64()
}
func (m *Rsetattr) encode(*enc)   {}
func (m *Rsetattr) decode(*dec)   {}
func (m *Treaddir) encode(e *enc) { e.u32(m.Fid); e.u64(m.Offset); e.u32(m.Count) }
func (m *Treaddir) decode(d *dec) { m.Fid, m.Offset, m.Count = d.u32(), d.u64(), d.u32() }
func (m *Rreaddir) encode(e *enc) {
	var body enc
	for _, de := range m.Entries {
		body.dirent(de)
	}
	e.u32(uint32(len(body.b)))
	e.b = append(e.b, body.b...)
}
func (m *Rreaddir) decode(d *dec) {
	n := d.u32()
	body := dec{b: d.take(int(n))}
	for len(body.b) > 0 && body.err == nil {
		var de Dirent
		de.Qid = body.qid()
		de.Offset = body.u64()
		de.Type = body.u8()
		de.Name = body.str()
		if body.err == nil {
			m.Entries = append(m.Entries, de)
		}
	}
	if body.err != nil && d.err == nil {
		d.err = body.err
	}
}

// DirentSize returns the bytes de takes in an Rreaddir body.
func DirentSize(de Dirent) int { return 13 + 8 + 1 + 2 + len(de.Name) }

func (m *Tmkdir) encode(e *enc) { e.u32(m.Dfid); e.str(m.Name); e.u32(m.Mode); e.u32(m.GID) }
func (m *Tmkdir) decode(d *dec) { m.Dfid, m.Name, m.Mode, m.GID = d.u32(), d.str(), d.u32(), d.u32() }
func (m *Rmkdir) encode(e *enc) { e.qid(m.Qid) }
func (m *Rmkdir) decode(d *dec) { m.Qid = d.qid() }
func (m *Trenameat) encode(e *enc) {
	e.u32(m.OldDir)
	e.str(m.OldName)
	e.u32(m.NewDir)
	e.str(m.NewName)
}
func (m *Trenameat) decode(d *dec) {
	m.OldDir, m.OldName, m.NewDir, m.NewName = d.u32(), d.str(), d.u32(), d.str()
}
func (m *Rrenameat) encode(*enc)   {}
func (m *Rrenameat) decode(*dec)   {}
func (m *Tunlinkat) encode(e *enc) { e.u32(m.Dir); e.str(m.Name); e.u32(m.Flags) }
func (m *Tunlinkat) decode(d *dec) { m.Dir, m.Name, m.Flags = d.u32(), d.str(), d.u32() }
func (m *Runlinkat) encode(*enc)   {}
func (m *Runlinkat) decode(*dec)   {}
func (m *Tversion) encode(e *enc)  { e.u32(m.Msize); e.str(m.Version) }
func (m *Tversion) decode(d *dec)  { m.Msize, m.Version = d.u32(), d.str() }
func (m *Rversion) encode(e *enc)  { e.u32(m.Msize); e.str(m.Version) }
func (m *Rversion) decode(d *dec)  { m.Msize, m.Version = d.u32(), d.str() }
func (m *Tattach) encode(e *enc) {
	e.u32(m.Fid)
	e.u32(m.Afid)
	e.str(m.Uname)
	e.str(m.Aname)
	e.u32(m.NUname)
}
func (m *Tattach) decode(d *dec) {
	m.Fid, m.Afid, m.Uname, m.Aname, m.NUname = d.u32(), d.u32(), d.str(), d.str(), d.u32()
}
func (m *Rattach) encode(e *enc) { e.qid(m.Qid) }
func (m *Rattach) decode(d *dec) { m.Qid = d.qid() }
func (m *Tflush) encode(e *enc)  { e.u16(m.OldTag) }
func (m *Tflush) decode(d *dec)  { m.OldTag = d.u16() }
func (m *Rflush) encode(*enc)    {}
func (m *Rflush) decode(*dec)    {}
func (m *Twalk) encode(e *enc) {
	e.u32(m.Fid)
	e.u32(m.NewFid)
	e.u16(uint16(len(m.Names)))
	for _, n := range m.Names {
		e.str(n)
	}
}
func (m *Twalk) decode(d *dec) {
	m.Fid, m.NewFid = d.u32(), d.u32()
	n := int(d.u16())
	for i := 0; i < n && d.err == nil; i++ {
		m.Names = append(m.Names, d.str())
	}
}
func (m *Rwalk) encode(e *enc) {
	e.u16(uint16(len(m.Qids)))
	for _, q := range m.Qids {
		e.qid(q)
	}
}
func (m *Rwalk) decode(d *dec) {
	n := int(d.u16())
	for i := 0; i < n && d.err == nil; i++ {
		m.Qids = append(m.Qids, d.qid())
	}
}
func (m *Tread) encode(e *enc) { e.u32(m.Fid); e.u64(m.Offset); e.u32(m.Count) }
func (m *Tread) decode(d *dec) { m.Fid, m.Offset, m.Count = d.u32(), d.u64(), d.u32() }
func (m *Rread) encode(e *enc) { e.u32(uint32(len(m.Data))); e.b = append(e.b, m.Data...) }
func (m *Rread) decode(d *dec) { n := d.u32(); m.Data = append([]byte(nil), d.take(int(n))...) }
func (m *Twrite) encode(e *enc) {
	e.u32(m.Fid)
	e.u64(m.Offset)
	e.u32(uint32(len(m.Data)))
	e.b = append(e.b, m.Data...)
}
func (m *Twrite) decode(d *dec) {
	m.Fid, m.Offset = d.u32(), d.u64()
	n := d.u32()
	m.Data = append([]byte(nil), d.take(int(n))...)
}
func (m *Rwrite) encode(e *enc)  { e.u32(m.Count) }
func (m *Rwrite) decode(d *dec)  { m.Count = d.u32() }
func (m *Tclunk) encode(e *enc)  { e.u32(m.Fid) }
func (m *Tclunk) decode(d *dec)  { m.Fid = d.u32() }
func (m *Rclunk) encode(*enc)    {}
func (m *Rclunk) decode(*dec)    {}
func (m *Tremove) encode(e *enc) { e.u32(m.Fid) }
func (m *Tremove) decode(d *dec) { m.Fid = d.u32() }
func (m *Rremove) encode(*enc)   {}
func (m *Rremove) decode(*dec)   {}

var registry = map[uint8]func() Msg{
	TypeRlerror: func() Msg { return &Rlerror{} }, TypeTstatfs: func() Msg { return &Tstatfs{} },
	TypeRstatfs: func() Msg { return &Rstatfs{} }, TypeTlopen: func() Msg { return &Tlopen{} },
	TypeRlopen: func() Msg { return &Rlopen{} }, TypeTlcreate: func() Msg { return &Tlcreate{} },
	TypeRlcreate: func() Msg { return &Rlcreate{} }, TypeTsymlink: func() Msg { return &Tsymlink{} },
	TypeRsymlink: func() Msg { return &Rsymlink{} }, TypeTreadlink: func() Msg { return &Treadlink{} },
	TypeRreadlink: func() Msg { return &Rreadlink{} }, TypeTgetattr: func() Msg { return &Tgetattr{} },
	TypeRgetattr: func() Msg { return &Rgetattr{} }, TypeTsetattr: func() Msg { return &Tsetattr{} },
	TypeRsetattr: func() Msg { return &Rsetattr{} }, TypeTreaddir: func() Msg { return &Treaddir{} },
	TypeRreaddir: func() Msg { return &Rreaddir{} }, TypeTmkdir: func() Msg { return &Tmkdir{} },
	TypeRmkdir: func() Msg { return &Rmkdir{} }, TypeTrenameat: func() Msg { return &Trenameat{} },
	TypeRrenameat: func() Msg { return &Rrenameat{} }, TypeTunlinkat: func() Msg { return &Tunlinkat{} },
	TypeRunlinkat: func() Msg { return &Runlinkat{} }, TypeTversion: func() Msg { return &Tversion{} },
	TypeRversion: func() Msg { return &Rversion{} }, TypeTattach: func() Msg { return &Tattach{} },
	TypeRattach: func() Msg { return &Rattach{} }, TypeTflush: func() Msg { return &Tflush{} },
	TypeRflush: func() Msg { return &Rflush{} }, TypeTwalk: func() Msg { return &Twalk{} },
	TypeRwalk: func() Msg { return &Rwalk{} }, TypeTread: func() Msg { return &Tread{} },
	TypeRread: func() Msg { return &Rread{} }, TypeTwrite: func() Msg { return &Twrite{} },
	TypeRwrite: func() Msg { return &Rwrite{} }, TypeTclunk: func() Msg { return &Tclunk{} },
	TypeRclunk: func() Msg { return &Rclunk{} }, TypeTremove: func() Msg { return &Tremove{} },
	TypeRremove: func() Msg { return &Rremove{} },
}

// ErrUnknownType is returned by ReadMsg for a message type the codec does
// not know. The message has been consumed, so the stream stays in step.
var ErrUnknownType = errors.New("wire: unknown message type")

// HeaderSize is the size of size[4] type[1] tag[2].
const HeaderSize = 7

// WriteMsg frames and writes one message.
func WriteMsg(w io.Writer, tag uint16, m Msg) error {
	e := enc{b: make([]byte, HeaderSize, 64)}
	m.encode(&e)
	binary.LittleEndian.PutUint32(e.b[0:], uint32(len(e.b)))
	e.b[4] = m.Type()
	binary.LittleEndian.PutUint16(e.b[5:], tag)
	_, err := w.Write(e.b)
	return err
}

// ReadMsg reads one framed message. It rejects messages larger than max
// bytes. For an unknown type it returns the type and tag with
// ErrUnknownType.
func ReadMsg(r io.Reader, max uint32) (uint8, uint16, Msg, error) {
	var hdr [HeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, 0, nil, err
	}
	size := binary.LittleEndian.Uint32(hdr[0:])
	typ := hdr[4]
	tag := binary.LittleEndian.Uint16(hdr[5:])
	if size < HeaderSize || (max > 0 && size > max) {
		return typ, tag, nil, fmt.Errorf("wire: bad message size %d", size)
	}
	body := make([]byte, size-HeaderSize)
	if _, err := io.ReadFull(r, body); err != nil {
		return typ, tag, nil, err
	}
	mk := registry[typ]
	if mk == nil {
		return typ, tag, nil, fmt.Errorf("%w %d", ErrUnknownType, typ)
	}
	m := mk()
	d := dec{b: body}
	m.decode(&d)
	if d.err != nil {
		return typ, tag, nil, fmt.Errorf("wire: decoding type %d: %w", typ, d.err)
	}
	return typ, tag, m, nil
}

type enc struct{ b []byte }

func (e *enc) u8(v uint8)   { e.b = append(e.b, v) }
func (e *enc) u16(v uint16) { e.b = binary.LittleEndian.AppendUint16(e.b, v) }
func (e *enc) u32(v uint32) { e.b = binary.LittleEndian.AppendUint32(e.b, v) }
func (e *enc) u64(v uint64) { e.b = binary.LittleEndian.AppendUint64(e.b, v) }
func (e *enc) str(s string) { e.u16(uint16(len(s))); e.b = append(e.b, s...) }
func (e *enc) qid(q Qid)    { e.u8(q.Type); e.u32(q.Version); e.u64(q.Path) }
func (e *enc) dirent(de Dirent) {
	e.qid(de.Qid)
	e.u64(de.Offset)
	e.u8(de.Type)
	e.str(de.Name)
}

var errShort = errors.New("message too short")

type dec struct {
	b   []byte
	err error
}

func (d *dec) take(n int) []byte {
	if d.err != nil {
		return nil
	}
	if n < 0 || len(d.b) < n {
		d.err = errShort
		d.b = nil
		return nil
	}
	v := d.b[:n]
	d.b = d.b[n:]
	return v
}

func (d *dec) u8() uint8 {
	if b := d.take(1); b != nil {
		return b[0]
	}
	return 0
}
func (d *dec) u16() uint16 {
	if b := d.take(2); b != nil {
		return binary.LittleEndian.Uint16(b)
	}
	return 0
}
func (d *dec) u32() uint32 {
	if b := d.take(4); b != nil {
		return binary.LittleEndian.Uint32(b)
	}
	return 0
}
func (d *dec) u64() uint64 {
	if b := d.take(8); b != nil {
		return binary.LittleEndian.Uint64(b)
	}
	return 0
}
func (d *dec) str() string { return string(d.take(int(d.u16()))) }
func (d *dec) qid() Qid    { return Qid{Type: d.u8(), Version: d.u32(), Path: d.u64()} }

// Error is an Rlerror as a Go error.
type Error struct{ Errno uint32 }

func (e *Error) Error() string {
	if s, ok := errnoNames[e.Errno]; ok {
		return "9p: " + s
	}
	return fmt.Sprintf("9p: errno %d", e.Errno)
}

var errnoNames = map[uint32]string{
	EPERM: "EPERM", ENOENT: "ENOENT", EIO: "EIO", EBADF: "EBADF", EEXIST: "EEXIST",
	ENOTDIR: "ENOTDIR", EISDIR: "EISDIR", EINVAL: "EINVAL", ENOSPC: "ENOSPC",
	ENOSYS: "ENOSYS", ENOTEMPTY: "ENOTEMPTY", EINTR: "EINTR", EACCES: "EACCES",
}

// IsErrno reports whether err is an Rlerror with errno n.
func IsErrno(err error, n uint32) bool {
	var e *Error
	return errors.As(err, &e) && e.Errno == n
}
