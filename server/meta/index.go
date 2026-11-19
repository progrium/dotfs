package meta

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"strings"

	"github.com/klauspost/compress/zstd"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

// cachedIndex is a scope's index at one seq: the plain body and, once
// asked for, its zstd encoding.
type cachedIndex struct {
	seq  uint64
	body []byte // plain body, or nil until needed
	z    []byte // zstd frame of body, or nil until asked for
}

var (
	zenc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	zdec, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
)

// OpenIndex returns the scope's .fs/index at the current seq, zstd-encoded
// if accept asks for it. Snapshots are cached per scope until the next
// change, and the zstd one is kept in the database across restarts.
func (m *Meta) OpenIndex(ctx context.Context, s server.Scope, accept []string) (io.ReadCloser, server.IndexInfo, error) {
	sc, err := m.scope(s)
	if err != nil {
		return nil, server.IndexInfo{}, err
	}
	enc := ""
	for _, a := range accept {
		if a == fsproto.EncodingZstd {
			enc = a
			break
		}
	}
	ci, err := m.index(ctx, sc, enc)
	if err != nil {
		return nil, server.IndexInfo{}, err
	}
	body := ci.body
	if enc != "" {
		body = ci.z
	}
	h := fsproto.IndexHeader{Version: fsproto.IndexFormatVersion, Seq: ci.seq, Encoding: enc}.Encode() + "\n"
	r := io.MultiReader(strings.NewReader(h), bytes.NewReader(body))
	return io.NopCloser(r), server.IndexInfo{Seq: ci.seq, Encoding: enc, Size: int64(len(h) + len(body))}, nil
}

// index returns the cached snapshot for sc's current seq with the body
// encoding enc filled in, building it if needed.
func (m *Meta) index(ctx context.Context, sc *scope, enc string) (cachedIndex, error) {
	sc.idxMu.Lock()
	defer sc.idxMu.Unlock()
	ci := sc.idx[""]
	if cur := sc.seq.Load(); ci.seq != cur || (ci.body == nil && ci.z == nil) {
		ci = cachedIndex{seq: cur}
		// A snapshot saved before a restart.
		var z []byte
		err := m.rdb.QueryRowContext(ctx, `SELECT bytes FROM index_cache WHERE scope = ? AND encoding = ? AND seq = ?`,
			sc.id, fsproto.EncodingZstd, cur).Scan(&z)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return ci, err
		}
		ci.z = z
	}
	if ci.body == nil && (enc == "" || ci.z == nil) {
		if ci.z != nil {
			body, err := zdec.DecodeAll(ci.z, nil)
			if err != nil {
				return ci, err
			}
			ci.body = body
		} else {
			seq, body, err := m.buildBody(ctx, sc)
			if err != nil {
				return ci, err
			}
			ci = cachedIndex{seq: seq, body: body}
		}
	}
	if enc == fsproto.EncodingZstd && ci.z == nil {
		ci.z = zenc.EncodeAll(ci.body, make([]byte, 0, len(ci.body)/4))
		z, seq := ci.z, ci.seq
		if err := m.tx(func(tx *sql.Tx) error {
			_, err := tx.Exec(`INSERT OR REPLACE INTO index_cache (scope, encoding, seq, bytes) VALUES (?, ?, ?, ?)`,
				sc.id, fsproto.EncodingZstd, seq, z)
			return err
		}); err != nil {
			return ci, err
		}
	}
	sc.idx[""] = ci
	return ci, nil
}

// buildBody writes every entry of sc as index lines, from one snapshot.
func (m *Meta) buildBody(ctx context.Context, sc *scope) (uint64, []byte, error) {
	var b strings.Builder
	var prev string
	n := 0
	seq, err := m.entries(ctx, sc, func(e fsproto.Entry) error {
		if n > 0 && fsproto.ComparePaths(prev, e.Path) >= 0 {
			return fsproto.ErrUnsorted
		}
		prev = e.Path
		n++
		b.WriteString(e.Encode())
		b.WriteByte('\n')
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	return seq, []byte(b.String()), nil
}
