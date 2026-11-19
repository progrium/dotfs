package meta

import (
	"database/sql"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/store"
)

// fill adds n files (100 per directory) to a scope's path table directly,
// as a stand-in for a user root of n files.
func fill(b *testing.B, m *Meta, s server.Scope, n int) {
	sc, _ := m.scope(s)
	err := m.tx(func(tx *sql.Tx) error {
		for i := 0; i < n; i++ {
			dir := fmt.Sprintf("/Documents/project-%04d", i/100)
			if i%100 == 0 {
				if err := putRow(tx, sc.id, fsproto.Entry{Path: dir, Type: fsproto.TypeDir, Mode: 0o755, Mtime: 1763491200, Version: uint64(i + 1)}); err != nil {
					return err
				}
			}
			e := fsproto.Entry{
				Path: fmt.Sprintf("%s/notes file %05d.md", dir, i), Type: fsproto.TypeFile, Mode: 0o644,
				Length: int64(1000 + i), Mtime: 1763491200 + int64(i), Version: uint64(i + 1),
				Hash: fsproto.HashBytes([]byte(fmt.Sprint(i))),
			}
			if err := putRow(tx, sc.id, e); err != nil {
				return err
			}
		}
		return putRow(tx, sc.id, fsproto.Entry{Path: "/Documents", Type: fsproto.TypeDir, Mode: 0o755, Mtime: 1763491200, Version: 1})
	})
	if err != nil {
		b.Fatal(err)
	}
}

func benchMeta(b *testing.B, sync bool) (*store.Store, *Meta) {
	dir := b.TempDir()
	st, err := store.New(filepath.Join(dir, "data"), &store.Options{NoSync: !sync})
	if err != nil {
		b.Fatal(err)
	}
	m, err := Open(filepath.Join(dir, "meta.db"), st, nil, &Options{NoSync: !sync})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { m.Close() })
	return st, m
}

// BenchmarkIndex100k builds the index of a 100,000-file scope from
// scratch (no cached snapshot), plain and zstd.
func BenchmarkIndex100k(b *testing.B) {
	_, m := benchMeta(b, false)
	s := server.RootScope("alice/home")
	m.CreateScope(ctx, s)
	fill(b, m, s, 100_000)
	sc, _ := m.scope(s)
	for _, enc := range []string{"plain", "zstd"} {
		b.Run(enc, func(b *testing.B) {
			var accept []string
			if enc == "zstd" {
				accept = []string{"zstd"}
			}
			var size int64
			for i := 0; i < b.N; i++ {
				sc.idxMu.Lock()
				sc.idx = map[string]cachedIndex{}
				sc.idxMu.Unlock()
				m.wdb.Exec(`DELETE FROM index_cache`)
				rc, info, err := m.OpenIndex(ctx, s, accept)
				if err != nil {
					b.Fatal(err)
				}
				n, _ := io.Copy(io.Discard, rc)
				rc.Close()
				size = n
				_ = info
			}
			b.ReportMetric(float64(size), "bytes")
		})
	}
	b.Run("cached", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rc, _, err := m.OpenIndex(ctx, s, []string{"zstd"})
			if err != nil {
				b.Fatal(err)
			}
			io.Copy(io.Discard, rc)
			rc.Close()
		}
	})
}

// BenchmarkCommit measures small-file commits through one scope's writer,
// with and without fsync, and spread over 8 scopes.
func BenchmarkCommit(b *testing.B) {
	for _, c := range []struct {
		name   string
		sync   bool
		scopes int
	}{{"nosync", false, 1}, {"sync", true, 1}, {"sync-8scopes", true, 8}} {
		b.Run(c.name, func(b *testing.B) {
			st, m := benchMeta(b, c.sync)
			var ws []server.Writer
			for i := 0; i < c.scopes; i++ {
				s := server.RootScope(server.NewRoot("alice", fmt.Sprint("r", i)))
				m.CreateScope(ctx, s)
				w, _ := m.Writer(s)
				ws = append(ws, w)
			}
			for i := 0; i < b.N; i++ {
				f, _ := st.OpenStage("alice", fmt.Sprint("s", i), true)
				f.WriteAt([]byte(fmt.Sprint("content ", i)), 0)
				f.Close()
			}
			b.ResetTimer()
			done := make(chan error, c.scopes)
			for j := range ws {
				go func(j int) {
					for i := j; i < b.N; i += len(ws) {
						_, err := ws[j].Commit(ctx, alice, fsproto.CommitCmd{Stage: fmt.Sprint("s", i), Path: fmt.Sprint("/f", i), Base: fsproto.At(0), Mode: 0o644, Mtime: 1})
						if err != nil {
							done <- err
							return
						}
					}
					done <- nil
				}(j)
			}
			for range ws {
				if err := <-done; err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
