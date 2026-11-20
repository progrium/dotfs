package mount

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/progrium/dotfs/client"
	"github.com/progrium/dotfs/fsproto"
)

// cache holds hydrated file content in <dir>/content, one file per
// content hash, so identical files are stored once and a file whose hash
// is already local needs no download. Files are only ever created by
// rename and never written in place, so a reader can hold one open while
// the path it came from changes.
type cache struct {
	dir   string // the content directory
	limit int64  // bytes; eviction keeps the total at or below it
	logf  func(string, ...any)

	mu       sync.Mutex
	size     int64
	inflight map[string]*fetch // key -> download in progress
	open     map[string]int    // key -> open readers, never evicted
}

type fetch struct {
	done chan struct{}
	key  string // the key the content was stored under
	err  error
}

func openCache(dir string, limit int64, logf func(string, ...any)) (*cache, error) {
	c := &cache{dir: dir, limit: limit, logf: logf, inflight: map[string]*fetch{}, open: map[string]int{}}
	if err := os.MkdirAll(filepath.Join(dir, "tmp"), 0o700); err != nil {
		return nil, err
	}
	// Leftover downloads from a crash.
	tmps, _ := os.ReadDir(filepath.Join(dir, "tmp"))
	for _, t := range tmps {
		os.Remove(filepath.Join(dir, "tmp", t.Name()))
	}
	filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() {
			if fi, err := d.Info(); err == nil {
				c.size += fi.Size()
			}
		}
		return nil
	})
	return c, nil
}

// key returns the cache key for e: the hex digest of its hash, or for an
// entry without one (a read-only tree may omit hashes) a key made from its
// path, version, length, and mtime.
func key(e fsproto.Entry) string {
	if e.Hash != "" {
		if algo, digest, err := fsproto.SplitHash(e.Hash); err == nil {
			return algo + "-" + strings.ToLower(digest)
		}
	}
	h := fsproto.NewB3()
	fmt.Fprintf(h, "%s\x00%d\x00%d\x00%d", e.Path, e.Version, e.Length, e.Mtime)
	return "nohash-" + hex.EncodeToString(h.Sum(nil))
}

func hashKey(hash string) string {
	algo, digest, _ := fsproto.SplitHash(hash)
	return algo + "-" + strings.ToLower(digest)
}

func (c *cache) file(k string) string {
	i := strings.IndexByte(k, '-') + 1
	return filepath.Join(c.dir, k[i:i+2], k)
}

// has reports whether content for k is local.
func (c *cache) has(k string) bool {
	_, err := os.Stat(c.file(k))
	return err == nil
}

// get returns the local content for e, downloading it with conn if it is
// not cached yet. Concurrent gets of the same content share one download.
// The returned key may differ from key(e) when the file changed on the
// server since e was read; the content is then the newer one.
func (c *cache) get(ctx context.Context, conn client.Conn, e fsproto.Entry) (string, error) {
	k := key(e)
	if fi, err := os.Stat(c.file(k)); err == nil {
		// The mtime is the last use, for eviction; refresh it at most
		// hourly so a warm open costs no write.
		if now := time.Now(); now.Sub(fi.ModTime()) > time.Hour {
			os.Chtimes(c.file(k), now, now)
		}
		return k, nil
	}
	c.mu.Lock()
	f := c.inflight[k]
	if f == nil {
		f = &fetch{done: make(chan struct{})}
		c.inflight[k] = f
		c.mu.Unlock()
		f.key, f.err = c.download(ctx, conn, e, k)
		c.mu.Lock()
		delete(c.inflight, k)
		close(f.done)
	}
	c.mu.Unlock()
	select {
	case <-f.done:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	return f.key, f.err
}

func (c *cache) download(ctx context.Context, conn client.Conn, e fsproto.Entry, k string) (string, error) {
	src, err := conn.Open(ctx, e.Path)
	if err != nil {
		return "", err
	}
	defer src.Close()
	tmp, err := os.CreateTemp(filepath.Join(c.dir, "tmp"), "fetch-")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	h := fsproto.NewB3()
	n, err := io.Copy(io.MultiWriter(tmp, h), io.NewSectionReader(src, 0, src.Size()))
	if err == nil {
		err = tmp.Close()
	} else {
		tmp.Close()
	}
	if err != nil {
		return "", err
	}
	got := fsproto.FormatHash("b3", h.Sum(nil))
	if algo, _, _ := fsproto.SplitHash(e.Hash); algo == fsproto.HashB3 && got != e.Hash {
		// The file changed after the entry was read. Keep what we
		// got under its own hash; an event will bring the new entry.
		k = hashKey(got)
	}
	return k, c.adopt(tmp.Name(), k, n)
}

// adopt moves the file at src into the cache under k.
func (c *cache) adopt(src, k string, size int64) error {
	dst := c.file(k)
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	if _, err := os.Stat(dst); err == nil {
		return os.Remove(src)
	}
	os.Chmod(src, 0o400)
	if err := os.Rename(src, dst); err != nil {
		return err
	}
	c.mu.Lock()
	c.size += size
	over := c.size > c.limit
	c.mu.Unlock()
	if over {
		c.evict()
	}
	return nil
}

// openFile opens the content for k for reading and pins it against
// eviction until release.
func (c *cache) openFile(k string) (*os.File, error) {
	c.mu.Lock()
	c.open[k]++
	c.mu.Unlock()
	f, err := os.Open(c.file(k))
	if err != nil {
		c.release(k)
		return nil, err
	}
	return f, nil
}

func (c *cache) release(k string) {
	c.mu.Lock()
	if c.open[k]--; c.open[k] <= 0 {
		delete(c.open, k)
	}
	c.mu.Unlock()
}

// evict removes the least recently used content that no reader holds
// until the cache is at 90% of its limit.
func (c *cache) evict() {
	type item struct {
		path, key string
		size      int64
		used      time.Time
	}
	var items []item
	var total int64
	filepath.WalkDir(c.dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() || filepath.Base(filepath.Dir(p)) == "tmp" {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return nil
		}
		total += fi.Size()
		items = append(items, item{p, d.Name(), fi.Size(), fi.ModTime()})
		return nil
	})
	sort.Slice(items, func(i, j int) bool { return items[i].used.Before(items[j].used) })
	target := c.limit / 10 * 9
	removed := 0
	for _, it := range items {
		if total <= target {
			break
		}
		c.mu.Lock()
		busy := c.open[it.key] > 0 || c.inflight[it.key] != nil
		c.mu.Unlock()
		if busy {
			continue
		}
		if os.Remove(it.path) == nil {
			total -= it.size
			removed++
		}
	}
	c.mu.Lock()
	c.size = total
	c.mu.Unlock()
	if removed > 0 {
		c.logf("cache: evicted %d files, %s left", removed, fmtBytes(total))
	}
}

func fmtBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return strconv.FormatFloat(float64(n)/(1<<30), 'f', 1, 64) + " GiB"
	case n >= 1<<20:
		return strconv.FormatFloat(float64(n)/(1<<20), 'f', 1, 64) + " MiB"
	}
	return strconv.FormatInt(n, 10) + " B"
}

var errNotLocal = errors.New("dotfs: content not cached and server unreachable")
