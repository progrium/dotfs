package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/progrium/dotfs/fsproto"
)

func init() {
	c := &command{
		name:    "index",
		usage:   "index [-hash] [-o file] <dir>",
		summary: "print a read-only .fs/index (seq 0, version 0) for a local directory",
	}
	c.run = func(ctx context.Context, args []string) error {
		fl := newFlags(c)
		hash := fl.Bool("hash", true, "include b3 content hashes for files")
		out := fl.String("o", "", "write to `file` instead of standard output")
		if err := fl.Parse(args); err != nil {
			return err
		}
		if fl.NArg() != 1 {
			return errUsage
		}
		var w io.Writer = os.Stdout
		if *out != "" {
			f, err := os.Create(*out)
			if err != nil {
				return err
			}
			defer f.Close()
			w = f
		}
		bw := bufio.NewWriter(w)
		if err := writeLocalIndex(ctx, bw, fl.Arg(0), *hash); err != nil {
			return err
		}
		return bw.Flush()
	}
	register(c)
}

// writeLocalIndex writes a read-only index of dir: seq 0, every version 0,
// no flags. It skips .fs directories and anything that is not a regular
// file, directory, or symlink.
func writeLocalIndex(ctx context.Context, w io.Writer, dir string, withHash bool) error {
	var es []fsproto.Entry
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		name := "/"
		if rel != "." {
			name = "/" + filepath.ToSlash(rel)
		}
		if d.IsDir() && d.Name() == ".fs" && name != "/" {
			return filepath.SkipDir
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		e := fsproto.Entry{Path: name, Mode: uint32(info.Mode().Perm()), Mtime: info.ModTime().Unix()}
		switch {
		case info.Mode().IsDir():
			e.Type = fsproto.TypeDir
		case info.Mode().IsRegular():
			e.Type = fsproto.TypeFile
			e.Length = info.Size()
			if withHash {
				f, err := os.Open(p)
				if err != nil {
					return err
				}
				e.Hash, _, err = fsproto.HashReader(f)
				f.Close()
				if err != nil {
					return err
				}
			}
		case info.Mode()&fs.ModeSymlink != 0:
			e.Type = fsproto.TypeSymlink
			if e.Target, err = os.Readlink(p); err != nil {
				return err
			}
			e.Length = int64(len(e.Target))
		default:
			return nil
		}
		if strings.ContainsRune(name, 0) {
			return nil
		}
		es = append(es, e)
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(es, func(i, j int) bool { return fsproto.ComparePaths(es[i].Path, es[j].Path) < 0 })
	iw, err := fsproto.NewIndexWriter(w, fsproto.IndexHeader{}, nil)
	if err != nil {
		return err
	}
	for _, e := range es {
		if err := iw.Write(e); err != nil {
			return fmt.Errorf("%s: %w", e.Path, err)
		}
	}
	return iw.Close()
}
