//go:build unix

package store

import (
	"io/fs"
	"syscall"
)

// nlink returns a file's hard-link count.
func nlink(fi fs.FileInfo) uint64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Nlink)
	}
	return 1
}
