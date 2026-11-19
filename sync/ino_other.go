//go:build !unix

package sync

import "io/fs"

// inode is unknown off Unix; renames are then recognized by hash only.
func inode(fi fs.FileInfo) uint64 { return 0 }
