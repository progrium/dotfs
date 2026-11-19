//go:build !unix

package store

import "io/fs"

// nlink returns a file's hard-link count. Without a portable way to read it,
// every blob counts as referenced, so GC keeps them all.
func nlink(fs.FileInfo) uint64 { return 2 }
