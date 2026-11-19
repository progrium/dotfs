//go:build unix

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockData takes an exclusive lock on a data directory, so that one
// process at a time opens its metadata store.
func lockData(dir string) (func(), error) {
	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("data dir %s is in use by another dotfs process", dir)
		}
		return nil, err
	}
	return func() { f.Close() }, nil
}
