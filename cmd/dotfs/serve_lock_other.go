//go:build !unix

package main

// lockData does not lock on this platform.
func lockData(dir string) (func(), error) { return func() {}, nil }
