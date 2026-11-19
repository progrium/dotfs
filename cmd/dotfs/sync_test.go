package main

import "testing"

func TestSplitRoot(t *testing.T) {
	for _, c := range []struct{ in, addr, name string }{
		{"localhost:5640/home", "localhost:5640", "home"},
		{"tcp:host:1/home", "tcp:host:1", "home"},
		{"unix:/run/dotfs.sock/home", "unix:/run/dotfs.sock", "home"},
		{"wss://example.com/9p/home", "wss://example.com/9p", "home"},
	} {
		addr, name, err := splitRoot(c.in)
		if err != nil || addr != c.addr || name != c.name {
			t.Errorf("splitRoot(%q) = %q, %q, %v", c.in, addr, name, err)
		}
	}
	for _, bad := range []string{"home", "host:1/", "wss://home", "/home"} {
		if _, _, err := splitRoot(bad); err == nil {
			t.Errorf("splitRoot(%q) succeeded", bad)
		}
	}
}
