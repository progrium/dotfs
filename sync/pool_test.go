package sync

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/progrium/dotfs/fsproto"
)

func TestHashReaderPooled(t *testing.T) {
	for _, n := range []int{0, 1, 1024, copyBufSize - 1, copyBufSize, 3*copyBufSize + 4097, 1 << 20} {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(i * 7)
		}
		got, m, err := hashReader(bytes.NewReader(b))
		if err != nil || m != int64(n) || got != fsproto.HashBytes(b) {
			t.Fatalf("%d bytes: %s %d %v, want %s", n, got, m, err, fsproto.HashBytes(b))
		}
	}
}

func TestHashReaderAllocs(t *testing.T) {
	b := make([]byte, 1<<20)
	hashReader(bytes.NewReader(b)) // warm the pools
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	for range 10 {
		hashReader(bytes.NewReader(b))
	}
	runtime.ReadMemStats(&m1)
	// Padded chunk copies or a buffer per copy would be over 32 KiB per
	// 32 KiB hashed; what is left is the hasher's goroutines.
	if per := (m1.TotalAlloc - m0.TotalAlloc) / 10; per > 256<<10 {
		t.Fatalf("%d bytes allocated hashing 1 MiB", per)
	} else {
		t.Logf("%d bytes allocated hashing 1 MiB", per)
	}
}
