package fsproto

import (
	"encoding/hex"
	"hash"
	"io"
	"strings"

	"lukechampine.com/blake3"
)

// HashB3 names BLAKE3-256, the hash the reference implementation uses.
const HashB3 = "b3"

// NewB3 returns a BLAKE3-256 hasher; format its sum with FormatHash.
func NewB3() hash.Hash { return blake3.New(32, nil) }

// FormatHash formats a digest as "<algo>:<hex>".
func FormatHash(algo string, sum []byte) string {
	return algo + ":" + hex.EncodeToString(sum)
}

// HashBytes returns the b3 hash of b.
func HashBytes(b []byte) string {
	sum := blake3.Sum256(b)
	return FormatHash(HashB3, sum[:])
}

// HashReader returns the b3 hash of everything read from r, and its length.
func HashReader(r io.Reader) (string, int64, error) {
	h := NewB3()
	n, err := io.Copy(h, r)
	if err != nil {
		return "", n, err
	}
	return FormatHash(HashB3, h.Sum(nil)), n, nil
}

// SplitHash splits "<algo>:<hex>" and checks its syntax: a lowercase
// alphanumeric algorithm and a nonempty, even-length, lowercase hex digest.
func SplitHash(s string) (algo, digest string, err error) {
	algo, digest, ok := strings.Cut(s, ":")
	if !ok || algo == "" || digest == "" || len(digest)%2 != 0 {
		return "", "", syntaxErr("bad hash %q", s)
	}
	for i := 0; i < len(algo); i++ {
		c := algo[i]
		if !('a' <= c && c <= 'z' || '0' <= c && c <= '9') {
			return "", "", syntaxErr("bad hash algorithm %q", s)
		}
	}
	for i := 0; i < len(digest); i++ {
		c := digest[i]
		if !('a' <= c && c <= 'f' || '0' <= c && c <= '9') {
			return "", "", syntaxErr("bad hash digest %q", s)
		}
	}
	return algo, digest, nil
}

// CheckHash reports whether s is a syntactically valid hash.
func CheckHash(s string) error {
	_, _, err := SplitHash(s)
	return err
}
