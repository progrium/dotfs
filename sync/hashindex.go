package sync

import (
	"encoding/hex"
	"strings"

	"lukechampine.com/blake3"
)

// hashKey is a content hash as 32 raw bytes: a b3 digest itself, or for
// any other hash string, the BLAKE3 of that string.
type hashKey [32]byte

func keyOf(h string) hashKey {
	var k hashKey
	if d, ok := strings.CutPrefix(h, "b3:"); ok && len(d) == 64 {
		if _, err := hex.Decode(k[:], []byte(d)); err == nil {
			return k
		}
	}
	return blake3.Sum256([]byte(h))
}

// hashIndex maps content hashes to the file paths that have them. Most
// hashes have one path, so it keeps one path per hash in a plain map and
// only the rest, for duplicate content, in a map of sets.
type hashIndex struct {
	one  map[hashKey]string
	more map[hashKey]map[string]struct{}
}

func newHashIndex() hashIndex {
	return hashIndex{one: map[hashKey]string{}, more: map[hashKey]map[string]struct{}{}}
}

func (x hashIndex) add(h, p string) {
	k := keyOf(h)
	q, ok := x.one[k]
	if !ok {
		x.one[k] = p
		return
	}
	if q == p {
		return
	}
	m := x.more[k]
	if m == nil {
		m = map[string]struct{}{}
		x.more[k] = m
	}
	m[p] = struct{}{}
}

func (x hashIndex) remove(h, p string) {
	k := keyOf(h)
	m := x.more[k]
	if x.one[k] != p {
		delete(m, p)
	} else {
		delete(x.one, k)
		for q := range m {
			x.one[k] = q
			delete(m, q)
			break
		}
	}
	if m != nil && len(m) == 0 {
		delete(x.more, k)
	}
}

// paths returns the paths with hash h's key, in no order.
func (x hashIndex) paths(h string) []string {
	k := keyOf(h)
	q, ok := x.one[k]
	if !ok {
		return nil
	}
	out := make([]string, 0, 1+len(x.more[k]))
	out = append(out, q)
	for p := range x.more[k] {
		out = append(out, p)
	}
	return out
}

// len returns the number of paths in the index.
func (x hashIndex) len() int {
	n := len(x.one)
	for _, m := range x.more {
		n += len(m)
	}
	return n
}
