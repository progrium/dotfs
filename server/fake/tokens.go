package fake

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

// Tokens is an in-memory server.Tokens.
type Tokens struct {
	mu    sync.Mutex
	users map[string]string
}

var _ server.Tokens = (*Tokens)(nil)

// NewTokens returns an empty Tokens.
func NewTokens() *Tokens { return &Tokens{users: map[string]string{}} }

func (t *Tokens) CreateToken(ctx context.Context, user string) (string, error) {
	b := make([]byte, 16)
	rand.Read(b)
	tok := hex.EncodeToString(b)
	t.mu.Lock()
	t.users[tok] = user
	t.mu.Unlock()
	return tok, nil
}

func (t *Tokens) LookupToken(ctx context.Context, token string) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	u, ok := t.users[token]
	if !ok {
		return "", fsproto.Denied("bad token")
	}
	return u, nil
}
