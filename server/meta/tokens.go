package meta

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"

	"github.com/progrium/dotfs/fsproto"
)

// CreateToken returns a new bearer token for user. Only its hash is
// stored.
func (m *Meta) CreateToken(ctx context.Context, user string) (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	tok := base64.RawURLEncoding.EncodeToString(b[:])
	err := m.tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO token (hash, user, created) VALUES (?, ?, ?)`,
			fsproto.HashBytes([]byte(tok)), user, m.opts.Now().Unix())
		return err
	})
	return tok, err
}

// LookupToken returns the user a token belongs to, or fsproto.ErrDenied.
func (m *Meta) LookupToken(ctx context.Context, token string) (string, error) {
	var user string
	err := m.rdb.QueryRowContext(ctx, `SELECT user FROM token WHERE hash = ?`, fsproto.HashBytes([]byte(token))).Scan(&user)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fsproto.Denied("unknown token")
	}
	return user, err
}
