package meta

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"time"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

const tokenSchema = `CREATE TABLE IF NOT EXISTS token (
	hash TEXT PRIMARY KEY,
	user TEXT NOT NULL,
	created INTEGER NOT NULL
);`

// Tokens is the token table of a metadata database on its own. It does no
// recovery and touches no content, so it is safe to open while a server
// holds the same database open, as "dotfs token create" does.
type Tokens struct {
	db *sql.DB
}

var _ server.Tokens = (*Tokens)(nil)

// OpenTokens opens (creating if needed) the token table of the metadata
// database at path.
func OpenTokens(path string) (*Tokens, error) {
	db, err := sql.Open("sqlite", dsn(path, "FULL", false))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(tokenSchema); err != nil {
		db.Close()
		return nil, err
	}
	return &Tokens{db: db}, nil
}

// Close closes the database.
func (t *Tokens) Close() error { return t.db.Close() }

func (t *Tokens) CreateToken(ctx context.Context, user string) (string, error) {
	return createToken(ctx, t.db, user, time.Now())
}

func (t *Tokens) LookupToken(ctx context.Context, token string) (string, error) {
	return lookupToken(ctx, t.db, token)
}

// CreateToken returns a new bearer token for user. Only its hash is
// stored.
func (m *Meta) CreateToken(ctx context.Context, user string) (string, error) {
	return createToken(ctx, m.wdb, user, m.opts.Now())
}

// LookupToken returns the user a token belongs to, or fsproto.ErrDenied.
func (m *Meta) LookupToken(ctx context.Context, token string) (string, error) {
	return lookupToken(ctx, m.rdb, token)
}

func createToken(ctx context.Context, db *sql.DB, user string, now time.Time) (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	tok := base64.RawURLEncoding.EncodeToString(b[:])
	_, err := db.ExecContext(ctx, `INSERT INTO token (hash, user, created) VALUES (?, ?, ?)`,
		fsproto.HashBytes([]byte(tok)), user, now.Unix())
	return tok, err
}

func lookupToken(ctx context.Context, db *sql.DB, token string) (string, error) {
	var user string
	err := db.QueryRowContext(ctx, `SELECT user FROM token WHERE hash = ?`, fsproto.HashBytes([]byte(token))).Scan(&user)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fsproto.Denied("unknown token")
	}
	return user, err
}
