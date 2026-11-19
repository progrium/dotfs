package main

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/progrium/dotfs/server/auth"
	"github.com/progrium/dotfs/server/meta"
)

func init() {
	c := &command{
		name:    "token",
		usage:   "token create [-data dir] <user>",
		summary: "issue a WebSocket bearer token for a user",
	}
	c.run = func(ctx context.Context, args []string) error {
		if len(args) == 0 || args[0] != "create" {
			return errUsage
		}
		fl := newFlags(c)
		data := fl.String("data", "dotfs-data", "data `dir` of the server")
		if err := fl.Parse(args[1:]); err != nil {
			return err
		}
		if fl.NArg() != 1 {
			return errUsage
		}
		user := fl.Arg(0)
		if err := auth.CheckName(user); err != nil {
			return fmt.Errorf("bad user name %q", user)
		}
		// Only the token table is opened, so this is safe beside a
		// running dotfs serve.
		tk, err := meta.OpenTokens(filepath.Join(*data, "meta.db"))
		if err != nil {
			return err
		}
		defer tk.Close()
		tok, err := tk.CreateToken(ctx, user)
		if err != nil {
			return err
		}
		fmt.Println(tok)
		return nil
	}
	register(c)
}
