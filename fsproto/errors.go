package fsproto

import "errors"

func asReplyError(err error) (*ReplyError, bool) {
	var re *ReplyError
	if errors.As(err, &re) {
		return re, true
	}
	return nil, false
}

func isSyntax(err error) bool {
	return errors.Is(err, ErrSyntax) || errors.Is(err, ErrUnknownCommand)
}
