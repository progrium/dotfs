// Package fsproto holds the line formats of the dotfs protocol (docs/9p-dotfs.md):
// index lines and headers, event records, ctl commands and replies, the
// writes that set up an events stream or an index read, and percent-escaping
// of paths and other fields.
//
// Every format is UTF-8 text, one record per line, with fields separated by
// single spaces. A "-" stands for an empty field, optional key=value fields
// may follow the positional ones, and unknown trailing fields are ignored.
// Each type has an Encode method that returns one line without its trailing
// newline, and a Parse function that accepts a line with or without one.
//
// Parsers are strict about what they need (field counts, numbers, escapes,
// and paths) and lenient about what they may ignore (unknown trailing fields
// and unnecessary escapes), so a value that parses always encodes to a line
// that parses back to the same value.
//
// Paths in every struct are unescaped and absolute from the scope root; the
// encoders and parsers do the escaping. docs/9p-dotfs.md defines the
// escaping, sorting, and reply rules this package implements.
package fsproto
