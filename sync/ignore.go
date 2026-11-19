package sync

import (
	"bufio"
	"io"
	"path"
	"strings"
)

// IgnoreFile is the name of the optional ignore file at the top of a synced
// folder. It is synced like any other file, so every device shares it.
const IgnoreFile = ".dotfsignore"

// builtinIgnores are always ignored: the engine's own state, the server's
// reserved name, OS metadata files, and common editor swap and lock files.
var builtinIgnores = []string{
	"/" + StateDir,
	".fs",
	".DS_Store",
	"Thumbs.db",
	"desktop.ini",
	"*.swp",
	"*.swo",
	"*.swx",
	"*~",
	".#*",
	"#*#",
	"~$*",
	".~lock.*#",
}

type ignoreRule struct {
	pattern  string
	negate   bool
	anchored bool // matched against the whole path, not any name
	dirOnly  bool
}

// Ignores decides which paths a sync engine leaves alone in both
// directions. Patterns are one per line, with # comments:
//
//   - a pattern without a slash matches a name at any depth (*.log);
//   - a pattern with a slash matches the path from the top of the folder
//     (/build, docs/drafts);
//   - a trailing slash matches directories only (cache/);
//   - a leading ! re-includes what an earlier pattern excluded.
//
// Patterns use path.Match syntax. Everything below an ignored directory is
// ignored. The last matching pattern wins.
type Ignores struct {
	rules []ignoreRule
}

// ParseIgnores reads patterns from r, after the built-in ones.
func ParseIgnores(r io.Reader) (*Ignores, error) {
	ig := &Ignores{}
	for _, p := range builtinIgnores {
		ig.add(p)
	}
	if r == nil {
		return ig, nil
	}
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), " \t\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		ig.add(line)
	}
	return ig, sc.Err()
}

func (ig *Ignores) add(p string) {
	r := ignoreRule{}
	if strings.HasPrefix(p, "!") {
		r.negate = true
		p = p[1:]
	}
	if strings.HasSuffix(p, "/") {
		r.dirOnly = true
		p = strings.TrimRight(p, "/")
	}
	if strings.Contains(p, "/") {
		r.anchored = true
		p = strings.TrimLeft(p, "/")
	}
	if p == "" {
		return
	}
	if _, err := path.Match(p, ""); err != nil {
		return // malformed pattern: skip it rather than ignore everything
	}
	r.pattern = p
	ig.rules = append(ig.rules, r)
}

func (ig *Ignores) match(p string, dir bool) bool {
	rel := strings.TrimPrefix(p, "/")
	name := path.Base(rel)
	ignored := false
	for _, r := range ig.rules {
		if r.dirOnly && !dir {
			continue
		}
		subject := name
		if r.anchored {
			subject = rel
		}
		if ok, _ := path.Match(r.pattern, subject); ok {
			ignored = !r.negate
		}
	}
	return ignored
}

// Ignored reports whether the protocol path p (a directory when dir is set)
// is ignored, either itself or through one of its parent directories.
func (ig *Ignores) Ignored(p string, dir bool) bool {
	if p == "/" || ig == nil {
		return false
	}
	for i := 1; i < len(p); i++ {
		if p[i] == '/' && ig.match(p[:i], true) {
			return true
		}
	}
	return ig.match(p, dir)
}
