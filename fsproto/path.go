package fsproto

import "strings"

// CheckPath reports whether p is a valid unescaped protocol path: absolute,
// "/" for the scope root, with no empty, ".", or ".." components, no trailing
// slash, and no NUL byte.
func CheckPath(p string) error {
	if p == "/" {
		return nil
	}
	if !strings.HasPrefix(p, "/") {
		return syntaxErr("path %q is not absolute", p)
	}
	if strings.IndexByte(p, 0) >= 0 {
		return syntaxErr("path %q contains NUL", p)
	}
	for _, c := range strings.Split(p[1:], "/") {
		if c == "" || c == "." || c == ".." {
			return syntaxErr("path %q has an invalid component", p)
		}
	}
	return nil
}

// ComparePaths orders paths as index lines are sorted: by the bytes of the
// unescaped path. Note that a directory's children are not always contiguous
// after it ("/a", "/a b", "/a/c").
func ComparePaths(a, b string) int {
	return strings.Compare(a, b)
}

// Within reports whether p is dir or below it.
func Within(p, dir string) bool {
	if dir == "/" {
		return strings.HasPrefix(p, "/")
	}
	return p == dir || (strings.HasPrefix(p, dir) && len(p) > len(dir) && p[len(dir)] == '/')
}

// Rebase moves p from below (or at) dir to the same place below to. It
// returns p unchanged if p is not within dir.
func Rebase(p, dir, to string) string {
	if !Within(p, dir) {
		return p
	}
	rest := p[len(dir):]
	if dir == "/" {
		rest = p
	}
	if to == "/" {
		if rest == "" {
			return "/"
		}
		return rest
	}
	return to + rest
}

// Join joins a directory path and a name.
func Join(dir, name string) string {
	if dir == "/" {
		return "/" + name
	}
	return dir + "/" + name
}

// Parent returns the directory containing p ("/" for "/").
func Parent(p string) string {
	i := strings.LastIndexByte(p, '/')
	if i <= 0 {
		return "/"
	}
	return p[:i]
}

// Name returns the last component of p ("" for "/").
func Name(p string) string {
	return p[strings.LastIndexByte(p, '/')+1:]
}
