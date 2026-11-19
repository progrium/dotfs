package sync

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/progrium/dotfs/fsproto"
)

// watcher turns fsnotify events into scan hints. fsnotify watches one
// directory at a time, so every directory gets a watch, and new ones are
// added as they appear.
type watcher struct {
	e     *Engine
	w     *fsnotify.Watcher
	hints chan hint
	done  chan struct{}
}

func (e *Engine) watch() (*watcher, error) {
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	w := &watcher{e: e, w: fw, hints: make(chan hint, 1024), done: make(chan struct{})}
	w.addTree(e.dir)
	go w.loop()
	return w, nil
}

func (w *watcher) close() {
	w.w.Close()
	<-w.done
}

// protoPath maps a local path below the folder to its protocol path.
func (w *watcher) protoPath(name string) (string, bool) {
	rel, err := filepath.Rel(w.e.dir, name)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	if rel == "." {
		return "/", true
	}
	return "/" + filepath.ToSlash(rel), true
}

func (w *watcher) ignored(p string, dir bool) bool {
	return p != "/" && (fsproto.Within(p, "/"+StateDir) || w.e.watchIgn.Load().Ignored(p, dir))
}

func (w *watcher) addTree(root string) {
	filepath.WalkDir(root, func(name string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return nil
		}
		p, ok := w.protoPath(name)
		if !ok || w.ignored(p, true) {
			return filepath.SkipDir
		}
		if err := w.w.Add(name); err != nil {
			w.e.logf("watch %s: %v", name, err)
		}
		return nil
	})
}

func (w *watcher) send(h hint) {
	select {
	case w.hints <- h:
	default:
		// The loop is busy; a full scan catches up on everything.
		select {
		case w.hints <- hint{"/", true}:
		default:
		}
	}
}

func (w *watcher) loop() {
	defer close(w.done)
	for {
		select {
		case ev, ok := <-w.w.Events:
			if !ok {
				return
			}
			p, ok := w.protoPath(ev.Name)
			if !ok || p == "/" || fsproto.Within(p, "/"+StateDir) {
				continue
			}
			if ev.Has(fsnotify.Create) {
				if fi, err := os.Lstat(ev.Name); err == nil && fi.IsDir() && !w.ignored(p, true) {
					w.addTree(ev.Name)
					w.send(hint{p, true})
				}
			}
			w.send(hint{fsproto.Parent(p), false})
		case err, ok := <-w.w.Errors:
			if !ok {
				return
			}
			w.e.logf("watch: %v; rescanning", err)
			w.send(hint{"/", true})
		}
	}
}
