package meta

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	gosync "sync"
	"testing"
	"time"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
	"github.com/progrium/dotfs/server/store"
)

// The kill tests run each change in a child process that exits at one step
// of the commit path, then reopen the data directory and check that the
// change either happened completely or not at all.

var (
	ctx       = context.Background()
	killScope = server.RootScope(server.NewRoot("alice", "home"))
	alice     = server.Origin{User: "alice", Dev: "d1"}
)

const killExit = 3

func openDir(t testing.TB, dir string) (*store.Store, *Meta) {
	t.Helper()
	st, err := store.New(filepath.Join(dir, "data"), nil)
	if err != nil {
		t.Fatal(err)
	}
	m, err := Open(filepath.Join(dir, "meta.db"), st, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return st, m
}

func stageData(t testing.TB, st *store.Store, name, data string) {
	t.Helper()
	f, err := st.OpenStage("alice", name, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte(data), 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
}

func must(t testing.TB) func(server.Result, error) {
	return func(_ server.Result, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
}

// killSetup is the tree every kill test starts from (seq 5).
func killSetup(t testing.TB, st *store.Store, m *Meta) {
	if err := m.CreateScope(ctx, killScope); err != nil {
		t.Fatal(err)
	}
	w, _ := m.Writer(killScope)
	must(t)(w.Mkdir(ctx, alice, fsproto.MkdirCmd{Path: "/d"}))
	stageData(t, st, "s1", "one")
	must(t)(w.Commit(ctx, alice, fsproto.CommitCmd{Stage: "s1", Path: "/d/f", Base: fsproto.At(0), Mode: 0o644, Mtime: 10}))
	stageData(t, st, "s2", "two")
	must(t)(w.Commit(ctx, alice, fsproto.CommitCmd{Stage: "s2", Path: "/g", Base: fsproto.At(0), Mode: 0o644, Mtime: 10}))
	must(t)(w.Mkdir(ctx, alice, fsproto.MkdirCmd{Path: "/e"}))
	must(t)(w.Mkdir(ctx, alice, fsproto.MkdirCmd{Path: "/d/sub"}))
}

const killSetupSeq = 5

var killOps = map[string]func(t testing.TB, st *store.Store, w server.Writer) (server.Result, error){
	"commit-new": func(t testing.TB, st *store.Store, w server.Writer) (server.Result, error) {
		stageData(t, st, "k", "new content")
		return w.Commit(ctx, alice, fsproto.CommitCmd{Stage: "k", Path: "/e/new", Base: fsproto.At(0), Mode: 0o600, Mtime: 20})
	},
	"commit-replace": func(t testing.TB, st *store.Store, w server.Writer) (server.Result, error) {
		stageData(t, st, "k", "replaced")
		return w.Commit(ctx, alice, fsproto.CommitCmd{Stage: "k", Path: "/g", Base: fsproto.At(3), Mode: 0o644, Mtime: 20})
	},
	"commit-hash": func(t testing.TB, st *store.Store, w server.Writer) (server.Result, error) {
		return w.CommitHash(ctx, alice, fsproto.CommitHashCmd{Hash: fsproto.HashBytes([]byte("one")), Path: "/e/h", Base: fsproto.At(0), Mode: 0o644, Mtime: 20})
	},
	"mkdir": func(t testing.TB, st *store.Store, w server.Writer) (server.Result, error) {
		return w.Mkdir(ctx, alice, fsproto.MkdirCmd{Path: "/e/x"})
	},
	"rm": func(t testing.TB, st *store.Store, w server.Writer) (server.Result, error) {
		return w.Rm(ctx, alice, fsproto.RmCmd{Path: "/g"})
	},
	"rmtree": func(t testing.TB, st *store.Store, w server.Writer) (server.Result, error) {
		return w.Rmtree(ctx, alice, fsproto.RmtreeCmd{Path: "/d"})
	},
	"mv-dir": func(t testing.TB, st *store.Store, w server.Writer) (server.Result, error) {
		return w.Mv(ctx, alice, fsproto.MvCmd{From: "/d", To: "/e/d"})
	},
	"mv-replace": func(t testing.TB, st *store.Store, w server.Writer) (server.Result, error) {
		return w.Mv(ctx, alice, fsproto.MvCmd{From: "/g", To: "/d/f"})
	},
	"cp": func(t testing.TB, st *store.Store, w server.Writer) (server.Result, error) {
		return w.Cp(ctx, alice, fsproto.CpCmd{From: "/d/f", To: "/e/f"})
	},
	"symlink": func(t testing.TB, st *store.Store, w server.Writer) (server.Result, error) {
		return w.Symlink(ctx, alice, "/e/l", "../g", fsproto.Any)
	},
	"setattr": func(t testing.TB, st *store.Store, w server.Writer) (server.Result, error) {
		mode, mtime := uint32(0o600), int64(99)
		return w.SetAttr(ctx, alice, "/g", &mode, &mtime)
	},
	"nest": func(t testing.TB, st *store.Store, w server.Writer) (server.Result, error) {
		return w.SetFlags(ctx, alice, fsproto.FlagsCmd{Path: "/d", Flags: "i"})
	},
	// group commits killGroupN changes together, one of which depends on
	// another, plus one with a stale base that fails on its own.
	"group": func(t testing.TB, st *store.Store, w server.Writer) (server.Result, error) {
		stageData(t, st, "k1", "group one")
		stageData(t, st, "k2", "group two")
		stageData(t, st, "k3", "group three")
		stageData(t, st, "k4", "stale")
		rs := inGroup(t, w, []func() (server.Result, error){
			func() (server.Result, error) {
				return w.Commit(ctx, alice, fsproto.CommitCmd{Stage: "k1", Path: "/e/a", Base: fsproto.At(0), Mode: 0o644, Mtime: 20})
			},
			func() (server.Result, error) {
				return w.Commit(ctx, alice, fsproto.CommitCmd{Stage: "k4", Path: "/d/f", Base: fsproto.At(1), Mode: 0o644, Mtime: 20})
			},
			func() (server.Result, error) { return w.Mkdir(ctx, alice, fsproto.MkdirCmd{Path: "/e/x"}) },
			func() (server.Result, error) {
				return w.Commit(ctx, alice, fsproto.CommitCmd{Stage: "k2", Path: "/e/x/b", Base: fsproto.At(0), Mode: 0o644, Mtime: 20})
			},
			func() (server.Result, error) {
				return w.Commit(ctx, alice, fsproto.CommitCmd{Stage: "k3", Path: "/g", Base: fsproto.At(3), Mode: 0o644, Mtime: 20})
			},
			func() (server.Result, error) {
				return w.CommitHash(ctx, alice, fsproto.CommitHashCmd{Hash: fsproto.HashBytes([]byte("one")), Path: "/e/x/h", Base: fsproto.At(0), Mode: 0o644, Mtime: 20})
			},
		})
		for i, r := range rs {
			if i == 1 {
				if !errors.Is(r.err, fsproto.ErrConflict) {
					t.Fatalf("stale base: %v, want a conflict", r.err)
				}
				continue
			}
			if r.err != nil {
				return r.res, r.err
			}
		}
		return rs[len(rs)-1].res, nil
	},
}

// killGroupN is how many changes of the "group" kill op commit.
const killGroupN = 5

type result struct {
	res server.Result
	err error
}

// inGroup runs fns concurrently so that they queue for w's scope in order
// and commit as one group, and returns their results.
func inGroup(t testing.TB, w server.Writer, fns []func() (server.Result, error)) []result {
	sc := w.(*writer).sc
	sc.mu.Lock()
	out := make([]result, len(fns))
	var wg gosync.WaitGroup
	for i, fn := range fns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out[i].res, out[i].err = fn()
		}()
		for {
			sc.qmu.Lock()
			n := len(sc.queue)
			sc.qmu.Unlock()
			if n == i+1 {
				break
			}
			time.Sleep(time.Millisecond)
		}
	}
	sc.mu.Unlock()
	wg.Wait()
	return out
}

// TestKillHelper is the child process; it does nothing in a normal run.
func TestKillHelper(t *testing.T) {
	dir, step, op := os.Getenv("KILL_DIR"), os.Getenv("KILL_STEP"), os.Getenv("KILL_OP")
	if dir == "" {
		t.Skip("child process only")
	}
	st, m := openDir(t, dir)
	crashHook = func(s string) {
		if s == step {
			os.Exit(killExit)
		}
	}
	w, err := m.Writer(killScope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := killOps[op](t, st, w); err != nil {
		t.Fatal(err)
	}
}

func TestKill(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	// "placed" stops after the first file is placed and before the
	// directory sync, so in a group the rest are still in tmp.
	steps := []string{"prepared", "intent", "placed", "fs", "applied"}
	for op := range killOps {
		for _, step := range steps {
			t.Run(op+"/"+step, func(t *testing.T) {
				dir := t.TempDir()
				st, m := openDir(t, dir)
				killSetup(t, st, m)
				m.Close()
				st.Close()

				cmd := exec.Command(os.Args[0], "-test.run=^TestKillHelper$", "-test.count=1")
				cmd.Env = append(os.Environ(), "KILL_DIR="+dir, "KILL_STEP="+step, "KILL_OP="+op)
				out, err := cmd.CombinedOutput()
				var ee *exec.ExitError
				if !errors.As(err, &ee) || ee.ExitCode() != killExit {
					if err == nil && (step == "intent" || step == "fs") && op == "nest" {
						return // the change has no filesystem step
					}
					if err == nil && step == "placed" {
						return // the change places no file
					}
					t.Fatalf("child did not stop at %s: %v\n%s", step, err, out)
				}

				st, m = openDir(t, dir)
				defer m.Close()
				defer st.Close()
				want := uint64(killSetupSeq + 1)
				if op == "group" {
					want = killSetupSeq + killGroupN
				}
				if step == "prepared" {
					want = killSetupSeq
				}
				seq, _ := m.Seq(ctx, killScope)
				if seq != want {
					t.Fatalf("seq %d after recovery, want %d", seq, want)
				}
				checkConsistent(t, st, m, killScope)
				// The scope keeps working.
				w, _ := m.Writer(killScope)
				r, err := w.Mkdir(ctx, alice, fsproto.MkdirCmd{Path: "/after"})
				if err != nil || r.Version != want+1 {
					t.Fatalf("change after recovery: %+v %v", r, err)
				}
				checkConsistent(t, st, m, killScope)
			})
		}
	}
}

// checkConsistent checks that a root scope's metadata, log, and files
// agree, and that nothing is left in tmp.
func checkConsistent(t *testing.T, st *store.Store, m *Meta, s server.Scope) {
	t.Helper()
	seq, _ := m.Seq(ctx, s)
	sc, _ := m.scope(s)
	start := sc.logStart.Load()
	next := start + 1
	if err := m.ReadLog(ctx, s, start, func(ev fsproto.Event) error {
		if ev.Seq != next {
			t.Fatalf("log record %d, want %d", ev.Seq, next)
		}
		next++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if next != seq+1 {
		t.Fatalf("log ends at %d, seq is %d", next-1, seq)
	}
	known := map[string]bool{}
	if _, err := m.Entries(ctx, s, func(e fsproto.Entry) error {
		known[e.Path] = true
		if e.Version > seq {
			t.Fatalf("%s has version %d past seq %d", e.Path, e.Version, seq)
		}
		full := filepath.Join(st.RootDir(s.Root), filepath.FromSlash(e.Path))
		fi, err := os.Lstat(full)
		if err != nil {
			t.Fatalf("%s: %v", e.Path, err)
		}
		switch e.Type {
		case fsproto.TypeDir:
			if !fi.IsDir() {
				t.Fatalf("%s is not a directory on disk", e.Path)
			}
		case fsproto.TypeSymlink:
			if target, err := os.Readlink(full); err != nil || target != e.Target {
				t.Fatalf("%s: link %q %v, want %q", e.Path, target, err, e.Target)
			}
		case fsproto.TypeFile:
			h, err := st.Hash(s.Root, e.Path)
			if err != nil || h != e.Hash || fi.Size() != e.Length {
				t.Fatalf("%s: hash %s size %d %v, want %s %d", e.Path, h, fi.Size(), err, e.Hash, e.Length)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	m.mu.RLock()
	nested := sc.nested
	m.mu.RUnlock()
	root := st.RootDir(s.Root)
	filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		rel, _ := filepath.Rel(root, p)
		pp := "/" + filepath.ToSlash(rel)
		if rel == "." {
			pp = "/"
		}
		if nested[pp] {
			return filepath.SkipDir
		}
		if !known[pp] {
			t.Fatalf("%s is on disk but not in the index", pp)
		}
		return nil
	})
	if des, _ := os.ReadDir(filepath.Join(st.Dir(), "tmp")); len(des) != 0 {
		t.Fatalf("tmp holds %d leftovers", len(des))
	}
}
