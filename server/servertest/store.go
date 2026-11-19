package servertest

import (
	"io"
	"testing"
	"time"

	"github.com/progrium/dotfs/fsproto"
	"github.com/progrium/dotfs/server"
)

// TestStore runs the Store behavior tests.
func TestStore(t *testing.T, newStore NewStore) {
	alice := server.NewRoot("alice", "home")
	t.Run("Stage", func(t *testing.T) {
		st := newStore(t)
		_, err := st.OpenStage("alice", "s1", false)
		wantStatus(t, "open missing stage", err, fsproto.ErrMissing)
		f := must[server.StageFile](t, "create")(st.OpenStage("alice", "s1", true))
		f.WriteAt([]byte("hello"), 0)
		f.WriteAt([]byte("world"), 10) // a hole is allowed
		f.Close()
		// Re-opening resumes, and Size says where to continue.
		f = must[server.StageFile](t, "reopen")(st.OpenStage("alice", "s1", false))
		if n, _ := f.Size(); n != 15 {
			t.Fatalf("Size = %d, want 15", n)
		}
		f.WriteAt([]byte("_____"), 5)
		buf := make([]byte, 15)
		if n, err := f.ReadAt(buf, 0); n != 15 || (err != nil && err != io.EOF) || string(buf) != "hello_____world" {
			t.Fatalf("ReadAt = %d %v %q", n, err, buf)
		}
		f.Close()
		info := must[server.StageInfo](t, "StatStage")(st.StatStage("alice", "s1"))
		if info.Size != 15 || info.Name != "s1" {
			t.Fatalf("StatStage = %+v", info)
		}
		// Stage files are per user.
		if l := must[[]server.StageInfo](t, "ListStages")(st.ListStages("bob")); len(l) != 0 {
			t.Fatalf("bob sees %v", l)
		}
		if l := must[[]server.StageInfo](t, "ListStages")(st.ListStages("alice")); len(l) != 1 {
			t.Fatalf("alice sees %v", l)
		}
		check(t, "RemoveStage", st.RemoveStage("alice", "s1"))
		check(t, "RemoveStage again", st.RemoveStage("alice", "s1"))
		_, err = st.StatStage("alice", "s1")
		wantStatus(t, "stat removed stage", err, fsproto.ErrMissing)

		stage(t, st, "alice", "old", []byte("x"))
		n := must[int](t, "ExpireStages")(st.ExpireStages(time.Now().Add(time.Hour)))
		if n != 1 {
			t.Fatalf("ExpireStages removed %d", n)
		}
	})

	t.Run("Publish", func(t *testing.T) {
		st := newStore(t)
		check(t, "CreateRoot", st.CreateRoot(alice))
		check(t, "CreateRoot again", st.CreateRoot(alice))
		stage(t, st, "alice", "s", []byte("content"))
		_, _, err := st.Publish("alice", "s", alice, "/a.txt", 0o644, 100, fsproto.HashBytes([]byte("other")))
		wantStatus(t, "hash mismatch", err, fsproto.ErrInvalid)
		_, _, err = st.Publish("bob", "s", alice, "/a.txt", 0o644, 100, "")
		wantStatus(t, "other user's stage", err, fsproto.ErrMissing)
		h, n, err := st.Publish("alice", "s", alice, "/a.txt", 0o644, 100, fsproto.HashBytes([]byte("content")))
		check(t, "Publish", err)
		if h != fsproto.HashBytes([]byte("content")) || n != 7 {
			t.Fatalf("Publish = %s %d", h, n)
		}
		if got := readAll(t, st, alice, "/a.txt"); got != "content" {
			t.Fatalf("content %q", got)
		}
		if _, err := st.StatStage("alice", "s"); err == nil {
			t.Fatal("stage file survived publish")
		}
		if got := must[string](t, "Hash")(st.Hash(alice, "/a.txt")); got != h {
			t.Fatalf("Hash = %s", got)
		}
		// Content dedupe by hash, limited to what the user can read.
		bob := server.NewRoot("bob", "home")
		check(t, "CreateRoot bob", st.CreateRoot(bob))
		_, err = st.PublishHash("bob", h, bob, "/b.txt", 0o644, 1)
		wantStatus(t, "PublishHash across users", err, fsproto.ErrMissing)
		alice2 := server.NewRoot("alice", "work")
		check(t, "CreateRoot alice2", st.CreateRoot(alice2))
		if n := must[int64](t, "PublishHash")(st.PublishHash("alice", h, alice2, "/b.txt", 0o600, 1)); n != 7 {
			t.Fatalf("PublishHash length %d", n)
		}
		if got := readAll(t, st, alice2, "/b.txt"); got != "content" {
			t.Fatalf("content %q", got)
		}
		_, err = st.PublishHash("alice", fsproto.HashBytes([]byte("nope")), alice, "/c.txt", 0o644, 1)
		wantStatus(t, "PublishHash miss", err, fsproto.ErrMissing)
		stage(t, st, "alice", "s2", []byte("v2"))
		_, _, err = st.Publish("alice", "s2", alice, "/no/such/dir/x", 0o644, 1, "")
		wantStatus(t, "publish without parent", err, fsproto.ErrMissing)
		// Replacing a published file.
		if _, _, err := st.Publish("alice", "s2", alice, "/a.txt", 0o644, 2, ""); err != nil {
			t.Fatal(err)
		}
		if got := readAll(t, st, alice, "/a.txt"); got != "v2" {
			t.Fatalf("content %q", got)
		}
	})

	t.Run("Namespace", func(t *testing.T) {
		st := newStore(t)
		check(t, "CreateRoot", st.CreateRoot(alice))
		check(t, "Mkdir", st.Mkdir(alice, "/d", 0o755, 1))
		check(t, "Mkdir nested", st.Mkdir(alice, "/d/e", 0o755, 1))
		stage(t, st, "alice", "s", []byte("x"))
		_, _, err := st.Publish("alice", "s", alice, "/d/e/f", 0o644, 1, "")
		check(t, "Publish", err)
		check(t, "Symlink", st.Symlink(alice, "/link", "d/e/f", 1))
		wantStatus(t, "Remove non-empty", st.Remove(alice, "/d", false), fsproto.ErrInvalid)
		check(t, "Copy", st.Copy(alice, "/d/e/f", "/g"))
		check(t, "Rename dir", st.Rename(alice, "/d", "/D"))
		if got := readAll(t, st, alice, "/D/e/f"); got != "x" {
			t.Fatalf("after rename: %q", got)
		}
		if _, err := st.Open(alice, "/d/e/f"); err == nil {
			t.Fatal("old path still opens")
		}
		mode, mtime := uint32(0o600), int64(5)
		check(t, "SetAttr", st.SetAttr(alice, "/g", &mode, &mtime))
		check(t, "Remove recursive", st.Remove(alice, "/D", true))
		if _, err := st.Open(alice, "/D/e/f"); err == nil {
			t.Fatal("removed file still opens")
		}
		check(t, "Remove file", st.Remove(alice, "/g", false))
		wantStatus(t, "Remove missing", st.Remove(alice, "/g", false), fsproto.ErrMissing)
	})
}
