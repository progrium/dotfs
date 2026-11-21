# The dotfs protocol

Jeff Lindsay. First written 2026-11-19 in the design doc; revised 2026-11-21 in this repository from what the reference implementation (server, client library, sync engine, on-demand mount, and conformance suite) taught. This file is the source of truth for the protocol and its 9P form, 9p-dotfs. [http-dotfs.md](http-dotfs.md) maps the same extensions onto HTTP and defers to this file for everything they share.

## Summary

dotfs is four small, independent extensions a file server exposes as synthetic files under `.fs/`. A server implements only the ones it needs, and a client discovers each one by whether its file exists.

| Extension | File | What it gives a client | Useful alone for |
|---|---|---|---|
| Index | `.fs/index` | the whole tree's metadata in one read | caching metadata of read-only trees, the `.h9index` use case |
| Events | `.fs/events` | a stream of changes after a sequence number | inotify-style watching of any mutable server |
| Path operations | `.fs/ctl` | `mv`, `rm`, `mkdir`, `cp`, and flag changes by path, optionally conditional on a version | quick operations without walk, open, and clunk sequences, and cross-directory rename on plain 9P2000 |
| Staged commit | `.fs/stage/` plus `commit` on ctl | resumable uploads published atomically if a path is still at an expected version | conflict-safe writes |

Together, index, events, path operations, and staged commit are enough to build a Dropbox-style synced folder or an on-demand mount, with one server ordering every change (the sync profile below), but nothing in the first three depends on sync.

The extensions are defined by their files, line formats, and replies, so they carry over to other transports. This document defines them over 9P, on any 9P transport; 9P2000.L over a WebSocket is the expected transport for browser clients, with authentication on the HTTP upgrade (see Over 9P). [http-dotfs.md](http-dotfs.md) defines the same extensions as HTTP resources, for clients that only have an HTTP library or `curl`. The two forms of one scope share its seq, versions, and log, so a server may offer both. This document is separate from hyper9's mount-index proposal and does not change it.

**Changes in this revision.** The wire formats are unchanged (`fsindex 1`, the same records and commands), so every client and server of the first revision still interoperates. The choices the reference implementation made where the first text was silent are now part of the text: escaping, sorting, reply edge cases, version rules for directories and moves, coalescing, nested scopes, and the exact 9P exchanges. Guidance learned from benchmarks (small-file throughput, index size, and parallel fetches) is in Implementation guidance, which is not normative. Four open questions are now answered.

## Conventions

These rules apply to every extension. "Must", "should", and "may" are used in their usual sense.

### Location and scope

- `.fs/` lives at the top of an attach root. A directory may carry its own `.fs/` (see Nested indexes); everything in that `.fs/` is scoped to that directory's subtree.
- `.fs/` is synthetic. It is never listed in its own index, never reported in events, and cannot be created, changed, removed, or renamed. A command naming any path with a `.fs` component is `invalid`.
- `.fs` can be walked to at every scope root, but it should not appear in directory reads of the root, so tools that copy a tree do not descend into it or block on `events`.
- A server that implements none of the extensions has no `.fs/`. Clients must treat `Enotfound` (or `ENOENT`) on any `.fs/` file as "not supported" and fall back to plain 9P.

### Discovery

A client walks to `.fs/` and reads the directory. Each extension is present if and only if its file is: `index`, `events`, `ctl`, `stage/`. `.fs/` lists exactly the extension files that can be walked to, and a name in it that is not an extension is not found. `ctl` additionally answers `caps` with the commands it accepts, so a server can offer path operations without staged commit.

### Paths

- Paths are absolute from the scope root and use `/`. The scope root itself is `/`. There is no trailing `/`, no empty component, and no `.` or `..` component.
- In every line format, paths and other free-form fields are escaped: bytes 0x21 to 0x7E other than `%` are written as they are; every other byte, including space, `%`, newline, and every byte of a non-ASCII UTF-8 character, is written as `%XX` with uppercase hex. Parsers also accept lowercase hex and unnecessary escapes (`%41`), and reject a raw byte that should have been escaped. Every valid name can be listed, so there is no `!unindexed`.
- The same escaping applies to every free-form field: paths, symlink targets, stage names, device ids, and `key=value` values. A free-form field whose value is literally `-` is written `%2D`, so it is not read as empty.

### Versions and sequence numbers

- A scope with events or staged commit has a **seq**: an unsigned 64-bit counter that increases by one on every change in the scope. A seq is never reused, including after a server crash.
- A path's **version** is the seq of its last change; `0` means the path does not exist.
- A directory's version changes when it is created, moved, or has its flags changed, not when its children change.
- A server with an index but no events (a read-only tree) may report version `0` for every path and a fixed seq.
- A change is visible in `stat`, the index, and events only once it is complete, so a client that sees a version can always read the content it names.
- 9P qid.vers carries the low 32 bits of the version.

### Line formats

Index lines, event records, ctl commands, and ctl replies are UTF-8 text, one record per line, fields separated by single spaces, unknown trailing fields ignored. A `-` stands for an empty field. Optional `key=value` fields may follow the positional ones; keys use letters, digits, `_`, `-`, and `.`, and a trailing field without `=` is an unknown field and is ignored. A line is at most 1 MiB.

- Flags are lowercase letters, written sorted and without duplicates, or `-` for none.
- Mode is written in octal with at least four digits (`0644`).
- Hashes are `<algo>:<hex>`, a lowercase alphanumeric algorithm and an even-length lowercase hex digest. `b3` is BLAKE3-256, the only algorithm defined; a client must treat an unknown algorithm like `-`.

### ctl exchange

A client writes one command to an open `ctl` fid, then reads the reply from the same fid before writing again. Replies start with `ok` or with an error word, and end with a newline. Different fids are independent, so a client opens one ctl fid per concurrent operation, and may keep a fid open and reuse it for its next command. The 9P details are in Over 9P.

### Errors

Normal 9P errors (`Rerror` or `Rlerror`) report transport and permission failures. Protocol outcomes that a client acts on are reply words on ctl rather than 9P errors:

| Word | Meaning | Carries |
|---|---|---|
| `conflict` | a base did not match, nothing changed | the current version (`0` if the path is gone) |
| `missing` | the path, its parent directory, the stage file, or the content does not exist | |
| `denied` | not allowed, or a resource limit was hit | |
| `invalid` | malformed, unknown, or impossible command | |

Any non-`ok` reply may carry `msg=<reason>` for people; clients must not parse it.

## Extension 1: index

`.fs/index` is a read-only snapshot of every path in the scope at one seq, so a client can answer walks, stats, and directory reads without asking the server.

### Format

```
fsindex 1 seq <N> [<encoding>]
<path> <type> <mode> <length> <mtime> <version> <hash> <flags> [<target>] [key=value...]
...
```

```
fsindex 1 seq 48211
/ d 0755 0 1763491200 48000 - -
/.config d 0755 0 1763491100 47950 - p
/.config/git/config f 0644 212 1763400000 31002 b3:5e1a… p
/Documents/Tax%202025.pdf f 0644 284113 1763480011 46990 b3:9f2c… -
/Photos d 0755 0 1763491000 48100 - i
/bin l 0777 7 1763000000 120 - - usr/bin
```

| Field | Meaning |
|---|---|
| path | escaped path from the scope root |
| type | `d` directory, `f` file, `l` symlink |
| mode | permission bits, octal |
| length | bytes; 0 for directories, target length for symlinks |
| mtime | seconds since the epoch |
| version | see Conventions; `0` allowed for read-only trees |
| hash | `<algo>:<hex>` content hash for files, or `-` |
| flags | `-`, or a set of letters: `p` prefetch, `i` nested index |
| target | symlink target, escaped, only for type `l` |

- Lines are sorted by the bytes of the unescaped path, and the first line is always `/`. A directory's children are therefore not always contiguous after it (`/a`, `/a b`, `/a/c`), so a client builds its tree by path, not by position.
- A reader must reject an index whose lines are out of order, and must ignore key=value fields it does not know.
- The same line, called an **index line**, appears in `put` event records and in `stat` replies.

### Reading

- The header line is always plain text. Before reading, a client may write `accept <encoding> [...]` to the index fid; the server then may name one of them as the header's last word and send the rest of the file in that encoding. `zstd` (one frame) is the only encoding defined. With no `accept`, the body is plain, so `cat .fs/index` works. A server that offers `zstd` compresses the body itself if it has no compressed copy.
- The snapshot is consistent at seq N: every line reflects the scope after change N and before change N+1. A fid takes its snapshot at its first read and keeps it, so reads at any offset come from one seq.
- qid.vers of `.fs/index` is the low 32 bits of N, and its qid.path is stable, so a client can tell from a stat whether a cached copy is current.
- Length in a stat may be 0 when the server generates the file on open.

### Nested indexes (flag i)

A directory flagged `i` is a **nested scope**: it has its own `.fs/` with its own index (and, if the server supports them, its own events and ctl). Use it for very large or busy subtrees, or for a different server mounted under a path. A client reads a nested index the first time it needs anything inside.

- Setting flag `i` on a directory makes it a nested scope, starting at seq 0. Its children move there with version 0, and from then on the parent's index, `stat`, and directory reads list only the directory itself.
- The parent rejects with `invalid` any change below a nested directory, and `rm`, `rmtree`, or `mv` of a directory that is or holds one. Flag `i` on a scope root, or on anything but a directory, is `invalid`.
- Clearing flag `i` merges the children back into the parent, each with the version of that `flags` change, and ends the nested scope and its log. A nested scope that holds another nested scope cannot be merged (`invalid`).
- Both are one `flags` record in the parent. A client that mirrors the parent and sees `i` added or removed re-reads what moved: the nested index, or the parent's.

### Prefetch (flag p)

A hint that a client keeping a local copy should fetch the file's content right after reading the index. On a directory it covers everything below. It is advisory and shared by all clients; per-device pinning is a client matter.

### Read-only servers

A server with only an index (like a package cache) may use version `0`, a fixed seq, and `-` for hashes and flags. That is exactly the role `/.h9index` plays today.

## Extension 2: events

`.fs/events` streams change records for the scope, so a client can watch a tree like inotify without polling, with or without an index.

### Opening a stream

Each open fid is its own stream. Before the first read a client may write any of these setup lines, one per write or several in one write separated by newlines:

| Write | Effect |
|---|---|
| `since <N>` | start with the first change after seq N |
| `path <dir>` | only report changes at or below `<dir>` (repeatable) |
| `coalesce` | allow the server to merge changes to one path into the latest (see Coalescing) |

Without `since`, the first record is `seq <current>` and only later changes follow. Setup written after the first read fails with a 9P error.

### Reading

- Each read returns one or more whole records, each ending with a newline, and never more than the read's count. A record never spans two reads; if the next record alone does not fit, the read fails with a 9P error (`EMSGSIZE`), so a client should read with a count of at least the maximum line length it accepts.
- A read blocks while there are no records. A pending read is the push channel. `Tflush` cancels it and is answered only once the read is canceled or answered; a canceled read takes no records from the stream, so the next read returns them.
- Reads use increasing offsets, which a server may ignore.
- Clunking the fid, or closing the connection, ends a pending read.

### Records

```
<seq> put <index line> [key=value...]
<seq> del <path> [key=value...]
<seq> mv <from> <to> [key=value...]
<seq> flags <path> <flags> [key=value...]
<seq> reset
```

```
48212 put /Notes/todo.md f 0644 830 1763491250 48212 b3:77d0… p dev=mac-a1
48213 del /Downloads/old.zip dev=linux-b2
48214 mv /Draft.md /Posts/Final.md dev=mac-a1
48215 flags /Projects p dev=mac-a1
```

- `put` is a create or modify and carries the whole index line, so a client never needs a follow-up stat. `mkdir`, `cp`, `commit`, and plain 9P changes (writes, symlink creation, and mode or mtime changes) are all `put` records.
- `mv` of a directory moves everything below it, and `del` of a directory (from `rmtree`) removes everything below it; there is no record per child.
- `dev=<id>` names the client that made the change, when known. In a `put` record it follows the index line.
- Records arrive in seq order. With `coalesce` or `path` a client may see gaps in seq; without them there are none.

### Coalescing

With `coalesce`, a record replaces an earlier unread record for the same path when nothing between them changes the tree's shape (a `mv`, a `del`, or a `put` of a directory). A `put` or `del` replaces a `put` or `flags`; a `flags` replaces a `flags`. A `mv` is never merged. The merged record keeps the later seq and position, so a client sees the latest state of the path, and every record it depends on still comes first. A watcher that only mirrors state (an on-demand mount, a UI) should use it; a client that needs every intermediate change should not.

### Falling behind

A server keeps a bounded log. If `since N` names a seq it no longer has, or one after the current seq, or a reader falls more than its queue bound behind the newest change, the server sends `<seq> reset` with the current seq and ends the stream: the records the reader had not read are dropped, and later reads return 0 bytes. A stream still replaying history reads it at its own pace and is not reset for being slow, only if retention trims the log past it. On `reset` (or a zero-byte read before one, which a client should treat the same way) the client re-reads the index, or simply rescans, and reopens with `since`.

### Bootstrapping with an index

Two gap-free ways: read the index at seq N, then open events with `since N`; or open events first (getting `seq <current>`), read the index at some M, and drop records at or below M.

## Extension 3: path operations

`ctl` performs namespace operations by path in one round trip, optionally conditional on a version. It needs no index or events, though a server with events reports every change it makes.

### Commands

| Command | Effect | Replies |
|---|---|---|
| `caps` | lists supported commands | `ok <command>...` |
| `hello <device-id>` | names this client in events (`dev=`) | `ok` |
| `mkdir <path> [<base>]` | creates a directory | `ok <version>`, `conflict <version>`, `missing` |
| `rm <path> [<base>]` | removes a file, symlink, or empty directory | `ok <seq>`, `conflict <version>`, `missing` |
| `rmtree <path> [<base>]` | removes a directory and everything below it | `ok <seq>`, `conflict <version>`, `missing` |
| `mv <from> <to> [<base>] [<tobase>]` | renames, across directories, atomically | `ok <version>`, `conflict <version>`, `missing` |
| `cp <from> <to> [<tobase>]` | copies a file on the server; cheap when content is deduplicated | `ok <version>`, `conflict <version>`, `missing` |
| `flags <path> <flags>` | sets the index flags (`p`, `i`) | `ok <version>`, `missing` |
| `stat <path>` | returns the path's index line | `ok <index line>`, `missing` |

In `mv`, an omitted base before a `tobase` is written `-`, as in `mv /a /b - 0`.

### Replies

- Parent directories are never created implicitly: `mkdir`, `mv`, `cp`, and `commit` into a directory that does not exist reply `missing`.
- `mkdir` of an existing path, with or without a base, replies `conflict <version>`.
- `invalid` answers: a parent that is not a directory; `rm` of a non-empty directory; replacing a directory with a file, or with `mv` in either direction; `mv` of a path into itself; `cp` of a directory; flag `i` on anything but a directory; an unknown command or one the server does not offer; and any change to a path with a `.fs` component.
- A command the server does not list in `caps` is `invalid`.

### Conditional operations

- `<base>` is the version the client expects `<path>` (or `<from>`) to have; `<tobase>` is the expected version of the destination, `0` meaning it must not exist.
- If a base is given and does not match, nothing changes and the reply is `conflict <current-version>`.
- Omitted bases make the operation unconditional, as with plain 9P.
- A server without versions (no events, no staged commit) rejects any base with `invalid`.

### Versions and records

- `mv` gives the moved path the new seq; paths below a moved directory keep their versions. `rmtree` and `mv` of a directory are one record each.
- `cp` gives the destination the new seq and no flags; `commit` keeps the path's existing flags.
- `rm` and `rmtree` reply with the seq of the `del` record.

### Why this exists

- 9P2000 `wstat` can only rename within one directory, so a cross-directory move otherwise means copy plus delete. 9P2000.L has `Trenameat`, but it is not conditional.
- Path operations need no walk, open, and clunk sequence, which matters over high-latency WebSockets.
- A conditional `rm` or `mv` lets a client act without racing another client's change.

## Extension 4: staged commit

Staged commit lets a client upload a file's bytes over any number of connections and attempts, then publish them atomically at a path only if that path is still at the version the client expects.

### Staging

- A client creates a file under `.fs/stage/` with any name it chooses (a UUID is recommended) and writes bytes at any offsets. Re-opening an existing stage file and writing more resumes an interrupted upload; the stage file's size is where an upload written in order continues.
- Stage files are private to the attaching user and invisible in the index and events.
- A server removes stage files that have not been written for a period it chooses, at least one hour.

### Publishing

| Command | Effect | Replies |
|---|---|---|
| `commit <stage> <path> <base> <mtime> <mode> [hash=<hash>]` | publishes the stage file at `<path>` if it is at `<base>`, then removes the stage file | `ok <version> <hash>`, `conflict <version>`, `missing`, `invalid` |
| `commit-hash <hash> <path> <base> <mtime> <mode>` | publishes content the server already holds, with no upload | `ok <version>`, `conflict <version>`, `missing` |
| `abort <stage>` | discards a stage file | `ok` |

- `<base>` is required, `0` for a new path. There is no unconditional commit; plain writes serve that role.
- With `hash=`, the server verifies the content and replies `invalid` on a mismatch. The `ok` reply always carries the content's hash.
- `commit` replies `missing` when the stage file is gone or the parent directory does not exist.
- The publish is atomic: other clients see either the old content or the new, never a mix. Once a `commit` has taken a stage file, further writes through fids still open on it fail with a 9P error, so published content always matches its hash.
- `commit-hash` answers `missing` when the server lacks the content; the client then stages and commits normally. A server decides how widely it searches (one scope, or every scope of the same user), but only among content the attaching user can read; see Security considerations.
- `commit-hash` with a path's current hash and a new mode or mtime is how a client changes only metadata.

### Ordering guarantee

On a server with events, the record for a commit (or any ctl change) is assigned before the `ok` reply is sent, and the reply carries that version. A client therefore recognizes its own change in the stream by version (a record at or below the version it already has for that path), and never mistakes it for a remote edit.

### Plain writes

A server with staged commit should still accept ordinary 9P writes, applied as last-writer-wins and reported like any other change:

- Creating a file with plain 9P (`Tlcreate` or `Tcreate`) commits an empty file at once, base `0`, so the path exists for a following walk, then commits the content when the fid is clunked or fsynced. A plain write of a new file is therefore two changes.
- A plain write to an existing file applies when the fid is clunked or fsynced, as one unconditional commit. Its mode is kept and its mtime is the commit time unless the client sets one.

## Over 9P

These rules are specific to the 9P form. [http-dotfs.md](http-dotfs.md) has its own.

**Dialects and transports.** A server may speak 9P2000.L, plain 9P2000, or both on one listener, picking by the `Tversion` string. Over a WebSocket, each 9P message is one binary WebSocket message. Raw TCP should be limited to localhost or wrapped in TLS.

**Attach and authentication.** 9P `Tauth` is not used. Over WebSocket the client authenticates on the HTTP upgrade (`Authorization: Bearer <token>`, or a cookie) and the server maps that to the attach user. The attach name selects a root; the reference implementation uses `[<user>/]<root>[?token=<token>]` and refuses a root of another user (`EACCES`).

**ctl.** A command is one `Twrite` at offset 0, with or without a trailing newline. The reply is read from offset 0 at increasing offsets until a read returns no data, and ends with a newline. The next write on the fid replaces the reply. A command over 64 KiB may fail with a 9P error.

**index.** To send `accept`, a client opens `.fs/index` for reading and writing and writes the accept line at offset 0; if the server refuses the write open, the client reads the index plain. Reads use increasing offsets from 0.

**events.** The setup lines go in writes before the first read. See Extension 2 for reads, `Tflush`, and `EMSGSIZE`. Some client libraries cannot send `Tflush`; with them a canceled read stays pending on the server, and its records are returned by the client's next read.

**stage.** A client walks to `.fs/stage/<name>` and opens it for writing, or creates it (`Tlcreate` or `Tcreate`) when the walk fails. A stat's length is where a resumed upload continues.

**Plain operations.** A Linux `rename` across scopes fails with `EXDEV`. A server may ignore the mode of a 9P `mkdir`, since the `mkdir` command has none. Servers that cannot represent them refuse hard links and device files (`EPERM`).

## Profiles

A profile is a named set of extensions plus the behavior a client can expect. Servers state the profiles they meet; the reference implementation meets all three.

| Profile | Extensions | Typical server |
|---|---|---|
| Read-only cache | index | package and image caches such as wanix-cache |
| Watchable | index (optional), events, path operations | a mutable file server that other tools watch |
| Sync | index, events, path operations, staged commit | a server holding user roots for synced folders |

### Sync, as a composition (non-normative)

A sync client keeps a local copy of a scope and needs nothing beyond the four extensions. This is what `dotfs sync` does.

1. **Bootstrap:** `hello`, read the index with `accept zstd`, store every entry, then fetch content for files not already held locally by hash (all of them, or only `p` files for a partial copy).
2. **Follow:** open events `since <N>`. Apply `put`, `del`, `mv`, and `flags` to the local copy; a changed hash makes a local copy stale. Skip records for the client's own changes by version.
3. **Write:** remember the version a local edit started from. Publish with `commit` (or `commit-hash` first), and use conditional `rm` and `mv` for deletes and renames. A local rename the client can identify (same inode, or a unique hash match) is sent as `mv` with the source's base and a destination base of `0`. A local delete of a directory is sent as conditional `rm` of each path, deepest first, never `rmtree`, so an edit made on the server meanwhile is not lost.
4. **Conflict:** on `conflict`, keep the server's copy at the path, rename the local copy to `name.sync-conflict-<YYYYMMDD-HHMMSS>-<device>.ext` (time in UTC; the extension is the text from the last dot unless that dot starts the name, so `.bashrc` becomes `.bashrc.sync-conflict-…-<device>`; `-2`, `-3`, and so on follow the device if the name is taken), and commit it as a new file with base `0`. A change on both sides to the same content (same hash and mode, or both directories) is not a conflict. A delete that conflicts with an edit loses, so the edit stays: a conflict whose current version is `0` commits again with base `0`, recreating a missing parent first.
5. **Remote structure changes:** a remote `del` or `mv` of a directory removes or moves only what is unchanged locally; files edited or created locally under it are committed again, as new files, under the old path or the new one.
6. **Offline:** queue changes with their base versions and replay them in order on reconnect.
7. **Reset:** on a `reset` record, re-read the index and reconcile. When a folder is pointed at a different server for the same root, versions mean nothing there, so the client forgets them and bootstraps as a merge: nothing local is deleted, files only on the client are sent, and files that differ become conflict copies.

Because the server assigns one version per path, this needs no version vectors, and every change reaches every connected client in one round trip. The first commit to arrive wins, which is deterministic and never loses the other side's work.

What the extensions do not carry, a sync client decides for itself. The reference client reads ignore rules from `.dotfsignore` at the top of the root (synced like any file) and has a built-in list (`.DS_Store`, editor swap and lock files, and so on); it receives symlinks but cannot send them, since no command creates one, and it does not send directory modes and mtimes.

### On-demand mount, as a composition (non-normative)

An on-demand mount (`dotfs mount`) shows a scope as a local filesystem without copying it, using the same extensions differently:

- Read the index once with `accept zstd`, then follow events from its seq with `coalesce`. Listing and stat are answered from that tree, never from the server.
- Read a file's content the first time it is opened and cache it by hash (or, without a hash, by path, version, length, and mtime). A changed hash in a `put` invalidates the cached copy.
- Copy a file opened for writing to a private working copy and commit it on close or fsync, with the version it was opened at as the base; resolve a `conflict` as the sync client does.
- Send `mkdir`, `rm`, and `mv` at once with the version last seen. An `rm` or `mv` that gets `conflict` fails with `EBUSY` and changes nothing.
- Show a nested scope as an empty directory until it is needed.
- When the server is unreachable, serve the saved tree and cached content, fail opens of uncached files (`EIO`), and retry commits until they go through.

## Implementation guidance (non-normative)

Measured on a 100,000-file, 2.5 GB corpus against Syncthing (see [perf-log.md](perf-log.md)). None of this changes the wire.

**The index scales.** 100,000 paths are 18.7 MB plain and 5.9 MB with zstd, read in about 0.2 s. One snapshot per scope is fine at user-data scale; nested scopes are for subtrees that are much larger or much busier than that. Bootstrapping a new device from one zstd index plus parallel fetches was 3.4 times faster than Syncthing.

**Small files cost round trips, not bytes.** A new file is up to four exchanges (`commit-hash`, stage create, write, `commit`), and a server that fsyncs each one serially is the bottleneck. What helped, in order of effect:

- Servers: commit every change waiting on a scope as a group (one transaction, one fsync per directory), with a stale base failing only its own change. This took a 10,000-file burst in 1.5 times faster.
- Servers: verify and fsync a stage file before taking the scope's write lock, not under it.
- Clients: keep several uploads in flight on separate ctl fids (eight is enough), as a rolling window rather than in batches.
- Clients: skip `commit-hash` for new files of 64 KiB or less; it almost always misses. Keep it for edits and large files, where a hit saves the upload.
- Receiving clients: when events are waiting, take a batch of them (up to about a thousand), fetch their files in parallel, then apply the records in order.

**Watching.** A change reached the other device about 10 ms after the sender's debounce. A client that also watches its local disk must never drop the hint that triggers its catch-up scan, or a burst waits for the next periodic scan.

**Memory.** ctl replies are short; a client should read them into a small reused buffer rather than one sized for the largest 9P message.

## Future extension: history (sketch)

A server that keeps old content and does not trim its log can expose past states with one more read-only extension, and no git-style checkout is needed: every seq already names a consistent state of the scope, so a past state can simply be served as a tree.

- **`.fs/at/<seq>/`** is a read-only view of the scope as it was right after change `<seq>`, with its own `.fs/index` at that seq. `.fs/at/<timestamp>/` (RFC 3339) resolves to the last seq at or before that time.
- **`history <path>` on ctl** lists a path's versions, newest first, as `<version> <mtime> <hash> <op> dev=<id>` lines.
- **Restore is an ordinary commit.** `cp /.fs/at/46990/Documents/Tax%202025.pdf /Documents/Tax%202025.pdf <base>` copies an old version over the current one with a base version, so it goes through conflict detection and reaches every client as a normal `put`. With content deduplicated by hash, the copy costs nothing.
- **Retention** is the server's policy (for example, every version for 30 days, then daily). `.fs/at/<seq>` answers `Enotfound` once a state is pruned.

Branches, merges, and per-client checkouts stay out of scope; the sketch only covers browsing and restoring the single line of history the server already orders. It needs nothing from the other extensions beyond what they define, so it can be added later without changing them.

## Security considerations

The protocol adds no authentication of its own; it relies on the transport and on the per-attach identity, and adds a few concerns to handle.

- **Authentication.** Over WebSocket, authenticate on the HTTP upgrade (cookie or bearer token) and map that to the attach user. 9P `Tauth` is not used. Raw TCP transports should be limited to localhost or wrapped in TLS.
- **Scope.** Every `.fs/` file and command is limited to the attach root. Paths in ctl are resolved within the scope, and `..` never leaves it.
- **Hash probing.** `commit-hash` and `cp` reveal whether the server holds some content. A server must only consider content the attaching user can already read (the reference server looks only at content still published in one of that user's roots), so a user cannot learn about another user's files or about content that was deleted.
- **Metadata exposure.** The index exposes every name in the scope at once. A server must only serve indexes of scopes the user can fully read, or leave unreadable subtrees out.
- **Resource limits.** Servers should cap index size, the number of open event streams per user, stage space per user, and ctl command length, and return `denied` or a 9P error when a limit is hit.
- **Event retention.** The log reveals recent names, including of deleted files, to anyone who can read the scope; keep the retention window no longer than needed.

## Relation to mount-index

The index extension is a deliberate iteration on `/.h9index`, but the mount-index proposal stays unchanged until this is proven. If it is later folded in:

|  | `/.h9index` today | `.fs/index` |
|---|---|---|
| Location | `/.h9index` at the root | `.fs/index`, alongside the other extensions |
| Names with spaces or non-UTF-8 | left out, directory marked `!unindexed` | percent-escaped, always listed |
| qid.vers of entries | 0 | the path's version, or 0 for read-only trees |
| Changes | regenerate the file | the events extension |
| Content identity | none | optional hash per file |
| Hints | none | `p` prefetch, `i` nested index |
| Encoding | plain text | plain, or zstd after an opt-in |

## Prior art

| System | What this borrows |
|---|---|
| Coda (CMU, from AFS) | disconnected operation: a log of local changes replayed with conflict detection |
| Plan 9 replica(1) | a database of paths plus a change log to bring a copy up to date |
| Upspin | `Watch(name, sequence)`, a change stream after a sequence number |
| Dropbox API v2 | snapshot with a cursor, deltas, and a long-poll to block; hash dedupe; resumable uploads |
| WebDAV sync-collection (RFC 6578), Nextcloud | sync tokens for deltas; `If-Match` compare-and-swap writes |
| Syncthing BEP | index and index updates keyed by sequence; conflict-copy naming |
| inotify, fanotify | event vocabulary for local watching |

These are from memory; links and checks against each system's documentation are still to add.

## Open questions

- [x] Is one snapshot per scope fine at user-data scale? Yes: 100,000 paths are 5.9 MB with zstd and read in about 0.2 s (see Implementation guidance).
- [ ] Should events offer an index diff for a client slightly behind, or is `since N` always enough? So far `since N` with a 100,000-change log has been enough.
- [x] Should ignore rules be part of the protocol? No. They are a client matter; the reference client syncs a `.dotfsignore` file like any other.
- [x] Is `hello` worth keeping, or should the device id come from the transport? Both: `hello` stays for 9P, where a connection is long-lived, and http-dotfs carries the device id in a request header.
- [ ] Which hash algorithms are allowed besides BLAKE3, and is the hash ever required? Still open; servers that cannot compute BLAKE3 (for example, one fronting Syncthing) send `-`.
- [ ] Should there be a one-round-trip upload for small files (content inline in a command, or a stage write that commits on clunk)? Small files are the one workload where the reference implementation is slower than Syncthing, and HTTP `PUT` already does this in one request.
- [ ] Should a command create symlinks, and should directory modes and mtimes be settable, so sync clients can send them?
- [ ] Should clients follow nested scopes automatically? The reference clients stop at them today.

## Notes from the reference implementation

The protocol text above settles what clients and servers must agree on. These are choices the reference implementation makes that are not protocol, recorded so they are not mistaken for it.

- **Fakes.** The in-memory fakes in `server/fake` treat a nested scope as an independent namespace and do not hide its children from the parent.
- **Conformance suite** (`dotfs conform`) speaks 9P2000.L and expects `Rversion` to name exactly `9P2000.L`.
- **Storage.** `commit-hash` looks at every root of the attaching user, and only at content still published at some path in one of them. The log keeps the last 100,000 changes of each scope by default (`dotfs serve -keep-log`). A server restarted after a crash finishes any change it had recorded.
- **Serving.** `dotfs serve` speaks both 9P dialects on each listener, accepts the token from the attach name, a WebSocket bearer token, or a `dotfs_token` cookie (TCP and Unix listeners trust the named user unless told to require a token), creates a missing root the first time its owner attaches (unless `-create-roots=false`), and removes stage files not written for 24 hours. Plain writes stage through private files whose names start with `.dotfs-plain-` and are not listed in `.fs/stage/`.
- **Mount.** `dotfs mount` uses kernel page and directory caching, renames over an empty directory by removing it first, and turns a chmod or mtime change into `commit-hash` with the current hash.
