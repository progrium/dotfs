# dotfs

dotfs is a file sync system built on 9P. A single `dotfs serve` holds each user's roots (folders) on disk, orders every change to them, and serves them over 9P2000.L, plain 9P2000, and 9P over WebSocket. `dotfs sync` keeps a local folder in step with a root, Dropbox-style: it bootstraps from one read of the whole tree, follows a live stream of changes, publishes local edits atomically, and turns concurrent edits into conflict copies instead of losing either side.

Underneath is a small protocol, also called dotfs (9p-dotfs in full): four extensions a 9P server can expose as synthetic files under `.fs/`.

| Extension | File | What it gives a client |
|---|---|---|
| Index | `.fs/index` | the whole tree's metadata in one read, optionally zstd-compressed |
| Events | `.fs/events` | a stream of changes after a sequence number |
| Path operations | `.fs/ctl` | `mv`, `rm`, `mkdir`, `cp`, and flag changes by path, optionally conditional on a version |
| Staged commit | `.fs/stage/` plus `commit` on ctl | resumable uploads, published atomically if the path is still at the expected version |

Each extension is useful on its own, and any 9P server can implement any of them. Since the root is still an ordinary 9P tree, plain 9P clients can read and write it too, and their changes show up in the index and the events stream like anyone else's. The protocol is specified in [docs/spec.md](docs/spec.md), whose last section records the choices this implementation made where the text is silent.

This repository holds the spec, the reference implementation in Go (server, client library, and sync engine), a conformance suite that checks any 9P server against the spec, and integration tests and benchmarks.

## Build

dotfs needs Go 1.27.4 or newer (see `go.mod`). It is pure Go, with no cgo (SQLite is modernc.org/sqlite).

```
go install github.com/progrium/dotfs/cmd/dotfs@latest
```

or, from a checkout:

```
go build -o dotfs ./cmd/dotfs
```

## Quick start

Start a server. By default it keeps its data in `./dotfs-data` and listens for 9P on `127.0.0.1:5640`; `-http` adds 9P over WebSocket at `/9p`.

```
dotfs serve -data ./dotfs-data -http 127.0.0.1:8080
```

Issue a token for a user. This only touches the token table, so it is safe to run beside the server:

```
dotfs token create -data ./dotfs-data alice
```

Sync a folder with alice's root named `home`. A missing root is created the first time its owner attaches.

```
export DOTFS_TOKEN=<token from above>
dotfs sync ws://127.0.0.1:8080/9p/home ~/Sync
```

Run the same command with another folder (on this machine or another) and the two stay in step. `dotfs sync` keeps running and follows changes; add `-once` to sync once and exit.

## Running the server

```
dotfs serve [flags]
```

| Flag | Default | Meaning |
|---|---|---|
| `-data dir` | `dotfs-data` | data directory: `meta.db`, `roots/`, `stage/`, and `blobs/` |
| `-tcp addr` | `127.0.0.1:5640` | TCP address for 9P; empty disables it |
| `-unix path` | | Unix socket for 9P |
| `-http addr` | | HTTP address serving 9P over WebSocket at `/9p`; empty disables it |
| `-tls-cert file`, `-tls-key file` | | serve `-http` over TLS (`wss://`) |
| `-origins patterns` | | comma-separated extra WebSocket origin patterns, for browser clients |
| `-require-token` | `false` | require a token in the attach name on TCP and Unix listeners |
| `-create-roots` | `true` | create a missing root when its owner attaches |
| `-stage-ttl d` | `24h` | remove stage files not written for this long (at least 1h) |
| `-no-dedupe` | `false` | turn off hard-link dedupe of identical content |
| `-keep-log n` | `0` (100,000) | changes each scope's log keeps; an older `since` gets a reset |

One listener speaks both 9P2000.L and plain 9P2000, chosen by `Tversion`. The data directory is locked, so only one server runs on it at a time. Roots are stored as plain files under `roots/`, metadata and the change log live in SQLite, and every ten minutes the server expires old stage files and collects unreferenced content.

### Users, roots, and tokens

Every root belongs to a user. The 9P attach name says which root to use:

```
[<user>/]<root>[?token=<token>]
```

The attaching user comes from, in order: a token in the attach name, the bearer token (or `dotfs_token` cookie) on the WebSocket upgrade, the attach's uname, or the user part of the attach name. Attaching to another user's root is refused.

- **WebSocket** always requires a token, sent as `Authorization: Bearer <token>` or, for browsers, the `dotfs_token` cookie.
- **TCP and Unix** listeners trust the named user unless the server runs with `-require-token`, so keep them on localhost or a trusted network. A token can still be passed in the attach name.

## Syncing a folder

```
dotfs sync [flags] <root> <dir>
```

`<root>` is a server address followed by the root's name:

```
ws://host:port/9p/<name>, wss://host/9p/<name>      WebSocket; token from -token or $DOTFS_TOKEN
host:port/<name>?token=<token>, tcp:host:port/...   TCP
unix:/path/to/socket/<name>?token=<token>           Unix socket
```

The sync client attaches by token, so over TCP and Unix the token goes in the root name as shown.

| Flag | Default | Meaning |
|---|---|---|
| `-token t` | `$DOTFS_TOKEN` | bearer token for a WebSocket server |
| `-device id` | from the folder's state, or the host name | device id shown in events and conflict copy names |
| `-once` | `false` | sync once and exit instead of following changes |
| `-debounce d` | `1s` | how long local changes settle before they are committed |
| `-scan d` | `1m` | how often to rescan the whole folder |
| `-nowatch` | `false` | don't watch the folder; rely on the periodic scan |
| `-q` | `false` | log only problems |
| `-memlimit MiB` | `128` | soft Go memory limit; 0 for none, and `$GOMEMLIMIT` wins |

What to expect:

- **State.** The folder's sync state lives in `<dir>/.dotfs/`, which is never synced. A folder remembers its root and refuses a root of another name. If only the server address changes, it bootstraps again as a merge, so nothing local is deleted.
- **Conflicts.** When two devices change the same file, the first commit to reach the server wins and the other becomes `name.sync-conflict-<YYYYMMDD-HHMMSS>-<device>.ext` beside it. A delete that conflicts with an edit loses, so the edit stays. The same change on both sides is not a conflict.
- **Offline.** Changes made while disconnected are queued with the versions they started from and replayed on reconnect.
- **Ignoring files.** Put patterns (`path.Match` syntax, a trailing `/` for directories only, `!` to negate, `#` for comments, and the last match wins) in `.dotfsignore` at the top of the root; it syncs like any other file. Editor swap files, `.DS_Store`, `Thumbs.db`, Office lock files, and similar are always ignored.
- **Limits.** Symlinks are received but not sent, and directory modes and mtimes are not sent. Nested scopes (directories with flag `i`) are mirrored as directories only.

## Other commands

```
dotfs conform [flags] <addr>     run the conformance suite against a 9P server
dotfs index [-hash] [-o file] <dir>
                                 print a read-only .fs/index for a local directory
```

`dotfs conform` connects to any 9P2000.L server, discovers which extensions it offers, tests each against the spec, and reports which profiles (read-only cache, watchable, and sync) it meets. Run it against a root nothing else is writing to:

```
dotfs conform -user alice -aname scratch 127.0.0.1:5640
dotfs conform shim                  # the built-in in-memory server
dotfs conform -list                 # every test, its level, and its spec section
```

See [conformance/README.md](conformance/README.md) for the rest of its flags and how the suite is built.

`dotfs index` writes the index format for a plain directory, the read-only cache profile's use case: a static file server can ship it so clients get a whole tree's metadata in one read.

## Using dotfs from Go

- `client` dials a server (`client.Dial` takes the same `ws://`, `tcp:`, and `unix:` addresses as `dotfs sync`) and exposes the extensions as Go calls: read the index, follow events, run ctl commands, and upload through stage files. `client.Conn` in `client/iface.go` is the interface, and `client/fake` is an in-process implementation for tests.
- `sync` is the engine behind `dotfs sync`: `sync.New(sync.Config{...})`, then `Sync` for one pass or `Run` to follow changes.
- `conformance.RunTest(t, target, opts)` runs the conformance suite inside `go test`, for other Go server implementations.
- `fsproto` parses and writes the protocol's line formats (index lines, event records, and ctl commands and replies).

## Tests and benchmarks

```
go test ./...          # everything, including integration tests that build and run the dotfs binary
go test -short ./...   # skip the integration tests (about 15 seconds)
go test -race ./...
go vet ./...
```

CI runs gofmt, vet, tests, and race tests on every push to `main` and `dev`.

`integration/` builds the binary and runs real `dotfs serve` and `dotfs sync` processes through a fault-injecting proxy: three clients bootstrapping, concurrent edits online and offline, edit against delete, rename against edit, long offline periods, resets past log retention, a server crash during commits, a flaky network, an upload cut and resumed, and a full disk. It also runs the conformance suite against `dotfs serve`.

`go run ./bench` measures dotfs against Syncthing on a generated 100,000-file, 2.5 GB corpus: bootstrapping a new device, time from a change to seeing it elsewhere, a burst of small files, and reconnecting after offline changes. See [bench/README.md](bench/README.md) for how to run it and [docs/perf-log.md](docs/perf-log.md) for the results so far, all on one machine over loopback.

## Layout

| Package | Role |
|---|---|
| `fsproto` | line formats: index lines, event records, and ctl commands and replies; path escaping |
| `server` | interfaces between the server's parts, and ctl command dispatch |
| `server/fake`, `server/servertest` | in-memory implementations, and the behavior tests every implementation passes |
| `server/store`, `server/meta` | plain files on disk with hard-link dedupe, and SQLite metadata, change log, and tokens |
| `server/events` | per-scope events fanout |
| `server/ninep`, `server/auth` | the 9P server and its transports, and attach names and token auth |
| `client`, `client/fake`, `client/clienttest` | the client library, an in-process fake, and its behavior tests |
| `sync` | the sync engine |
| `conformance` | protocol tests runnable against any server |
| `integration`, `bench` | end-to-end tests of the binary, and benchmarks against Syncthing |
| `cmd/dotfs` | the `dotfs` binary |

## License

MIT; see [LICENSE](LICENSE).
