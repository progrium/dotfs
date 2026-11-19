# dotfs

dotfs (9p-dotfs in full) is a set of four small extensions a 9P server can expose as synthetic files under `.fs/`: an index of the whole tree, a stream of change events, path operations by name, and staged commits. Together they are enough for a Dropbox-style synced folder with one server ordering every change, and each is useful on its own. The protocol is in [docs/spec.md](docs/spec.md).

This repository holds the spec, a reference implementation in Go, and a conformance suite.

```
dotfs serve                 serve roots over 9P (TCP, Unix socket, and WebSocket)
dotfs sync <root> <dir>     mirror a root into a local folder and keep it in sync
dotfs conform <addr>        run the conformance suite against a 9P server
dotfs index <dir>           print a read-only .fs/index for a local directory
dotfs token create <user>   issue a WebSocket bearer token
```

The reference implementation is under construction; `index` works today.

## Layout

| Package | Role |
|---|---|
| `fsproto` | line formats: index lines, event records, and ctl commands and replies; path escaping |
| `server` | interfaces between the server's parts, and ctl command dispatch |
| `server/fake`, `server/servertest` | in-memory implementations, and the behavior tests every implementation passes |
| `server/store`, `server/meta` | plain files on disk, and SQLite metadata |
| `server/events` | per-scope events fanout |
| `server/ninep`, `server/auth` | the 9P server and its transports, and token auth |
| `client`, `client/fake`, `client/clienttest` | the client library, an in-process fake, and its behavior tests |
| `sync` | the sync engine |
| `conformance` | protocol tests runnable against any server |
| `cmd/dotfs` | the `dotfs` binary |
