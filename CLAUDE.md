# Notes for Claude

- **Sources of truth.** `docs/9p-dotfs.md` is the protocol and its 9P form, and `docs/http-dotfs.md` maps it onto HTTP. When the implementation makes a choice the text is silent on, add it to the protocol text if clients and servers must agree on it, or to the last section of `docs/9p-dotfs.md` if it is only this implementation's. The design behind the code is the "dotfs reference implementation design" doc.
- **Branches.** All work lands on `dev` in small commits; rebase on `dev` before each push. Nothing goes to `main` until the integration work is done, as one PR from `dev` to `main`.
- **Packages and owners.** Stay inside your workstream's packages. Touching another workstream's package needs a message to that thread first.
  - `fsproto/`, `server/iface.go`, `server/ctl.go`, `server/fake/`, `server/servertest/`, `client/iface.go`, `client/fake/`, and `client/clienttest/`: foundation (W0)
  - `server/store/` and `server/meta/`: storage and metadata (W1)
  - `server/events/`: events fanout (W2)
  - `server/ninep/`, `server/auth/`, `cmd/dotfs/serve.go`, and `cmd/dotfs/token.go`: 9P server, transports, and auth (W3)
  - `client/` (except `client/fake/` and `client/clienttest/`): client library (W4)
  - `conformance/` and `cmd/dotfs/conform.go`: conformance suite (W5)
  - `sync/` and `cmd/dotfs/sync.go`: sync engine (W6)
  - `integration/`, `bench/`, and `docs/perf-log.md`: integration and benchmarks (W7)
  - `mount/`, `mount/fuse/`, and `cmd/dotfs/mount.go`: native on-demand files, the OS mount (W8)
- **Frozen interfaces.** `fsproto`, `server/iface.go`, and `client/iface.go` change only in their own small commit, announced to the other threads before it lands.
- **Shared behavior tests.** Every implementation of a `server` interface runs the matching `server/servertest` tests, and every `client.Conn` runs `client/clienttest`, so fakes and real code stay in step. A behavior the tests don't cover yet gets a test there, not only in your package.
- **Subcommands.** Each `dotfs` subcommand is its own file in `cmd/dotfs` that calls `register` in `init`; don't edit `main.go` to add one.
- **Tests.** `go test ./...` and `go vet ./...` pass on every push; CI runs both, plus `go test -race`.
- **Dependencies.** hugelgupf/p9 for 9P2000.L, 9fans.net/go for plain 9P2000, modernc.org/sqlite, klauspost/compress/zstd, lukechampine.com/blake3, and nhooyr.io/websocket (or its successor, github.com/coder/websocket). Adding anything else needs a line in the commit message saying why.
- **Prose.** Use the Oxford (serial) comma in docs and code comments.
