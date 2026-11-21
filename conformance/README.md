# dotfs conformance suite

`dotfs conform <addr>` connects to a 9P2000.L server, finds which dotfs
extensions it offers by walking to `.fs/index`, `.fs/events`, `.fs/ctl`, and
`.fs/stage/`, and runs the tests for each. It prints a pass or fail report per
extension and says which profiles (read-only cache, watchable, and sync) the
server meets. It exits 1 if any test fails.

```
dotfs conform localhost:5640                  # tcp; also tcp:host:port
dotfs conform unix:/run/dotfs.sock
dotfs conform -aname home -user jeff host:564 # attach name and user
dotfs conform -json localhost:5640 > report.json
dotfs conform -run '^events/' -v localhost:5640
dotfs conform -list                           # every test, its level, and its spec section
dotfs conform shim                            # the built-in server over the in-memory fakes
```

Run it against a scope nothing else is writing to. Tests that change the tree
work below `/conform-<random>/` and remove it at the end; several of them
expect the scope's seq to move only by their own changes.

## How it is built

- `conformance` holds the tests, the runner, and the reports. Each test has an
  ID (`<extension>/<name>`), the spec section it checks (headings in
  `docs/9p-dotfs.md` joined by `>`), and a level: `must` failures fail the run,
  `should` failures are warnings.
- `conformance/internal/wire` is a small 9P2000.L codec and client. The suite
  does not use the reference client (package `client`), so the two check each
  other, and it does not use hugelgupf/p9's client, which cannot send `Tflush`.
  Tests in `conformance/shim` check the codec against p9 in both directions.
- `conformance/shim` serves the in-memory fakes from `server/fake` over 9P with
  every extension, for testing the suite itself. It refuses flag `i`, since the
  fakes do not implement nested scopes as the spec settles them, and it has
  switchable bugs (`shim.Bug`) that the suite's own tests check it catches. The
  reference client's behavior tests (`client/clienttest`) also run against it.
- Go implementations can run the suite in `go test` with
  `conformance.RunTest(t, target, opts)`.

The `events/reset-trimmed` test needs a way to trim the server's log on demand,
so it only runs against in-process targets that set `Target.Trim`.
`stage/private` needs a second user that can attach (`-other-user`).
`index/nested` is skipped when the server answers flag `i` with `invalid`.

## Coverage

Every check in the conformance table of the reference implementation design
maps to tests here:

| Extension | Design check | Tests |
|---|---|---|
| Index | header and line grammar | `index/header`, `index/grammar` |
| | sort order | `index/sorted` |
| | escaping of every byte class | `index/escaping` |
| | consistency at one seq under concurrent writes | `index/consistent` |
| | `accept zstd` | `index/accept-zstd`, `index/accept-unknown` |
| | qid.vers freshness | `index/qid-vers`, `index/matches-stat` |
| | nested `i` scopes | `index/nested` |
| Events | `since` replay with no gaps | `events/since-replay`, `events/bootstrap` |
| | `seq <current>` start | `events/seq-start` |
| | blocking reads and `Tflush` | `events/blocking-read`, `events/flush`, `events/whole-records` |
| | `path` filters | `events/path-filter`, `events/setup-lines` |
| | `coalesce` | `events/coalesce` |
| | `reset` on overflow and on trimmed history | `events/overflow`, `events/reset-trimmed`, `events/reset-future` |
| | directory `mv` as one record | `events/dir-mv` |
| Path operations | every command with and without bases | `ctl/hello`, `ctl/stat`, `ctl/mkdir`, `ctl/rm`, `ctl/mv`, `ctl/cp`, `ctl/flags`, `ctl/versions` |
| | `conflict` replies | `ctl/mkdir`, `ctl/rm`, `ctl/rmtree`, `ctl/mv`, `ctl/cp` |
| | cross-directory `mv` | `ctl/mv`, `ctl/mv-invalid` |
| | `rmtree` | `ctl/rmtree` |
| | `caps` | `ctl/caps` |
| | scope escapes rejected | `ctl/scope-escape`, `ctl/bad-commands` |
| Staged commit | resumable stage writes | `stage/commit`, `stage/resume` |
| | atomic publish seen by a concurrent reader | `stage/atomic` |
| | `hash=` verification | `stage/hash-verify` |
| | `commit-hash` hit and miss | `stage/commit-hash` |
| | own-commit version ordering | `stage/own-commit` |
| | plain-write fallback | `stage/plain-write` |

Beyond the table, the suite checks discovery (`discovery/*`), ctl fid
independence, stage file privacy and invisibility, `abort`, base rules for
`commit`, and that nothing under `.fs/` shows up in the index or events.
