# The dotfs protocol in HTTP

Jeff Lindsay, 2026-11-21. http-dotfs is the four dotfs extensions (index, events, path operations, and staged commit) as HTTP resources, for clients that have an HTTP library or `curl` but no 9P client, such as agents, scripts, and browsers without a 9P library. It defines only the mapping: the model, the line formats, the version rules, and the reply words are those of [9p-dotfs.md](9p-dotfs.md), which wins wherever the two seem to differ.

## Summary

| Extension | 9p-dotfs | http-dotfs |
|---|---|---|
| Index | read `.fs/index` | `GET .fs/index` |
| Events | read `.fs/events` | `GET .fs/events?since=N`, streamed, as server-sent events, or long-polled |
| Path operations | write a command to `.fs/ctl`, read the reply | `POST .fs/ctl` with the command, reply in the body |
| Staged commit | write `.fs/stage/<name>`, then `commit` on ctl | `PUT .fs/stage/<name>` with `Content-Range`, then `commit` on ctl |
| Plain files | walk, open, read, write, and clunk | `GET`, `HEAD`, `PUT`, and `DELETE` on the path, with `ETag` as the version |

Two ideas carry the whole mapping:

1. **Everything under `.fs/` uses the same text as 9P.** The index body, event records, ctl commands, and ctl replies are byte for byte the formats in 9p-dotfs.md, so one parser serves both forms.
2. **Plain files are ordinary HTTP resources whose `ETag` is the dotfs version.** `If-Match` is a base version and `If-None-Match: *` is base `0`, so a conditional `PUT` is a one-request `commit` and a conditional `DELETE` is a conditional `rm`.

A server may serve a scope over 9P and HTTP at once. Both forms then share one seq, one set of versions, and one log, and a change made through either appears in the other's index and events.

## Conventions

### Base URL and scope

A scope is served at a **base URL** ending in `/`, chosen by the server, for example `https://host/s/7f3k/` or `https://host/r/jeff/home/`. A path `/a/b` in the scope is the URL `<base>a/b`, and the scope root is the base URL itself. `.fs/` is `<base>.fs/`. A nested scope (flag `i`) at `/Photos` has its own `.fs/` at `<base>Photos/.fs/`.

Everything below is relative to the base URL.

### Paths in URLs

The request path below the base is percent-decoded to get the raw bytes of the dotfs path. A client builds a URL from a dotfs path by escaping each component as a URL path segment (RFC 3986); escaping the bytes dotfs escapes, plus `?` and `#`, is enough, so a percent-escaped dotfs path is a valid URL path once `?` and `#` are escaped too. A server must reject `.` and `..` segments and empty segments (`400`), and must not normalize them away.

Paths inside bodies and headers (index lines, records, commands, and `Dotfs-Entry`) use dotfs escaping, as in 9p-dotfs.md.

### Discovery

`GET .fs/` returns `text/plain`, one extension name per line, exactly as a 9P directory read of `.fs/` would list them: `index`, `events`, `ctl`, and `stage/`. A `404` means the server offers no dotfs extensions at that base, and a client falls back to plain `GET` and `PUT` (or to nothing, for a plain static server). `POST .fs/ctl` with `caps` lists the commands offered.

`.fs/` never appears in directory listings, the index, or events, and any request that would change a path with a `.fs` component other than the ones defined here is `invalid` (`400`).

### Versions as ETags

- A path's `ETag` is its version as a strong entity tag: `"48213"`.
- `If-Match: "<version>"` on a request that changes a path is that path's base. `If-None-Match: *` is base `0`: the path must not exist.
- A response that changes a path carries the path's new `ETag`, and `Dotfs-Seq` with the scope's seq after the change.
- A read-only server whose versions are all `0` should send the content hash as the `ETag` (`"b3:9f2c…"`), or a weak `ETag`, or none; it has no conditional writes to match against.

### Headers

| Header | Direction | Meaning |
|---|---|---|
| `Authorization: Bearer <token>` | request | the user, as on a 9p-dotfs WebSocket upgrade |
| `Dotfs-Device: <device-id>` | request | the client's device id, reported as `dev=` in events; replaces `hello`, since HTTP has no long-lived session |
| `Dotfs-Seq: <N>` | response | the scope's seq the response reflects (index, file reads, and changes) |
| `Dotfs-Entry: <index line>` | response | the path's index line, on `GET`, `HEAD`, and successful changes, so no `stat` is needed |
| `Dotfs-Mode: <octal>` | request | the mode for a `PUT` (default: keep the existing mode, or `0644` for a new file) |
| `Dotfs-Mtime: <seconds>` | request | the mtime for a `PUT` (default: the time of the commit) |
| `Dotfs-Hash: <hash>` | request | the content hash for a `PUT`; the server verifies it, as `commit … hash=` |

Header values are ASCII, which index lines already are, since dotfs escaping leaves only printable ASCII.

### Outcomes and status codes

Every response to a request that names a path, other than `POST .fs/ctl`, maps the ctl reply word to a status code, and its body (`text/plain`) is the reply line itself:

| Reply word | Status | Notes |
|---|---|---|
| `ok` | `200 OK`, `201 Created` for a new path, `204 No Content` for `DELETE` | |
| `conflict <version>` | `409 Conflict` | `ETag` is the current version, absent if the current version is `0`; body `conflict <version>` |
| `missing` | `404 Not Found` | also for a missing parent directory |
| `denied` | `403 Forbidden`, `413 Content Too Large`, or `429 Too Many Requests` | `413` and `429` for resource limits |
| `invalid` | `400 Bad Request` | `422 Unprocessable Content` for a hash mismatch |

A conflict is `409`, not `412 Precondition Failed`, because it is a protocol outcome a client resolves, the same as the `conflict` reply word, and a generic HTTP client or proxy should not treat it as a malformed request. A client should still treat a `412` from an intermediary as a conflict of unknown version and re-read the path.

Transport and authentication failures use their usual status codes (`401`, `5xx`) with any body.

## Extension 1: index

`GET .fs/index` returns the index format unchanged: the `fsindex 1 seq <N>` header line, then one index line per path, sorted as 9p-dotfs.md requires. `Content-Type` is `text/plain; charset=utf-8`.

- **Encoding.** A client that sends `Accept-Encoding: zstd` may get `Content-Encoding: zstd`, which then covers the whole body, header line included. The header line then names no encoding, since HTTP has already removed it. Without `zstd` in `Accept-Encoding`, the body is plain, so `curl .../.fs/index` works. A server may also offer `gzip` or `br` this way.
- **Caching.** The response carries `ETag: "<N>"` and `Dotfs-Seq: <N>`. A client that holds a copy sends `If-None-Match: "<N>"` and gets `304 Not Modified` while the seq is unchanged. Servers should send `Cache-Control: private, no-cache`, so shared caches never keep an index and private ones always revalidate.
- **Consistency.** The body is one snapshot at seq N, as in 9P.
- **Nested scopes.** A directory flagged `i` is listed without its children; its own index is `GET <dir>/.fs/index`.

## Extension 2: events

`GET .fs/events` opens a stream. The 9P setup lines become query parameters:

| Query | 9P setup line |
|---|---|
| `since=<N>` | `since <N>` |
| `path=<dir>` (repeatable) | `path <dir>` |
| `coalesce=1` | `coalesce` |

The records are those of 9p-dotfs.md (`put`, `del`, `mv`, `flags`, and `reset`), and without `since` the first record is `seq <current>`. A response delivers them in one of three ways, chosen by the client:

1. **Streamed lines** (the default): `Content-Type: text/plain`, one record per line, sent as they happen, with the response held open. Servers should send a blank line about every 30 seconds while idle, which clients ignore, so proxies do not close the connection.
2. **Server-sent events,** when the request has `Accept: text/event-stream`: each record is one event whose `data` is the record line and whose `id` is its seq. A browser's `EventSource` reconnects with `Last-Event-ID`, which the server treats as `since`, so reconnection is gap-free without client code. Idle keepalives are SSE comments.
3. **Long poll,** with `wait=<seconds>`: the server answers as soon as at least one record is available, or after `<seconds>` with an empty body, and then ends the response. The client asks again with `since` set to the last seq it saw. This suits clients and proxies that buffer or cut long responses.

In every form, `reset` is the last record: the server ends the response after it, and the client re-reads the index and reopens with `since`. A `since` the server can no longer serve, or one after the current seq, gets `reset` at once. Coalescing, falling behind, and bootstrapping with an index follow 9p-dotfs.md; the gap-free bootstrap is `GET .fs/index` (seq N from `Dotfs-Seq`), then `GET .fs/events?since=N`.

## Extension 3: path operations

`POST .fs/ctl` takes ctl commands in a `text/plain` body and answers with their replies in a `text/plain` body. The command and reply lines are exactly those of 9p-dotfs.md, including `stat`, `caps`, and the conditional forms with bases.

```
POST /s/7f3k/.fs/ctl
Content-Type: text/plain

mv /Draft.md /Posts/Final.md 48100 0
```

```
200 OK
Content-Type: text/plain

ok 48214
```

- The status is `200` whenever the commands were read and run, whatever their reply words, so a client acts on the reply line as it would over 9P. Only transport, authentication, and size failures (`401`, `413`, and `5xx`) use other codes.
- **Several commands.** A body may hold several commands, one per line. They run in order, each on its own, as if sent on one ctl fid one after another, and the body holds one reply line per command in the same order. They are not a transaction: a `conflict` on one does not stop the rest. This is the HTTP form of keeping several ctl fids busy, without a round trip per command.
- `hello` is accepted for symmetry but applies only to that request; clients send `Dotfs-Device` instead.

## Extension 4: staged commit

A stage file is the resource `.fs/stage/<name>`, private to the authenticated user.

| Request | 9P equivalent | Response |
|---|---|---|
| `PUT .fs/stage/<name>` with `Content-Range: bytes <first>-<last>/*` | write at offset `<first>` | `204`, with `Dotfs-Stage-Size: <bytes>` |
| `PUT .fs/stage/<name>` without `Content-Range` | create or truncate, then write from offset 0 | `204`, with `Dotfs-Stage-Size` |
| `HEAD .fs/stage/<name>` | stat | `Content-Length` is the stage file's size, where an upload written in order resumes; `404` if it is gone |
| `DELETE .fs/stage/<name>` | `abort <name>` | `204` |

Publishing is the `commit` command on `POST .fs/ctl`, exactly as in 9p-dotfs.md:

```
commit 6f1c… /Videos/trip.mp4 0 1763491250 0644 hash=b3:41aa…
```

and so is `commit-hash`, which publishes content the server already holds with no upload. Stage files a server has removed for inactivity are `404` on `HEAD` and `missing` on `commit`.

A file of any size can be uploaded in one request with a conditional `PUT` on its path (below); staging is for uploads that must survive a dropped connection, and for content larger than an intermediary accepts in one body.

## Plain files

Plain requests on paths are the HTTP form of plain 9P access, with one difference: a `PUT` can be conditional, which makes it a one-request `commit`.

### Reading

- `GET <path>` on a file returns its content, with `ETag` (the version), `Last-Modified` (the mtime), `Dotfs-Entry`, and `Dotfs-Seq`. `Range` requests must be supported, for resumable and partial reads. `If-None-Match` with the current version gets `304`.
- `HEAD <path>` is `stat`: the same headers with no body.
- `GET <path>` on a directory returns `text/plain` in index format: the `fsindex 1 seq <N>` header, the directory's own line, then the lines of its immediate children. With `?depth=infinity` it returns every path below the directory, which is the scope's index restricted to that subtree. A client may ask for `/` this way when it cannot read `.fs/index`.
- `GET <path>` on a symlink returns `200` with an empty body and the link in `Dotfs-Entry`; servers do not follow symlinks.

### Writing

`PUT <path>` with the content as the body publishes it at the path, atomically, as one change.

| Preconditions | 9P equivalent | On a stale base |
|---|---|---|
| `If-Match: "<version>"` | `commit … <version> …` | `409` with the current version |
| `If-None-Match: *` | `commit … 0 …` | `409` with the current version |
| none | a plain write, last writer wins | (never) |

- The response is `201 Created` for a new path and `200 OK` otherwise, with the new `ETag`, `Dotfs-Entry`, and `Dotfs-Seq`. The ordering guarantee holds: the change's event record is assigned before the response is sent, and carries the version in `ETag`, so a client recognizes its own change in the stream.
- `Dotfs-Mode`, `Dotfs-Mtime`, and `Dotfs-Hash` set the mode, mtime, and expected hash, as `commit` does. A hash mismatch is `422` and changes nothing.
- A server may require a precondition on every `PUT` and answer one without it with `428 Precondition Required`; a server that offers staged commit should accept unconditional ones, as it accepts plain 9P writes.
- Parent directories are never created: a missing parent is `404`. Directories are made with `mkdir` on ctl.
- `PUT` cannot replace a directory (`400`), and cannot create symlinks.

### Removing

`DELETE <path>` removes a file, symlink, or empty directory: it is `rm <path> [<base>]`, with the base from `If-Match`. The response is `204` with `Dotfs-Seq` set to the seq of the `del` record, or `409`, `404`, or `400` (for a non-empty directory). Removing a tree is `rmtree` on ctl, so a stray `DELETE` cannot remove one.

Moves, copies, directories, and flags have no plain request; they are ctl commands. HTTP's `MOVE`, `COPY`, and `MKCOL` (WebDAV) are deliberately not used, so http-dotfs needs only the methods every HTTP client and proxy supports.

## Example session

```sh
B=https://host/s/7f3k
H='Authorization: Bearer tok_…'

curl -sH "$H" --compressed $B/.fs/index           # whole tree; note Dotfs-Seq
curl -sH "$H" $B/Notes/todo.md -D -                # content, ETag: "48212"

curl -sH "$H" -X PUT -H 'If-Match: "48212"' \
     --data-binary @todo.md $B/Notes/todo.md        # 200 and ETag: "48230", or 409

curl -sH "$H" -X PUT -H 'If-None-Match: *' \
     --data-binary @new.md $B/Notes/new.md          # 201, or 409 if someone made it first

curl -sH "$H" -d 'mv /Notes/new.md /Archive/new.md - 0' $B/.fs/ctl   # ok 48232

curl -sNH "$H" "$B/.fs/events?since=48232"         # follow changes
```

## Profiles

The profiles of 9p-dotfs.md apply unchanged. A read-only cache can be served by any static file server that also serves a generated `.fs/index`, since plain `GET` with `Range` and an index file are all the read-only profile needs over HTTP.

A sync client or on-demand mount can run over http-dotfs alone: bootstrap with `GET .fs/index`, follow `GET .fs/events`, fetch with `GET` (several in parallel), and write with conditional `PUT`, `DELETE`, and ctl. Small new files are one request each (`PUT` with `If-None-Match: *`), where 9P needs up to four exchanges.

## Security considerations

Everything in 9p-dotfs.md applies. In addition:

- **Authentication.** Use `Authorization: Bearer` on every request; a server may also accept a cookie. A server that accepts a cookie must protect state-changing requests from cross-site use: require a header a cross-site form cannot send (`Dotfs-Device` or a `Content-Type` other than a form type) and set `SameSite` on the cookie.
- **CORS.** A server that serves browser clients from other origins must list them; `Access-Control-Expose-Headers` must include `ETag` and the `Dotfs-` headers, or a browser client cannot see versions.
- **Caching.** Responses for an authenticated user must not be stored by shared caches (`Cache-Control: private`), and versions in `ETag` let private caches revalidate cheaply.
- **Paths.** Decode the URL path once, reject `.`, `..`, and empty segments, and resolve the result inside the scope, exactly as for ctl paths.
- **Limits.** Cap body sizes for `PUT` and `POST .fs/ctl`, the number of commands in one ctl body, and open event streams per user, answering `413` or `429`.
- **Read-only scopes.** A scope shared read-only answers every change (`PUT`, `DELETE`, stage writes, and changing ctl commands) with `denied` (`403`), while reads, the index, events, and the `caps` and `stat` commands still work.

## Open questions

- [ ] Should a directory `GET` also offer HTML or JSON when a browser or a JSON client asks for it, or stay text only?
- [ ] Should there be a JSON form of index lines and records for clients that would rather not parse the line format?
- [ ] Is one ctl body with several commands enough batching, or should index-shaped batch uploads (many small files in one request) be defined?
