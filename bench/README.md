# bench

`go run ./bench` measures `dotfs serve` and `dotfs sync` against Syncthing on one machine, with every process on loopback. Results go in `docs/perf-log.md`.

```
go run ./bench -work /tmp/dotfs-bench -syncthing /path/to/syncthing -o results.jsonl
```

It generates a deterministic corpus (100,000 files and 2.5 GB by default; `-files`, `-bytes`, and `-seed` change it) shaped like a home folder: about 4,000 directories up to seven deep, file sizes log-normal around 4 KB, about 40% of the bytes in 2 to 64 MB photos and videos, text files that compress and media files that don't. Then, for each system in `-systems`, with devices A and B:

| Phase | What is timed |
| --- | --- |
| Seed | A takes in the corpus: dotfs uploads it to the server, and Syncthing scans and hashes it. Not comparable across systems, since dotfs moves the bytes here and Syncthing moves them in the bootstrap. |
| Bootstrap | B starts empty and gets the whole corpus. |
| Change to visible | One small file written on A until it is readable on B, 20 times, half a second to 1.5 s apart. |
| Small files | 10,000 files of 4 KiB written on A until all are on B. |
| Reconnect | B stops; each side makes 1,000 changes on its own files (a quarter each new files, appends, deletes, and renames); B starts again, timed until A and B have the same paths and sizes. |

For dotfs it also reads `.fs/index` plain and with `accept zstd`, and records the size of the server's and client's metadata and each process's peak RSS. Both systems settle local changes for `-debounce` (1 s, dotfs's default; Syncthing's `fsWatcherDelayS` is set to match, from its default of 10 s). Syncthing runs without discovery, relays, or NAT traversal, so its two devices talk directly over TLS.

When run as root on Linux it drops the page cache before the seed and the bootstrap, so both start cold.
