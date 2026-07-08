# shard

Native concurrent HTTP range download engine in Rust. No `aria2c` subprocesses,
no per-chunk temp files, no merge step: a worker pool pulls byte ranges in
parallel and writes them straight into the final file at their own offsets.

## Features

- Parallel range downloads over a dynamic worker pool (single-stream fallback
  when the server does not support ranges).
- Positional writes: one file handle per worker, `pwrite` into a preallocated
  sparse file.
- Resumable downloads via a versioned `.shard` sidecar manifest with
  crash-safe checkpointing and remote identity validation on resume.
- Cooperative pause / cancel over a per-download Unix control socket.
- Automatic retries with exponential backoff and jitter.
- Whole-file SHA-256 verification.
- Filetype routing: without `-o`, files land in `download_dir/<category>/`
  by extension (`Videos`, `Images`, `Audio`, `Archives`, `Documents`, `Other`).
- Clipboard fallback: run `shard download` with no URL and it pulls the URL
  from the clipboard (`wl-paste` / `xclip` / `xsel`).
- SQLite download history with `shard history` and `shard redo <id|url>`.
- Live progress bar with a per-worker chunk view.

## Install

Requires a Rust toolchain (2024 edition).

```sh
cargo install --path .
```

## Usage

```sh
# download, routed into the download dir (default ~/Downloads)
shard download <url>

# save explicitly to a path or directory
shard download <url> --output /path/to/dir

# from clipboard: no URL argument
shard download

# resume / redo a previous download
shard resume <id>
shard redo <id|url>

# control a running download
shard list
shard status <id>
shard pause <id>
shard resume <id>
shard cancel <id>

# history of past downloads
shard history

# configuration
shard config show
shard config init
shard config set <key> <value>
shard config edit
shard config keys
```

### Options

| Flag | Meaning |
| --- | --- |
| `-c, --connections N` | concurrent workers (default 8) |
| `-o, --output PATH` | file or directory output (default: routed download dir) |
| `--chunk-size N` | range chunk size in bytes (default 8 MiB) |
| `--max-attempts N` | per-chunk retry attempts (default 5) |
| `--color auto\|always\|never` | colorize output |
| `--eyecandy [true\|false]` | prefer Nerd Font glyphs over TTY-safe hash bars |

## Configuration

TOML config at `$XDG_CONFIG_HOME/shard/shard.conf`
(or `~/.config/shard/shard.conf`):

```toml
[style]
color = "auto"
prefer-eyecandy = false

[download]
connections = 8
chunk_size = 8388608
max_attempts = 5
retry_base_ms = 500
retry_max_ms = 30000
checkpoint_ms = 3000
resume = true
download_dir = "~/Downloads"

[filetype]
video = "Videos"
image = "Images"
audio = "Audio"
archive = "Archives"
document = "Documents"
other = "Other"
```

`~` is expanded in `download_dir`. When `-o` is omitted the final file is
placed into `<download_dir>/<category>/<name>` based on its extension.

## How it works

1. Preflight: follow redirects, read content length, probe `GET bytes=0-0`
   for `206` + `Content-Range` to confirm range support and capture
   `ETag` / `Last-Modified`.
2. Plan chunk layout from the resolved size and configured chunk size.
3. Dispatch chunks to a worker pool; each worker owns a file handle and
   `pwrite`s at its chunk offset via `tokio::task::spawn_blocking`.
4. Progress events flow to the renderer; a `CheckpointGuard` flushes chunk
   state to `file.shard.tmp` -> fsync -> rename on an interval.
5. On completion: verify the whole-file SHA-256 and record the run in SQLite
   history.

Resume reconciles the manifest against the real local file and the re-resolved
remote identity before deciding how much to re-fetch.

## Layout

```
src/engine/       the download engine (metadata, planner, worker, writer, ...)
src/cli/          subcommands, renderer, control socket client
src/history.rs    SQLite history store
src/registry.rs   per-download entry registry (status, pid, control sockets)
src/config.rs     TOML configuration
```

## License

MIT. See `LICENSE`.