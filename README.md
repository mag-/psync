# psync

Fast Python rsync clone. Single file, runs anywhere with `uv`.

## Install

None needed. Just run:

```bash
uv run psync.py src/ dst/
```

## Usage

```bash
# Local sync
uv run psync.py -avz /data/ /backup/

# Remote sync (requires uv on remote host)
uv run psync.py -avz /data/ server:/backup/

# Common flags
-a, --archive     # recursive, preserve permissions/times/symlinks
-v, --verbose     # increase verbosity
-z, --compress    # compress during transfer (zstd)
-r, --recursive   # recurse into directories
-n, --dry-run     # show what would be transferred
-c, --checksum    # skip based on checksum, not mod-time
-u, --update      # skip files newer on receiver
--delete          # delete extraneous files from dest
--exclude=PAT     # exclude files matching pattern
--progress        # show progress during transfer
```

## How It Works

1. **Fast path**: If file size+mtime match, skip (no checksum needed)
2. **Rolling checksum**: Adler-32 variant finds matching blocks in O(1) per byte
3. **Strong hash**: xxh3_128 verifies block matches (~30GB/s)
4. **Delta transfer**: Only send changed blocks
5. **Compression**: Multi-threaded zstd on the wire

### Block Sizes

Adaptive based on file size for optimal performance:

| File Size | Block Size |
|-----------|------------|
| < 128KB   | whole file |
| < 16MB    | 128KB      |
| < 256MB   | 1MB        |
| < 4GB     | 16MB       |
| < 64GB    | 128MB      |
| >= 64GB   | 1GB        |

## Remote Execution

psync bootstraps itself on the remote host:

```bash
ssh host "cat > /tmp/psync.py && uv run /tmp/psync.py --server"
```

No installation required - just needs `uv` available.

## Performance

- **xxh3_128**: ~30GB/s hashing (not the bottleneck)
- **zstd**: Multi-threaded compression, adapts to CPU cores
- **mmap**: Zero-copy file I/O
- **Pipelining**: Overlap network I/O with computation

## Requirements

- Python 3.11+
- `uv` (for running)
- Remote: just `uv`

## License

MIT
