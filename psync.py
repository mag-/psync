#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["zstandard>=0.22", "xxhash>=3.4"]
# ///
"""psync - fast Python rsync clone with rolling checksums and zstd compression"""
from __future__ import annotations
import sys, os, stat, mmap, struct, json, fnmatch, shlex, time
from pathlib import Path
from typing import NamedTuple, Union
from dataclasses import dataclass, field
from enum import IntEnum
from collections import defaultdict
from contextlib import contextmanager
from subprocess import Popen, PIPE
from concurrent.futures import ThreadPoolExecutor
import xxhash
import zstandard as zstd

# === Progress Bar (tinygrad-style) ===
class Progress:
    """Minimal tinygrad-style progress display"""
    W = 40  # bar width
    def __init__(self, total: int, desc: str = "", unit: str = "B", enabled: bool = True):
        self.total, self.desc, self.unit, self.enabled = total, desc, unit, enabled
        self.n, self.start = 0, time.perf_counter()

    def update(self, n: int = 1):
        self.n += n
        if not self.enabled: return
        self._draw()

    def _fmt_size(self, n: int) -> str:
        for u in ['', 'K', 'M', 'G', 'T']:
            if abs(n) < 1024: return f"{n:6.1f}{u}{self.unit}"
            n /= 1024
        return f"{n:.1f}P{self.unit}"

    def _fmt_rate(self, bps: float) -> str:
        return self._fmt_size(int(bps)) + "/s"

    def _draw(self):
        pct = self.n / max(self.total, 1)
        filled = int(self.W * pct)
        bar = "\033[32m" + "━" * filled + "\033[90m" + "━" * (self.W - filled) + "\033[0m"
        elapsed = time.perf_counter() - self.start
        rate = self.n / max(elapsed, 0.001)
        eta = (self.total - self.n) / max(rate, 1)
        desc = f"{self.desc[:20]:<20}" if self.desc else ""
        stat = f"{self._fmt_size(self.n)}/{self._fmt_size(self.total)} {self._fmt_rate(rate)} eta {eta:5.1f}s"
        print(f"\r{desc} {bar} {pct*100:5.1f}% {stat}", end="", flush=True, file=sys.stderr)

    def close(self):
        if self.enabled: print(file=sys.stderr)

    def __enter__(self): return self
    def __exit__(self, *_): self.close()

def tqdm(it, total=None, desc="", unit="", enabled=True):
    """tinygrad-style iterator wrapper"""
    total = total or len(it) if hasattr(it, '__len__') else 0
    with Progress(total, desc, unit, enabled) as p:
        for x in it:
            yield x
            p.update(1)

# === Constants & Types ===
M16 = 1 << 16
Sig = NamedTuple('Sig', [('weak', int), ('strong', bytes)])
Delta = Union[int, bytes]  # block index or literal bytes

def blk_size(sz: int) -> int:
    """Adaptive block sizing based on file size"""
    if sz < 128<<10: return 0          # <128KB: whole file
    if sz < 16<<20: return 128<<10     # <16MB: 128KB blocks
    if sz < 256<<20: return 1<<20      # <256MB: 1MB blocks
    if sz < 4<<30: return 16<<20       # <4GB: 16MB blocks
    if sz < 64<<30: return 128<<20     # <64GB: 128MB blocks
    return 1<<30                        # >=64GB: 1GB blocks

# === Weak Hash (xxhash32 - C-implemented, ~10GB/s) ===
def weak_hash(data: bytes | memoryview) -> int:
    """Fast 32-bit hash for block matching. Not rollable but very fast."""
    if isinstance(data, memoryview): data = bytes(data)
    return xxhash.xxh32_intdigest(data)

# === Strong Hash (xxh3_128) ===
def strong(data: bytes | memoryview) -> bytes:
    """128-bit xxhash - extremely fast (~30GB/s)"""
    return xxhash.xxh3_128_digest(data)

# === mmap Helpers ===
@contextmanager
def mmap_read(path: Path):
    """Memory-map file for reading, yields memoryview or empty bytes"""
    if not path.exists() or path.stat().st_size == 0:
        yield b''
        return
    with open(path, 'rb') as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        try: yield mm
        finally: mm.close()

# === Signatures & Delta ===
def signatures(data: bytes | memoryview, bs: int) -> list[Sig]:
    """Generate block signatures for basis file using xxhash (10GB/s)"""
    if not data or bs == 0: return []
    return [Sig(weak_hash(data[i:i+bs]), strong(data[i:i+bs]))
            for i in range(0, len(data), bs)]

def delta(src: bytes | memoryview, sigs: list[Sig], bs: int) -> list[Delta]:
    """Generate delta using fast block-boundary matching.

    Simple and fast: only check at block boundaries.
    Unmatched blocks sent as literal. No byte-by-byte search."""
    if not src: return []
    if not sigs or bs == 0: return [bytes(src)]

    # Build lookup: weak -> [(strong, idx), ...]
    lookup: dict[int, list[tuple[bytes, int]]] = defaultdict(list)
    for i, s in enumerate(sigs): lookup[s.weak].append((s.strong, i))

    result: list[Delta] = []
    length = len(src)

    # Process block by block
    for pos in range(0, length, bs):
        block = src[pos:pos+bs]
        if len(block) < bs:
            # Final partial block - send as literal
            result.append(bytes(block))
            break

        weak = weak_hash(block)
        matched = False
        if weak in lookup:
            sh = strong(block)
            for sig_strong, idx in lookup[weak]:
                if sig_strong == sh:
                    result.append(idx)
                    matched = True
                    break

        if not matched:
            result.append(bytes(block))

    return result

def patch(basis: bytes | memoryview, deltas: list[Delta], bs: int) -> bytes:
    """Reconstruct file from basis and delta"""
    out = bytearray()
    for d in deltas:
        if isinstance(d, int):
            start = d * bs
            out.extend(basis[start:start+bs])
        else:
            out.extend(d)
    return bytes(out)

# === Wire Protocol ===
class M(IntEnum):
    HELLO=0; FILES=1; NEED=2; SIGS=3; DELTA=4; DATA=5; DEL=6; DONE=7; ERR=8

PROTO_VER = 1
COMPRESS_THRESH = 512

# Compression (multi-threaded zstd)
_cctx = zstd.ZstdCompressor(level=3, threads=-1)
_dctx = zstd.ZstdDecompressor()

def enc(typ: M, payload: bytes, compress: bool = True) -> bytes:
    """Encode message: [type:1][flags:1][len:4][payload]"""
    flags = 0
    if compress and len(payload) > COMPRESS_THRESH:
        payload = _cctx.compress(payload)
        flags |= 1
    return struct.pack('>BBL', typ, flags, len(payload)) + payload

def dec(stream) -> tuple[M, bytes]:
    """Decode message from stream"""
    header = stream.read(6)
    if len(header) < 6: raise EOFError("Connection closed")
    typ, flags, length = struct.unpack('>BBL', header)
    payload = stream.read(length)
    if len(payload) < length: raise EOFError("Incomplete message")
    if flags & 1:
        payload = _dctx.decompress(payload)
    return M(typ), payload

def enc_sigs(sigs: list[Sig], bs: int) -> bytes:
    """Encode signatures: [bs:4][count:4][(weak:4 + strong:16) * count]"""
    buf = struct.pack('>II', bs, len(sigs))
    for s in sigs:
        buf += struct.pack('>I', s.weak) + s.strong
    return buf

def dec_sigs(data: bytes) -> tuple[int, list[Sig]]:
    """Decode signatures, returns (block_size, sigs)"""
    bs, count = struct.unpack('>II', data[:8])
    sigs = []
    off = 8
    for _ in range(count):
        weak = struct.unpack('>I', data[off:off+4])[0]
        strong = data[off+4:off+20]
        sigs.append(Sig(weak, strong))
        off += 20
    return bs, sigs

def enc_delta(deltas: list[Delta]) -> bytes:
    """Encode delta: each entry is [type:1][data...]"""
    buf = b''
    for d in deltas:
        if isinstance(d, int):
            buf += struct.pack('>BI', 0, d)  # block ref
        else:
            buf += struct.pack('>BI', 1, len(d)) + d  # literal
    return buf

def dec_delta(data: bytes) -> list[Delta]:
    """Decode delta"""
    result = []
    off = 0
    while off < len(data):
        typ = data[off]
        if typ == 0:  # block ref
            idx = struct.unpack('>I', data[off+1:off+5])[0]
            result.append(idx)
            off += 5
        else:  # literal
            length = struct.unpack('>I', data[off+1:off+5])[0]
            result.append(data[off+5:off+5+length])
            off += 5 + length
    return result

# === File Metadata ===
@dataclass(slots=True)
class FileMeta:
    path: str
    size: int
    mtime: float
    mode: int
    is_dir: bool = False
    is_link: bool = False
    link_target: str = ""

    def to_dict(self) -> dict:
        d = {'p': self.path, 's': self.size, 'm': self.mtime, 'o': self.mode}
        if self.is_dir: d['d'] = 1
        if self.is_link: d['l'] = self.link_target
        return d

    @staticmethod
    def from_dict(d: dict) -> FileMeta:
        return FileMeta(
            path=d['p'], size=d['s'], mtime=d['m'], mode=d['o'],
            is_dir=bool(d.get('d')), is_link='l' in d, link_target=d.get('l', '')
        )

    @staticmethod
    def from_path(p: Path, base: Path) -> FileMeta:
        s = p.lstat()
        is_link = stat.S_ISLNK(s.st_mode)
        return FileMeta(
            path=str(p.relative_to(base)),
            size=0 if is_link else s.st_size,
            mtime=s.st_mtime,
            mode=s.st_mode,
            is_dir=stat.S_ISDIR(s.st_mode) and not is_link,
            is_link=is_link,
            link_target=os.readlink(p) if is_link else ""
        )

def walk(root: Path, recursive: bool, exclude: list[str]) -> list[FileMeta]:
    """Walk directory tree, return file metadata list"""
    def excluded(p: Path) -> bool:
        return any(fnmatch.fnmatch(p.name, pat) or fnmatch.fnmatch(str(p), pat) for pat in exclude)

    result = []
    if root.is_file() or root.is_symlink():
        if not excluded(root):
            result.append(FileMeta.from_path(root, root.parent))
    elif root.is_dir():
        items = sorted(root.rglob('*') if recursive else root.glob('*'))
        for p in items:
            if not excluded(p):
                result.append(FileMeta.from_path(p, root))
    return result

def needs_sync(src: FileMeta, dst: FileMeta | None, checksum: bool) -> str:
    """Determine sync action: 'skip', 'data' (whole file), 'delta'"""
    if dst is None: return 'data'
    if src.is_dir: return 'skip' if dst.is_dir else 'data'
    if src.is_link: return 'skip' if dst.is_link and src.link_target == dst.link_target else 'data'
    if src.size != dst.size: return 'data'
    if checksum: return 'delta'
    if src.mtime <= dst.mtime: return 'skip'
    return 'delta'

# === CLI Parsing ===
@dataclass
class Args:
    src: str = ''
    dst: str = ''
    archive: bool = False
    verbose: int = 0
    compress: bool = False
    recursive: bool = False
    delete: bool = False
    exclude: list[str] = field(default_factory=list)
    progress: bool = False
    dry_run: bool = False
    checksum: bool = False
    update: bool = False
    server: bool = False
    pipe_out: bool = False
    stats: bool = False

    def __post_init__(self):
        if self.archive:
            self.recursive = True

def parse(argv: list[str]) -> Args:
    args = Args()
    positional = []
    i = 0
    while i < len(argv):
        a = argv[i]
        if a in ('-a', '--archive'): args.archive = True
        elif a in ('-v', '--verbose'): args.verbose += 1
        elif a in ('-z', '--compress'): args.compress = True
        elif a in ('-r', '--recursive'): args.recursive = True
        elif a == '--delete': args.delete = True
        elif a.startswith('--exclude='):
            args.exclude.append(a.split('=', 1)[1])
        elif a == '--exclude':
            i += 1; args.exclude.append(argv[i])
        elif a == '--progress': args.progress = True
        elif a in ('-n', '--dry-run'): args.dry_run = True
        elif a in ('-c', '--checksum'): args.checksum = True
        elif a in ('-u', '--update'): args.update = True
        elif a == '--server': args.server = True
        elif a == '--pipe-out': args.pipe_out = True
        elif a == '--stats': args.stats = True
        elif a.startswith('-') and not a.startswith('--') and len(a) > 1:
            for c in a[1:]:
                if c == 'a': args.archive = True
                elif c == 'v': args.verbose += 1
                elif c == 'z': args.compress = True
                elif c == 'r': args.recursive = True
                elif c == 'n': args.dry_run = True
                elif c == 'c': args.checksum = True
                elif c == 'u': args.update = True
        else:
            positional.append(a)
        i += 1

    if len(positional) >= 2:
        args.src, args.dst = positional[0], positional[1]
    elif len(positional) == 1:
        args.src = positional[0]

    args.__post_init__()
    return args

# === Transport ===
class Transport:
    """Bidirectional transport over file handles with byte counting"""
    def __init__(self, stdin, stdout, compress: bool = True):
        self.stdin, self.stdout, self.compress = stdin, stdout, compress
        self.bytes_sent = 0
        self.bytes_recv = 0

    def send(self, typ: M, payload: bytes):
        data = enc(typ, payload, self.compress)
        self.bytes_sent += len(data)
        self.stdout.write(data)
        self.stdout.flush()

    def recv(self) -> tuple[M, bytes]:
        typ, payload = dec(self.stdin)
        # Estimate received bytes (header + payload after decompression)
        self.bytes_recv += 6 + len(payload)  # approximate
        return typ, payload

class SSHTransport(Transport):
    """SSH transport that bootstraps psync on remote"""
    def __init__(self, host: str, remote_path: str, compress: bool):
        import base64
        script = Path(__file__).read_bytes()
        script_b64 = base64.b64encode(script).decode()
        # Upload script via base64 (avoids stdin/EOF issues), then run server mode
        remote_cmd = f'echo {script_b64} | base64 -d > /tmp/_psync.py && uv run /tmp/_psync.py --server {shlex.quote(remote_path)}'
        cmd = ['ssh', '-o', 'Compression=no', host, remote_cmd]
        self.proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        super().__init__(self.proc.stdout, self.proc.stdin, compress)

    def close(self):
        self.proc.stdin.close()
        self.proc.stdout.close()
        self.proc.wait()

# === Sender ===
class Sender:
    def __init__(self, args: Args, transport: Transport, root: Path):
        self.args, self.tr, self.root = args, transport, root
        self.files = walk(root, args.recursive, args.exclude)

    def sync(self):
        # 1. Hello
        self.tr.send(M.HELLO, struct.pack('>I', PROTO_VER))
        typ, payload = self.tr.recv()
        if typ != M.HELLO: raise RuntimeError(f"Expected HELLO, got {typ}")

        # 2. Send file list
        file_dicts = [f.to_dict() for f in self.files]
        self.tr.send(M.FILES, json.dumps(file_dicts).encode())

        # 3. Receive NEED list
        typ, payload = self.tr.recv()
        if typ != M.NEED: raise RuntimeError(f"Expected NEED, got {typ}")
        need = json.loads(payload.decode())  # {'delta': [...], 'data': [...]}

        # 4. Process files needing delta
        for path in need.get('delta', []):
            fm = next(f for f in self.files if f.path == path)
            typ, payload = self.tr.recv()
            if typ != M.SIGS: raise RuntimeError(f"Expected SIGS for {path}")
            bs, sigs = dec_sigs(payload)

            with mmap_read(self.root / path) as mm:
                d = delta(mm, sigs, bs)
            self.tr.send(M.DELTA, enc_delta(d))
            if self.args.verbose: print(f"  delta: {path}")

        # 5. Process files needing full data
        for path in need.get('data', []):
            fm = next(f for f in self.files if f.path == path)
            if fm.is_dir or fm.is_link:
                self.tr.send(M.DATA, json.dumps(fm.to_dict()).encode())
            else:
                data = (self.root / path).read_bytes()
                self.tr.send(M.DATA, data)
            if self.args.verbose: print(f"  data: {path}")

        # 6. Delete phase
        if self.args.delete:
            self.tr.send(M.DEL, json.dumps(need.get('delete', [])).encode())

        # 7. Done
        self.tr.send(M.DONE, b'')
        typ, _ = self.tr.recv()
        if typ != M.DONE: raise RuntimeError(f"Expected DONE, got {typ}")

# === Receiver ===
class Receiver:
    def __init__(self, args: Args, transport: Transport, root: Path):
        self.args, self.tr, self.root = args, transport, root
        self.root.mkdir(parents=True, exist_ok=True)

    def serve(self):
        # 1. Hello
        typ, payload = self.tr.recv()
        if typ != M.HELLO: raise RuntimeError(f"Expected HELLO, got {typ}")
        self.tr.send(M.HELLO, struct.pack('>I', PROTO_VER))

        # 2. Receive file list
        typ, payload = self.tr.recv()
        if typ != M.FILES: raise RuntimeError(f"Expected FILES, got {typ}")
        src_files = [FileMeta.from_dict(d) for d in json.loads(payload.decode())]

        # Build local file map
        local = {f.path: f for f in walk(self.root, True, [])}

        # 3. Determine what we need
        need_delta, need_data, to_delete = [], [], []
        for sf in src_files:
            dst = local.get(sf.path)
            action = needs_sync(sf, dst, self.args.checksum)
            if action == 'delta': need_delta.append(sf.path)
            elif action == 'data': need_data.append(sf.path)

        # Files to delete (in dst but not in src)
        if self.args.delete:
            src_paths = {f.path for f in src_files}
            to_delete = [p for p in local if p not in src_paths]

        self.tr.send(M.NEED, json.dumps({
            'delta': need_delta, 'data': need_data, 'delete': to_delete
        }).encode())

        # 4. Process delta files
        for path in need_delta:
            dst_path = self.root / path
            bs = blk_size(dst_path.stat().st_size if dst_path.exists() else 0)
            if bs == 0: bs = 128 << 10  # default for small files

            with mmap_read(dst_path) as mm:
                sigs = signatures(mm, bs)
            self.tr.send(M.SIGS, enc_sigs(sigs, bs))

            typ, payload = self.tr.recv()
            if typ != M.DELTA: raise RuntimeError(f"Expected DELTA for {path}")
            deltas = dec_delta(payload)

            with mmap_read(dst_path) as mm:
                new_data = patch(mm, deltas, bs)

            dst_path.parent.mkdir(parents=True, exist_ok=True)
            dst_path.write_bytes(new_data)
            if self.args.verbose: print(f"  patched: {path}", file=sys.stderr)

        # 5. Process full data files
        for path in need_data:
            typ, payload = self.tr.recv()
            if typ != M.DATA: raise RuntimeError(f"Expected DATA for {path}")

            dst_path = self.root / path
            # Check if it's metadata for dir/symlink
            try:
                meta = json.loads(payload.decode('utf-8'))
                fm = FileMeta.from_dict(meta)
                if fm.is_dir:
                    dst_path.mkdir(parents=True, exist_ok=True)
                elif fm.is_link:
                    dst_path.parent.mkdir(parents=True, exist_ok=True)
                    if dst_path.exists() or dst_path.is_symlink():
                        dst_path.unlink()
                    dst_path.symlink_to(fm.link_target)
            except (json.JSONDecodeError, KeyError, UnicodeDecodeError):
                # Regular file data (binary)
                dst_path.parent.mkdir(parents=True, exist_ok=True)
                dst_path.write_bytes(payload)
            if self.args.verbose: print(f"  wrote: {path}", file=sys.stderr)

        # 6. Delete phase
        if self.args.delete:
            typ, payload = self.tr.recv()
            if typ == M.DEL:
                paths = json.loads(payload.decode())
                for p in sorted(paths, reverse=True):  # delete deepest first
                    dp = self.root / p
                    if dp.is_dir(): dp.rmdir()
                    elif dp.exists(): dp.unlink()
                    if self.args.verbose: print(f"  deleted: {p}", file=sys.stderr)

        # 7. Done
        typ, _ = self.tr.recv()
        if typ != M.DONE: raise RuntimeError(f"Expected DONE, got {typ}")
        self.tr.send(M.DONE, b'')

# === Local Sync ===
def local_sync(args: Args):
    """Sync locally without network"""
    src_root, dst_root = Path(args.src.rstrip('/')), Path(args.dst.rstrip('/'))
    src_files = walk(src_root, args.recursive, args.exclude)
    dst_root.mkdir(parents=True, exist_ok=True)
    local = {f.path: f for f in walk(dst_root, True, [])}

    # Calculate total bytes to sync
    to_sync = [(sf, needs_sync(sf, local.get(sf.path), args.checksum))
               for sf in src_files]
    total_bytes = sum(sf.size for sf, action in to_sync if action != 'skip' and not sf.is_dir)

    with Progress(total_bytes, "syncing", enabled=args.progress) as prog:
        for sf, action in to_sync:
            src_path = src_root / sf.path
            dst_path = dst_root / sf.path

            if action == 'skip':
                if args.verbose > 1: print(f"\033[90mskip: {sf.path}\033[0m", file=sys.stderr)
                continue

            if args.dry_run:
                print(f"would sync: {sf.path}")
                continue

            if sf.is_dir:
                dst_path.mkdir(parents=True, exist_ok=True)
            elif sf.is_link:
                dst_path.parent.mkdir(parents=True, exist_ok=True)
                if dst_path.exists() or dst_path.is_symlink(): dst_path.unlink()
                dst_path.symlink_to(sf.link_target)
            elif action == 'data':
                dst_path.parent.mkdir(parents=True, exist_ok=True)
                dst_path.write_bytes(src_path.read_bytes())
                prog.update(sf.size)
            elif action == 'delta':
                bs = blk_size(sf.size)
                if bs == 0:
                    dst_path.parent.mkdir(parents=True, exist_ok=True)
                    dst_path.write_bytes(src_path.read_bytes())
                else:
                    with mmap_read(dst_path) as basis_mm:
                        sigs = signatures(basis_mm, bs)
                    with mmap_read(src_path) as src_mm:
                        d = delta(src_mm, sigs, bs)
                    with mmap_read(dst_path) as basis_mm:
                        new_data = patch(basis_mm, d, bs)
                    dst_path.write_bytes(new_data)
                prog.update(sf.size)

            if args.verbose and not args.progress:
                print(f"\033[32m✓\033[0m {sf.path}", file=sys.stderr)

    # Delete phase
    if args.delete:
        src_paths = {f.path for f in src_files}
        for p in sorted(local.keys(), reverse=True):
            if p not in src_paths:
                dp = dst_root / p
                if args.dry_run:
                    print(f"would delete: {p}")
                else:
                    if dp.is_dir(): dp.rmdir()
                    else: dp.unlink()
                    if args.verbose: print(f"deleted: {p}")

# === Main ===
def main():
    args = parse(sys.argv[1:])

    if args.server:
        # Server mode: receive from stdin, send to stdout
        dst = Path(args.src) if args.src else Path('.')
        tr = Transport(sys.stdin.buffer, sys.stdout.buffer, args.compress)
        Receiver(args, tr, dst).serve()
    elif args.pipe_out:
        # Pipe mode for testing: send to stdout
        src = Path(args.src.rstrip('/'))
        tr = Transport(sys.stdin.buffer, sys.stdout.buffer, args.compress)
        Sender(args, tr, src).sync()
    elif ':' in args.dst:
        # Remote sync
        host, remote_path = args.dst.split(':', 1)
        tr = SSHTransport(host, remote_path, args.compress)
        try:
            src = Path(args.src.rstrip('/'))
            Sender(args, tr, src).sync()
        finally:
            tr.close()
        if args.stats:
            print(f"\nTotal bytes sent: {tr.bytes_sent:,}", file=sys.stderr)
            print(f"Total bytes received: {tr.bytes_recv:,}", file=sys.stderr)
    else:
        # Local sync
        local_sync(args)

if __name__ == '__main__':
    main()
