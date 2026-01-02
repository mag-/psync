"""
Tests with simulated slow network using pipes and pv (pipe viewer).

Simulates network conditions without SSH by running sender/receiver
as subprocesses connected via pipes, optionally rate-limited with pv.

Usage:
    pytest tests/test_slow_net.py -v
    pytest tests/test_slow_net.py -v -k "slow"  # only slow tests

Requirements:
    - pv (pipe viewer): apt install pv / brew install pv
"""
import subprocess
import tempfile
import shutil
import time
import os
import sys
from pathlib import Path
import pytest

# Check if pv is available
HAS_PV = shutil.which("pv") is not None

def run_sync_over_pipes(src_dir: Path, dst_dir: Path, rate_limit: str | None = None,
                        extra_args: list[str] = None) -> tuple[float, int, int]:
    """
    Run psync sender/receiver connected via pipes.

    Args:
        src_dir: Source directory
        dst_dir: Destination directory
        rate_limit: pv rate limit (e.g., "100k", "1m") or None for unlimited
        extra_args: Extra CLI args for psync

    Returns:
        (duration_seconds, bytes_sent, bytes_received)
    """
    psync = Path(__file__).parent.parent / "psync.py"
    extra_args = extra_args or []

    # Build command pipeline
    sender_cmd = [sys.executable, str(psync)] + extra_args + [str(src_dir) + "/", "--pipe-out"]
    receiver_cmd = [sys.executable, str(psync), "--server", str(dst_dir)]

    if rate_limit and HAS_PV:
        # sender | pv -L rate | receiver
        # We use shell for this pipeline
        cmd = f"{' '.join(sender_cmd)} | pv -q -L {rate_limit} | {' '.join(receiver_cmd)}"
        shell = True
    else:
        # Direct pipe without rate limiting
        # We'll do this manually with subprocess
        shell = False

    start = time.perf_counter()

    if shell:
        result = subprocess.run(cmd, shell=True, capture_output=True)
        bytes_sent = bytes_received = 0  # Can't easily measure with shell pipeline
    else:
        # Manual pipe: sender stdout -> receiver stdin
        receiver = subprocess.Popen(
            receiver_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        sender = subprocess.Popen(
            sender_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Pipe sender stdout to receiver stdin
        # This is a simple blocking implementation
        sender_out, sender_err = sender.communicate()
        receiver_out, receiver_err = receiver.communicate(input=sender_out)

        bytes_sent = len(sender_out)
        bytes_received = len(receiver_out) if receiver_out else 0

        if sender.returncode != 0:
            raise RuntimeError(f"Sender failed: {sender_err.decode()}")
        if receiver.returncode != 0:
            raise RuntimeError(f"Receiver failed: {receiver_err.decode()}")

    duration = time.perf_counter() - start
    return duration, bytes_sent, bytes_received


def compare_dirs(dir1: Path, dir2: Path) -> bool:
    """Compare two directories recursively, return True if identical."""
    import filecmp

    dcmp = filecmp.dircmp(dir1, dir2)

    if dcmp.left_only or dcmp.right_only or dcmp.diff_files:
        return False

    for subdir in dcmp.common_dirs:
        if not compare_dirs(dir1 / subdir, dir2 / subdir):
            return False

    return True


class TestPipeSync:
    """Test sync over pipes (no network, fast)"""

    def test_empty_dir(self, tmp_src, tmp_dst):
        """Sync empty directory"""
        # Just ensure it doesn't crash
        # Will need psync.py to exist first
        pass

    def test_single_small_file(self, tmp_src, tmp_dst, gen_file):
        """Sync single small file"""
        gen_file(tmp_src / "test.txt", 1024, "text")

        # Will run actual sync once psync.py exists
        # duration, sent, recv = run_sync_over_pipes(tmp_src, tmp_dst)
        # assert compare_dirs(tmp_src, tmp_dst)

    def test_multiple_files(self, tmp_src, tmp_dst, gen_file):
        """Sync multiple files of different sizes"""
        gen_file(tmp_src / "small.txt", 100, "text")
        gen_file(tmp_src / "medium.bin", 10000, "random")
        gen_file(tmp_src / "large.bin", 100000, "random")
        (tmp_src / "subdir").mkdir()
        gen_file(tmp_src / "subdir" / "nested.txt", 500, "text")

        # Will run actual sync once psync.py exists

    def test_delta_efficiency(self, tmp_src, tmp_dst, gen_file):
        """Modified file should transfer less than full size"""
        # Create identical file in both src and dst
        gen_file(tmp_src / "data.bin", 100000, "random", seed=42)
        gen_file(tmp_dst / "data.bin", 100000, "random", seed=42)

        # Modify 10% of src file
        data = bytearray((tmp_src / "data.bin").read_bytes())
        import random
        rng = random.Random(99)
        for i in range(45000, 55000):  # Modify 10KB in middle
            data[i] = rng.randint(0, 255)
        (tmp_src / "data.bin").write_bytes(bytes(data))

        # Sync should transfer much less than 100KB
        # Will verify once psync.py exists


@pytest.mark.skipif(not HAS_PV, reason="pv not installed")
class TestSlowNetwork:
    """Test with simulated slow network using pv rate limiting"""

    def test_slow_100kbps(self, tmp_src, tmp_dst, gen_file):
        """Sync at 100KB/s simulated speed"""
        gen_file(tmp_src / "test.bin", 50000, "random")  # 50KB file

        # At 100KB/s, should take ~0.5s
        # duration, _, _ = run_sync_over_pipes(tmp_src, tmp_dst, rate_limit="100k")
        # assert 0.4 < duration < 2.0  # Some tolerance

    def test_slow_with_compression(self, tmp_src, tmp_dst, gen_file):
        """Compression should help on slow network with compressible data"""
        gen_file(tmp_src / "test.txt", 100000, "text")  # 100KB text

        # Without compression
        # dur_uncompressed, _, _ = run_sync_over_pipes(
        #     tmp_src, tmp_dst, rate_limit="50k", extra_args=[]
        # )

        # Clear dst
        # shutil.rmtree(tmp_dst)
        # tmp_dst.mkdir()

        # With compression
        # dur_compressed, _, _ = run_sync_over_pipes(
        #     tmp_src, tmp_dst, rate_limit="50k", extra_args=["-z"]
        # )

        # Compressed should be faster for text
        # assert dur_compressed < dur_uncompressed * 0.8


class TestLargeFiles:
    """Test with larger files to verify mmap and block sizing"""

    @pytest.mark.slow
    def test_1mb_file(self, tmp_src, tmp_dst, gen_file):
        """Sync 1MB file"""
        gen_file(tmp_src / "large.bin", 1024 * 1024, "random")
        # Will run sync

    @pytest.mark.slow
    def test_10mb_file_delta(self, tmp_src, tmp_dst, gen_file):
        """10MB file with small modification should transfer little data"""
        # Create same file in both
        gen_file(tmp_src / "big.bin", 10 * 1024 * 1024, "random", seed=1)
        gen_file(tmp_dst / "big.bin", 10 * 1024 * 1024, "random", seed=1)

        # Modify 1% of src
        data = bytearray((tmp_src / "big.bin").read_bytes())
        import random
        rng = random.Random(99)
        start = 5 * 1024 * 1024  # Middle of file
        for i in range(start, start + 100 * 1024):  # 100KB change
            data[i] = rng.randint(0, 255)
        (tmp_src / "big.bin").write_bytes(bytes(data))

        # Should transfer << 10MB


class TestEdgeCases:
    """Edge cases and error handling"""

    def test_symlink(self, tmp_src, tmp_dst, gen_file):
        """Symlinks should be preserved"""
        gen_file(tmp_src / "target.txt", 100, "text")
        (tmp_src / "link.txt").symlink_to("target.txt")

    def test_empty_file(self, tmp_src, tmp_dst):
        """Empty files should sync correctly"""
        (tmp_src / "empty.txt").touch()

    def test_binary_filename(self, tmp_src, tmp_dst, gen_file):
        """Files with special characters in name"""
        gen_file(tmp_src / "file with spaces.txt", 100, "text")
        gen_file(tmp_src / "file-with-dashes.txt", 100, "text")

    def test_deep_nesting(self, tmp_src, tmp_dst, gen_file):
        """Deeply nested directory structure"""
        deep = tmp_src / "a" / "b" / "c" / "d" / "e"
        deep.mkdir(parents=True)
        gen_file(deep / "deep.txt", 100, "text")


# Utility for manual testing
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate test files")
    parser.add_argument("--size", type=int, default=1024*1024, help="File size in bytes")
    parser.add_argument("--pattern", choices=["random", "text", "zeros", "sparse"], default="random")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    import random
    rng = random.Random(42)

    with open(args.output, 'wb') as f:
        if args.pattern == "random":
            f.write(rng.randbytes(args.size))
        elif args.pattern == "zeros":
            f.write(b'\x00' * args.size)
        elif args.pattern == "text":
            chunk = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit.\n" * 100
            written = 0
            while written < args.size:
                to_write = min(len(chunk), args.size - written)
                f.write(chunk[:to_write])
                written += to_write
        elif args.pattern == "sparse":
            pos = 0
            while pos < args.size:
                if rng.random() < 0.1:
                    chunk_size = min(rng.randint(1024, 8192), args.size - pos)
                    f.write(rng.randbytes(chunk_size))
                else:
                    chunk_size = min(rng.randint(4096, 32768), args.size - pos)
                    f.write(b'\x00' * chunk_size)
                pos += chunk_size

    print(f"Generated {args.output} ({args.size} bytes, {args.pattern})")
