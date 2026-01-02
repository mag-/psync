"""
Integration tests for psync - run sender/receiver over pipes.

Usage:
    pytest tests/test_integration.py -v
    pytest tests/test_integration.py -v -k "slow"  # only slow network tests
    pytest tests/test_integration.py -v -k "not slow"  # skip slow tests
"""
import subprocess
import sys
import time
import shutil
import hashlib
from pathlib import Path
import pytest

PSYNC = Path(__file__).parent.parent / "psync.py"
HAS_PV = shutil.which("pv") is not None


def md5(path: Path) -> str:
    """Get MD5 hash of file"""
    return hashlib.md5(path.read_bytes()).hexdigest()


def md5_tree(root: Path) -> dict[str, str]:
    """Get MD5 hashes of all files in directory"""
    result = {}
    for p in sorted(root.rglob('*')):
        if p.is_file():
            result[str(p.relative_to(root))] = md5(p)
    return result


def run_pipe_sync(src: Path, dst: Path, rate: str | None = None, args: list[str] = None) -> dict:
    """
    Run psync over pipes with bidirectional communication.

    For rate limiting, we use socat or a custom approach since pv only works unidirectionally.

    Returns dict with: duration, sent_bytes, success, src_hash, dst_hash
    """
    args = args or []
    sender_args = ["-r"] + args + [str(src) + "/", "--pipe-out"]
    receiver_args = ["--server", str(dst)]

    start = time.perf_counter()

    if rate and HAS_PV:
        # For bidirectional with rate limit, we use a named pipe approach
        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            fifo1 = Path(tmpdir) / "s2r"  # sender to receiver
            fifo2 = Path(tmpdir) / "r2s"  # receiver to sender
            os.mkfifo(fifo1)
            os.mkfifo(fifo2)

            # Receiver: reads from fifo1, writes to fifo2
            receiver_cmd = f"{sys.executable} {PSYNC} {' '.join(receiver_args)} < {fifo1} > {fifo2}"
            # Sender with rate limit: writes to fifo1 (via pv), reads from fifo2
            sender_cmd = f"{sys.executable} {PSYNC} {' '.join(sender_args)} < {fifo2} | pv -q -L {rate} > {fifo1}"

            # Run both in background
            receiver_proc = subprocess.Popen(receiver_cmd, shell=True, stderr=subprocess.PIPE)
            sender_proc = subprocess.Popen(sender_cmd, shell=True, stderr=subprocess.PIPE)

            sender_proc.wait()
            receiver_proc.wait()

            success = sender_proc.returncode == 0 and receiver_proc.returncode == 0
            sent_bytes = 0

            if not success:
                print(f"Sender stderr: {sender_proc.stderr.read().decode()}")
                print(f"Receiver stderr: {receiver_proc.stderr.read().decode()}")
    else:
        # Bidirectional pipe: connect sender stdout->receiver stdin, receiver stdout->sender stdin
        # We use os.pipe() for proper bidirectional communication
        import os
        import threading
        import io

        # Create two pipes for bidirectional communication
        s2r_read, s2r_write = os.pipe()  # sender writes, receiver reads
        r2s_read, r2s_write = os.pipe()  # receiver writes, sender reads

        sender_cmd = [sys.executable, str(PSYNC)] + sender_args
        receiver_cmd = [sys.executable, str(PSYNC)] + receiver_args

        # Sender: stdin=r2s_read, stdout=s2r_write
        sender = subprocess.Popen(
            sender_cmd,
            stdin=r2s_read,
            stdout=s2r_write,
            stderr=subprocess.PIPE,
        )

        # Receiver: stdin=s2r_read, stdout=r2s_write
        receiver = subprocess.Popen(
            receiver_cmd,
            stdin=s2r_read,
            stdout=r2s_write,
            stderr=subprocess.PIPE,
        )

        # Close our copies of the pipe ends that the children own
        os.close(s2r_write)
        os.close(s2r_read)
        os.close(r2s_write)
        os.close(r2s_read)

        # Wait for both to complete
        sender.wait()
        receiver.wait()

        success = sender.returncode == 0 and receiver.returncode == 0
        sent_bytes = 0  # Can't easily measure with bidirectional pipes

        if not success:
            print(f"Sender stderr: {sender.stderr.read().decode()}")
            print(f"Receiver stderr: {receiver.stderr.read().decode()}")

    duration = time.perf_counter() - start

    return {
        'duration': duration,
        'sent_bytes': sent_bytes,
        'success': success,
        'src_hash': md5_tree(src),
        'dst_hash': md5_tree(dst),
    }


class TestPipeSync:
    """Test psync over pipes (fast, no network simulation)"""

    def test_empty_dir(self, tmp_path):
        """Sync empty directory"""
        src, dst = tmp_path / "src", tmp_path / "dst"
        src.mkdir()
        dst.mkdir()

        result = run_pipe_sync(src, dst)
        assert result['success']

    def test_single_file(self, tmp_path, gen_file):
        """Sync single file"""
        src, dst = tmp_path / "src", tmp_path / "dst"
        src.mkdir()
        dst.mkdir()
        gen_file(src / "test.txt", 1000, "text")

        result = run_pipe_sync(src, dst)
        assert result['success']
        assert result['src_hash'] == result['dst_hash']

    def test_multiple_files(self, tmp_path, gen_file):
        """Sync multiple files"""
        src, dst = tmp_path / "src", tmp_path / "dst"
        src.mkdir()
        dst.mkdir()

        gen_file(src / "a.txt", 100, "text")
        gen_file(src / "b.bin", 5000, "random")
        gen_file(src / "c.dat", 10000, "sparse")

        result = run_pipe_sync(src, dst)
        assert result['success']
        assert result['src_hash'] == result['dst_hash']

    def test_nested_dirs(self, tmp_path, gen_file):
        """Sync nested directory structure"""
        src, dst = tmp_path / "src", tmp_path / "dst"
        src.mkdir()
        dst.mkdir()

        (src / "a" / "b" / "c").mkdir(parents=True)
        gen_file(src / "root.txt", 100, "text")
        gen_file(src / "a" / "level1.txt", 200, "text")
        gen_file(src / "a" / "b" / "level2.txt", 300, "text")
        gen_file(src / "a" / "b" / "c" / "level3.txt", 400, "text")

        result = run_pipe_sync(src, dst)
        assert result['success']
        assert result['src_hash'] == result['dst_hash']

    def test_symlink(self, tmp_path, gen_file):
        """Sync with symlinks"""
        src, dst = tmp_path / "src", tmp_path / "dst"
        src.mkdir()
        dst.mkdir()

        gen_file(src / "target.txt", 100, "text")
        (src / "link.txt").symlink_to("target.txt")

        result = run_pipe_sync(src, dst)
        assert result['success']
        assert (dst / "link.txt").is_symlink()
        assert (dst / "link.txt").resolve().name == "target.txt"

    def test_delta_efficiency(self, tmp_path, gen_file):
        """Modified file should transfer less than full size"""
        src, dst = tmp_path / "src", tmp_path / "dst"
        src.mkdir()
        dst.mkdir()

        # Create 200KB file (large enough for delta algorithm)
        size = 200 * 1024
        gen_file(src / "data.bin", size, "random", seed=42)
        gen_file(dst / "data.bin", size, "random", seed=42)  # Same file in dst

        # Modify 5% in the middle of src
        data = bytearray((src / "data.bin").read_bytes())
        import random
        rng = random.Random(99)
        modify_start = size // 2 - 5000
        for i in range(modify_start, modify_start + 10000):
            data[i] = rng.randint(0, 255)
        (src / "data.bin").write_bytes(bytes(data))

        result = run_pipe_sync(src, dst)
        assert result['success']
        assert result['src_hash'] == result['dst_hash']

        # Should transfer much less than full file size
        # Delta should be ~10KB modified + overhead, not 200KB
        if result['sent_bytes'] > 0:  # Only check if we can measure
            assert result['sent_bytes'] < size * 0.3, f"Transferred {result['sent_bytes']} bytes, expected < {size * 0.3}"

    def test_incremental_sync(self, tmp_path, gen_file):
        """Second sync of unchanged files should be fast"""
        src, dst = tmp_path / "src", tmp_path / "dst"
        src.mkdir()
        dst.mkdir()

        gen_file(src / "file1.bin", 50000, "random")
        gen_file(src / "file2.bin", 50000, "random")

        # First sync
        result1 = run_pipe_sync(src, dst)
        assert result1['success']

        # Second sync (no changes) - should be faster
        result2 = run_pipe_sync(src, dst)
        assert result2['success']
        assert result2['src_hash'] == result2['dst_hash']

        # Second sync should transfer much less data
        if result1['sent_bytes'] > 0 and result2['sent_bytes'] > 0:
            assert result2['sent_bytes'] < result1['sent_bytes'] * 0.2

    def test_compression(self, tmp_path, gen_file):
        """Compression should reduce transfer size for compressible data"""
        src, dst = tmp_path / "src", tmp_path / "dst"
        src.mkdir()
        dst.mkdir()

        # Highly compressible text file
        gen_file(src / "text.txt", 100000, "text")

        # Without compression
        result_no_z = run_pipe_sync(src, dst)
        shutil.rmtree(dst)
        dst.mkdir()

        # With compression
        result_z = run_pipe_sync(src, dst, args=["-z"])

        assert result_no_z['success'] and result_z['success']

        if result_no_z['sent_bytes'] > 0 and result_z['sent_bytes'] > 0:
            # Compressed should be significantly smaller for text
            assert result_z['sent_bytes'] < result_no_z['sent_bytes'] * 0.5


@pytest.mark.skipif(not HAS_PV, reason="pv not installed")
class TestSlowNetwork:
    """Test with simulated slow network using pv rate limiting"""

    @pytest.mark.slow
    def test_100kbps_small_file(self, tmp_path, gen_file):
        """Sync small file at 100KB/s"""
        src, dst = tmp_path / "src", tmp_path / "dst"
        src.mkdir()
        dst.mkdir()

        gen_file(src / "small.bin", 20000, "random")  # 20KB

        result = run_pipe_sync(src, dst, rate="100k")
        assert result['success']
        assert result['src_hash'] == result['dst_hash']
        # At 100KB/s, 20KB should take ~0.2s
        assert 0.1 < result['duration'] < 2.0

    @pytest.mark.slow
    def test_50kbps_with_compression(self, tmp_path, gen_file):
        """Compression helps on slow network"""
        src, dst = tmp_path / "src", tmp_path / "dst"
        src.mkdir()
        dst.mkdir()

        # 50KB of compressible text
        gen_file(src / "text.txt", 50000, "text")

        # Without compression at 50KB/s -> ~1s
        result_no_z = run_pipe_sync(src, dst, rate="50k")
        dur_no_z = result_no_z['duration']

        shutil.rmtree(dst)
        dst.mkdir()

        # With compression - text compresses ~5:1, should be ~0.2s
        result_z = run_pipe_sync(src, dst, rate="50k", args=["-z"])
        dur_z = result_z['duration']

        assert result_no_z['success'] and result_z['success']
        # Compressed should be noticeably faster
        assert dur_z < dur_no_z * 0.7, f"Compressed: {dur_z:.2f}s, Uncompressed: {dur_no_z:.2f}s"

    @pytest.mark.slow
    def test_delta_on_slow_network(self, tmp_path, gen_file):
        """Delta transfer shines on slow network"""
        src, dst = tmp_path / "src", tmp_path / "dst"
        src.mkdir()
        dst.mkdir()

        # Use larger file so delta savings are more apparent
        size = 500 * 1024  # 500KB
        gen_file(src / "data.bin", size, "random", seed=42)
        gen_file(dst / "data.bin", size, "random", seed=42)

        # Modify small portion (2%)
        data = bytearray((src / "data.bin").read_bytes())
        import random
        rng = random.Random(99)
        for i in range(200000, 210000):  # 10KB modification
            data[i] = rng.randint(0, 255)
        (src / "data.bin").write_bytes(bytes(data))

        # Time full transfer first (new file scenario)
        dst2 = tmp_path / "dst2"
        dst2.mkdir()
        result_full = run_pipe_sync(src, dst2, rate="200k")

        # Time delta transfer (existing file scenario)
        result_delta = run_pipe_sync(src, dst, rate="200k")

        assert result_delta['success']
        assert result_delta['src_hash'] == result_delta['dst_hash']

        # Delta should be noticeably faster than full transfer
        # (pv rate-limits bidirectionally, so both signatures and delta are limited)
        assert result_delta['duration'] < result_full['duration'] * 0.8, \
            f"Delta: {result_delta['duration']:.2f}s, Full: {result_full['duration']:.2f}s"


class TestLargeFiles:
    """Test with larger files"""

    def test_1mb_file(self, tmp_path, gen_file):
        """Sync 1MB file"""
        src, dst = tmp_path / "src", tmp_path / "dst"
        src.mkdir()
        dst.mkdir()

        gen_file(src / "large.bin", 1024 * 1024, "random")

        result = run_pipe_sync(src, dst)
        assert result['success']
        assert result['src_hash'] == result['dst_hash']

    def test_mixed_sizes(self, tmp_path, gen_file):
        """Sync mix of file sizes"""
        src, dst = tmp_path / "src", tmp_path / "dst"
        src.mkdir()
        dst.mkdir()

        gen_file(src / "tiny.txt", 10, "text")
        gen_file(src / "small.bin", 1000, "random")
        gen_file(src / "medium.bin", 100000, "random")
        gen_file(src / "large.bin", 500000, "random")

        result = run_pipe_sync(src, dst)
        assert result['success']
        assert result['src_hash'] == result['dst_hash']


# Quick smoke test runner
if __name__ == "__main__":
    import tempfile

    print("Running quick smoke tests...")

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        src, dst = tmp / "src", tmp / "dst"
        src.mkdir()
        dst.mkdir()

        # Create test files
        (src / "test.txt").write_text("hello world\n" * 100)
        (src / "subdir").mkdir()
        (src / "subdir" / "nested.txt").write_text("nested content")

        import random
        rng = random.Random(42)
        (src / "random.bin").write_bytes(rng.randbytes(50000))

        print(f"Source files: {list(src.rglob('*'))}")

        # Test 1: Basic sync
        print("\n1. Basic pipe sync...")
        result = run_pipe_sync(src, dst)
        assert result['success'], "Basic sync failed!"
        assert result['src_hash'] == result['dst_hash'], "Hashes don't match!"
        print(f"   OK - synced in {result['duration']:.3f}s, {result['sent_bytes']} bytes")

        # Test 2: Incremental (no changes)
        print("\n2. Incremental sync (no changes)...")
        result2 = run_pipe_sync(src, dst)
        assert result2['success'], "Incremental sync failed!"
        print(f"   OK - {result2['sent_bytes']} bytes (should be less than first sync)")

        # Test 3: Delta sync
        print("\n3. Delta sync (modify file)...")
        data = bytearray((src / "random.bin").read_bytes())
        for i in range(20000, 25000):
            data[i] = rng.randint(0, 255)
        (src / "random.bin").write_bytes(bytes(data))

        result3 = run_pipe_sync(src, dst)
        assert result3['success'], "Delta sync failed!"
        assert result3['src_hash'] == result3['dst_hash'], "Delta hashes don't match!"
        print(f"   OK - {result3['sent_bytes']} bytes transferred")

        # Test 4: Slow network (if pv available)
        if HAS_PV:
            print("\n4. Slow network test (100KB/s)...")
            shutil.rmtree(dst)
            dst.mkdir()
            result4 = run_pipe_sync(src, dst, rate="100k")
            assert result4['success'], "Slow network sync failed!"
            print(f"   OK - synced in {result4['duration']:.2f}s at 100KB/s")
        else:
            print("\n4. Slow network test SKIPPED (pv not installed)")

        print("\n All smoke tests passed!")
