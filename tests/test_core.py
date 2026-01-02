"""Tests for core rsync algorithms: rolling checksum, delta, patch"""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# These will be imported from psync.py

class TestBlockSize:
    """Test adaptive block sizing"""

    def test_small_files_whole(self):
        """Files < 128KB should use whole file (blk_size=0)"""
        from psync import blk_size
        assert blk_size(0) == 0
        assert blk_size(1024) == 0
        assert blk_size(128 * 1024 - 1) == 0

    def test_medium_files_128k(self):
        """Files 128KB-16MB should use 128KB blocks"""
        from psync import blk_size
        assert blk_size(128 * 1024) == 128 * 1024
        assert blk_size(1024 * 1024) == 128 * 1024
        assert blk_size(16 * 1024 * 1024 - 1) == 128 * 1024

    def test_large_files_scale(self):
        """Larger files should use larger blocks"""
        from psync import blk_size
        assert blk_size(16 * 1024 * 1024) == 1024 * 1024  # 16MB -> 1MB blocks
        assert blk_size(256 * 1024 * 1024) == 16 * 1024 * 1024  # 256MB -> 16MB blocks
        assert blk_size(4 * 1024 * 1024 * 1024) == 128 * 1024 * 1024  # 4GB -> 128MB blocks


class TestWeakHash:
    """Test xxhash32 weak hash for block matching"""

    def test_basic_hash(self):
        """Basic hash computation"""
        from psync import weak_hash
        data = b"hello world"
        h = weak_hash(data)
        assert isinstance(h, int)
        assert h == weak_hash(data)  # deterministic

    def test_different_inputs(self):
        """Different inputs should give different hashes"""
        from psync import weak_hash
        h1 = weak_hash(b"hello")
        h2 = weak_hash(b"world")
        assert h1 != h2

    def test_memoryview(self):
        """Should handle memoryview input"""
        from psync import weak_hash
        data = b"hello world"
        mv = memoryview(data)
        assert weak_hash(mv) == weak_hash(data)


class TestStrongHash:
    """Test xxh3_128 hashing"""

    def test_deterministic(self):
        """Same input should always give same hash"""
        from psync import strong
        data = b"test data"
        h1 = strong(data)
        h2 = strong(data)
        assert h1 == h2

    def test_different_inputs(self):
        """Different inputs should give different hashes"""
        from psync import strong
        h1 = strong(b"hello")
        h2 = strong(b"world")
        assert h1 != h2

    def test_hash_size(self):
        """xxh3_128 should produce 16-byte digest"""
        from psync import strong
        h = strong(b"test")
        assert len(h) == 16


class TestSignatures:
    """Test signature generation"""

    def test_empty_file(self):
        """Empty file should have no signatures"""
        from psync import signatures
        assert signatures(b"", 128) == []

    def test_single_block(self):
        """File smaller than block should have one signature"""
        from psync import signatures
        data = b"x" * 100
        sigs = signatures(data, 128)
        assert len(sigs) == 1

    def test_multiple_blocks(self):
        """File should have ceil(size/block_sz) signatures"""
        from psync import signatures
        data = b"x" * 1000
        sigs = signatures(data, 128)
        assert len(sigs) == 8  # ceil(1000/128) = 8


class TestDeltaPatch:
    """Test delta generation and patching"""

    def test_identical_files(self):
        """Identical files should produce only block references"""
        from psync import signatures, delta, patch
        data = b"x" * 1024
        sigs = signatures(data, 128)
        d = delta(data, sigs, 128)
        # All deltas should be block references (integers)
        assert all(isinstance(x, int) for x in d)
        # Patch should reconstruct original
        assert patch(data, d, 128) == data

    def test_completely_different(self):
        """Completely different files should produce only literals"""
        from psync import signatures, delta, patch
        basis = b"a" * 1024
        new_data = b"b" * 1024
        sigs = signatures(basis, 128)
        d = delta(new_data, sigs, 128)
        # All deltas should be literals (bytes)
        assert all(isinstance(x, bytes) for x in d)
        # Patch should produce new data
        assert patch(basis, d, 128) == new_data

    def test_partial_change(self):
        """File with partial change should have mixed delta"""
        from psync import signatures, delta, patch
        basis = b"a" * 512 + b"b" * 512
        new_data = b"a" * 512 + b"c" * 512  # Changed second half
        sigs = signatures(basis, 128)
        d = delta(new_data, sigs, 128)
        # Should have some block refs and some literals
        has_refs = any(isinstance(x, int) for x in d)
        has_literals = any(isinstance(x, bytes) for x in d)
        assert has_refs and has_literals
        # Patch should produce new data
        assert patch(basis, d, 128) == new_data

    def test_inserted_data(self):
        """Insertion in middle should still match surrounding blocks"""
        from psync import signatures, delta, patch
        basis = b"a" * 256 + b"b" * 256
        new_data = b"a" * 256 + b"INSERT" + b"b" * 256
        sigs = signatures(basis, 128)
        d = delta(new_data, sigs, 128)
        # Should reconstruct correctly
        assert patch(basis, d, 128) == new_data

    def test_roundtrip_random_data(self, gen_file, tmp_path):
        """Round-trip with random binary data"""
        from psync import signatures, delta, patch

        # Generate basis file
        basis_path = tmp_path / "basis.bin"
        gen_file(basis_path, 10000, "random", seed=1)
        basis = basis_path.read_bytes()

        # Generate new file with some overlap
        new_path = tmp_path / "new.bin"
        gen_file(new_path, 10000, "random", seed=1)
        # Modify 20% in the middle
        new_data = bytearray(new_path.read_bytes())
        import random
        rng = random.Random(99)
        for i in range(4000, 6000):
            new_data[i] = rng.randint(0, 255)
        new_data = bytes(new_data)

        sigs = signatures(basis, 128)
        d = delta(new_data, sigs, 128)
        result = patch(basis, d, 128)

        assert result == new_data


class TestWireProtocol:
    """Test message encoding/decoding"""

    def test_encode_decode_roundtrip(self):
        """Message should survive encode/decode"""
        from psync import M, enc, dec
        import io

        for msg_type in [M.HELLO, M.FILES, M.DELTA, M.DONE]:
            payload = b"test payload data"
            encoded = enc(msg_type, payload, compress=False)

            stream = io.BytesIO(encoded)
            decoded_type, decoded_payload = dec(stream)

            assert decoded_type == msg_type
            assert decoded_payload == payload

    def test_compression(self):
        """Compressible data should be smaller when compressed"""
        from psync import M, enc, dec
        import io

        # Highly compressible data
        payload = b"aaaa" * 10000

        uncompressed = enc(M.DATA, payload, compress=False)
        compressed = enc(M.DATA, payload, compress=True)

        assert len(compressed) < len(uncompressed)

        # Both should decode to same payload
        stream = io.BytesIO(compressed)
        _, decoded = dec(stream)
        assert decoded == payload
