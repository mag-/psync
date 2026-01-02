"""pytest fixtures for psync tests"""
import os, tempfile, random, shutil
from pathlib import Path
import pytest

@pytest.fixture
def tmp_src(tmp_path):
    """Source directory with test files"""
    src = tmp_path / "src"
    src.mkdir()
    return src

@pytest.fixture
def tmp_dst(tmp_path):
    """Destination directory"""
    dst = tmp_path / "dst"
    dst.mkdir()
    return dst

@pytest.fixture
def gen_file():
    """Factory to generate test files with specific patterns"""
    def _gen(path: Path, size: int, pattern: str = "random", seed: int = 42):
        path.parent.mkdir(parents=True, exist_ok=True)
        rng = random.Random(seed)
        with open(path, 'wb') as f:
            if pattern == "random":
                # Random data - won't compress well
                f.write(rng.randbytes(size))
            elif pattern == "zeros":
                # All zeros - compresses extremely well
                f.write(b'\x00' * size)
            elif pattern == "text":
                # Repeating text - compresses well
                chunk = b"The quick brown fox jumps over the lazy dog.\n" * 100
                written = 0
                while written < size:
                    to_write = min(len(chunk), size - written)
                    f.write(chunk[:to_write])
                    written += to_write
            elif pattern == "sparse":
                # Mostly zeros with some random chunks
                pos = 0
                while pos < size:
                    if rng.random() < 0.1:  # 10% random data
                        chunk_size = min(rng.randint(1024, 8192), size - pos)
                        f.write(rng.randbytes(chunk_size))
                        pos += chunk_size
                    else:  # 90% zeros
                        chunk_size = min(rng.randint(4096, 32768), size - pos)
                        f.write(b'\x00' * chunk_size)
                        pos += chunk_size
        return path
    return _gen

@pytest.fixture
def gen_modified_file(gen_file):
    """Generate a file then modify a portion of it"""
    def _gen(path: Path, size: int, modify_offset: int, modify_size: int, seed: int = 42):
        gen_file(path, size, "random", seed)
        # Modify a portion
        with open(path, 'r+b') as f:
            f.seek(modify_offset)
            f.write(random.Random(seed + 1).randbytes(modify_size))
        return path
    return _gen

@pytest.fixture
def file_tree():
    """Generate a directory tree with various file types"""
    def _gen(root: Path, spec: dict):
        """
        spec format: {
            "file.txt": 1024,           # file with size
            "subdir/": {                # subdirectory
                "nested.bin": 4096
            },
            "link.txt": ("symlink", "file.txt"),  # symlink
        }
        """
        root.mkdir(parents=True, exist_ok=True)
        rng = random.Random(42)

        def create(base: Path, items: dict):
            for name, val in items.items():
                path = base / name
                if name.endswith('/'):
                    path = base / name.rstrip('/')
                    path.mkdir(exist_ok=True)
                    if isinstance(val, dict):
                        create(path, val)
                elif isinstance(val, tuple) and val[0] == "symlink":
                    path.symlink_to(val[1])
                elif isinstance(val, int):
                    path.write_bytes(rng.randbytes(val))

        create(root, spec)
        return root
    return _gen
