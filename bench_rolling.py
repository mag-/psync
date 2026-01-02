#!/usr/bin/env python3
"""Microbenchmark for rolling checksum implementations"""
import time
import os

M16 = 1 << 16

def rolling_v1_loop(data: bytes) -> tuple[int, int, int]:
    """Original: Python for loop"""
    a = sum(data) % M16
    b = cumsum = 0
    for byte in data:
        cumsum += byte
        b += cumsum
    b %= M16
    return a, b, a + (b << 16)

def rolling_v2_map(data: bytes) -> tuple[int, int, int]:
    """v2: map(mul, range, data)"""
    from operator import mul
    n = len(data)
    a = sum(data) % M16
    b = sum(map(mul, range(n, 0, -1), data)) % M16
    return a, b, a + (b << 16)

def rolling_v3_array(data: bytes) -> tuple[int, int, int]:
    """v3: array.array for faster iteration"""
    import array
    arr = array.array('B', data)
    n = len(arr)
    a = sum(arr) % M16
    # Still need weighted sum
    b = 0
    weight = n
    for byte in arr:
        b += weight * byte
        weight -= 1
    b %= M16
    return a, b, a + (b << 16)

def rolling_v4_struct(data: bytes) -> tuple[int, int, int]:
    """v4: struct.unpack to process 8 bytes at a time"""
    import struct
    n = len(data)
    a = sum(data) % M16

    # Process 8 bytes at a time for weighted sum
    b = 0
    i = 0
    while i + 8 <= n:
        # Unpack 8 bytes
        b0, b1, b2, b3, b4, b5, b6, b7 = data[i:i+8]
        w = n - i  # weight for first byte
        b += w*b0 + (w-1)*b1 + (w-2)*b2 + (w-3)*b3 + (w-4)*b4 + (w-5)*b5 + (w-6)*b6 + (w-7)*b7
        i += 8
    # Handle remaining bytes
    while i < n:
        b += (n - i) * data[i]
        i += 1
    b %= M16
    return a, b, a + (b << 16)

def rolling_v5_chunks(data: bytes) -> tuple[int, int, int]:
    """v5: Process in larger chunks with local vars"""
    n = len(data)
    a = sum(data) % M16

    b = 0
    i = 0
    # Unroll loop - process 4 bytes at a time
    n4 = n - (n % 4)
    while i < n4:
        w = n - i
        b += w * data[i] + (w-1) * data[i+1] + (w-2) * data[i+2] + (w-3) * data[i+3]
        i += 4
    while i < n:
        b += (n - i) * data[i]
        i += 1
    b %= M16
    return a, b, a + (b << 16)

def rolling_v6_memoryview(data: bytes) -> tuple[int, int, int]:
    """v6: Use memoryview indexing"""
    mv = memoryview(data)
    n = len(mv)
    a = sum(mv) % M16

    b = 0
    for i in range(n):
        b += (n - i) * mv[i]
    b %= M16
    return a, b, a + (b << 16)

def rolling_v7_formula(data: bytes) -> tuple[int, int, int]:
    """v7: Alternative formula - b = (n+1)*sum(data) - sum((i+1)*data[i])"""
    n = len(data)
    a = sum(data) % M16
    # b = sum((n-i)*data[i]) = n*sum(data) - sum(i*data[i])
    # where sum(i*data[i]) needs enumeration
    total = a * n  # n * sum(data), but a is already mod M16 so this is wrong for big data
    # Actually need: n * sum(data) mod M16
    s = sum(data)
    idx_sum = sum(i * byte for i, byte in enumerate(data))
    b = (n * s - idx_sum) % M16
    return a, b, a + (b << 16)

def rolling_v8_listcomp(data: bytes) -> tuple[int, int, int]:
    """v8: List comprehension with enumerate"""
    n = len(data)
    a = sum(data) % M16
    b = sum((n - i) * b for i, b in enumerate(data)) % M16
    return a, b, a + (b << 16)

def rolling_v9_zlib(data: bytes) -> tuple[int, int, int]:
    """v9: Use zlib.adler32 (C-implemented) - different algorithm but fast"""
    import zlib
    checksum = zlib.adler32(data)
    a = checksum & 0xFFFF
    b = (checksum >> 16) & 0xFFFF
    return a, b, checksum

def rolling_v10_xxhash(data: bytes) -> tuple[int, int, int]:
    """v10: Use xxhash for weak checksum too (not rollable but very fast)"""
    import xxhash
    h = xxhash.xxh32_intdigest(data)
    a = h & 0xFFFF
    b = (h >> 16) & 0xFFFF
    return a, b, h

def rolling_v11_crc32(data: bytes) -> tuple[int, int, int]:
    """v11: Use zlib.crc32 (C-implemented)"""
    import zlib
    checksum = zlib.crc32(data)
    a = checksum & 0xFFFF
    b = (checksum >> 16) & 0xFFFF
    return a, b, checksum & 0xFFFFFFFF

def bench(name: str, fn, data: bytes, iterations: int = 3):
    """Benchmark a rolling function"""
    times = []
    result = None
    for _ in range(iterations):
        start = time.perf_counter()
        result = fn(data)
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    avg = sum(times) / len(times)
    mb = len(data) / 1024 / 1024
    throughput = mb / avg
    print(f"{name:25} {avg*1000:8.1f}ms  {throughput:7.1f} MB/s  weak={result[2]:12d}")
    return result

def main():
    # Test data sizes
    sizes = [
        ("128KB block", 128 * 1024),
        ("1MB block", 1024 * 1024),
        ("16MB block", 16 * 1024 * 1024),
    ]

    implementations = [
        ("v1_loop", rolling_v1_loop),
        ("v2_map", rolling_v2_map),
        ("v9_zlib_adler32", rolling_v9_zlib),
        ("v10_xxhash32", rolling_v10_xxhash),
        ("v11_crc32", rolling_v11_crc32),
    ]

    for size_name, size in sizes:
        print(f"\n{'='*60}")
        print(f"Data size: {size_name} ({size:,} bytes)")
        print(f"{'='*60}")

        data = os.urandom(size)
        results = {}

        for name, fn in implementations:
            try:
                result = bench(name, fn, data)
                results[name] = result
            except Exception as e:
                print(f"{name:25} ERROR: {e}")

        # Note: Different algorithms give different checksums, that's OK
        # The important thing is they're consistent within their algorithm

if __name__ == "__main__":
    main()
