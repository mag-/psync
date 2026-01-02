#!/usr/bin/env python3
"""Compare hash strategies for rsync-like delta"""
import time
import os
import xxhash
import zlib

def bench_signatures(data: bytes, block_size: int):
    """Benchmark generating signatures for all blocks"""
    n_blocks = len(data) // block_size

    # xxhash32 (fastest)
    start = time.perf_counter()
    sigs_xx32 = [xxhash.xxh32_intdigest(data[i:i+block_size])
                 for i in range(0, len(data) - block_size + 1, block_size)]
    t_xx32 = time.perf_counter() - start

    # xxhash64
    start = time.perf_counter()
    sigs_xx64 = [xxhash.xxh64_intdigest(data[i:i+block_size])
                 for i in range(0, len(data) - block_size + 1, block_size)]
    t_xx64 = time.perf_counter() - start

    # adler32
    start = time.perf_counter()
    sigs_adler = [zlib.adler32(data[i:i+block_size])
                  for i in range(0, len(data) - block_size + 1, block_size)]
    t_adler = time.perf_counter() - start

    mb = len(data) / 1024 / 1024
    print(f"Signatures for {mb:.0f}MB, {block_size//1024}KB blocks ({n_blocks} blocks):")
    print(f"  xxhash32:  {t_xx32*1000:6.1f}ms  {mb/t_xx32:7.0f} MB/s")
    print(f"  xxhash64:  {t_xx64*1000:6.1f}ms  {mb/t_xx64:7.0f} MB/s")
    print(f"  adler32:   {t_adler*1000:6.1f}ms  {mb/t_adler:7.0f} MB/s")

def bench_search_strategies(data: bytes, block_size: int):
    """Compare search strategies for finding block matches"""
    n = len(data)

    # Strategy 1: Check every byte position with xxhash (worst case)
    # This simulates searching for a block that doesn't exist
    positions_to_check = min(10000, n - block_size)  # limit for benchmark

    start = time.perf_counter()
    for i in range(positions_to_check):
        _ = xxhash.xxh32_intdigest(data[i:i+block_size])
    t_every_byte = time.perf_counter() - start
    rate_byte = positions_to_check / t_every_byte

    # Strategy 2: Check every 1KB (coarse search)
    step = 1024
    positions_coarse = list(range(0, min(positions_to_check * step, n - block_size), step))
    start = time.perf_counter()
    for i in positions_coarse:
        _ = xxhash.xxh32_intdigest(data[i:i+block_size])
    t_coarse = time.perf_counter() - start
    rate_coarse = len(positions_coarse) / t_coarse

    # Strategy 3: Block boundaries only
    positions_block = list(range(0, n - block_size, block_size))
    start = time.perf_counter()
    for i in positions_block:
        _ = xxhash.xxh32_intdigest(data[i:i+block_size])
    t_block = time.perf_counter() - start
    rate_block = len(positions_block) / t_block

    print(f"\nSearch strategies ({block_size//1024}KB blocks):")
    print(f"  Every byte:     {rate_byte:9.0f} positions/sec ({1e6/rate_byte:.1f}µs each)")
    print(f"  Every 1KB:      {rate_coarse:9.0f} positions/sec ({1e6/rate_coarse:.1f}µs each)")
    print(f"  Block boundary: {rate_block:9.0f} positions/sec ({1e6/rate_block:.1f}µs each)")

    # How long to scan 200MB file with each strategy?
    file_size = 200 * 1024 * 1024
    print(f"\n  Time to scan 200MB file (worst case, no matches):")
    print(f"    Every byte:     {file_size / rate_byte:6.1f}s")
    print(f"    Every 1KB:      {file_size / 1024 / rate_coarse:6.3f}s")
    print(f"    Block boundary: {file_size / block_size / rate_block:6.3f}s")

def main():
    print("="*60)
    print("Hash Strategy Benchmark")
    print("="*60)

    # Create test data
    data = os.urandom(16 * 1024 * 1024)  # 16MB

    for block_size in [128*1024, 1024*1024]:
        print()
        bench_signatures(data, block_size)
        bench_search_strategies(data, block_size)

if __name__ == "__main__":
    main()
