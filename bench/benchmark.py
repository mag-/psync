#!/usr/bin/env python3
"""
Benchmark psync vs rsync using Silesia corpus.

Usage:
    python bench/benchmark.py
    python bench/benchmark.py --quick      # Fewer iterations
    python bench/benchmark.py --only psync # Only test psync
"""
from __future__ import annotations
import subprocess
import tempfile
import shutil
import time
import os
import sys
import json
import random
import resource
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Callable

# Paths
BENCH_DIR = Path(__file__).parent
CORPUS_DIR = BENCH_DIR / "silesia"
PSYNC = BENCH_DIR.parent / "psync.py"

# ANSI colors
G, Y, R, B, RST = "\033[32m", "\033[33m", "\033[31m", "\033[34m", "\033[0m"

@dataclass
class Result:
    name: str
    scenario: str
    wall_time: float = 0.0
    user_time: float = 0.0
    sys_time: float = 0.0
    bytes_sent: int = 0
    bytes_received: int = 0
    src_size: int = 0
    iterations: int = 1

    @property
    def cpu_time(self) -> float:
        return self.user_time + self.sys_time

    @property
    def throughput_mbps(self) -> float:
        if self.wall_time == 0:
            return 0
        return (self.src_size / 1024 / 1024) / self.wall_time

    @property
    def transfer_ratio(self) -> float:
        """Ratio of data transferred to source size (lower is better for delta)"""
        if self.src_size == 0:
            return 0
        return (self.bytes_sent + self.bytes_received) / self.src_size

    def __str__(self) -> str:
        return (
            f"{self.name:8} {self.scenario:20} "
            f"wall:{self.wall_time:6.2f}s cpu:{self.cpu_time:6.2f}s "
            f"throughput:{self.throughput_mbps:6.1f}MB/s "
            f"xfer_ratio:{self.transfer_ratio:.2%}"
        )


def get_dir_size(path: Path) -> int:
    """Get total size of directory"""
    total = 0
    for p in path.rglob('*'):
        if p.is_file():
            total += p.stat().st_size
    return total


def run_with_stats(cmd: list[str], cwd: Path = None) -> tuple[float, float, float]:
    """Run command and return (wall_time, user_time, sys_time)"""
    start = time.perf_counter()
    usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)

    result = subprocess.run(cmd, cwd=cwd, capture_output=True)

    usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    wall_time = time.perf_counter() - start

    user_time = usage_after.ru_utime - usage_before.ru_utime
    sys_time = usage_after.ru_stime - usage_before.ru_stime

    if result.returncode != 0:
        print(f"{R}Command failed: {' '.join(cmd)}{RST}")
        print(result.stderr.decode()[:500])

    return wall_time, user_time, sys_time


def run_rsync(src: Path, dst: Path, compress: bool = True, delete: bool = False) -> tuple[float, float, float, int, int]:
    """Run rsync and return timing + transfer stats"""
    cmd = ["rsync", "-a", "--stats"]
    if compress:
        cmd.append("-z")
    if delete:
        cmd.append("--delete")
    cmd.extend([str(src) + "/", str(dst) + "/"])

    start = time.perf_counter()
    usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)

    result = subprocess.run(cmd, capture_output=True, text=True)

    usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    wall_time = time.perf_counter() - start

    user_time = usage_after.ru_utime - usage_before.ru_utime
    sys_time = usage_after.ru_stime - usage_before.ru_stime

    # Parse rsync stats for transfer info
    bytes_sent = bytes_received = 0
    for line in result.stdout.split('\n'):
        if 'Total bytes sent:' in line:
            bytes_sent = int(line.split(':')[1].strip().replace(',', ''))
        elif 'Total bytes received:' in line:
            bytes_received = int(line.split(':')[1].strip().replace(',', ''))

    return wall_time, user_time, sys_time, bytes_sent, bytes_received


def run_psync(src: Path, dst: Path, compress: bool = True, delete: bool = False) -> tuple[float, float, float, int, int]:
    """Run psync and return timing + transfer stats"""
    cmd = [sys.executable, str(PSYNC), "-a"]
    if compress:
        cmd.append("-z")
    if delete:
        cmd.append("--delete")
    cmd.extend([str(src) + "/", str(dst) + "/"])

    wall_time, user_time, sys_time = run_with_stats(cmd)

    # For psync, we estimate transfer by dst size change (not exact but indicative)
    # In a real network scenario, we'd measure actual bytes
    bytes_sent = get_dir_size(src)  # Approximation
    bytes_received = 0

    return wall_time, user_time, sys_time, bytes_sent, bytes_received


def modify_files(directory: Path, modification_pct: float, seed: int = 42):
    """Modify a percentage of bytes across files in directory"""
    rng = random.Random(seed)
    files = [f for f in directory.rglob('*') if f.is_file()]

    for f in files:
        data = bytearray(f.read_bytes())
        n_modify = int(len(data) * modification_pct)
        positions = rng.sample(range(len(data)), min(n_modify, len(data)))
        for pos in positions:
            data[pos] = rng.randint(0, 255)
        f.write_bytes(bytes(data))


def benchmark_scenario(
    name: str,
    scenario: str,
    runner: Callable,
    src: Path,
    dst: Path,
    compress: bool = True,
    iterations: int = 3,
) -> Result:
    """Run benchmark scenario multiple times and average"""
    results = []

    for i in range(iterations):
        # Clean dst for consistent measurement
        if dst.exists():
            shutil.rmtree(dst)
        dst.mkdir(parents=True)

        wall, user, sys_, sent, recv = runner(src, dst, compress=compress)
        results.append((wall, user, sys_, sent, recv))

    # Average results
    avg_wall = sum(r[0] for r in results) / len(results)
    avg_user = sum(r[1] for r in results) / len(results)
    avg_sys = sum(r[2] for r in results) / len(results)
    avg_sent = sum(r[3] for r in results) // len(results)
    avg_recv = sum(r[4] for r in results) // len(results)

    return Result(
        name=name,
        scenario=scenario,
        wall_time=avg_wall,
        user_time=avg_user,
        sys_time=avg_sys,
        bytes_sent=avg_sent,
        bytes_received=avg_recv,
        src_size=get_dir_size(src),
        iterations=iterations,
    )


def run_benchmarks(quick: bool = False, only: str = None) -> list[Result]:
    """Run all benchmark scenarios"""
    results = []
    iterations = 1 if quick else 3

    if not CORPUS_DIR.exists():
        print(f"{R}Silesia corpus not found at {CORPUS_DIR}{RST}")
        print("Run: cd bench && curl -LO https://sun.aei.polsl.pl/~sdeor/corpus/silesia.zip && unzip silesia.zip")
        sys.exit(1)

    src_size = get_dir_size(CORPUS_DIR)
    print(f"{B}Silesia corpus: {src_size / 1024 / 1024:.1f} MB{RST}")
    print(f"{B}Iterations per scenario: {iterations}{RST}\n")

    runners = []
    if only is None or only == "rsync":
        runners.append(("rsync", run_rsync))
    if only is None or only == "psync":
        runners.append(("psync", run_psync))

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # === Scenario 1: Full initial sync ===
        print(f"{Y}━━━ Scenario 1: Full initial sync (cold) ━━━{RST}")
        for name, runner in runners:
            dst = tmpdir / f"{name}_dst1"
            result = benchmark_scenario(name, "full_sync", runner, CORPUS_DIR, dst, iterations=iterations)
            results.append(result)
            print(f"  {G}{result}{RST}")

        # === Scenario 2: No changes (incremental) ===
        print(f"\n{Y}━━━ Scenario 2: No changes (should be instant) ━━━{RST}")
        for name, runner in runners:
            # First sync
            dst = tmpdir / f"{name}_dst2"
            dst.mkdir(parents=True)
            runner(CORPUS_DIR, dst)

            # Measure second sync (no changes)
            start = time.perf_counter()
            usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
            wall, user, sys_, sent, recv = runner(CORPUS_DIR, dst)
            result = Result(
                name=name,
                scenario="no_changes",
                wall_time=wall,
                user_time=user,
                sys_time=sys_,
                bytes_sent=sent,
                bytes_received=recv,
                src_size=src_size,
            )
            results.append(result)
            print(f"  {G}{result}{RST}")

        # === Scenario 3: Small modification (1% of bytes) ===
        print(f"\n{Y}━━━ Scenario 3: 1% modification (delta efficiency) ━━━{RST}")
        for name, runner in runners:
            # Setup: copy corpus and sync
            src_mod = tmpdir / f"{name}_src3"
            dst = tmpdir / f"{name}_dst3"
            shutil.copytree(CORPUS_DIR, src_mod)
            dst.mkdir(parents=True)
            runner(src_mod, dst)

            # Modify 1% of source
            modify_files(src_mod, 0.01, seed=42)

            # Measure delta sync
            wall, user, sys_, sent, recv = runner(src_mod, dst)
            result = Result(
                name=name,
                scenario="1%_modified",
                wall_time=wall,
                user_time=user,
                sys_time=sys_,
                bytes_sent=sent,
                bytes_received=recv,
                src_size=src_size,
            )
            results.append(result)
            print(f"  {G}{result}{RST}")

        # === Scenario 4: Large modification (10% of bytes) ===
        print(f"\n{Y}━━━ Scenario 4: 10% modification ━━━{RST}")
        for name, runner in runners:
            src_mod = tmpdir / f"{name}_src4"
            dst = tmpdir / f"{name}_dst4"
            shutil.copytree(CORPUS_DIR, src_mod)
            dst.mkdir(parents=True)
            runner(src_mod, dst)

            modify_files(src_mod, 0.10, seed=43)

            wall, user, sys_, sent, recv = runner(src_mod, dst)
            result = Result(
                name=name,
                scenario="10%_modified",
                wall_time=wall,
                user_time=user,
                sys_time=sys_,
                bytes_sent=sent,
                bytes_received=recv,
                src_size=src_size,
            )
            results.append(result)
            print(f"  {G}{result}{RST}")

        # === Scenario 5: No compression ===
        print(f"\n{Y}━━━ Scenario 5: Full sync without compression ━━━{RST}")
        for name, runner in runners:
            dst = tmpdir / f"{name}_dst5"
            result = benchmark_scenario(name, "no_compress", runner, CORPUS_DIR, dst,
                                        compress=False, iterations=iterations)
            results.append(result)
            print(f"  {G}{result}{RST}")

        # === Scenario 6: Append-only workload ===
        print(f"\n{Y}━━━ Scenario 6: Append to files (log-like) ━━━{RST}")
        for name, runner in runners:
            src_mod = tmpdir / f"{name}_src6"
            dst = tmpdir / f"{name}_dst6"
            shutil.copytree(CORPUS_DIR, src_mod)
            dst.mkdir(parents=True)
            runner(src_mod, dst)

            # Append 10KB to each file
            for f in src_mod.rglob('*'):
                if f.is_file():
                    with open(f, 'ab') as fh:
                        fh.write(os.urandom(10 * 1024))

            wall, user, sys_, sent, recv = runner(src_mod, dst)
            result = Result(
                name=name,
                scenario="append_10kb",
                wall_time=wall,
                user_time=user,
                sys_time=sys_,
                bytes_sent=get_dir_size(src_mod),  # Updated size
                bytes_received=recv,
                src_size=get_dir_size(src_mod),
            )
            results.append(result)
            print(f"  {G}{result}{RST}")

    return results


def print_summary(results: list[Result]):
    """Print comparison summary"""
    print(f"\n{B}{'═' * 80}{RST}")
    print(f"{B}SUMMARY{RST}")
    print(f"{B}{'═' * 80}{RST}\n")

    # Group by scenario
    scenarios = {}
    for r in results:
        if r.scenario not in scenarios:
            scenarios[r.scenario] = {}
        scenarios[r.scenario][r.name] = r

    print(f"{'Scenario':<20} {'rsync':>12} {'psync':>12} {'Speedup':>10}")
    print("─" * 56)

    for scenario, tools in scenarios.items():
        rsync_time = tools.get('rsync', Result('', '')).wall_time
        psync_time = tools.get('psync', Result('', '')).wall_time

        if rsync_time > 0 and psync_time > 0:
            speedup = rsync_time / psync_time
            speedup_str = f"{speedup:.2f}x"
            if speedup > 1:
                speedup_str = f"{G}{speedup_str}{RST}"
            elif speedup < 1:
                speedup_str = f"{R}{speedup_str}{RST}"
        else:
            speedup_str = "N/A"

        rsync_str = f"{rsync_time:.2f}s" if rsync_time > 0 else "N/A"
        psync_str = f"{psync_time:.2f}s" if psync_time > 0 else "N/A"

        print(f"{scenario:<20} {rsync_str:>12} {psync_str:>12} {speedup_str:>10}")

    print()

    # CPU efficiency
    print(f"{'Scenario':<20} {'rsync CPU':>12} {'psync CPU':>12}")
    print("─" * 44)
    for scenario, tools in scenarios.items():
        rsync_cpu = tools.get('rsync', Result('', '')).cpu_time
        psync_cpu = tools.get('psync', Result('', '')).cpu_time
        rsync_str = f"{rsync_cpu:.2f}s" if rsync_cpu > 0 else "N/A"
        psync_str = f"{psync_cpu:.2f}s" if psync_cpu > 0 else "N/A"
        print(f"{scenario:<20} {rsync_str:>12} {psync_str:>12}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Benchmark psync vs rsync")
    parser.add_argument("--quick", action="store_true", help="Run fewer iterations")
    parser.add_argument("--only", choices=["rsync", "psync"], help="Only run one tool")
    parser.add_argument("--json", action="store_true", help="Output JSON results")
    args = parser.parse_args()

    print(f"\n{B}╔{'═' * 60}╗{RST}")
    print(f"{B}║{'psync vs rsync Benchmark':^60}║{RST}")
    print(f"{B}╚{'═' * 60}╝{RST}\n")

    results = run_benchmarks(quick=args.quick, only=args.only)

    if args.json:
        print(json.dumps([asdict(r) for r in results], indent=2))
    else:
        print_summary(results)


if __name__ == "__main__":
    main()
