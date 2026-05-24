#!/usr/bin/env python3
"""
Memory Benchmark Automation Script

Usage:
  python3 run_benchmark.py --devdax /dev/dax0.0          # full benchmark using DevDAX
  python3 run_benchmark.py --membind 4                   # full benchmark using NUMA node
  python3 run_benchmark.py --devdax /dev/dax0.0 --large-mem   # large memory only (cache miss)
  python3 run_benchmark.py --membind 4 --small-mem            # small memory only (cache hit)
  python3 run_benchmark.py --devdax /dev/dax0.0 4        # quick mode

Cache behavior:
  --large-mem (65536 MiB/thread):  Dummy read uses large offset -> cache flush (miss)
  --small-mem (8192 MiB/thread):   Dummy read uses offset 0     -> cache warmup (hit)

Full mode:
  seq_read, seq_write,
  random_read  x3 block sizes (2M, 1M, 512K),
  random_write x3 block sizes,
  zipfian_read x3 block sizes

Quick mode (4):
  seq_read, seq_write, random_read (1MiB), random_write (1MiB)
"""

import argparse
import re
import subprocess
import sys
import time
import signal
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MICROBENCH = REPO_ROOT / "build" / "microbench"
SUMMARY_DIR = REPO_ROOT / "summary"
RESULT_DIR = REPO_ROOT / "result"

MEMORY_PER_THREAD = [65536, 8192]

BLOCK_SIZES = [524288, 1048576, 2097152]
FLUSH_OFFSET = "0x10000000000"


def build_sequence(mode_count: int | None) -> list[tuple[str, bool, int | None]]:
    """Build benchmark sequence with dummy reads interleaved."""
    if mode_count == 4:
        tests = [
            ("seq_read", False, None),
            ("seq_write", False, None),
            ("random_read", False, 1048576),
            ("random_write", False, 1048576),
        ]
    else:
        tests = [
            ("seq_read", False, None),
            ("seq_write", False, None),
        ]
        for mode in ("random_read", "zipfian_read"):
            for bs in BLOCK_SIZES:
                tests.append((mode, False, bs))
        tests.append(("random_write", False, 1048576))

    seq: list[tuple[str, bool, int | None]] = []
    for entry in tests:
        seq.append(("seq_read", True, None))
        seq.append(entry)
    return seq


def parse_bandwidth(output: str) -> str | None:
    m = re.search(r"Bandwidth:\s+([\d.]+\s+GB/s)", output)
    return m.group(1) if m else None


def command_rt(cmd_list, is_exit=True):
    """Run a command and print stdout in real-time."""
    out = ""
    print(f"{' '.join(cmd_list)}")

    proc = subprocess.Popen(
        cmd_list,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    try:
        for line in proc.stdout:
            print(line, end="", flush=True)
            out += line
    except KeyboardInterrupt:
        print("\nKeyboardInterrupt detected. Terminating subprocess...")
        proc.send_signal(signal.SIGINT)
        proc.wait()
        sys.exit(-1)
    finally:
        proc.stdout.close()

    ret = proc.wait()

    if ret != 0 and is_exit:
        print(f"\nfailed {ret}")
        sys.exit(-1)

    return ret, out


def build_cmd(mode: str, memory_per_thread: int, mem_args: list[str],
              offset: str = "0x0",
              block_size: int | None = None, result_dir: Path | None = None) -> list[str]:
    cmd = [
        "sudo", str(MICROBENCH),
        "--mode", mode,
        "--memory-per-thread", str(memory_per_thread),
        "--threads", "16",
        *mem_args,
        "--offset", offset,
    ]
    if block_size:
        cmd += ["--block-size", str(block_size)]
    if result_dir:
        cmd += ["--result-dir", str(result_dir)]
    return cmd


def run_init(base_result_dir: Path, memory_per_thread: int, mem_args: list[str]):
    """Run initial seq_write to initialize memory region."""
    print(f"\n{'#'*60}")
    print(f"  [INIT] seq_write to initialize memory")
    print(f"{'#'*60}")

    init_dir = base_result_dir / "00_init_seq_write"
    init_dir.mkdir(parents=True, exist_ok=True)

    cmd = build_cmd("seq_write", memory_per_thread, mem_args, result_dir=init_dir)
    ret, _ = command_rt(cmd)
    if ret != 0:
        print("Init seq_write failed. Aborting.")
        sys.exit(1)
    print("  Init done.\n")


def run_bench(
    mode: str,
    result_dir: Path,
    step: int,
    total: int,
    is_dummy: bool,
    block_size: int | None,
    memory_per_thread: int,
    cache_hit: bool,
    mem_args: list[str],
) -> tuple[bool, str | None]:
    if is_dummy:
        label = f"cache_warmup_{step}" if cache_hit else f"cache_flush_{step}"
    else:
        label = mode
    if block_size and not is_dummy:
        label += f"_bs{block_size}"

    step_dir = result_dir / f"{step:02d}_{label}"
    step_dir.mkdir(parents=True, exist_ok=True)

    if is_dummy:
        offset = "0x0" if cache_hit else FLUSH_OFFSET
    else:
        offset = "0x0"

    cmd = build_cmd(mode, memory_per_thread, mem_args, offset, block_size, step_dir)

    tag = ("[WARMUP]" if cache_hit else "[FLUSH]") if is_dummy else "[BENCH]"
    bs_info = f"  block={block_size}" if block_size and not is_dummy else ""
    print(f"\n{'='*60}")
    print(f"  Step {step}/{total}  {tag}  {mode}{bs_info}")
    print(f"  Result: {step_dir}")
    print(f"{'='*60}")

    start = time.time()
    ret, out = command_rt(cmd)
    elapsed = time.time() - start

    if ret != 0:
        print(f"  FAILED (exit code {ret}, {elapsed:.1f}s)")
        return False, None

    bw = parse_bandwidth(out)
    print(f"  Done ({elapsed:.1f}s)")
    return True, bw


def parse_args():
    parser = argparse.ArgumentParser(description="Memory Benchmark Automation Script")
    parser.add_argument(
        "mode_count", nargs="?", type=int, default=None,
        help="Quick mode: 4 for seq r/w + random r/w (1MiB block)",
    )

    mem_group = parser.add_mutually_exclusive_group(required=True)
    mem_group.add_argument("--devdax", type=str, metavar="DEVICE",
                           help="DevDAX device path (e.g., /dev/dax0.0)")
    mem_group.add_argument("--membind", type=int, metavar="NODE",
                           help="NUMA memory node (e.g., 0, 4)")

    size_group = parser.add_mutually_exclusive_group()
    size_group.add_argument("--small-mem", action="store_true",
                            help="128 GiB total (8192 MiB/thread x 16 threads) — cache hit scenario")
    size_group.add_argument("--large-mem", action="store_true",
                            help="1 TiB total (65536 MiB/thread x 16 threads) — cache miss scenario")

    parser.add_argument("--no-init", action="store_true",
                        help="Skip initial seq_write (memory already initialized)")

    parser.add_argument("--hugepage", action="store_true",
                        help="Use 2 MiB HugeTLB pages "
                             "(requires per-node pool reserved at "
                             "/sys/devices/system/node/nodeN/hugepages/hugepages-2048kB/nr_hugepages)")

    return parser.parse_args()


def main():
    if not MICROBENCH.exists():
        print(f"Error: microbench binary not found at {MICROBENCH}")
        print("Run build.sh first.")
        sys.exit(1)

    args = parse_args()

    # Build memory binding arguments for microbench
    if args.devdax:
        mem_args = ["--devdax", args.devdax]
    else:
        mem_args = ["--membind", str(args.membind)]
    if args.hugepage:
        mem_args.append("--hugepage")

    if args.small_mem:
        memory_sizes = [8192]
    elif args.large_mem:
        memory_sizes = [65536]
    else:
        memory_sizes = MEMORY_PER_THREAD

    sequence = build_sequence(args.mode_count)
    total = len(sequence)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    base_result_dir = RESULT_DIR / timestamp

    summary_subdir = SUMMARY_DIR / timestamp
    summary_subdir.mkdir(parents=True, exist_ok=True)
    summary_file = summary_subdir / "benchmark_summary.txt"

    preset = f"quick ({args.mode_count})" if args.mode_count else "full"
    mem_target = f"devdax={args.devdax}" if args.devdax else f"membind={args.membind}"
    print(f"Memory Benchmark Automation  [{preset}]")
    print(f"Memory target: {mem_target}")
    print(f"Base results directory: {base_result_dir}")
    print(f"Summary file: {summary_file}")
    print(f"Memory per thread sizes: {memory_sizes}")
    print(f"Steps per memory size: {total}")

    with open(summary_file, "w") as f:
        f.write(f"Memory Benchmark Summary  ({timestamp})  [{preset}]\n")
        f.write(f"{'='*70}\n")
        f.write(f"{'MemPerThread':>12} {'Mode':<20} {'Block Size':>12} {'Bandwidth':>15}\n")
        f.write(f"{'-'*12} {'-'*20} {'-'*12} {'-'*15}\n")

    if not args.no_init:
        run_init(base_result_dir, memory_sizes[0], mem_args)

    for mem_size in memory_sizes:
        print(f"\n{'#'*60}")
        print(f"  Running with --memory-per-thread {mem_size}")
        print(f"{'#'*60}")

        result_dir = base_result_dir / f"mem_{mem_size}"
        cache_hit = mem_size == 8192

        for i, (mode, is_dummy, block_size) in enumerate(sequence, start=1):
            ok, bw = run_bench(
                mode, result_dir, i, total, is_dummy, block_size, mem_size, cache_hit,
                mem_args,
            )
            if not ok:
                print(f"\nStep {i} ({mode}) failed. Aborting.")
                sys.exit(1)

            if not is_dummy and bw:
                bs_str = f"{block_size} B" if block_size else "-"
                line = f"{mem_size:>12} {mode:<20} {bs_str:>12} {bw:>15}"
                print(f"  >> {line}")
                with open(summary_file, "a") as f:
                    f.write(line + "\n")
                    f.flush()

        with open(summary_file, "a") as f:
            f.write(f"{'-'*70}\n")

    with open(summary_file, "a") as f:
        f.write(f"{'='*70}\n")

    print(f"\n{'='*60}")
    print(f"  All steps completed successfully!")
    print(f"  Results: {base_result_dir}")
    print(f"  Summary: {summary_file}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
