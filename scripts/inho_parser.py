#!/usr/bin/env python3
"""Parse mio-xcena benchmark results and plot them.

Two data sources, two charts — all driven from this one module:

mio  (bandwidth sweep)
    Run dirs named  <rw>_<workload>_bs<bs>_t<threads>_rep<n>/  each holding a
    <timestamp>.txt with a "# Threads <Mode>(GB/s)" header and a
    "<threads> <bw>" data line (see scripts/sweep_lat_bw.sh).
        parse_mio(root)  -> list[MioRecord]
        plot_bar(root)   -> grouped bar chart, x=workload, bars=thread count

mmpt (pointer-chase loaded latency)
    A summary.csv with columns
    threads,delay_cycles,bw_gbps,mean_ns,p50_ns,p99_ns,samples
    (see scripts/sweep_pointer_chase.sh). One curve per thread count, walking
    the inject-delay sweep.
        parse_mmpt(root)   -> list[MmptRecord]
        plot_lat_bw(root)  -> latency-vs-bandwidth chart, p50 + p99 per curve

Both plot functions are single-call APIs: hand them a result directory (or an
already-parsed record list) and they parse, draw, and save.

CLI:
    ./inho_parser.py bar    result/sweep01              [-o bar.png]
    ./inho_parser.py latbw  result/..._pointer_chase    [-o latbw.png]
"""

import argparse
import csv
import pathlib
import re
import sys
from dataclasses import dataclass

import numpy as np
import matplotlib.pyplot as plt


# ─── mio: directory-name convention from sweep_lat_bw.sh ──────────────────
#   read_random_read_bs4096_t128_rep1 -> read, random_read, 4096, 128, 1
_MIO_DIR_RE = re.compile(
    r"^(?P<rw>read|write)_(?P<workload>.+?)_bs(?P<bs>\d+)_t(?P<threads>\d+)_rep(?P<rep>\d+)$"
)

# x-axis order and short labels for the bar chart. Reads first, then writes.
_WORKLOAD_ORDER = [
    "seq_read", "random_read", "zipfian_read", "stride_read",
    "seq_write", "random_write", "stride_write",
]
_WORKLOAD_LABEL = {
    "seq_read": "SeqR", "random_read": "RandR", "zipfian_read": "ZipfR",
    "stride_read": "StrideR", "seq_write": "SeqW", "random_write": "RandW",
    "stride_write": "StrideW",
}


@dataclass
class MioRecord:
    rw: str           # "read" | "write"
    workload: str     # seq_read, random_read, ...
    block_size: int
    threads: int
    rep: int
    bw_gbps: float
    path: pathlib.Path


@dataclass
class MmptRecord:
    threads: int
    delay_cycles: int
    bw_gbps: float
    mean_ns: float
    p50_ns: float
    p99_ns: float
    samples: int


def _read_bw(txt_path: pathlib.Path, threads: int) -> float | None:
    """Pull the bandwidth from one <timestamp>.txt result file.

    The file has a single workload column, so the bandwidth is column 1 of the
    data row. We match the row whose thread count equals the dir's, falling
    back to the last data row.
    """
    rows: list[tuple[int, float]] = []
    with open(txt_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            try:
                rows.append((int(parts[0]), float(parts[1])))
            except ValueError:
                continue
    if not rows:
        return None
    for t, bw in rows:
        if t == threads:
            return bw
    return rows[-1][1]


def _parse_mio_dir(d: pathlib.Path) -> MioRecord | None:
    """Parse one run dir into a MioRecord, or None if the name doesn't match."""
    m = _MIO_DIR_RE.match(d.name)
    if not m:
        return None
    txts = sorted(d.glob("*.txt"))
    if not txts:
        print(f"warn: no .txt result in {d}", file=sys.stderr)
        return None
    threads = int(m.group("threads"))
    bw = _read_bw(txts[-1], threads)
    if bw is None:
        print(f"warn: no bandwidth parsed from {txts[-1]}", file=sys.stderr)
        return None
    return MioRecord(
        rw=m.group("rw"),
        workload=m.group("workload"),
        block_size=int(m.group("bs")),
        threads=threads,
        rep=int(m.group("rep")),
        bw_gbps=bw,
        path=d,
    )


def parse_mio(root: pathlib.Path) -> list[MioRecord]:
    """Parse every mio run dir under (or equal to) `root`."""
    root = pathlib.Path(root)
    candidates = [root, *sorted(p for p in root.iterdir() if p.is_dir())] \
        if root.is_dir() else []
    records = [r for r in (_parse_mio_dir(d) for d in candidates) if r]
    return records


def parse_mmpt(root: pathlib.Path) -> list[MmptRecord]:
    """Parse the pointer-chase summary.csv under (or at) `root`."""
    root = pathlib.Path(root)
    csv_path = root if root.is_file() else root / "summary.csv"
    if not csv_path.exists():
        print(f"warn: no summary.csv at {csv_path}", file=sys.stderr)
        return []
    records: list[MmptRecord] = []
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            # Skip points with no bandwidth or no latency samples (NA).
            try:
                records.append(MmptRecord(
                    threads=int(row["threads"]),
                    delay_cycles=int(row["delay_cycles"]),
                    bw_gbps=float(row["bw_gbps"]),
                    mean_ns=float(row["mean_ns"]),
                    p50_ns=float(row["p50_ns"]),
                    p99_ns=float(row["p99_ns"]),
                    samples=int(row["samples"]),
                ))
            except (ValueError, KeyError):
                continue
    return records


def _as_mio(data) -> list[MioRecord]:
    return data if isinstance(data, list) else parse_mio(pathlib.Path(data))


def _as_mmpt(data) -> list[MmptRecord]:
    return data if isinstance(data, list) else parse_mmpt(pathlib.Path(data))


def plot_bar(data, block_size: int = 4096, out: pathlib.Path | None = None,
             title: str | None = None) -> pathlib.Path:
    """Grouped bar chart: x = workload, one bar per thread count, y = BW.

    `data` is a result directory (parsed here) or a pre-parsed MioRecord list.
    Only the chosen `block_size` is plotted (the sweep mixes several).
    """
    recs = [r for r in _as_mio(data) if r.block_size == block_size]
    if not recs:
        raise SystemExit(f"No mio records at block_size={block_size}.")

    workloads = [w for w in _WORKLOAD_ORDER if any(r.workload == w for r in recs)]
    threads = sorted({r.threads for r in recs})
    bw = {(r.workload, r.threads): r.bw_gbps for r in recs}

    x = np.arange(len(workloads))
    width = 0.8 / len(threads)
    fig, ax = plt.subplots(figsize=(max(7, 1.4 * len(workloads)), 5.0))
    for i, t in enumerate(threads):
        vals = [bw.get((w, t), 0.0) for w in workloads]
        offset = (i - (len(threads) - 1) / 2) * width
        ax.bar(x + offset, vals, width, label=f"{t} threads")

    ax.set_xticks(x)
    ax.set_xticklabels([_WORKLOAD_LABEL.get(w, w) for w in workloads])
    ax.set_ylabel("Bandwidth (GB/s)")
    ax.set_title(title or f"mio bandwidth by workload (block size = {block_size} B)")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend(title="threads")

    out = pathlib.Path(out) if out else _default_out(data, "bar.png")
    fig.tight_layout()
    fig.savefig(out, dpi=140)
    print(f"saved: {out}")
    return out


def plot_lat_bw(data, out: pathlib.Path | None = None,
                title: str | None = None) -> pathlib.Path:
    """Loaded-latency vs bandwidth: one curve per thread count, p50 + p99.

    p50 is drawn solid, p99 dashed, sharing a color per thread count. `data` is
    a pointer-chase result dir (with summary.csv) or a MmptRecord list.
    """
    recs = _as_mmpt(data)
    if not recs:
        raise SystemExit("No mmpt records found.")

    threads = sorted({r.threads for r in recs})
    fig, ax = plt.subplots(figsize=(8.5, 5.0))
    color_cycle = plt.rcParams["axes.prop_cycle"].by_key()["color"]

    for i, t in enumerate(threads):
        curve = sorted((r for r in recs if r.threads == t), key=lambda r: r.bw_gbps)
        c = color_cycle[i % len(color_cycle)]
        bws = [r.bw_gbps for r in curve]
        ax.plot(bws, [r.p50_ns for r in curve], "-o", color=c, lw=1.3, ms=4,
                label=f"t={t}")
        ax.plot(bws, [r.p99_ns for r in curve], "--x", color=c, lw=1.1, ms=5)

    ax.set_xlabel("Load bandwidth (GB/s)")
    ax.set_ylabel("Pointer-chase latency (ns)")
    ax.grid(True, alpha=0.3)
    ax.set_title(title or "mmpt loaded latency vs bandwidth")

    thread_legend = ax.legend(title="threads", loc="upper left", fontsize=8)
    ax.add_artist(thread_legend)
    style_handles = [
        plt.Line2D([], [], color="k", ls="-", marker="o", label="p50"),
        plt.Line2D([], [], color="k", ls="--", marker="x", label="p99"),
    ]
    ax.legend(handles=style_handles, loc="lower right", fontsize=8)

    out = pathlib.Path(out) if out else _default_out(data, "lat_bw.png")
    fig.tight_layout()
    fig.savefig(out, dpi=140)
    print(f"saved: {out}")
    return out


def _default_out(data, name: str) -> pathlib.Path:
    """Put the image in the source dir when `data` is a path, else cwd."""
    if not isinstance(data, list):
        p = pathlib.Path(data)
        base = p if p.is_dir() else p.parent
        return base / name
    return pathlib.Path(name)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("bar", help="mio grouped bar chart (workload x threads)")
    b.add_argument("root", type=pathlib.Path, help="mio sweep result dir")
    b.add_argument("-o", "--out", type=pathlib.Path, default=None)
    b.add_argument("--block-size", type=int, default=4096)
    b.add_argument("--title", default=None)

    l = sub.add_parser("latbw", help="mmpt latency-vs-bandwidth chart")
    l.add_argument("root", type=pathlib.Path, help="pointer-chase result dir")
    l.add_argument("-o", "--out", type=pathlib.Path, default=None)
    l.add_argument("--title", default=None)

    args = ap.parse_args()
    if args.cmd == "bar":
        plot_bar(args.root, block_size=args.block_size, out=args.out, title=args.title)
    else:
        plot_lat_bw(args.root, out=args.out, title=args.title)
    return 0


if __name__ == "__main__":
    sys.exit(main())
