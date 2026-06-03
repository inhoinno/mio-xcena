#!/usr/bin/env python3
"""Plot pointer-chase latency CDFs.

Each pointer_chase_latency.log holds raw ns samples (one per line, '#' for
comments). One curve per log file. Labels come from the file's parent
directory name (e.g. 'pc_d100', 'pc_t16_d0') unless --label is given.

Examples
--------
  # Plot every log under one sweep dir, one curve per delay:
  ./scripts/plot_cdf.py result/2026-05-28_04-24-33_pointer_chase

  # Compare several sweeps on one figure (different devices/targets):
  ./scripts/plot_cdf.py result/cxl_sweep result/dram_sweep -o compare.png

  # Pick specific logs and force labels:
  ./scripts/plot_cdf.py \\
      result/sweep/pc_t1_d0/pointer_chase_latency.log --label "CXL t=1" \\
      result/sweep/pc_t16_d0/pointer_chase_latency.log --label "CXL t=16"
"""

import argparse
import pathlib
import re
import sys

import numpy as np
import matplotlib.pyplot as plt


# Match the sweep script's dir-name convention.
#   pc_t16_d100  → ('16', '100')
#   pc_t8        → ('8', None)
#   pc_d1000     → (None, '1000')
_DIR_RE = re.compile(r"^pc(?:_t(?P<t>\d+))?(?:_d(?P<d>\d+))?$")


def derive_label(dir_name: str, varying_t: bool, varying_d: bool) -> str:
    """Pretty legend label from the run directory name.

    When the sweep only varies one of (threads, delay), the legend hides
    the constant dimension so the legend is uncluttered.
    """
    m = _DIR_RE.match(dir_name)
    if not m:
        return dir_name
    t, d = m.group("t"), m.group("d")
    if varying_t and varying_d:
        return f"t={t}, d={d}"
    if varying_t:
        return f"t={t}" if t else dir_name
    if varying_d:
        return f"d={d}" if d else dir_name
    return dir_name  # single point


def load_samples(path: pathlib.Path) -> np.ndarray:
    """Read raw ns samples from a latency log, skipping comments."""
    vals = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                vals.append(float(line))
            except ValueError:
                pass
    return np.asarray(vals, dtype=np.float64)


def collect_inputs(args) -> list[tuple[pathlib.Path, str | None]]:
    """Return (log_path, explicit_label_or_None) pairs."""
    pairs: list[tuple[pathlib.Path, str | None]] = []
    forced_labels = list(args.label) if args.label else []
    for p in args.paths:
        if p.is_dir():
            logs = sorted(p.rglob("pointer_chase_latency.log"))
            if not logs:
                print(f"warn: no pointer_chase_latency.log under {p}", file=sys.stderr)
            for log in logs:
                pairs.append((log, None))
        elif p.is_file():
            pairs.append((p, forced_labels.pop(0) if forced_labels else None))
        else:
            print(f"warn: {p} not a file or directory", file=sys.stderr)
    return pairs


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Plot CDF of pointer-chase latency logs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("paths", nargs="+", type=pathlib.Path,
                    help="Result dirs (recursively scanned) or specific .log files")
    ap.add_argument("-o", "--out", type=pathlib.Path, default=None,
                    help="Output image path (default: <first-dir>/cdf.png)")
    ap.add_argument("--label", action="append",
                    help="Force label for each subsequent .log argument (repeatable)")
    ap.add_argument("--xmin", type=float, default=0, help="X-axis min (default 0)")
    ap.add_argument("--xmax", type=float, default=None, help="X-axis max (default auto)")
    ap.add_argument("--logx", action="store_true", help="Log-scale latency axis")
    ap.add_argument("--title", default="Pointer Chase Latency CDF")
    ap.add_argument("--clip-percentile", type=float, default=99.5,
                    help="Clip x-axis at this percentile of the widest curve (default 99.5)")
    args = ap.parse_args()

    pairs = collect_inputs(args)
    if not pairs:
        sys.exit("No input logs found.")

    # Decide which dimensions vary across the selected dirs, so labels
    # can hide the constant one and stay readable.
    parsed = []
    for log_path, _ in pairs:
        m = _DIR_RE.match(log_path.parent.name)
        parsed.append((m.group("t"), m.group("d")) if m else (None, None))
    varying_t = len({t for (t, _) in parsed if t is not None}) > 1
    varying_d = len({d for (_, d) in parsed if d is not None}) > 1
    # If neither varies, fall back to showing whatever is present.
    if not varying_t and not varying_d:
        varying_t = any(t is not None for (t, _) in parsed)
        varying_d = any(d is not None for (_, d) in parsed)

    fig, ax = plt.subplots(figsize=(8.5, 5.0))
    auto_xmax = 0.0

    for log_path, forced_label in pairs:
        samples = load_samples(log_path)
        if samples.size == 0:
            print(f"warn: {log_path} has 0 samples — skipping", file=sys.stderr)
            continue
        samples.sort()
        cdf = np.arange(1, samples.size + 1) / samples.size

        if forced_label is not None:
            label = forced_label
        else:
            label = derive_label(log_path.parent.name, varying_t, varying_d)

        mean = samples.mean()
        p50 = np.median(samples)
        p99 = np.percentile(samples, 99)
        ax.plot(samples, cdf, lw=1.2,
                label=f"{label}  (mean={mean:.0f}, p50={p50:.0f}, p99={p99:.0f} ns)")
        auto_xmax = max(auto_xmax, np.percentile(samples, args.clip_percentile))

    ax.set_xlabel("Latency (ns)")
    ax.set_ylabel("CDF")
    ax.set_ylim(0.0, 1.005)
    ax.set_xlim(args.xmin, args.xmax if args.xmax is not None else auto_xmax * 1.05)
    if args.logx:
        ax.set_xscale("log")
    ax.grid(True, alpha=0.3)
    ax.set_title(args.title)
    ax.legend(loc="lower right", fontsize=8)

    out = args.out
    if out is None:
        first_dir = next((p for p in args.paths if p.is_dir()), None)
        out = (first_dir or pathlib.Path(".")) / "cdf.png"
    fig.tight_layout()
    fig.savefig(out, dpi=140)
    fig.savefig(out.with_suffix(".pdf"))
    print(f"saved: {out}  ({out.with_suffix('.pdf').name} alongside)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
