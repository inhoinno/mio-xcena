#!/usr/bin/env python3
"""Loaded-latency vs bandwidth figure ("[a] CXL Average Latency vs. BW").

One curve per memory device, drawn through the shared two-API contract in
figlib.py: one parse() call loads one device's CSV, one add_data() call draws
one curve. Data lives in data/latbw/<device>.csv (stub points digitized from
the reference figure).

Run from this directory:
    ./latbw.py            # writes latbw.png
    ./latbw.py -o foo.png
"""

import argparse
import pathlib

import matplotlib.pyplot as plt

from figlib import parse, add_data, setup_style, LATBW_CSV, LAT, LATBW

HERE = pathlib.Path(__file__).resolve().parent

# (label, csv stem, color, marker) — colors/markers match the reference figure.
DEVICES = [
    ("CXL-A", "cxl-a", "blue",        "v"),  # down-triangle
    ("CXL-B", "cxl-b", "red",         "s"),  # square
    ("CXL-C", "cxl-c", "grey",        "o"),  # filled circle
    ("CXL-D", "cxl-d", "limegreen",   "^"),  # up-triangle
    ("Local", "local", "mediumpurple", "D"),  # diamond
    ("NUMA",  "numa",  "orange",      "p"),  # pentagon
]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-o", "--out", type=pathlib.Path, default=HERE / "latbw.png")
    ap.add_argument("--title", default="[a] CXL Average Latency vs. BW")
    args = ap.parse_args()

    setup_style()
    fig, ax = plt.subplots(figsize=(7.0, 4.0))

    for label, stem, color, marker in DEVICES:
        d = parse(LATBW_CSV, HERE / "data" / "latbw" / f"{stem}.csv")
        add_data(ax, (LAT, LATBW), d["bw"], d["latency"],
                 label=label, color=color, marker=marker, markersize=5, linewidth=2)

    ax.set_ylim(0, 800)
    ax.set_title(args.title, fontsize=14)
    ax.legend(ncol=3, loc="upper left", fontsize=10, frameon=False)

    fig.tight_layout()
    fig.savefig(args.out, dpi=140)
    print(f"saved: {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
