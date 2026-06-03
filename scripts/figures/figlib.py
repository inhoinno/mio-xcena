"""
Figure generation library.

API:
    data = parse(CACHELIB_LOG, "path/to/file.log")
    add_data(ax, (LAT, CDF), data["read_p50"], data["read_p99"], ...)
"""

import re
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.font_manager as fm
from matplotlib.ticker import FuncFormatter
from collections import defaultdict

# ── Type constants ──
CACHELIB_LOG = "cachelib_log"
FIO_OUTPUT = "fio_output"
IOSTAT_LOG = "iostat_log"
LATBW_CSV = "latbw_csv"

# Figure type constants
LAT = "lat"
PERF = "perf"
CDF = "cdf"
CDF99 = "cdf99"
SCATTER = "scatter"
BAR = "bar"
LATBW = "latbw"

# Bandwidth tick marks for loaded-latency (LAT, LATBW) plots — log2-spaced.
_LATBW_XTICKS = [0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256]

# ── Plot style defaults ──
SMALL_SIZE = 12
MEDIUM_SIZE = 16
BIGGER_SIZE = 24

PERCENTILES = [0.50, 0.90, 0.99, 0.999, 0.9999, 0.99999, 0.999999]
_PERCENTILE_KEYS = ["p50", "p90", "p99", "p999", "p9999", "p99999", "p999999"]
PERCENTILES99 = [ 0.99, 0.999, 0.9999, 0.99999, 0.999999]

def setup_style():
    """Call once to configure matplotlib defaults."""
    def _first_available(candidates):
        available = {f.name for f in fm.fontManager.ttflist}
        for name in candidates:
            if name in available:
                return name
        return "DejaVu Sans"

    font = _first_available(["Helvetica", "Arial", "Liberation Sans", "DejaVu Sans"])
    mpl.rcParams.update({
        "font.family": font,
        "mathtext.fontset": "custom",
        "mathtext.rm": font,
        "mathtext.it": f"{font}:italic",
        "mathtext.bf": f"{font}:bold",
    })


# ═══════════════════════════════════════════════════════
#  Parsers  (Strategy pattern)
# ═══════════════════════════════════════════════════════

class _CachelibParser:
    """Parse a CacheBench soc*.log file."""

    _header_re = re.compile(
        r"(\d{2}:\d{2}:\d{2})\s+"
        r"([\d.]+)M ops completed\.\s+"
        r"Hit Ratio\s+([\d.]+)%\s+"
        r"\(RAM\s+([\d.]+)%,\s+NVM\s+([\d.]+)%\)"
    )
    _rw_re = re.compile(r"Read|Write")
    _lat_re = re.compile(r"p[0-9][0-9]+")
    _float_re = re.compile(r"[0-9]+\.[0-9]+")
    _tp_re = re.compile(r"(get|set|del)\s+:\s+([\d,]+)/s,\s+\w+\s+:\s+([\d.]+)%")
    _items_ram_re = re.compile(r"Items in RAM\s+:\s+([\d,]+)")
    _items_nvm_re = re.compile(r"Items in NVM\s+:\s+([\d,]+)")
    _app_wa_re = re.compile(r"NVM app write amplification\s+:\s+([\d.]+)")

    def parse(self, filepath):
        results = defaultdict(list)
        current_lat = {}
        in_block = False

        with open(filepath) as f:
            for line in f:
                line = line.rstrip()

                m = self._header_re.search(line)
                if m:
                    if in_block:
                        self._flush(results, current_lat)
                        current_lat = {}
                    in_block = True
                    results["timestamp"].append(m.group(1))
                    results["ops_completed_M"].append(float(m.group(2)))
                    results["hit_ratio"].append(float(m.group(3)))
                    results["ram_hit_ratio"].append(float(m.group(4)))
                    results["nvm_hit_ratio"].append(float(m.group(5)))
                    continue

                if not in_block:
                    continue

                # Latency
                rw = self._rw_re.findall(line)
                pct = self._lat_re.findall(line)
                if "NVM" in line and "Latency" in line and rw and pct:
                    val = float(self._float_re.findall(line)[-1])
                    current_lat[f"{rw[0].lower()}_{pct[0]}"] = val
                    continue

                # Items
                m = self._items_ram_re.search(line)
                if m:
                    results["items_in_ram"].append(int(m.group(1).replace(",", "")))
                    continue
                m = self._items_nvm_re.search(line)
                if m:
                    results["items_in_nvm"].append(int(m.group(1).replace(",", "")))
                    continue

                # Write amp
                m = self._app_wa_re.search(line)
                if m:
                    results["app_write_amp"].append(float(m.group(1)))
                    continue

                # Throughput
                m = self._tp_re.search(line)
                if m:
                    op = m.group(1)
                    results[f"{op}_per_sec"].append(int(m.group(2).replace(",", "")))
                    results[f"{op}_success_pct"].append(float(m.group(3)))

        if current_lat:
            self._flush(results, current_lat)
        return dict(results)

    @staticmethod
    def _flush(results, lat_dict):
        for rw in ("read", "write"):
            for pct in _PERCENTILE_KEYS:
                key = f"{rw}_{pct}"
                results[key].append(lat_dict.get(key, 0))


class _FioParser:
    """Parse an fio text output file."""

    _bw_re = re.compile(r"BW=(\d+)MiB/s")
    _iops_re = re.compile(r"IOPS=(\d+)k")
    _clat_pct_re = re.compile(r"([\d.]+)th=\[\s*(\d+)\]")
    _rw_label_re = re.compile(r"^\s*(read|write):\s")

    def parse(self, filepath):
        results = {}
        current_rw = None

        with open(filepath) as f:
            for line in f:
                m = self._rw_label_re.match(line)
                if m:
                    current_rw = m.group(1)

                if current_rw and "BW=" in line:
                    m = self._bw_re.search(line)
                    if m:
                        results[f"{current_rw}_bw_mib"] = int(m.group(1))
                    m = self._iops_re.search(line)
                    if m:
                        results[f"{current_rw}_iops_k"] = int(m.group(1))

                if "clat percentiles" in line:
                    unit = "us"
                    if "nsec" in line:
                        unit = "ns"
                    results[f"{current_rw}_clat_unit"] = unit

                if "th=[" in line and current_rw:
                    for pct_str, val_str in self._clat_pct_re.findall(line):
                        pct = pct_str.replace(".", "")
                        results[f"{current_rw}_p{pct}"] = int(val_str)

        return results


class _IostatParser:
    """Parse iostat bandwidth log.

    Handles both plain-number and suffix-scaled (k/M/G/T) iostat formats,
    and both Device-first and Device-last column orderings.
    """

    _SUFFIXES = {"k": 1e-3, "M": 1, "G": 1e3, "T": 1e6}
    # Matches a number (int or float) with an optional k/M/G/T suffix
    _val_re = re.compile(r"(\d+(?:\.\d+)?)([kMGT])?")
    _header_re = re.compile(r"MB_read/s")

    def parse(self, filepath):
        read_bw, write_bw = [], []
        read_idx, write_idx = None, None

        with open(filepath) as f:
            for line in f:
                # Detect column positions from header line
                if self._header_re.search(line):
                    cols = line.split()
                    # Strip "Device" to get pure metric columns
                    metric_cols = [c for c in cols if c != "Device"]
                    read_idx = metric_cols.index("MB_read/s")
                    write_idx = metric_cols.index("MB_wrtn/s")
                    continue

                if "nvme" not in line:
                    continue

                # Extract all numeric tokens (skip the device name)
                tokens = [t for t in line.split() if not t.startswith("nvme")]
                if read_idx is None or len(tokens) <= max(read_idx, write_idx):
                    continue

                read_bw.append(self._parse_val(tokens[read_idx]))
                write_bw.append(self._parse_val(tokens[write_idx]))

        return {"read_bw": read_bw, "write_bw": write_bw}

    @classmethod
    def _parse_val(cls, token):
        """Convert a possibly-suffixed value like '24.3M' to float in MB."""
        m = cls._val_re.fullmatch(token)
        if not m:
            return 0.0
        num = float(m.group(1))
        suffix = m.group(2)
        return num * cls._SUFFIXES.get(suffix, 1) if suffix else num


class _LatBwParser:
    """Parse a loaded-latency CSV: one 'bw_gbps,latency_ns' row per load point.

    Each file is one device curve (CXL-A, Local, NUMA, ...). Header and blank
    or '#' comment lines are skipped. Rows are returned in file order, which is
    increasing offered load — bandwidth rises until saturation, then latency
    climbs near-vertically.
    """

    def parse(self, filepath):
        bw, lat = [], []
        with open(filepath) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or line[0].isalpha():
                    continue
                parts = line.replace(",", " ").split()
                if len(parts) < 2:
                    continue
                bw.append(float(parts[0]))
                lat.append(float(parts[1]))
        return {"bw": bw, "latency": lat}


_PARSERS = {
    CACHELIB_LOG: _CachelibParser(),
    FIO_OUTPUT: _FioParser(),
    IOSTAT_LOG: _IostatParser(),
    LATBW_CSV: _LatBwParser(),
}


# ═══════════════════════════════════════════════════════
#  Public API
# ═══════════════════════════════════════════════════════

def parse(dtype, filepath):
    """Parse a data file. One call = one data dict."""
    return _PARSERS[dtype].parse(filepath)


def add_data(ax, fig_type, data, data2=None, label=None, color=None, marker="o", **kwargs):
    """
    Draw one line/bar onto ax.  One call = one visual element.

    fig_type: (LAT, CDF)      – CDF latency curve
              (LAT, SCATTER)   – latency over time
              (LAT, LATBW)     – loaded latency vs bandwidth (one device curve)
              (PERF, BAR)      – single bar (average performance)
              (PERF, SCATTER)  – performance over time
    data:     list or array of values to plot (x for LATBW, y otherwise)
    data2:    second array, only used by (LAT, LATBW): the latency (y) values
    """
    category, style = fig_type

    if category == LAT and style == CDF:
        # data = list of 7 latency values matching PERCENTILES
        ax.plot(data, PERCENTILES, f"{marker}-", color=color,
             label=label, **kwargs)
        ax.set_xscale("log")
        ax.set_xlabel("Latency (µs)", fontsize=MEDIUM_SIZE)
        ax.set_yticks(PERCENTILES)
        ax.set_yticklabels(["p50", "p90", "p99", "p999", "p9999", "p99999", "p999999"],
                           fontsize=SMALL_SIZE)

    elif category == LAT and style == CDF99:
        # data = list of 7 latency values matching PERCENTILES
        ax.plot(data, PERCENTILES99, f"{marker}-", color=color,
             label=label, **kwargs)
        ax.set_xscale("log")
        ax.set_xlabel("Latency (µs)", fontsize=MEDIUM_SIZE)
        ax.set_yticks(PERCENTILES99)
        ax.set_yticklabels(["p99", "p999", "p9999", "p99999", "p999999"],
                           fontsize=SMALL_SIZE)
        
    elif category == LAT and style == SCATTER:
        # data = list of latency values over time
        x = np.arange(len(data))
        ax.plot(x, data, f"{marker}-", color=color, label=label, **kwargs)
        ax.set_xlabel("Time (min)", fontsize=MEDIUM_SIZE)
        ax.set_ylabel("Latency (µs)", fontsize=MEDIUM_SIZE)
        ax.grid(True, alpha=0.3)

    elif category == LAT and style == LATBW:
        # data = bandwidth (x), data2 = latency (y); one device curve per call.
        # log2 BW axis with literal GB/s tick labels, matching the paper figure.
        ax.plot(data, data2, f"{marker}-", color=color, label=label, **kwargs)
        ax.set_xscale("log", base=2)
        ax.set_xticks(_LATBW_XTICKS)
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
        ax.set_xlabel("Bandwidth (GB/s)", fontsize=MEDIUM_SIZE)
        ax.set_ylabel("Average Latency (ns)", fontsize=MEDIUM_SIZE)
        ax.grid(True, alpha=0.3)

    elif category == PERF and style == BAR:
        # data = (label_str, value) or just value; use kwargs for bar position
        bar_pos = kwargs.pop("bar_pos", 0)
        bar_width = kwargs.pop("bar_width", 0.4)
        bar = ax.bar(bar_pos, data, width=bar_width, color=color, label=label, **kwargs)
        ax.bar_label(bar, fmt="%.1f")

    elif category == PERF and style == SCATTER:
        # data = list of throughput values over time
        x = np.arange(len(data))
        ax.plot(x, data, f"{marker}-", color=color, label=label, **kwargs)
        ax.set_xlabel("Time (min)", fontsize=MEDIUM_SIZE)
        ax.set_ylabel("MB/s", fontsize=MEDIUM_SIZE)
        ax.grid(True, alpha=0.3)

    else:
        raise ValueError(f"Unknown fig_type: {fig_type}")


def add_cdf_decorations(ax, title=None):
    """Optional: add p99+ highlight band and title to a CDF axis."""
    ax.axhspan(0.99, 1.0, color="red", alpha=0.05)
    ax.axhline(y=0.99, color="grey", linestyle=":", linewidth=1, alpha=0.7)
    ax.text(ax.get_xlim()[0] * 1.5, 0.993, "p99+",
            fontsize=SMALL_SIZE, color="grey", fontstyle="italic")
    if title:
        ax.set_title(title, fontsize=14, fontweight="bold")
