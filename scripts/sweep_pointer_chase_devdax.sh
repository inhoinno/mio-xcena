#!/usr/bin/env bash
# Pointer-chase 2-D sweep (--threads × --inject-delay) on a CXL device in
# DevDAX mode. DevDAX variant of scripts/sweep_pointer_chase.sh.
#
# Differences from the NUMA (--membind) version:
#   * --devdax <dev> instead of --membind <node>.
#   * No hugepage reservation and no --hugepage: the devdax mmap path ignores
#     MAP_HUGETLB (huge pages come from the namespace alignment, set by ndctl).
#   * No --pre-touch / AutoNUMA disable: devdax mmaps MAP_POPULATE, so pages are
#     faulted at map time — the kernel zero-page problem does not exist here.
#   * Capacity is bounded by the namespace size, not a NUMA node.
#   * Optional --cpu-affinity: devdax has no NUMA locality, so pin the chase +
#     load threads to the socket that owns the CXL host bridge (set CPU_LIST).
#
# One chase thread walks a 2 GiB randomly-linked 256 B-node chain; the other
# threads generate background bandwidth. Sweeping --inject-delay walks the
# loaded-latency-vs-bandwidth curve; sweeping --threads gives per-thread CDFs.
#
# Output per (t, d):
#   $OUTDIR/pc_t<t>_d<d>/pointer_chase_latency.log   raw ns samples
#   $OUTDIR/pc_t<t>_d<d>/stdout.log                  microbench stdout
# Aggregated:
#   $OUTDIR/summary.csv  threads,delay_cycles,bw_gbps,mean_ns,p50_ns,p99_ns,samples

set -euo pipefail

# ─── Config ───────────────────────────────────────────────────────────
DEVDAX=${DEVDAX:-/dev/dax0.0}                         # CXL device in devdax mode
DEVDAX_OFFSET=${DEVDAX_OFFSET:-0}                     # base offset into the namespace
CXL_SIZE_MIB=${CXL_SIZE_MIB:-$(( 128 * 1024 ))}      # 128 GiB working set: 2 GiB chase + load
CPU_LIST=${CPU_LIST:-}                               # e.g. "0-31"; empty = no pinning
THREAD_COUNTS=( 1 2 4 8 16 32 )
INJECT_DELAYS=( 100000 30000 10000 3000 1000 500 200 100 50 20 10 0 )

MICROBENCH=./build/microbench
OUTDIR=result/$(date +%Y-%m-%d_%H-%M-%S)_pointer_chase_devdax
LOG="$OUTDIR/sweep.log"

# ─── Pre-flight ───────────────────────────────────────────────────────
[[ -x "$MICROBENCH" ]] || { echo "Build first: ENABLE_LATENCY_MEASURE=ON ./build.sh"; exit 1; }
[[ -e "$DEVDAX"     ]] || { echo "Missing $DEVDAX — convert CXL to devdax: sudo ./scripts/setup.sh --with-cxl --devdax $(basename "$DEVDAX")"; exit 1; }
mkdir -p "$OUTDIR"

# Namespace capacity: the largest run (offset + per_thread×threads) must fit.
# daxctl reports size in bytes when present; otherwise we only sanity-check the
# chase-region floor and trust the operator.
DEV_BYTES=$(daxctl list -d "$(basename "$DEVDAX")" 2>/dev/null \
            | grep -m1 -oE '"size":[0-9]+' | grep -oE '[0-9]+' || true)
NEED_MIB=$(( DEVDAX_OFFSET / 1024 / 1024 + CXL_SIZE_MIB ))
if [[ -n "$DEV_BYTES" ]]; then
  DEV_MIB=$(( DEV_BYTES / 1024 / 1024 ))
  (( NEED_MIB <= DEV_MIB )) || { echo "Need ${NEED_MIB} MiB but $DEVDAX is only ${DEV_MIB} MiB."; exit 1; }
  echo "Device $DEVDAX: ${DEV_MIB} MiB, using ${NEED_MIB} MiB" | tee "$LOG"
else
  echo "daxctl size unknown for $DEVDAX; ensure it holds ≥ ${NEED_MIB} MiB." | tee "$LOG"
fi

# Every thread count must yield a working set ≥ 2 GiB (pointer_chase requirement).
for t in "${THREAD_COUNTS[@]}"; do
  per=$(( CXL_SIZE_MIB / t )); total=$(( per * t ))
  (( total >= 2048 )) || { echo "t=$t → total=${total} MiB < 2 GiB. Raise CXL_SIZE_MIB."; exit 1; }
done

# ─── 2-D sweep: outer = thread count, inner = inject delay ────────────
TOTAL_POINTS=$(( ${#THREAD_COUNTS[@]} * ${#INJECT_DELAYS[@]} ))
echo -e "\nSweeping --threads ∈ {${THREAD_COUNTS[*]}}  ×  --inject-delay ∈ {${INJECT_DELAYS[*]}}" | tee -a "$LOG"
echo "Target: $DEVDAX (offset ${DEVDAX_OFFSET}), ${CXL_SIZE_MIB} MiB, ${CPU_LIST:+cpu-affinity ${CPU_LIST}, }${TOTAL_POINTS} points" | tee -a "$LOG"

i=0
for t in "${THREAD_COUNTS[@]}"; do
  per_thread_mib=$(( CXL_SIZE_MIB / t ))
  total_mib=$(( per_thread_mib * t ))
  for d in "${INJECT_DELAYS[@]}"; do
    i=$(( i + 1 ))
    stem="pc_t${t}_d${d}"
    run_dir="$OUTDIR/$stem"
    mkdir -p "$run_dir"
    echo -e "\n── [${i}/${TOTAL_POINTS}] ${stem}  (load-threads=${t} + 1 chase, per-thread=${per_thread_mib} MiB, total=${total_mib} MiB, inject-delay=${d})" | tee -a "$LOG"

    sudo "$MICROBENCH" \
      --mode               pointer_chase     \
      --threads            "$t"              \
      --memory-per-thread  "$per_thread_mib" \
      --devdax             "$DEVDAX"         \
      --offset             "$DEVDAX_OFFSET"  \
      ${CPU_LIST:+--cpu-affinity "$CPU_LIST"} \
      --inject-delay       "$d"              \
      --result-dir         "$run_dir"        \
      2>&1 | tee "$run_dir/stdout.log" | tee -a "$LOG" \
      || { echo "  !! run failed, continuing" | tee -a "$LOG"; }
  done
done

# ─── Aggregate to summary.csv ─────────────────────────────────────────
SUMMARY="$OUTDIR/summary.csv"
echo "threads,delay_cycles,bw_gbps,mean_ns,p50_ns,p99_ns,samples" > "$SUMMARY"

for t in "${THREAD_COUNTS[@]}"; do
  for d in "${INJECT_DELAYS[@]}"; do
    run_dir="$OUTDIR/pc_t${t}_d${d}"
    lat_log="$run_dir/pointer_chase_latency.log"
    out_log="$run_dir/stdout.log"

    bw=$(grep -m1 -E '^Load Bandwidth:' "$out_log" 2>/dev/null | awk '{print $3}')
    bw=${bw:-NA}

    if [[ -s "$lat_log" ]]; then
      read mean p50 p99 n < <(
        grep -v '^#' "$lat_log" \
          | awk 'NF==1 && $1+0==$1' \
          | sort -n \
          | awk '
              { v[NR]=$1; s+=$1 }
              END {
                n=NR
                if (n==0) { print "NA NA NA 0"; exit }
                printf "%.2f %.2f %.2f %d\n", s/n, v[int(n*0.5)], v[int(n*0.99)], n
              }'
      )
    else
      mean=NA; p50=NA; p99=NA; n=0
    fi

    echo "$t,$d,$bw,$mean,$p50,$p99,$n" | tee -a "$SUMMARY"
  done
done

echo
echo "Per-run dirs:   $OUTDIR/pc_t<threads>_d<delay>/"
echo "Aggregate CSV:  $SUMMARY"
echo
echo "Slice the 2-D grid for plotting:"
echo "  # CDF per thread count (fix delay = 0, max load):"
echo "    python3 scripts/plot_cdf.py $OUTDIR/pc_t*_d0 --title '[CXL devdax] Pointer Chase Latency CDF' --xmax 700"
echo
echo "  # Loaded-latency vs BW (mean latency, fix threads or overlay all):"
echo "    python3 scripts/inho_parser.py latbw $OUTDIR --title '[CXL devdax] Latency vs BW'"
