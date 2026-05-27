#!/usr/bin/env bash
# Pointer-chase 2-D sweep: (--threads × --inject-delay).
#
# Microbench's pointer_chase mode spawns ONE dedicated chase thread that
# walks a 2 GiB randomly-linked 256 B-node chain (each hop is a serial
# dependent load the OoO engine cannot hide), PLUS --threads load
# threads that bang on the remaining memory to generate background BW.
# Total OS threads = --threads + 1.
#
# Sweeping both dimensions in one run lets the plotter slice as needed:
#   * CDF per thread count   → use all dirs with one chosen --inject-delay
#     (e.g. pc_t*_d0/pointer_chase_latency.log for max-load CDF)
#   * Loaded-latency vs BW   → use all dirs with one chosen --threads
#     (e.g. pc_t16_d*/   varying delay walks the curve)
#
# Output per (t, d) point:
#   $OUTDIR/pc_t<t>_d<d>/pointer_chase_latency.log   raw ns samples
#   $OUTDIR/pc_t<t>_d<d>/stdout.log                  microbench stdout
# Aggregated:
#   $OUTDIR/summary.csv  threads,delay_cycles,bw_gbps,mean_ns,p50_ns,p99_ns,samples

set -euo pipefail

# ─── Config ───────────────────────────────────────────────────────────
CXL_NODE=2
CXL_SIZE_MIB=$(( 128 * 1024 ))                       # 128 GiB total: 2 GiB chase + load
THREAD_COUNTS=( 1 2 4 8 16 32 )
INJECT_DELAYS=( 100000 30000 10000 3000 1000 500 200 100 50 20 10 0 )

# 2 MiB hugepages cover the whole working set
HUGEPAGE_SIZE_MIB=2
HUGEPAGE_NEEDED=$(( CXL_SIZE_MIB / HUGEPAGE_SIZE_MIB ))
HUGE_PATH=/sys/devices/system/node/node${CXL_NODE}/hugepages/hugepages-2048kB/nr_hugepages

MICROBENCH=./build/microbench
OUTDIR=result/$(date +%Y-%m-%d_%H-%M-%S)_pointer_chase
LOG="$OUTDIR/sweep.log"

# ─── Pre-flight ───────────────────────────────────────────────────────
[[ -x "$MICROBENCH" ]] || { echo "Build first: ./build.sh"; exit 1; }
[[ -e "$HUGE_PATH"  ]] || { echo "Missing $HUGE_PATH — per-node hugepages not exposed by this kernel."; exit 1; }
mkdir -p "$OUTDIR"

# Every thread count must yield a working set ≥ 2 GiB (pointer_chase requirement).
for t in "${THREAD_COUNTS[@]}"; do
  per=$(( CXL_SIZE_MIB / t ))
  total=$(( per * t ))
  (( total >= 2048 )) || { echo "t=$t → total=${total} MiB < 2 GiB. Raise CXL_SIZE_MIB."; exit 1; }
done

# ─── Reserve 2 MiB hugepages on the CXL node ──────────────────────────
echo "Reserving ${HUGEPAGE_NEEDED} × 2 MiB hugepages on node ${CXL_NODE} (= ${CXL_SIZE_MIB} MiB)..." | tee "$LOG"
echo "$HUGEPAGE_NEEDED" | sudo tee "$HUGE_PATH" > /dev/null
ACTUAL=$(cat "$HUGE_PATH")
echo "  requested=$HUGEPAGE_NEEDED  granted=$ACTUAL" | tee -a "$LOG"
if (( ACTUAL < HUGEPAGE_NEEDED )); then
  echo "ABORT: kernel only granted $ACTUAL hugepages on node $CXL_NODE (fragmentation?)" | tee -a "$LOG"
  numactl -H | awk -v n=$CXL_NODE 'NR<=4 || ($1=="node" && $2==n)'
  exit 1
fi
trap 'echo "Releasing hugepages..."; echo 0 | sudo tee "$HUGE_PATH" > /dev/null' EXIT

# ─── 2-D sweep: outer = thread count, inner = inject delay ────────────
TOTAL_POINTS=$(( ${#THREAD_COUNTS[@]} * ${#INJECT_DELAYS[@]} ))
echo -e "\nSweeping --threads ∈ {${THREAD_COUNTS[*]}}  ×  --inject-delay ∈ {${INJECT_DELAYS[*]}}" | tee -a "$LOG"
echo "Total points: $TOTAL_POINTS" | tee -a "$LOG"

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

    # Tee the run's own stdout so we can grep "Load Bandwidth:" later.
    sudo "$MICROBENCH" \
      --mode               pointer_chase     \
      --threads            "$t"              \
      --memory-per-thread  "$per_thread_mib" \
      --membind            "$CXL_NODE"       \
      --hugepage                             \
      --pre-touch                            \
      --inject-delay       "$d"              \
      --result-dir         "$run_dir"        \
      2>&1 | tee "$run_dir/stdout.log" | tee -a "$LOG"
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

    # Bandwidth: "Load Bandwidth: X.XX GB/s (Y.YY ms)"
    bw=$(grep -m1 -E '^Load Bandwidth:' "$out_log" 2>/dev/null \
         | awk '{print $3}')
    bw=${bw:-NA}

    if [[ -s "$lat_log" ]]; then
      # Strip comment header lines, then compute mean/p50/p99 via sort+awk.
      read mean p50 p99 n < <(
        grep -v '^#' "$lat_log" \
          | awk 'NF==1 && $1+0==$1' \
          | sort -n \
          | awk '
              { v[NR]=$1; s+=$1 }
              END {
                n=NR
                if (n==0) { print "NA NA NA 0"; exit }
                printf "%.2f %.2f %.2f %d\n",
                       s/n, v[int(n*0.5)], v[int(n*0.99)], n
              }
            '
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
echo "    python3 scripts/plot_cdf.py $OUTDIR/pc_t*_d0 \\"
echo "        --title '[CXL] Pointer Chase Latency CDF' --xmax 700"
echo
echo "  # Loaded-latency vs BW (fix threads, sweep delay):"
echo "    python3 scripts/plot_cdf.py $OUTDIR/pc_t16_d* \\"
echo "        --title '[CXL] Latency CDF at t=16 across load levels'"
