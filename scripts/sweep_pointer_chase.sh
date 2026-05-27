#!/usr/bin/env bash
# Loaded-latency-vs-bandwidth sweep using microbench's pointer_chase mode.
#
# One dedicated thread walks a 2 GiB randomly-linked 256 B-node chain
# (each hop is a cache-miss-defeating dependent load). The other
# (--threads - 1) threads bang on the remaining memory to generate
# bandwidth. --inject-delay <cycles> throttles the load threads; sweep
# it from very high (low BW, low chase latency) down to 0 (max BW,
# knee latency) to walk the curve.
#
# Output per delay point:
#   $OUTDIR/pc_d<delay>/pointer_chase_latency.log   raw ns samples
#   $OUTDIR/pc_d<delay>/stdout.log                   microbench stdout (BW line)
# Aggregated:
#   $OUTDIR/summary.csv                              delay,bw_gbps,mean_ns,p50,p99

set -euo pipefail

# ─── Config ───────────────────────────────────────────────────────────
CXL_NODE=2
CXL_SIZE_MIB=$(( 128 * 1024 ))                       # 128 GiB total: 2 GiB chase + 126 GiB load
THREADS=16                                            # 1 chase + (THREADS-1) load
# Delay sweep in cycles. Big → load nearly idle. 0 → load at full tilt.
# Tune the dense region around your link's knee.
INJECT_DELAYS=( 100000 30000 10000 3000 1000 500 200 100 50 20 10 0 )

# 2 MiB hugepages cover the whole working set
HUGEPAGE_SIZE_MIB=2
HUGEPAGE_NEEDED=$(( CXL_SIZE_MIB / HUGEPAGE_SIZE_MIB ))
HUGE_PATH=/sys/devices/system/node/node${CXL_NODE}/hugepages/hugepages-2048kB/nr_hugepages

MICROBENCH=./build/microbench
OUTDIR=result/$(date +%Y-%m-%d_%H-%M-%S)_pointer_chase
LOG="$OUTDIR/sweep.log"

PER_THREAD_MIB=$(( CXL_SIZE_MIB / THREADS ))         # 8192 for 128 GiB / 16
TOTAL_MIB=$(( PER_THREAD_MIB * THREADS ))

# ─── Pre-flight ───────────────────────────────────────────────────────
[[ -x "$MICROBENCH" ]] || { echo "Build first: ./build.sh"; exit 1; }
[[ -e "$HUGE_PATH"  ]] || { echo "Missing $HUGE_PATH — per-node hugepages not exposed by this kernel."; exit 1; }
(( TOTAL_MIB >= 2048 )) || { echo "Total ${TOTAL_MIB} MiB < 2 GiB chase region — increase CXL_SIZE_MIB or lower THREADS."; exit 1; }
mkdir -p "$OUTDIR"

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

# ─── Sweep ───────────────────────────────────────────────────────────
echo -e "\nThreads=${THREADS} (1 chase + $((THREADS-1)) load), per-thread=${PER_THREAD_MIB} MiB, total=${TOTAL_MIB} MiB" | tee -a "$LOG"
for d in "${INJECT_DELAYS[@]}"; do
  stem="pc_d${d}"
  run_dir="$OUTDIR/$stem"
  mkdir -p "$run_dir"
  echo -e "\n── ${stem}  (inject-delay=${d} cycles)" | tee -a "$LOG"

  # Tee the run's own stdout so we can grep "Load Bandwidth:" later.
  sudo "$MICROBENCH" \
    --mode               pointer_chase   \
    --threads            "$THREADS"      \
    --memory-per-thread  "$PER_THREAD_MIB" \
    --membind            "$CXL_NODE"     \
    --hugepage                           \
    --pre-touch                          \
    --inject-delay       "$d"            \
    --result-dir         "$run_dir"      \
    2>&1 | tee "$run_dir/stdout.log" | tee -a "$LOG"
done

# ─── Aggregate to summary.csv ─────────────────────────────────────────
SUMMARY="$OUTDIR/summary.csv"
echo "delay_cycles,bw_gbps,mean_ns,p50_ns,p99_ns,samples" > "$SUMMARY"

for d in "${INJECT_DELAYS[@]}"; do
  run_dir="$OUTDIR/pc_d${d}"
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

  echo "$d,$bw,$mean,$p50,$p99,$n" | tee -a "$SUMMARY"
done

echo
echo "Per-run dirs:   $OUTDIR/pc_d<delay>/"
echo "Aggregate CSV:  $SUMMARY"
echo
echo "Plot with gnuplot (one-liner):"
cat <<EOF
  gnuplot -p -e "
    set datafile separator ',';
    set logscale x;
    set xlabel 'Bandwidth (GB/s)';
    set ylabel 'Average Latency (ns)';
    set key top left;
    plot '$SUMMARY' using 2:3 every ::1 with linespoints pt 7 title 'CXL node $CXL_NODE'
  "
EOF
