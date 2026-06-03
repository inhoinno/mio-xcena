#!/usr/bin/env bash
# Sweep (block_size × threads × workload) on a fixed-size CXL region
# bound to a single NUMA node, capturing one microbench run per point.
# Output of every run lands in $OUTDIR/<stem>/, with the per-run
# bandwidth (and, if built with ENABLE_LATENCY_MEASURE=ON, the latency
# log) ready to be aggregated into a loaded-latency-vs-bandwidth plot.

set -euo pipefail

# ─── Config ───────────────────────────────────────────────────────────
CXL_NODE=2
CXL_SIZE_MIB=$(( 128 * 1024 ))                       # 128 GiB working set, total
TARGET_THREADS=( 8 16 32 64 128 )
TARGET_BLOCKSIZE=( 4096 2097152 536870912 )          # 4 KiB, 2 MiB, 512 MiB
READ_WORKLOADS=( seq_read random_read zipfian_read stride_read )
WRITE_WORKLOADS=( seq_write random_write stride_write )   # no zipfian_write in microbench
REPS=1                                               # bump if you want repeated trials

# 2 MiB hugepages: one per 2 MiB of working set
HUGEPAGE_SIZE_MIB=2
HUGEPAGE_NEEDED=$(( (CXL_SIZE_MIB - 3072) / HUGEPAGE_SIZE_MIB ))    # 65536 for 128 GiB
HUGE_PATH=/sys/devices/system/node/node${CXL_NODE}/hugepages/hugepages-2048kB/nr_hugepages

MICROBENCH=./build/microbench
OUTDIR=result/$(date +%Y-%m-%d_%H-%M-%S)_sweep
LOG="$OUTDIR/sweep.log"

# ─── Pre-flight ───────────────────────────────────────────────────────
[[ -x "$MICROBENCH" ]] || { echo "Build first: ./build.sh"; exit 1; }
[[ -e "$HUGE_PATH"  ]] || { echo "Missing $HUGE_PATH — per-node hugepages not exposed by this kernel."; exit 1; }
mkdir -p "$OUTDIR"

# Each thread count must divide the working set evenly (we use integer division).
for t in "${TARGET_THREADS[@]}"; do
  if (( CXL_SIZE_MIB % t != 0 )); then
    echo "warning: ${CXL_SIZE_MIB} MiB not divisible by $t threads — last $(( CXL_SIZE_MIB % t )) MiB will be unused"
  fi
done

# ─── Reserve 2 MiB hugepages on the CXL node ──────────────────────────
#echo "Reserving $HUGEPAGE_NEEDED × 2 MiB hugepages on node $CXL_NODE (= $CXL_SIZE_MIB MiB)..."
#echo "$HUGEPAGE_NEEDED" | sudo tee "$HUGE_PATH" > /dev/null
#ACTUAL=$(cat "$HUGE_PATH")
#echo "  requested=$HUGEPAGE_NEEDED  granted=$ACTUAL"
#if (( ACTUAL < HUGEPAGE_NEEDED )); then
#  echo "ABORT: kernel only granted $ACTUAL hugepages (fragmentation on node $CXL_NODE?)"
#  echo "  numactl -H | head:"; numactl -H | awk -v n=$CXL_NODE 'NR<=4 || ($1=="node" && $2==n)'
#  exit 1
#fi

#trap 'echo "Releasing hugepages..."; echo 0 | sudo tee "$HUGE_PATH" > /dev/null' EXIT

# ─── One sweep over (block_size × workload × threads × reps) ──────────
run_sweep() {
  local label="$1"; shift
  local -a workloads=( "$@" )

  echo -e "\n══════════ ${label} sweep ══════════" | tee -a "$LOG"
  for bs in "${TARGET_BLOCKSIZE[@]}"; do
    for w in "${workloads[@]}"; do
      for t in "${TARGET_THREADS[@]}"; do
        local per_thread_mib=$(( (CXL_SIZE_MIB - 1900) / t ))           # safe int division
        local total_mib=$(( per_thread_mib * t ))
        for rep in $(seq 1 "$REPS"); do
          local stem="${label,,}_${w}_bs${bs}_t${t}_rep${rep}"
          echo -e "\n── ${stem}  (per-thread=${per_thread_mib} MiB, total=${total_mib} MiB)" \
              | tee -a "$LOG"
	  echo " sudo $MICROBENCH --mode $w  --threads $t --memory-per-thread $per_thread_mib --block-size $bs --membind  $CXL_NODE --pre-touch "
          sudo "$MICROBENCH" \
            --mode           "$w"               \
            --threads        "$t"               \
            --memory-per-thread "$per_thread_mib" \
            --block-size     "$bs"              \
            --membind        "$CXL_NODE"        \
            --pre-touch                         \
            --result-dir     "$OUTDIR/$stem"    2>&1 | tee -a "$LOG"
        done
      done
    done
  done
}

# ─── Run reads, then writes ───────────────────────────────────────────
run_sweep "READ"  "${READ_WORKLOADS[@]}"
run_sweep "WRITE" "${WRITE_WORKLOADS[@]}"

# ─── Quick summary ────────────────────────────────────────────────────
echo
echo "Per-run dirs:  $OUTDIR/<stem>/"
echo "Bandwidth grep:"
echo "  grep -rh 'Bandwidth:' $OUTDIR/ | sort"
echo
echo "Latency (only present if you built with ENABLE_LATENCY_MEASURE=ON):"
echo "  find $OUTDIR/ -name 'latency*.log'"
