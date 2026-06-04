#!/bin/bash
# Loaded-latency-vs-bandwidth sweep by THREAD COUNT on a single NUMA/CXL node.
#
# Fixed total working set (1 TiB by default) split evenly across threads:
#       per-thread = TOTAL / threads
# Raising the thread count walks the bandwidth axis (more load -> higher BW,
# higher latency), so plotting each run's mean/p50/p99 latency against its
# achieved bandwidth traces the lat x bw curve (Image [a]).
#
# Requirements:
#   * Build WITH latency instrumentation, else no *_latency.log is emitted:
#         ENABLE_LATENCY_MEASURE=ON ./build.sh
#   * Uses --pre-touch (patched binary) to fault pages off the kernel
#     zero-page onto the bound node -> honest read bandwidth.
#
# Note: stride_read has NO latency log in the binary, so it yields a BW
#       number but no lat x bw point (mean/p50/p99 = NA).

set -euo pipefail

# ─── Config ───────────────────────────────────────────────────────────
MEMBIND=${MEMBIND:-1}                 # node under test (1 here; CXL node was 2 earlier)
TOTAL_GIB=${TOTAL_GIB:-1024}          # total working set = 1 TiB, divided across threads
REPS=${REPS:-1}                       # repeats per point (bump for averaging)
TOTAL_MIB=$(( TOTAL_GIB * 1024 ))     # 1048576 MiB for 1 TiB

target_threads=( ${TARGET_THREADS:-1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 28 32 64 128} )   # override via TARGET_THREADS env
target_blocksize=( ${TARGET_BLOCKSIZE:-4096 2097152 536870912} )      # 4 KiB, 2 MiB, 512 MiB; override via TARGET_BLOCKSIZE env
workloads=( ${WORKLOADS:-seq_read random_read zipfian_read stride_read} )   # override via WORKLOADS env

MICROBENCH=./build/microbench
OUTDIR=result/$(date +%Y-%m-%d_%H-%M-%S)_latbw_tsweep
LOG="$OUTDIR/run.log"
SUMMARY="$OUTDIR/summary.csv"
CHECK_CMD="./scripts/setup.sh --check"

# ─── Pre-flight ───────────────────────────────────────────────────────
[[ -x "$MICROBENCH" ]] || { echo "Build first: ENABLE_LATENCY_MEASURE=ON ./build.sh"; exit 1; }

echo "Disabling AutoNUMA.."
echo 0 | sudo tee /proc/sys/kernel/numa_balancing > /dev/null   # (was a broken 'sudo echo 0 >')
$CHECK_CMD
#echo "Disabling Swap disk setting.."
#sudo swapoff -a

mkdir -p "$OUTDIR"
echo "workload,block_size,threads,per_thread_mib,total_mib,rep,bw_gbps,mean_ns,p50_ns,p99_ns,samples" > "$SUMMARY"

echo "Begin [zero-page disabled via --pre-touch | hugepage disabled] bench..." | tee -a "$LOG"
echo "Total working set: ${TOTAL_GIB} GiB on node ${MEMBIND}, swept across threads {${target_threads[*]}}" | tee -a "$LOG"

# ─── Sweep: block_size × threads × workload × reps ────────────────────
for bs in "${target_blocksize[@]}"; do
  for t in "${target_threads[@]}"; do
    # The piece that was missing: 1 TiB divided into the thread count.
    per_thread_mib=$(( TOTAL_MIB / t ))
    total_mib=$(( per_thread_mib * t ))
    for w in "${workloads[@]}"; do
      for rep in $(seq 1 "$REPS"); do
        stem="${w}_bs${bs}_t${t}_rep${rep}"
        run_dir="$OUTDIR/$stem"
        mkdir -p "$run_dir"
        echo -e "\n── ${stem}  (per-thread=${per_thread_mib} MiB, total=${total_mib} MiB)" | tee -a "$LOG"

        sudo "$MICROBENCH" \
          --mode              "$w"              \
          --threads           "$t"              \
          --memory-per-thread "$per_thread_mib" \
          --block-size        "$bs"             \
          --membind           "$MEMBIND"        \
          --pre-touch                           \
          --result-dir        "$run_dir"        \
          2>&1 | tee "$run_dir/stdout.log" | tee -a "$LOG" \
          || { echo "  !! run failed, continuing" | tee -a "$LOG"; }

        # ─── Aggregate this point into summary.csv ───────────────────
        # Read/Write modes print "Read Bandwidth:  X.XX GB/s (Y.YY ms)".
        bw=$(grep -m1 -E '^(Read|Write) Bandwidth:' "$run_dir/stdout.log" 2>/dev/null | awk '{print $3}')
        bw=${bw:-NA}

        lat_log="$run_dir/${w}_latency.log"      # stride_read emits none
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

        echo "$w,$bs,$t,$per_thread_mib,$total_mib,$rep,$bw,$mean,$p50,$p99,$n" | tee -a "$SUMMARY"
      done
    done
  done
done

# ─── Done ─────────────────────────────────────────────────────────────
echo
echo "Per-run dirs:  $OUTDIR/<workload>_bs<bs>_t<threads>_rep<n>/"
echo "Summary CSV:   $SUMMARY"
echo
echo "lat x bw figure:  x = bw_gbps,  y = mean_ns (or p50_ns/p99_ns)."
echo "  One curve per (workload, block_size); the points along it are the thread counts."
echo "  stride_read has BW but NA latency (no latency log in the binary)."
