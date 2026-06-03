#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# One-shot AFK runner for a CXL device in DevDAX mode (/dev/dax1.0, > 1 TiB).
# Runs every benchmark/metric we discussed, hard-capped at ~MAX_HOURS:
#
#   Phase A  pointer_chase 2-D sweep  → lat×BW figure  AND  latency CDF
#   Phase B  run_benchmark --small/--large-mem → CXL cache hit vs miss
#   Phase C  access-pattern bandwidth sweep → BW per workload × block × threads
#
# Designed to start and walk away:
#   * Hard wall-clock deadline (MAX_HOURS); never launches work past it.
#   * Per-run `timeout` so one hung run can't eat the budget.
#   * No `set -e` — an individual failure is logged and the sweep continues.
#   * Incremental CSVs — a deadline cutoff keeps everything finished so far.
#   * Phases ordered cheap→expensive so truncation drops the least value.
#
# Usage (AFK):
#   ENABLE_LATENCY_MEASURE=ON ./build.sh         # once
#   CPU_LIST=0 ./run_all_devdax.sh                # in tmux; CPU_LIST = CXL-local NUMA node(s)
#
# Knobs (env overrides): DEVDAX MAX_HOURS PER_RUN_TIMEOUT CPU_LIST PHASE_C_GIB
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail

# ─── Knobs ────────────────────────────────────────────────────────────
DEVDAX=${DEVDAX:-/dev/dax1.0}
MAX_HOURS=${MAX_HOURS:-4}
PER_RUN_TIMEOUT=${PER_RUN_TIMEOUT:-600}       # 10 min hard cap per microbench run
CPU_LIST=${CPU_LIST:-}                        # NUMA NODE number(s) of the CXL-local socket, comma-sep (e.g. "0" or "0,1"); NOT core IDs, NOT ranges. Empty = no pinning.
PHASE_C_GIB=${PHASE_C_GIB:-512}               # working set for the BW sweep (Phase C)
MICROBENCH=./build/microbench

# pointer_chase grid (Phase A) — dense thread sweep (22 counts: 1–16, then 28/32/48/64/96/128)
PC_THREADS=( 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 28 32 48 64 96 128 )
PC_DELAYS=( 100000 30000 10000 3000 1000 500 200 100 50 20 10 0 )
# bandwidth grid (Phase C) — reads first, then writes (writes have no latency log → NA)
BW_WORKLOADS=( seq_read random_read zipfian_read stride_read
               seq_write random_write stride_write )
BW_BLOCKSIZES=( 4096 2097152 536870912 )      # 4 KiB, 2 MiB, 512 MiB
BW_THREADS=( 8 16 32 )

# run_benchmark.py hardcodes FLUSH_OFFSET = 1 TiB; a flush run needs
# offset(1 TiB) + working_set to fit in the namespace.
FLUSH_OFFSET_MIB=1048576
SMALL_WS_MIB=$(( 8192  * 16 ))                 # --small-mem  = 128 GiB
LARGE_WS_MIB=$(( 65536 * 16 ))                 # --large-mem  = 1 TiB

# ─── Bookkeeping ──────────────────────────────────────────────────────
START=$(date +%s)
BUDGET=$(( MAX_HOURS * 3600 ))
DEADLINE=$(( START + BUDGET ))
# Per-phase budget slices so a dense Phase A can't starve B and C.
A_END=$(( START + BUDGET * 50 / 100 ))   # pointer_chase  ≤ 50% of budget
B_END=$(( START + BUDGET * 80 / 100 ))   # hit/miss       ≤ next 30%
# Phase C gets the remainder, up to DEADLINE (100%).
STAMP=$(date +%Y-%m-%d_%H-%M-%S)
ROOT=result/${STAMP}_devdax_afk
LOG="$ROOT/run.log"
mkdir -p "$ROOT"

log(){ echo "[$(date +%H:%M:%S) +$(( ($(date +%s)-START)/60 ))m] $*" | tee -a "$LOG"; }
have_until(){ local until=$1 need=${2:-120}; (( until - $(date +%s) > need )); }  # >need s left in this phase?

# Failure/timeout-tolerant microbench launcher (sudo timeout runs as root).
mb(){ # args after a run_dir: passed to microbench
  local rc
  sudo timeout --kill-after=30 "$PER_RUN_TIMEOUT" "$MICROBENCH" "$@"; rc=$?
  (( rc == 124 )) && log "  TIMEOUT(${PER_RUN_TIMEOUT}s)"
  (( rc != 0 && rc != 124 )) && log "  FAILED rc=$rc"
  return 0
}

# mean p50 p99 n  from a *_latency.log (NA if absent/empty)
agg_lat(){
  local f="$1"
  if [[ -s "$f" ]]; then
    grep -v '^#' "$f" | awk 'NF==1 && $1+0==$1' | sort -n | awk '
      { v[NR]=$1; s+=$1 }
      END { n=NR; if(n==0){print "NA NA NA 0";exit}
            printf "%.2f %.2f %.2f %d\n", s/n, v[int(n*0.5)], v[int(n*0.99)], n }'
  else
    echo "NA NA NA 0"
  fi
}

SUDO_KEEPALIVE_PID=""
trap 'kill "${SUDO_KEEPALIVE_PID:-}" 2>/dev/null; log "EXIT — elapsed $(( ($(date +%s)-START)/60 )) min. Results under $ROOT"; finalize 2>/dev/null' EXIT

# ─── Pre-flight ───────────────────────────────────────────────────────
[[ -x "$MICROBENCH" ]] || { echo "Build first: ENABLE_LATENCY_MEASURE=ON ./build.sh"; exit 1; }
[[ -e "$DEVDAX"     ]] || { echo "Missing $DEVDAX — sudo ./scripts/setup.sh --with-cxl --devdax $(basename "$DEVDAX")"; exit 1; }

HAS_LAT=0
strings "$MICROBENCH" 2>/dev/null | grep -q "Latency log saved" && HAS_LAT=1
(( HAS_LAT )) || log "WARNING: binary built WITHOUT ENABLE_LATENCY_MEASURE — Phase A/C will have BW only, NO latency/CDF. Rebuild: ENABLE_LATENCY_MEASURE=ON ./build.sh"

# Namespace size → decide which hit/miss sizes can run the 1 TiB flush.
DEV_BYTES=$(daxctl list -d "$(basename "$DEVDAX")" 2>/dev/null | grep -m1 -oE '"size":[0-9]+' | grep -oE '[0-9]+' || true)
DEV_MIB=$(( ${DEV_BYTES:-0} / 1024 / 1024 ))
log "Device $DEVDAX = ${DEV_MIB:-unknown} MiB | budget ${MAX_HOURS}h (A≤50% / B≤30% / C rest) | cpu=${CPU_LIST:-none}"

# Acquire sudo once (tmux: type the password here), then keep the credential
# warm for the whole run so no later sudo blocks on a prompt while you're AFK.
sudo -v || { echo "Need sudo to run microbench. Aborting."; exit 1; }
( while true; do sudo -n -v 2>/dev/null || exit; sleep 60; done ) &
SUDO_KEEPALIVE_PID=$!
log "sudo credential acquired; keep-alive PID=$SUDO_KEEPALIVE_PID"

echo 0 | sudo tee /proc/sys/kernel/numa_balancing > /dev/null 2>&1 || true   # harmless on devdax
log "AutoNUMA balancing disabled (no-op for devdax)."

# ═══════════════════════ Phase A — pointer_chase ══════════════════════
# lat×BW (mean vs bw) AND per-thread CDF (pc_t*_d0). 22 thread counts × 12 delays.
PC_DIR="$ROOT/pointer_chase"; mkdir -p "$PC_DIR"
PC_CSV="$PC_DIR/summary.csv"
echo "threads,delay_cycles,bw_gbps,mean_ns,p50_ns,p99_ns,samples" > "$PC_CSV"
log "═══ Phase A: pointer_chase sweep ($(( ${#PC_THREADS[@]} * ${#PC_DELAYS[@]} )) points) ═══"

for t in "${PC_THREADS[@]}"; do
  per=$(( (128*1024) / t ))                      # 128 GiB working set: 2 GiB chase + load
  (( per * t >= 2048 )) || { log "  skip t=$t (< 2 GiB total)"; continue; }
  for d in "${PC_DELAYS[@]}"; do
    have_until "$A_END" 120 || { log "  Phase A budget (50%) spent — moving on"; break 2; }
    stem="pc_t${t}_d${d}"; rd="$PC_DIR/$stem"; mkdir -p "$rd"
    log "  $stem (per-thread=${per} MiB, delay=${d})"
    mb --mode pointer_chase --threads "$t" --memory-per-thread "$per" \
       --devdax "$DEVDAX" --offset 0 ${CPU_LIST:+--cpu-affinity "$CPU_LIST"} \
       --inject-delay "$d" --result-dir "$rd" 2>&1 | tee "$rd/stdout.log" >> "$LOG"
    bw=$(grep -m1 -E '^Load Bandwidth:' "$rd/stdout.log" 2>/dev/null | awk '{print $3}'); bw=${bw:-NA}
    read m p50 p99 n <<<"$(agg_lat "$rd/pointer_chase_latency.log")"
    echo "$t,$d,$bw,$m,$p50,$p99,$n" >> "$PC_CSV"
  done
done
log "Phase A done → $PC_CSV"

# ═══════════════════════ Phase B — hit / miss ═════════════════════════
# run_benchmark.py: --small-mem (cache hit) and --large-mem (cache miss).
# On devdax the FLUSH_OFFSET trick is real, but needs offset+ws ≤ namespace.
log "═══ Phase B: cache hit/miss (run_benchmark.py) ═══"
run_hitmiss(){ # $1=flag  $2=ws_mib  $3=label
  local flag="$1" ws="$2" label="$3" need=$(( FLUSH_OFFSET_MIB + ws ))
  have_until "$B_END" 600 || { log "  Phase B budget spent — skipping $label"; return; }
  if (( DEV_MIB > 0 && need > DEV_MIB )); then
    log "  SKIP $label: needs ${need} MiB (1 TiB flush + ws) but device is ${DEV_MIB} MiB"
    return
  fi
  local budget=$(( B_END - $(date +%s) - 60 )); (( budget < 60 )) && return
  log "  $label ($flag) — up to $(( budget/60 )) min"
  timeout --kill-after=30 "$budget" sudo python3 scripts/run_benchmark.py \
      --devdax "$DEVDAX" "$flag" 2>&1 | tee -a "$LOG"
}
run_hitmiss --small-mem "$SMALL_WS_MIB" "hit (small-mem 128 GiB)"
run_hitmiss --large-mem "$LARGE_WS_MIB" "miss (large-mem 1 TiB)"
log "Phase B done → see result/<timestamp>/ (run_benchmark.py default dir) + summary/<timestamp>/"

# ═══════════════════════ Phase C — bandwidth sweep ════════════════════
# All access patterns × block sizes × thread counts (BW; latency where logged).
BW_DIR="$ROOT/bandwidth"; mkdir -p "$BW_DIR"
BW_CSV="$BW_DIR/summary.csv"
echo "workload,block_size,threads,per_thread_mib,bw_gbps,mean_ns,p50_ns,p99_ns,samples" > "$BW_CSV"
PHASE_C_MIB=$(( PHASE_C_GIB * 1024 ))
log "═══ Phase C: access-pattern BW sweep (${PHASE_C_GIB} GiB working set) ═══"

for bs in "${BW_BLOCKSIZES[@]}"; do
  for t in "${BW_THREADS[@]}"; do
    per=$(( PHASE_C_MIB / t ))
    for w in "${BW_WORKLOADS[@]}"; do
      have_until "$DEADLINE" 120 || { log "  deadline reached — ending Phase C early"; break 3; }
      stem="${w}_bs${bs}_t${t}"; rd="$BW_DIR/$stem"; mkdir -p "$rd"
      log "  $stem (per-thread=${per} MiB)"
      mb --mode "$w" --threads "$t" --memory-per-thread "$per" --block-size "$bs" \
         --devdax "$DEVDAX" --offset 0 ${CPU_LIST:+--cpu-affinity "$CPU_LIST"} \
         --result-dir "$rd" 2>&1 | tee "$rd/stdout.log" >> "$LOG"
      bw=$(grep -m1 -E '^(Read|Write) Bandwidth:' "$rd/stdout.log" 2>/dev/null | awk '{print $3}'); bw=${bw:-NA}
      read m p50 p99 n <<<"$(agg_lat "$rd/${w}_latency.log")"   # stride_read → NA
      echo "$w,$bs,$t,$per,$bw,$m,$p50,$p99,$n" >> "$BW_CSV"
    done
  done
done
log "Phase C done → $BW_CSV"

# ─── Finalize: print where everything is + how to plot ────────────────
finalize(){
  cat > "$ROOT/HOW_TO_PLOT.txt" <<EOF
Results root: $ROOT

[a] lat × BW (Image #1) — average latency vs load bandwidth:
    python3 scripts/inho_parser.py latbw $ROOT/pointer_chase \\
        --title '[CXL devdax] Latency vs BW'     # (plots p50/p99; use the gnuplot
                                                 #  one-liner on summary.csv col 3:4 for mean)

[b] Latency CDF (Image #2) — one curve per thread count at max load:
    python3 scripts/plot_cdf.py $ROOT/pointer_chase/pc_t*_d0 \\
        --title '[CXL devdax] Pointer Chase Latency CDF' --xmax 700

[c] Cache hit vs miss — compare bandwidth between:
    result/<ts>/mem_8192/   (hit, small-mem)   vs   result/<ts>/mem_65536/ (miss, large-mem)
    (run_benchmark.py wrote these + summary/<ts>/benchmark_summary.txt)

[d] Access-pattern bandwidth:
    $ROOT/bandwidth/summary.csv   (workload × block_size × threads)
EOF
  log "Wrote $ROOT/HOW_TO_PLOT.txt"
}
finalize
log "ALL DONE."
