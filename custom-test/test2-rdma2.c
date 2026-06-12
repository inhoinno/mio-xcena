/*
 * mem_chunk_stripe_sweep.c
 *
 * Memory-only example of the mode2-style chunk-stripe populate pattern and a
 * concurrent read-request sweep. No RDMA, no SPDK: just malloc'd memory,
 * pthreads, memcpy, and verification tags.
 *
 * Dataflow added in this version:
 *   N rdma_reader_thread (producers) --mpsc rnic_queue--> 1 rnic_thread (consumer)
 *   rnic_thread simulates NIC bandwidth, stamps expire_time, then signals the
 *   owning reader through dctx->response_ready[req_id].
 *
 * Build:
 *   gcc -O2 -g -std=c11 -Wall -Wextra -pthread mem_chunk_stripe_sweep.c -o a.out
 *
 * Example:
 *   ./a.out --block-mb 32 --req_per_blocks 1024 --rounds 3
 */

#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/mman.h>
#include <math.h>
#include <unistd.h>
#include <stdatomic.h>

#define RING_NAME "RNIC_QUEUE"
#define RING_SIZE 1024

#define DEFAULT_REQUESTS     32u
#define DEFAULT_REQ_PER_BLOCKS 64u
#define DEFAULT_CHUNK_SLOTS  32u
#define DEFAULT_BLOCK_MB     16u
#define DEFAULT_BLOCK_BYTES  (DEFAULT_BLOCK_MB * 1024u * 1024u)
#define DEFAULT_ROUNDS       1u
#define ALIGN_BYTES          4096u
#define BLOCK_MAGIC          UINT64_C(0x4d454d5f43484b31) /* MEM_CHK1 */

#define KiB     (INT64_C(1) << 10)
#define MiB     (INT64_C(1) << 20)
#define GiB     (INT64_C(1) << 30)

#ifndef MAP_HUGE_2MB
#define MAP_HUGE_SHIFT 26
#define MAP_HUGE_2MB    (21 << MAP_HUGE_SHIFT)
#endif
#define HUGEPAGE_2MB (2UL * 1024 * 1024)

#define NAND_SLC_READ 20UL * 1000
#define NAND_SLC_WRITE 200UL * 1000
#define NVME_DEFAULT_MAX_AZ_SIZE (128 * MiB)

#define Interface_PCIeGen3x4_bwmb (4034 * MiB)      /* MB/s */
#define Interface_PCIeGen3x4_bw 4034

#define Interface_RNICGen6x100G_bwmb (12500 * MiB)  /* MB/s */
#define Interface_RNICGen6x100G_bw 12500            /* MB/s */
#define Interface_RNICGen6x200G_bwmb (25000 * MiB)  /* MB/s */
#define Interface_RNICGen6x200G_bw 25000            /* MB/s */

/* === CHANGED: 1 = the READER busy-polls until the simulated expire_time
 * after it receives its completion (NIC bandwidth gates the readers in
 * real time). The rnic_thread itself never waits — it only stamps
 * expire_time and signals. If the queueing/processing path already took
 * longer than the simulated transfer (now >= expire_time), the reader
 * does not wait at all.
 * 0 = expire_time is recorded but nobody waits on it. */
#define RNIC_SIMULATE_DELAY 1

/* === CHANGED: polling primitive for accurate link simulation.
 * sched_yield() hands the CPU to the scheduler — wakeup latency is
 * unbounded (µs to ms), which distorts both dequeue latency and the
 * expire_time deadline. A pause/yield *instruction* keeps the thread
 * on-CPU spinning, so the polling granularity is nanoseconds. */
#if defined(__x86_64__) || defined(__i386__)
# define cpu_relax() __builtin_ia32_pause()
#elif defined(__aarch64__) || defined(__arm__)
# define cpu_relax() __asm__ __volatile__("yield" ::: "memory")
#else
# define cpu_relax() __asm__ __volatile__("" ::: "memory")
#endif

enum custom_ring_type {
    FEMU_RING_TYPE_SP_SC,   /* Single-producer, single-consumer */
    FEMU_RING_TYPE_MP_SC,   /* Multi-producer, single-consumer */
    FEMU_RING_TYPE_MP_MC,   /* Multi-producer, multi-consumer */
};

/* 1. FORWARD DECLARATIONS */
struct mpsc_node;
struct mpsc_queue;
struct cfg;
struct rdma_req;
struct nic;
struct device_ctx;
struct worker_arg;

static void *rnic_thread(void *arg);   /* === CHANGED: fwd decl, used in run_one_sweep */

/* 2. QUEUE STRUCTS */
struct mpsc_node {
    _Atomic(struct mpsc_node *) next;  /* === CHANGED: must be _Atomic — both
                                        * producers and the consumer touch it
                                        * with atomic_store/atomic_load */
    struct rdma_req *req;              /* The payload */
};

struct mpsc_queue {
    _Atomic(struct mpsc_node *) head;  /* consumer end */
    _Atomic(struct mpsc_node *) tail;  /* producers XCHG here */
    struct mpsc_node stub;             /* sentinel node */
};

struct cfg {
    uint32_t requests;
    uint32_t req_per_blocks;
    uint32_t chunk_slots;
    uint32_t block_bytes;
    uint32_t rounds;
    uint32_t sweep_min;
    uint32_t sweep_max;
    bool verify;
    bool copy_to_dst;
    char csv_path[256];
};

struct rdma_req {
    uint64_t req_id;
    double stime;       /* ns */
    double expire_time; /* ns */
};

struct block_tag {
    uint64_t magic;
    uint64_t request_id;
    uint64_t block_id;
    uint64_t fill_byte;
};

struct start_gate {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    uint32_t total;
    uint32_t ready;
    bool open;
};

struct sweep_ctx {
    const struct cfg *cfg;
    const uint8_t *store;
    uint64_t chunks_per_block;
    uint32_t active_requests;
    atomic_uint_fast64_t blocks_read;
    atomic_uint_fast64_t verify_fail;
    atomic_uint_fast64_t checksum;
    struct start_gate start;
};

struct worker_arg {
    struct sweep_ctx *ctx;
    struct device_ctx *dctx;
    uint8_t *scratch;
    uint32_t request_id;
    struct rdma_req *rdma_req;

    struct mpsc_queue *to_nic;     /* MPSC: many readers -> 1 RNIC (shared ptr) */
    struct mpsc_node *node_pool;   /* pre-allocated node for this worker */
};

double lag = 0;

typedef struct nic {
    uint64_t bw;        /* MB/s */
    double stime;
    double ntime;       /* next time the NIC is free (ns) */
    bool busy;
} nic;

struct device_ctx {
    uint64_t bw;
    double stime;
    double ntime;
    bool busy;
    bool dataplane_started;

    const struct cfg *cfg;             /* === CHANGED: rnic needs bytes/request */
    uint32_t num_readers;
    struct worker_arg **reader_args;

    /* Single queue instance for the RNIC thread to consume from */
    struct mpsc_queue rnic_queue;

    /* RNIC -> readers signalling: one flag per request_id.
     * The result itself lives in the reader's own rdma_req; the
     * release-store on the flag publishes expire_time to the reader. */
    _Atomic(struct rdma_req *) *responses;  /* (unused for now) */
    _Atomic(bool) *response_ready;          /* flag by request_id */

    struct nic *nic_bandwidth_simulation;
};

/* ------------------------------------------------------------------ */
/* MPSC queue (Vyukov)                                                 */
/* ------------------------------------------------------------------ */

static void mpsc_init(struct mpsc_queue *q)
{
    atomic_store_explicit(&q->stub.next, NULL, memory_order_relaxed);
    atomic_store(&q->head, &q->stub);
    atomic_store(&q->tail, &q->stub);
}

/* Enqueue (multi-producer safe) */
static bool mpsc_enqueue(struct mpsc_queue *q, struct mpsc_node *node)
{
    atomic_store_explicit(&node->next, NULL, memory_order_relaxed);

    /* Swap the node into the tail position atomically */
    struct mpsc_node *prev_tail =
        atomic_exchange_explicit(&q->tail, node, memory_order_acq_rel);

    /* Link the previous tail to the new node */
    atomic_store_explicit(&prev_tail->next, node, memory_order_release);

    return true;
}

/* Dequeue (single-consumer only). NULL means "empty or a producer is
 * mid-insert" — always just retry later, never treat it as an error.
 *
 * === CHANGED (CRITICAL FIX): the previous version could never dequeue the
 * LAST node in the queue. Once head advanced off the stub onto the final
 * node, next==NULL and head==tail, and it returned NULL forever — every
 * reader then spins on response_ready[] for a request the RNIC never sees.
 * The Vyukov scheme requires re-inserting the stub at that point so the
 * last real node becomes poppable. */
static struct mpsc_node *mpsc_dequeue(struct mpsc_queue *q)
{
    struct mpsc_node *head = atomic_load_explicit(&q->head, memory_order_relaxed);
    struct mpsc_node *next = atomic_load_explicit(&head->next, memory_order_acquire);

    if (head == &q->stub) {
        if (next == NULL)
            return NULL;                /* queue is empty */
        atomic_store_explicit(&q->head, next, memory_order_relaxed);
        head = next;
        next = atomic_load_explicit(&head->next, memory_order_acquire);
    }

    if (next != NULL) {
        atomic_store_explicit(&q->head, next, memory_order_relaxed);
        return head;
    }

    /* head looks like the last node: confirm no producer is mid-insert */
    struct mpsc_node *tail = atomic_load_explicit(&q->tail, memory_order_acquire);
    if (head != tail)
        return NULL;                    /* producer between XCHG and link: retry */

    /* Re-arm the stub so 'head' (the last real node) gets a successor
     * and can be popped. */
    mpsc_enqueue(q, &q->stub);

    next = atomic_load_explicit(&head->next, memory_order_acquire);
    if (next != NULL) {
        atomic_store_explicit(&q->head, next, memory_order_relaxed);
        return head;
    }
    return NULL;
}

/* ------------------------------------------------------------------ */

static void usage(const char *prog)
{
    fprintf(stderr,
        "Usage: %s [options]\n"
        "\n"
        "Memory-only chunk-stripe populate + concurrent read sweep.\n"
        "\n"
        "Options:\n"
        "  --requests N       max logical read requests to populate/sweep [default %u]\n"
        "  --req_per_blocks N blocks per logical request [default %u]\n"
        "  --req-per-blocks N alias for --req_per_blocks\n"
        "  --blocks N         alias for --req_per_blocks\n"
        "  --req-blocks N     alias for --req_per_blocks\n"
        "  --chunk-slots N    request slots per chunk [default %u]\n"
        "  --block-mb N       MiB per block [default %u]\n"
        "  --block-bytes N    exact bytes per block; supports K/M/G\n"
        "                     (mainly for small smoke tests)\n"
        "  --rounds N         repeat the full sweep N times [default %u]\n"
        "  --sweep-min N      first power-of-two read concurrency [default 1]\n"
        "  --sweep-max N      last power-of-two read concurrency [default requests]\n"
        "  --no-verify        skip block tag verification\n"
        "  --no-copy          do not memcpy into scratch; only touch source\n"
        "  --csv PATH         write sweep rows as CSV\n"
        "  --help             show this help\n",
        prog, DEFAULT_REQUESTS, DEFAULT_REQ_PER_BLOCKS, DEFAULT_CHUNK_SLOTS,
        DEFAULT_BLOCK_MB, DEFAULT_ROUNDS);
}

static uint64_t parse_size(const char *s, bool *ok)
{
    errno = 0;
    char *end = NULL;
    uint64_t v = strtoull(s, &end, 10);
    if (errno || end == s) {
        *ok = false;
        return 0;
    }

    if (*end) {
        uint64_t mul = 1;
        if ((end[0] == 'k' || end[0] == 'K') && end[1] == '\0') {
            mul = 1024ULL;
        } else if ((end[0] == 'm' || end[0] == 'M') && end[1] == '\0') {
            mul = 1024ULL * 1024ULL;
        } else if ((end[0] == 'g' || end[0] == 'G') && end[1] == '\0') {
            mul = 1024ULL * 1024ULL * 1024ULL;
        } else {
            *ok = false;
            return 0;
        }
        if (v > UINT64_MAX / mul) {
            *ok = false;
            return 0;
        }
        v *= mul;
    }

    *ok = true;
    return v;
}

static int parse_u32_arg(const char *name, const char *value, uint32_t *out)
{
    bool ok = false;
    uint64_t v = parse_size(value, &ok);
    if (!ok || v == 0 || v > UINT32_MAX) {
        fprintf(stderr, "bad %s: %s\n", name, value);
        return -1;
    }
    *out = (uint32_t)v;
    return 0;
}

static int parse_args(int argc, char **argv, struct cfg *cfg)
{
    cfg->requests = DEFAULT_REQUESTS;
    cfg->req_per_blocks = DEFAULT_REQ_PER_BLOCKS;
    cfg->chunk_slots = DEFAULT_CHUNK_SLOTS;
    cfg->block_bytes = DEFAULT_BLOCK_BYTES;
    cfg->rounds = DEFAULT_ROUNDS;
    cfg->sweep_min = 1;
    cfg->sweep_max = 0; /* filled after parsing */
    cfg->verify = true;
    cfg->copy_to_dst = true;
    cfg->csv_path[0] = '\0';

    enum {
        OPT_REQUESTS = 1000,
        OPT_REQ_PER_BLOCKS,
        OPT_BLOCKS,
        OPT_REQ_BLOCKS,
        OPT_CHUNK_SLOTS,
        OPT_BLOCK_MB,
        OPT_BLOCK_BYTES,
        OPT_ROUNDS,
        OPT_SWEEP_MIN,
        OPT_SWEEP_MAX,
        OPT_NO_VERIFY,
        OPT_NO_COPY,
        OPT_CSV,
        OPT_HELP,
    };

    static const struct option opts[] = {
        {"requests", required_argument, NULL, OPT_REQUESTS},
        {"req_per_blocks", required_argument, NULL, OPT_REQ_PER_BLOCKS},
        {"req-per-blocks", required_argument, NULL, OPT_REQ_PER_BLOCKS},
        {"blocks", required_argument, NULL, OPT_BLOCKS},
        {"req-blocks", required_argument, NULL, OPT_REQ_BLOCKS},
        {"chunk-slots", required_argument, NULL, OPT_CHUNK_SLOTS},
        {"block-mb", required_argument, NULL, OPT_BLOCK_MB},
        {"block-bytes", required_argument, NULL, OPT_BLOCK_BYTES},
        {"rounds", required_argument, NULL, OPT_ROUNDS},
        {"sweep-min", required_argument, NULL, OPT_SWEEP_MIN},
        {"sweep-max", required_argument, NULL, OPT_SWEEP_MAX},
        {"no-verify", no_argument, NULL, OPT_NO_VERIFY},
        {"no-copy", no_argument, NULL, OPT_NO_COPY},
        {"csv", required_argument, NULL, OPT_CSV},
        {"help", no_argument, NULL, OPT_HELP},
        {NULL, 0, NULL, 0},
    };

    for (;;) {
        int c = getopt_long(argc, argv, "", opts, NULL);
        if (c == -1) break;

        switch (c) {
        case OPT_REQUESTS:
            if (parse_u32_arg("--requests", optarg, &cfg->requests)) return -1;
            break;
        case OPT_REQ_PER_BLOCKS:
            if (parse_u32_arg("--req_per_blocks", optarg, &cfg->req_per_blocks)) return -1;
            break;
        case OPT_BLOCKS:
            if (parse_u32_arg("--blocks", optarg, &cfg->req_per_blocks)) return -1;
            break;
        case OPT_REQ_BLOCKS:
            if (parse_u32_arg("--req-blocks", optarg, &cfg->req_per_blocks)) return -1;
            break;
        case OPT_CHUNK_SLOTS:
            if (parse_u32_arg("--chunk-slots", optarg, &cfg->chunk_slots)) return -1;
            break;
        case OPT_BLOCK_MB: {
            uint32_t mb = 0;
            if (parse_u32_arg("--block-mb", optarg, &mb)) return -1;
            if (mb > UINT32_MAX / (1024u * 1024u)) {
                fprintf(stderr, "bad --block-mb: %s\n", optarg);
                return -1;
            }
            cfg->block_bytes = mb * 1024u * 1024u;
            break;
        }
        case OPT_BLOCK_BYTES:
            if (parse_u32_arg("--block-bytes", optarg, &cfg->block_bytes)) return -1;
            break;
        case OPT_ROUNDS:
            if (parse_u32_arg("--rounds", optarg, &cfg->rounds)) return -1;
            break;
        case OPT_SWEEP_MIN:
            if (parse_u32_arg("--sweep-min", optarg, &cfg->sweep_min)) return -1;
            break;
        case OPT_SWEEP_MAX:
            if (parse_u32_arg("--sweep-max", optarg, &cfg->sweep_max)) return -1;
            break;
        case OPT_NO_VERIFY:
            cfg->verify = false;
            break;
        case OPT_NO_COPY:
            cfg->copy_to_dst = false;
            break;
        case OPT_CSV:
            snprintf(cfg->csv_path, sizeof(cfg->csv_path), "%s", optarg);
            break;
        case OPT_HELP:
            usage(argv[0]);
            exit(0);
        default:
            usage(argv[0]);
            return -1;
        }
    }

    if (cfg->sweep_max == 0) cfg->sweep_max = cfg->requests;
    if (cfg->sweep_min == 0) cfg->sweep_min = 1;
    if (cfg->sweep_min > cfg->sweep_max || cfg->sweep_max > cfg->requests) {
        fprintf(stderr, "bad sweep range: min=%u max=%u requests=%u\n",
                cfg->sweep_min, cfg->sweep_max, cfg->requests);
        return -1;
    }
    if (cfg->block_bytes < sizeof(struct block_tag) + 1) {
        fprintf(stderr, "--block-bytes must be at least %zu\n",
                sizeof(struct block_tag) + 1);
        return -1;
    }
    return 0;
}

static bool checked_mul_u64(uint64_t a, uint64_t b, uint64_t *out)
{
    if (a != 0 && b > UINT64_MAX / a) return false;
    *out = a * b;
    return true;
}

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static double now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e9 + (double)ts.tv_nsec;
}

static void gate_init(struct start_gate *g, uint32_t total)
{
    pthread_mutex_init(&g->mu, NULL);
    pthread_cond_init(&g->cv, NULL);
    g->total = total;
    g->ready = 0;
    g->open = false;
}

static void gate_wait(struct start_gate *g)
{
    pthread_mutex_lock(&g->mu);
    g->ready++;
    if (g->ready == g->total)
        pthread_cond_broadcast(&g->cv);
    while (!g->open)
        pthread_cond_wait(&g->cv, &g->mu);
    pthread_mutex_unlock(&g->mu);
}

static void gate_open(struct start_gate *g)
{
    pthread_mutex_lock(&g->mu);
    while (g->ready < g->total)
        pthread_cond_wait(&g->cv, &g->mu);
    g->open = true;
    pthread_cond_broadcast(&g->cv);
    pthread_mutex_unlock(&g->mu);
}

static void gate_open_after_created(struct start_gate *g, uint32_t created)
{
    pthread_mutex_lock(&g->mu);
    g->total = created;
    while (g->ready < g->total)
        pthread_cond_wait(&g->cv, &g->mu);
    g->open = true;
    pthread_cond_broadcast(&g->cv);
    pthread_mutex_unlock(&g->mu);
}

static void gate_destroy(struct start_gate *g)
{
    pthread_mutex_destroy(&g->mu);
    pthread_cond_destroy(&g->cv);
}

static uint8_t fill_for(uint64_t req, uint64_t blk)
{
    return (uint8_t)(0xa5u ^ (uint8_t)(req * 31u) ^ (uint8_t)(blk * 17u));
}

static void fill_block(uint8_t *dst, uint32_t block_bytes,
                       uint64_t req, uint64_t blk)
{
    uint8_t fill = fill_for(req, blk);
    struct block_tag tag = {
        .magic = BLOCK_MAGIC,
        .request_id = req,
        .block_id = blk,
        .fill_byte = fill,
    };
    memset(dst, fill, block_bytes);
    memcpy(dst, &tag, sizeof(tag));
}

static bool verify_block(const uint8_t *src, uint32_t block_bytes,
                         uint64_t req, uint64_t blk)
{
    struct block_tag tag;
    memcpy(&tag, src, sizeof(tag));
    uint8_t fill = fill_for(req, blk);
    return tag.magic == BLOCK_MAGIC &&
           tag.request_id == req &&
           tag.block_id == blk &&
           tag.fill_byte == fill &&
           src[block_bytes - 1] == fill;
}

/* HUGE working */
static uint8_t *xaligned_alloc(size_t bytes)
{
    void *p = NULL;
    size_t len = (bytes + HUGEPAGE_2MB - 1) & ~(HUGEPAGE_2MB - 1);
    int rc = posix_memalign(&p, HUGEPAGE_2MB, len);
    if (rc != 0) {                      /* === CHANGED: restore the rc check;
                                         * posix_memalign never sets p to
                                         * MAP_FAILED, it returns an errno */
        errno = rc;
        return NULL;
    }
    madvise(p, len, MADV_HUGEPAGE);
    return p;
}

static const uint8_t *block_ptr(const struct cfg *cfg, const uint8_t *store,
                                uint64_t chunks_per_block,
                                uint64_t req, uint64_t blk)
{
    uint64_t chunk = blk * chunks_per_block + req / cfg->chunk_slots;
    uint64_t slot = req % cfg->chunk_slots;
    uint64_t off = chunk * (uint64_t)cfg->chunk_slots * cfg->block_bytes +
                   slot * (uint64_t)cfg->block_bytes;
    return store + off;
}

static int populate_chunk_stripe(const struct cfg *cfg, uint8_t *store,
                                 uint64_t chunks_per_block,
                                 uint64_t chunk_bytes)
{
    uint64_t chunk_id = 0;
    for (uint64_t blk = 0; blk < cfg->req_per_blocks; blk++) {
        for (uint64_t group = 0; group < chunks_per_block; group++) {
            uint8_t *chunk = store + chunk_id * chunk_bytes;
            memset(chunk, 0, (size_t)chunk_bytes);
            for (uint64_t slot = 0; slot < cfg->chunk_slots; slot++) {
                uint64_t req = group * cfg->chunk_slots + slot;
                if (req >= cfg->requests) continue;
                fill_block(chunk + slot * (uint64_t)cfg->block_bytes,
                           cfg->block_bytes, req, blk);
            }
            chunk_id++;
        }
    }

    return 0;
}

/* ------------------------------------------------------------------ */
/* Producer: reader thread                                             */
/* ------------------------------------------------------------------ */

static void *rdma_reader_thread(void *arg)
{
    struct worker_arg *wa = arg;
    struct sweep_ctx *ctx = wa->ctx;
    const struct cfg *cfg = ctx->cfg;

    struct rdma_req *rdma_req = wa->rdma_req;
    struct device_ctx *dctx = wa->dctx;

    uint64_t local_blocks = 0;
    uint64_t local_fail = 0;
    uint64_t local_sum = 0;
    uint64_t req = wa->request_id;
    rdma_req->req_id = req;

    double t0 = 0;
    double t1 = 0;

    t0 = now_ns();
    rdma_req->stime = t0;
    rdma_req->expire_time = rdma_req->stime;

    /* 1. Enqueue to RNIC (lock-free). Re-use the pre-allocated node for
     * this worker — no malloc in the hot path. */
    wa->node_pool->req = rdma_req;
    mpsc_enqueue(wa->to_nic, wa->node_pool);
    /* fprintf(stderr, "Thread %lu sent NIC, wait (now %.2f)\n", req, now_ns()); */

    /* 2. Wait for the RNIC response (poll the per-request flag).
     * The acquire here pairs with the rnic_thread's release-store, so
     * rdma_req->expire_time written by the RNIC is visible after this. */
    while (!atomic_load_explicit(&dctx->response_ready[req], memory_order_acquire)) {
        cpu_relax();
    }

// #if RNIC_SIMULATE_DELAY
//     /* === CHANGED: 3. The READER enforces the simulated link deadline.
//      * The RNIC only stamped expire_time; the transfer "completes" here.
//      * If the queueing/signalling path already took longer than the
//      * simulated transfer (now >= expire_time), this loop never runs —
//      * the reader does not wait. */
//     while (now_ns() < rdma_req->expire_time)
//         cpu_relax();
// #endif
    t1 = now_ns();
    (void)t1;
    /* fprintf(stderr, "Thread %" PRIu64 " queuing+nic %.2lf us (lat %.2f us)\n",
            req, (t1 - t0) / 1000.0,
            (rdma_req->expire_time - rdma_req->stime) / 1000.0); */

    gate_wait(&ctx->start);
    for (uint64_t blk = 0; blk < cfg->req_per_blocks; blk++) {
        const uint8_t *src = block_ptr(cfg, ctx->store,
                                       ctx->chunks_per_block, req, blk);
        const uint8_t *check = src;
        if (cfg->copy_to_dst) {
            memcpy(wa->scratch, src, cfg->block_bytes);
            check = wa->scratch;
        }
        local_sum += (uint64_t)check[0] + check[cfg->block_bytes - 1];
        if (cfg->verify && !verify_block(check, cfg->block_bytes, req, blk))
            local_fail++;
        local_blocks++;
    }

    atomic_fetch_add_explicit(&ctx->blocks_read, local_blocks,
                              memory_order_relaxed);
    atomic_fetch_add_explicit(&ctx->verify_fail, local_fail,
                              memory_order_relaxed);
    atomic_fetch_add_explicit(&ctx->checksum, local_sum, memory_order_relaxed);
    return NULL;
}

/* ------------------------------------------------------------------ */
/* === CHANGED: Consumer: single RNIC thread (timestamp-only)          */
/*                                                                     */
/* Dequeues rdma_reqs from the shared MPSC queue, models a serialized  */
/* bandwidth-limited link via ntime accumulation, stamps expire_time,  */
/* and signals the owner immediately. The RNIC NEVER sleeps or waits:  */
/* the empty-queue path is a pure poll (cpu_relax), and the simulated  */
/* transfer delay is enforced by the READER, not here — so the RNIC    */
/* can keep draining the queue and timestamping requests back-to-back  */
/* while earlier readers are still waiting out their transfers.        */
/* Exits after exactly num_readers requests (one per reader/sweep).    */
/* ------------------------------------------------------------------ */

static void *rnic_thread(void *arg)
{
    struct device_ctx *dctx = arg;
    const struct cfg *cfg = dctx->cfg;
    struct nic *n = dctx->nic_bandwidth_simulation;

    /* bytes the NIC must move per logical request */
    const uint64_t req_bytes = (uint64_t)cfg->req_per_blocks * cfg->block_bytes;
    /* service time in ns:  bytes / (bw MB/s)  ==  bytes * 1000 / bw  ns */
    const double service_ns = (double)req_bytes * 1000.0 / (double)n->bw;
    const double delta_time_ns = service_ns;

    /* link timeline starts when the dataplane comes up, not at 0 —
     * otherwise the first request would see a bogus multi-second idle gap */
    n->ntime = now_ns();

    uint32_t done = 0;

    while (done < dctx->num_readers) {
        struct mpsc_node *node = mpsc_dequeue(&dctx->rnic_queue);
        if (node == NULL) {
            cpu_relax();    /* poll — stay on-CPU for accurate dequeue latency */
            continue;
        }

        struct rdma_req *req = node->req;
        double now = now_ns();
        req->stime = now;
        req->expire_time = now;

        /* ---- calculate: serialized link occupancy ---- */
        if (true){ 
            if (n->ntime < req->stime ){
                lag=0;
                n->stime = req->stime;
                //High bandwidth = large window , Small bandwidth = small window
                n->ntime = n->stime + 1000000; //1ms //Interface_RNICGen6x100G_bwmb/NVME_DEFAULT_MAX_AZ_SIZE/1000 * delta_time_ns;
                //req->expire_time += 1000; //when NIC is idle , almost no latency * (nk);
            }
            else if (n->ntime < (n->stime + delta_time_ns)){
                //update lag
                lag = (n->ntime - req->stime);
                n->stime = n->ntime;
                n->ntime = n->stime + 1000000; //1ms //+ Interface_RNICGen6x100G_bwmb/NVME_DEFAULT_MAX_AZ_SIZE/1000 * delta_time_ns; //1ms
                req->expire_time += lag;   
                n->stime += delta_time_ns;   
            }else if((req->stime < n->ntime )&& (lag > 0)){
                req->expire_time += lag;
            }
            //req->expire_time += 2000;
            //req->expire_time += delta_time_ns;
            n->stime += delta_time_ns;
        }else {
            if (n->ntime < now){
                n->ntime = now;                 /* link was idle */   
            }
            n->busy = true;
            n->stime = n->ntime;                /* when this transfer starts */
            n->ntime += service_ns;             /* link busy until then */
            req->expire_time = n->ntime;        /* completion timestamp */
            lag = n->ntime - now;               /* current backlog on the link */
        }

#if RNIC_SIMULATE_DELAY
        /* hold the completion until the simulated transfer finishes */
        while (now_ns() < req->expire_time)
            cpu_relax();
#endif

        /* ---- dispatch back immediately: no waiting in the RNIC ----
         * release pairs with the reader's acquire load and publishes
         * expire_time. The reader enforces the deadline. */
        atomic_store_explicit(&dctx->response_ready[req->req_id], true,
                              memory_order_release);
        done++;
    }
    return NULL;
}

/* ------------------------------------------------------------------ */

static int run_one_sweep(const struct cfg *cfg, const uint8_t *store,
                         uint64_t chunks_per_block, uint32_t round,
                         uint32_t active_requests, FILE *csv)
{
    pthread_t *threads = calloc(active_requests, sizeof(*threads));
    pthread_t rnic_tid;
    bool rnic_started = false;          /* === CHANGED: only join if created */

    struct worker_arg *args = calloc(active_requests, sizeof(*args));
    struct device_ctx *dctx = calloc(1, sizeof(*dctx));

    int ret = 0;
    uint32_t created = 0;

    struct sweep_ctx ctx;
    memset(&ctx, 0, sizeof(ctx));

    if (!threads || !args || !dctx) {
        perror("alloc thread state");
        ret = -1;
        goto join_fail;
    }

    /* device context setup */
    dctx->cfg = cfg;                                    /* === CHANGED */
    dctx->nic_bandwidth_simulation = calloc(1, sizeof(struct nic));
    if (!dctx->nic_bandwidth_simulation) { ret = -1; goto join_fail; }
    dctx->nic_bandwidth_simulation->bw = Interface_RNICGen6x100G_bw; /* === CHANGED: init */
    dctx->nic_bandwidth_simulation->ntime = 0;
    dctx->nic_bandwidth_simulation->busy = false;
    dctx->dataplane_started = false;

    mpsc_init(&dctx->rnic_queue);

    dctx->response_ready = calloc(cfg->requests, sizeof(_Atomic(bool)));
    if (!dctx->response_ready) { ret = -1; goto join_fail; }
    for (uint32_t i = 0; i < cfg->requests; i++)
        atomic_store(&dctx->response_ready[i], false);

    dctx->num_readers = active_requests;
    dctx->reader_args = calloc(cfg->requests, sizeof(struct worker_arg *));
    if (!dctx->reader_args) { ret = -1; goto join_fail; }

    ctx.cfg = cfg;
    ctx.store = store;
    ctx.chunks_per_block = chunks_per_block;
    ctx.active_requests = active_requests;
    atomic_init(&ctx.blocks_read, 0);
    atomic_init(&ctx.verify_fail, 0);
    atomic_init(&ctx.checksum, 0);
    gate_init(&ctx.start, active_requests);

    /* === CHANGED: start the consumer BEFORE the producers. Readers
     * enqueue and then spin on response_ready before reaching the gate;
     * if reader creation fails midway with no RNIC running, the
     * already-created readers would spin forever and the cleanup joins
     * would hang. With the consumer already up, they always drain. */
    if (pthread_create(&rnic_tid, NULL, rnic_thread, dctx) != 0) {
        perror("pthread_create rnic");
        ret = -1;
        goto join_fail;
    }
    rnic_started = true;
    dctx->dataplane_started = true;

    for (uint32_t i = 0; i < active_requests; i++) {
        args[i].ctx = &ctx;
        args[i].request_id = i;
        args[i].scratch = cfg->copy_to_dst
            ? xaligned_alloc(cfg->block_bytes)
            : NULL;
        if (cfg->copy_to_dst && !args[i].scratch) {
            perror("alloc scratch");
            gate_open_after_created(&ctx.start, created);
            ret = -1;
            goto join_fail;
        }
        args[i].rdma_req = calloc(1, sizeof(*args[i].rdma_req));
        args[i].node_pool = calloc(1, sizeof(struct mpsc_node));
        if (!args[i].rdma_req || !args[i].node_pool) {
            perror("alloc req/node");
            gate_open_after_created(&ctx.start, created);
            ret = -1;
            goto join_fail;
        }
        args[i].to_nic = &dctx->rnic_queue;   /* same shared queue for all */
        args[i].rdma_req->stime = 0;
        args[i].rdma_req->expire_time = 0;
        args[i].dctx = dctx;

        int rc = pthread_create(&threads[i], NULL, rdma_reader_thread, &args[i]);
        if (rc != 0) {
            errno = rc;
            perror("pthread_create");
            gate_open_after_created(&ctx.start, created);
            ret = -1;
            goto join_fail;
        }
        dctx->reader_args[i] = &args[i];
        created++;
    }

    double t0 = now_sec();
    gate_open(&ctx.start);
    for (uint32_t i = 0; i < created; i++)
        pthread_join(threads[i], NULL);
    created = 0;
    double t1 = now_sec();
    pthread_join(rnic_tid, NULL);
    rnic_started = false;

    {
        uint64_t blocks = atomic_load_explicit(&ctx.blocks_read,
                                               memory_order_relaxed);
        uint64_t fails = atomic_load_explicit(&ctx.verify_fail,
                                              memory_order_relaxed);
        uint64_t sum = atomic_load_explicit(&ctx.checksum, memory_order_relaxed);
        double sec = t1 - t0;
        double read_gib = ((double)blocks * cfg->block_bytes) /
                          (1024.0 * 1024.0 * 1024.0);
        double gbps = sec > 0.0 ? read_gib / sec : 0.0;
        double reqps = sec > 0.0 ? (double)active_requests / sec : 0.0;

        printf("%5u  %4u  %10" PRIu64 "  %10" PRIu64 "  %8.2f  %8.3f  %8.2f  %10.0f  %6" PRIu64 "  0x%08" PRIx64 "\n",
               round, active_requests, (uint64_t)active_requests, blocks, read_gib,
               sec, gbps, reqps, fails, sum);
        if (csv) {
            fprintf(csv, "%u,%u,%" PRIu64 ",%" PRIu64 ",%.6f,%.9f,%.6f,%.3f,%" PRIu64 ",%" PRIu64 "\n",
                    round, active_requests, (uint64_t)active_requests, blocks,
                    read_gib, sec, gbps, reqps, fails, sum);
            fflush(csv);
        }
    }

join_fail:
    for (uint32_t i = 0; i < created; i++)
        pthread_join(threads[i], NULL);
    if (rnic_started)
        pthread_join(rnic_tid, NULL);

    if (args) {
        for (uint32_t i = 0; i < active_requests; i++) {
            free(args[i].scratch);
            free(args[i].rdma_req);        /* === CHANGED: were leaked */
            free(args[i].node_pool);       /* === CHANGED: were leaked */
        }
    }
    gate_destroy(&ctx.start);

    if (dctx) {
        free(dctx->nic_bandwidth_simulation);
        free(dctx->reader_args);
        free((void *)dctx->response_ready); /* === CHANGED: was leaked */
        free(dctx);
    }

    free(threads);
    free(args);
    return ret;
}

int main(int argc, char **argv)
{
    struct cfg cfg;
    if (parse_args(argc, argv, &cfg) != 0) {
        usage(argv[0]);
        return 1;
    }

    uint64_t chunks_per_block =
        ((uint64_t)cfg.requests + cfg.chunk_slots - 1) / cfg.chunk_slots;
    uint64_t chunk_count = (uint64_t)cfg.req_per_blocks * chunks_per_block;
    uint64_t chunk_bytes = 0, store_bytes = 0;
    if (!checked_mul_u64(cfg.chunk_slots, cfg.block_bytes, &chunk_bytes) ||
        !checked_mul_u64(chunk_count, chunk_bytes, &store_bytes) ||
        store_bytes > (uint64_t)SIZE_MAX) {
        fprintf(stderr, "configured memory size overflows\n");
        return 1;
    }

    printf("=== memory chunk-stripe sweep ===\n");
    printf("requests        : %u\n", cfg.requests);
    printf("req_per_blocks  : %u\n", cfg.req_per_blocks);
    printf("chunk_slots     : %u\n", cfg.chunk_slots);
    printf("chunks/block    : %" PRIu64 "\n", chunks_per_block);
    printf("chunk writes    : %" PRIu64 "\n", chunk_count);
    printf("write chunk     : %u blocks x %u bytes = %.2f MiB\n",
           cfg.chunk_slots, cfg.block_bytes,
           (double)chunk_bytes / (1024.0 * 1024.0));
    printf("block_bytes     : %u (%.2f MiB)\n",
           cfg.block_bytes, (double)cfg.block_bytes / (1024.0 * 1024.0));
    printf("store_bytes     : %" PRIu64 " (%.2f GiB)\n",
           store_bytes, (double)store_bytes / (1024.0 * 1024.0 * 1024.0));
    printf("rounds          : %u full sweep(s)\n", cfg.rounds);
    printf("sweep           : powers of two, %u..%u concurrent reqs\n",
           cfg.sweep_min, cfg.sweep_max);
    printf("nic simulation  : %u MB/s, serialized transfers%s\n",
           Interface_RNICGen6x100G_bw,
           RNIC_SIMULATE_DELAY ? " (reader-side gated)" : " (timestamp only)");
    printf("read mapping    : one pthread per active request (req i -> thread i)\n");
    printf("copy_to_dst     : %s\n", cfg.copy_to_dst ? "yes" : "no");
    printf("verify          : %s\n", cfg.verify ? "yes" : "no");
    printf("layout example  : chunk 0 = req 0..%u, block 0\n",
           cfg.chunk_slots - 1 < cfg.requests
               ? cfg.chunk_slots - 1
               : cfg.requests - 1);
    printf("===============================\n");

    uint8_t *store = xaligned_alloc((size_t)store_bytes);
    if (!store) {
        perror("alloc store");
        return 1;
    }

    double wt0 = now_sec();
    int rc = populate_chunk_stripe(&cfg, store, chunks_per_block, chunk_bytes);
    double wt1 = now_sec();
    if (rc != 0) {
        free(store);
        return 1;
    }

    double write_gib = (double)store_bytes / (1024.0 * 1024.0 * 1024.0);
    double write_sec = wt1 - wt0;
    printf("[populate] %.2f GiB in %.3f sec (%.2f GiB/s)\n",
           write_gib, write_sec, write_sec > 0.0 ? write_gib / write_sec : 0.0);

    FILE *csv = NULL;
    if (cfg.csv_path[0]) {
        csv = fopen(cfg.csv_path, "w");
        if (!csv) {
            perror("open csv");
            free(store);
            return 1;
        }
        fprintf(csv, "round,active_requests,read_requests,blocks,read_gib,seconds,gib_per_sec,req_per_sec,verify_fail,checksum\n");
    }

    printf("\nround  reqs  read_reqs  blocks_read  read_GiB   seconds   GiB/s       req/s   fails   checksum\n");
    printf("-----  ----  ---------  -----------  --------  --------  ------  ----------  ------  ----------\n");
    for (uint32_t r = 1; r <= cfg.rounds; r++) {
        for (uint32_t c = 1; c <= cfg.sweep_max; ) {
            if (c >= cfg.sweep_min) {
                if (run_one_sweep(&cfg, store, chunks_per_block, r, c, csv) != 0) {
                    if (csv) fclose(csv);
                    free(store);
                    return 1;
                }
            }
            if (c > UINT32_MAX / 2) break;
            c <<= 1;
        }
    }

    if (csv) fclose(csv);
    free(store);
    return 0;
}
