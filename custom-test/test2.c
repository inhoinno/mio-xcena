/* 
 * mem_chunk_stripe_sweep.c 
 * 
 * Memory-only example of the mode2-style chunk-stripe populate pattern and a 
 * concurrent read-request sweep. No RDMA, no SPDK: just malloc'd memory, 
 * pthreads, memcpy, and verification tags. 
 * 
 * Default shape: 
 *   - 32 logical requests 
 *   - 64 blocks per request 
 *   - 32 slots per chunk 
 *   - 16 MiB per block 
 * 
 * Populate layout, with defaults: 
 *   chunk 0  = 32 blocks: req 0..31, each request's block 0 
 *   chunk 1  = 32 blocks: req 0..31, each request's block 1 
 *   ... 
 *   chunk 63 = 32 blocks: req 0..31, each request's block 63 
 * 
 * A read of request R copies one slot from each of the 64 chunks. The sweep 
 * runs 1, 2, 4, 8, 16, 32 concurrent logical read requests, and the read volume 
 * grows with N: 
 * 
 *   N=1  ->  1 * 64 * 16 MiB 
 *   N=32 -> 32 * 64 * 16 MiB 
 * 
 * Build: 
 *   gcc -O2 -g -std=c11 -Wall -Wextra -pthread \ 
 *       examples/mem_chunk_stripe_sweep.c \ 
 *       -o /tmp/mem_chunk_stripe_sweep 
 * 
 * Example: 
 *   /tmp/mem_chunk_stripe_sweep --block-mb 32 --req_per_blocks 1024 --rounds 3 
 */ 
 
#define _POSIX_C_SOURCE 200809L 
 
#include <errno.h> 
#include <getopt.h> 
#include <inttypes.h> 
#include <pthread.h> 
#include <stdatomic.h> 
#include <stdbool.h> 
#include <stdint.h> 
#include <stdio.h> 
#include <stdlib.h> 
#include <string.h> 
#include <time.h> 
 
#define DEFAULT_REQUESTS     32u 
#define DEFAULT_REQ_PER_BLOCKS 64u 
#define DEFAULT_CHUNK_SLOTS  32u 
#define DEFAULT_BLOCK_MB     16u 
#define DEFAULT_BLOCK_BYTES  (DEFAULT_BLOCK_MB * 1024u * 1024u) 
#define DEFAULT_ROUNDS       1u 
#define ALIGN_BYTES          4096u 
#define BLOCK_MAGIC          UINT64_C(0x4d454d5f43484b31) /* MEM_CHK1 */ 
 
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
    uint8_t *scratch; 
    uint32_t request_id; 
}; 
 
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
 
static uint8_t *xaligned_alloc(size_t bytes) 
{ 
    void *p = NULL; 
    int rc = posix_memalign(&p, ALIGN_BYTES, bytes); 
    if (rc != 0) { 
        errno = rc; 
        return NULL; 
    } 
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
 
static void *reader_thread(void *arg) 
{ 
    struct worker_arg *wa = arg; 
    struct sweep_ctx *ctx = wa->ctx; 
    const struct cfg *cfg = ctx->cfg; 
    uint64_t local_blocks = 0;
uint64_t local_fail = 0; 
    uint64_t local_sum = 0; 
    uint64_t req = wa->request_id; 
 
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
 
static int run_one_sweep(const struct cfg *cfg, const uint8_t *store, 
                         uint64_t chunks_per_block, uint32_t round, 
                         uint32_t active_requests, FILE *csv) 
{ 
    pthread_t *threads = calloc(active_requests, sizeof(*threads)); 
    struct worker_arg *args = calloc(active_requests, sizeof(*args)); 
    if (!threads || !args) { 
        perror("alloc thread state"); 
        free(threads); 
        free(args); 
        return -1; 
    } 
 
    struct sweep_ctx ctx; 
    memset(&ctx, 0, sizeof(ctx)); 
    ctx.cfg = cfg; 
    ctx.store = store; 
    ctx.chunks_per_block = chunks_per_block; 
    ctx.active_requests = active_requests; 
    atomic_init(&ctx.blocks_read, 0); 
    atomic_init(&ctx.verify_fail, 0); 
    atomic_init(&ctx.checksum, 0); 
    gate_init(&ctx.start, active_requests); 
 
    int ret = 0; 
    uint32_t created = 0; 
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
        int rc = pthread_create(&threads[i], NULL, reader_thread, &args[i]); 
        if (rc != 0) { 
            errno = rc; 
            perror("pthread_create"); 
            gate_open_after_created(&ctx.start, created); 
            ret = -1; 
            goto join_fail; 
        } 
        created++; 
    } 
 
    double t0 = now_sec(); 
    gate_open(&ctx.start); 
    for (uint32_t i = 0; i < active_requests; i++) 
        pthread_join(threads[i], NULL); 
    created = 0; 
    double t1 = now_sec(); 
 
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
 
join_fail: 
    for (uint32_t i = 0; i < created; i++) { 
        if (threads[i]) pthread_join(threads[i], NULL); 
        free(args[i].scratch); 
    } 
    for (uint32_t i = created; i < active_requests; i++) 
        free(args[i].scratch); 
    gate_destroy(&ctx.start); 
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
    printf("read mapping    :one pthread per active request (req i -> thread i)\n"); 
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
            if (c> UINT32_MAX / 2) break; 
            c <<= 1; 
        } 
    } 
 
    if (csv) fclose(csv); 
    free(store); 
    return 0; 
}