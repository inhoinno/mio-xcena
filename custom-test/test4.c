/* 
 * mem_rdma_rtt_simulation.c 
 * 
 * Memory-only multi-turn chunk-stripe test. No RDMA, no SPDK. 
 * 
 * Default shape: 
 *   - requests        = 32 
 *   - req_per_blocks  = 64 blocks appended per request on every turn 
 *   - block size      = 16 MiB 
 * 
 * Write layout per turn: 
 *   turn 1 writes block 0..63 
 *   turn 2 writes block 64..127 
 *   turn 3 writes block 128..191 
 * 
 * Each written chunk contains one block from every request: 
 *   chunk 0  = req0.block0,  req1.block0,  ..., req31.block0 
 *   chunk 1  = req0.block1,  req1.block1,  ..., req31.block1 
 *   ... 
 * 
 * Read layout per turn: 
 *   turn 1 reads cumulative blocks 0..63      per request 
 *   turn 2 reads cumulative blocks 0..127     per request 
 *   turn 3 reads cumulative blocks 0..191     per request 
 * 
 * The read phase always runs one pthread per request: 
 *   requests=32 => 32 pthreads, req i -> thread i 
 * 
 * Build: 
 *   gcc -O2 -g -std=c11 -Wall -Wextra -pthread \ 
 *       mem_multiturn_accum_read.c -o a.out 
 * 
 * Example: 
 *   ./a.out --rounds 4 
 *   ./a.out --requests 16 --req_per_blocks 128 --block-mb 8 --rounds 3 
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
#include <sys/mman.h>
#include <math.h>
 
#define DEFAULT_REQUESTS       32u 
#define DEFAULT_REQ_PER_BLOCKS 64u 
#define DEFAULT_BLOCK_MB       16u 
#define DEFAULT_BLOCK_BYTES    (DEFAULT_BLOCK_MB * 1024u * 1024u) 
#define DEFAULT_ROUNDS         1u 
#define ALIGN_BYTES            4096u 
#define BLOCK_MAGIC            UINT64_C(0x4d5455524e424c4b) /* MTURNBLK */ 

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

#define Interface_PCIeGen3x4_bwmb (4034 * MiB) //MB.s
#define Interface_PCIeGen3x4_bw 4034

#define Interface_RNICGen6x100G_bwmb (12500 * MiB) //MB.s
#define Interface_RNICGen6x100G_bw 12500
#define Interface_RNICGen6x200G_bwmb (25000 * MiB) //MB.s
#define Interface_RNICGen6x200G_bw 25000

typedef struct nic{
    uint64_t bw;
    double stime;
    double ntime;   //Next available time
    bool busy;
} nic;

struct cfg { 
    uint32_t requests; 
    uint32_t req_per_blocks; 
    uint32_t block_bytes; 
    uint32_t rounds; 
    bool verify; 
    bool copy_to_dst; 
    char csv_path[256]; 
    struct nic *nic_bandwidth_simulation;
}; 

struct rdma_req{
    uint64_t start_time;
    uint64_t end_time;
    uint64_t elapsed_time;
};
 
struct block_tag { 
    uint64_t magic; 
    uint64_t request_id;
uint64_t block_id; 
    uint64_t turn_id; 
    uint64_t fill_byte; 
}; 
 
struct start_gate { 
    pthread_mutex_t mu; 
    pthread_cond_t cv; 
    uint32_t total; 
    uint32_t ready; 
    bool open; 
}; 



struct read_ctx { 
    const struct cfg *cfg; 
    const uint8_t *store; 
    uint64_t chunk_bytes; 
    uint64_t read_blocks_per_req; 

    double expire_time;
    double stime;

    atomic_uint_fast64_t blocks_read; 
    atomic_uint_fast64_t verify_fail; 
    atomic_uint_fast64_t checksum; 
    struct start_gate start; 
}; 
 
struct reader_arg { 
    struct read_ctx *ctx; 
    uint8_t *scratch; 
    uint32_t request_id; 

}; 
 
static void usage(const char *prog) 
{ 
    fprintf(stderr, 
        "Usage: %s [options]\n" 
        "\n" 
        "Memory-only multi-turn append-write + cumulative-read test.\n" 
        "\n" 
        "Options:\n" 
        "  --requests N       concurrent logical requests / pthreads [default %u]\n" 
        "  --req_per_blocks N blocks appended per request on each turn [default %u]\n" 
        "  --req-per-blocks N alias for --req_per_blocks\n" 
        "  --blocks N         alias for --req_per_blocks\n" 
        "  --block-mb N       MiB per block [default %u]\n" 
        "  --block-bytes N    exact bytes per block; supports K/M/G\n" 
        "                     (mainly for small smoke tests)\n" 
        "  --rounds N         number of turns [default %u]\n" 
        "  --no-verify        skip block tag verification\n" 
        "  --no-copy          do not memcpy into scratch; only touch source\n" 
        "  --csv PATH         write per-turn rows as CSV\n" 
        "  --help             show this help\n", 
        prog, DEFAULT_REQUESTS, DEFAULT_REQ_PER_BLOCKS, 
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
    cfg->block_bytes = DEFAULT_BLOCK_BYTES; 
    cfg->rounds = DEFAULT_ROUNDS; 
    cfg->verify = true; 
    cfg->copy_to_dst = true; 
    cfg->csv_path[0] = '\0'; 
 
    enum { 
        OPT_REQUESTS = 1000, 
        OPT_REQ_PER_BLOCKS, 
        OPT_BLOCKS, 
        OPT_BLOCK_MB, 
        OPT_BLOCK_BYTES, 
        OPT_ROUNDS, 
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
        {"block-mb", required_argument, NULL, OPT_BLOCK_MB}, 
        {"block-bytes", required_argument, NULL, OPT_BLOCK_BYTES}, 
        {"rounds", required_argument, NULL, OPT_ROUNDS}, 
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
 
    if (cfg->block_bytes < sizeof(struct block_tag) + 1) { 
        fprintf(stderr, "--block-bytes must be at least %zu\n", 
                sizeof(struct block_tag) +1); 
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
                       uint64_t req, uint64_t blk, uint64_t turn) 
{ 
    uint8_t fill = fill_for(req, blk); 
    struct block_tag tag = { 
        .magic = BLOCK_MAGIC, 
        .request_id = req, 
        .block_id = blk, 
        .turn_id = turn, 
        .fill_byte = fill, 
    }; 
    memset(dst, fill, block_bytes); 
    memcpy(dst, &tag, sizeof(tag)); 
} 
 
static bool verify_block(const struct cfg *cfg, const uint8_t *src, 
                         uint64_t req, uint64_t blk) 
{ 
    struct block_tag tag; 
    memcpy(&tag, src, sizeof(tag)); 
    uint8_t fill = fill_for(req, blk); 
    uint64_t turn = blk / cfg->req_per_blocks + 1; 
    return tag.magic == BLOCK_MAGIC && 
           tag.request_id == req && 
           tag.block_id == blk && 
           tag.turn_id == turn && 
           tag.fill_byte == fill && 
           src[cfg->block_bytes - 1] == fill; 
} 
 
// static uint8_t *xaligned_alloc(size_t bytes) 
// { 
//     void *p = NULL; 
//     size_t len = (bytes + HUGEPAGE_2M - 1) & ~(HUGEPAGE_2MB -1 );
//     //int rc = posix_memalign(&p, ALIGN_BYTES, bytes);
//     void *p = mmap(NULL, len, PROT_READ | PROT_WRITE , 
//                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_HUGE_2MB, 
//                 -1, 0 );
//     if (p == MAP_FAILED) {
//         /* errno already set (ENOMEM if the 2MiB pool is too small)*/
//         return NULL;
//     }
//     //if (rc != 0) { 
//     //    errno = rc; 
//     //    return NULL; 
//     //}
//     return p; 
// } 
   
static uint8_t *xaligned_alloc(size_t bytes)
{
    if (bytes == 0 || bytes > SIZE_MAX - (HUGEPAGE_2MB - 1)) {
        errno = EINVAL;                       /* 0 -> len 0; near-SIZE_MAX -> overflow */
        return NULL;
    }
    size_t len = (bytes + HUGEPAGE_2MB - 1) & ~(HUGEPAGE_2MB - 1);

    void *p = mmap(NULL, len, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_HUGE_2MB | MAP_POPULATE,
                   -1, 0);


    if (p == MAP_FAILED) {
        fprintf(stderr, "hugealloc FAIL: req=%zu rounded=%zu (%zu pages) errno=%d %s\n",
                bytes, len, len / HUGEPAGE_2MB, errno, strerror(errno));
        return NULL;
    }
    fprintf(stderr, "hugealloc OK: req=%zu rounded=%zu (%zu pages) @%p\n",
            bytes, len, len / HUGEPAGE_2MB, p);
    return (uint8_t *)p;
}
 
static uint8_t *block_ptr(const struct cfg *cfg, uint8_t *store, 
                          uint64_t chunk_bytes, uint64_t req, uint64_t blk) 
{ 
    return store + blk * chunk_bytes + req * (uint64_t)cfg->block_bytes; 
} 
 
static const uint8_t *const_block_ptr(const struct cfg *cfg, 
                                      const uint8_t *store, 
                                      uint64_t chunk_bytes, 
                                      uint64_t req, uint64_t blk) 
{ 
    return store + blk * chunk_bytes + req * (uint64_t)cfg->block_bytes; 
} 
 
static int write_turn(const struct cfg *cfg, uint8_t *store, 
                      uint64_t chunk_bytes, uint32_t turn) 
{ 
    uint64_t first_blk = (uint64_t)(turn - 1) * cfg->req_per_blocks; 
    for (uint64_t i = 0; i < cfg->req_per_blocks; i++) { 
        uint64_t blk = first_blk + i; 
        uint8_t *chunk = store + blk * chunk_bytes; 
        for (uint64_t req = 0; req < cfg->requests; req++) { 
            fill_block(block_ptr(cfg, chunk, chunk_bytes, req, 0), 
                       cfg->block_bytes, req, blk, turn); 
        } 
    } 
    return 0; 
} 
double rdma_read ( struct cfg *cfg , struct read_ctx *ctx){

//#if PCIe_TIME_SIMULATION
    uint64_t nk = nlb/2;    //# of 4K
    uint64_t delta_time = (uint64_t)nk*pow(10,9);   // 4GB : 1s = n KB > 4096*1KB*2^10:10^9ns = 1KB : (10^9 / 2^10 / 4096)ns
    //femu_err("[Inho ] delt : %lx            ",delta_time);
    delta_time_ns = delta_time/pow(2,10)/(Interface_RNICGen6x100G_bw);

    //100Gbps 200Gbps 400Gbps simulation
    struct nic *nic = cfg->nic_bandwidth_simulation;
    if ( False ) {
        //lock
        //pthread_spin_lock(&n->pci_lock);
        if(nic->ntime + 100 <  ctx->stime ){
            lag=0;
            nic->stime = ctx->stime;
            nic->ntime = nic->stime + Interface_RNICGen6x100G_bwmb/NVME_DEFAULT_MAX_AZ_SIZE/1000 * delta_time_ns;

            ctx->expire_time += 968*(ctx->nlb/8);
        
        }else if(nic->ntime < (nic->stime + delta_time_ns)){
            //update lag
            lag = (nic->ntime - ctx->stime);
            nic->stime = nic->ntime;
            nic->ntime = nic->stime + Interface_RNICGen6x100G_bwmb/NVME_DEFAULT_MAX_AZ_SIZE/1000 * delta_time_ns; //1ms
            ctx->expire_time += lag;
            nic->stime += delta_time_ns;
        }else if (ctx->stime < nic->ntime && lag != 0 ){
            ctx->expire_time+=lag;
        }
    }
    nic->stime += delta_time_ns;
    //femu_err("[inho] lag : %lx\n", lag);
    //pthread_spin_unlock(&n->pci_lock);
//#endif

}

static void *rdma_reader_thread(void *arg) 
{ 
    struct reader_arg *ra = arg; 
    struct read_ctx *ctx = ra->ctx; 
    const struct cfg *cfg = ctx->cfg; 
    uint64_t req = ra->request_id; 
    uint64_t local_blocks = 0; 
    uint64_t local_fail = 0; 
    uint64_t local_sum = 0; 
 
    gate_wait(&ctx->start); 
    
    ctx->stime  = now_sec();
    ctx->expired_time = ctx->stime;

    /* 1 RDMA */
    ctx->expired_time += rdma_read(cfg, ctx);

    /* 1 DRAM read*/
    for (uint64_t blk = 0; blk < ctx->read_blocks_per_req; blk++) { 
        const uint8_t *src = const_block_ptr(cfg, ctx->store, 
                                             ctx->chunk_bytes, req, blk); 
        const uint8_t *check = src; 
        if (cfg->copy_to_dst) { 
            memcpy(ra->scratch, src, cfg->block_bytes); 
            check = ra->scratch; 
        } 
        local_sum += (uint64_t)check[0] + check[cfg->block_bytes - 1]; 
        if (cfg->verify && !verify_block(cfg, check, req, blk)) 
            local_fail++; 
        local_blocks++; 
    } 
 
    atomic_fetch_add_explicit(&ctx->blocks_read, local_blocks, 
                              memory_order_relaxed); 
    atomic_fetch_add_explicit(&ctx->verify_fail, local_fail, 
                              memory_order_relaxed); 
    atomic_fetch_add_explicit(&ctx->checksum, local_sum, memory_order_relaxed); 
    double tmp = now_sec();
    ctx->expired_time += tmp - ctx->stime;  
    return NULL; 
} 
static void *reader_thread(void *arg) 
{ 
    struct reader_arg *ra = arg; 
    struct read_ctx *ctx = ra->ctx; 
    const struct cfg *cfg = ctx->cfg; 
    uint64_t req = ra->request_id; 
    uint64_t local_blocks = 0; 
    uint64_t local_fail = 0; 
    uint64_t local_sum = 0; 
 
    gate_wait(&ctx->start); 
 
    for (uint64_t blk = 0; blk < ctx->read_blocks_per_req; blk++) { 
        const uint8_t *src = const_block_ptr(cfg, ctx->store, 
                                             ctx->chunk_bytes, req, blk); 
        const uint8_t *check = src; 
        if (cfg->copy_to_dst) { 
            memcpy(ra->scratch, src, cfg->block_bytes); 
            check = ra->scratch; 
        } 
        local_sum += (uint64_t)check[0] + check[cfg->block_bytes - 1]; 
        if (cfg->verify && !verify_block(cfg, check, req, blk)) 
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
static int read_rdma(const struct cfg *cfg, const uint8_t *store, 
                           uint64_t chunk_bytes, uint32_t turn, 
                           double *out_sec, uint64_t *out_blocks, 
                           uint64_t *out_fails, uint64_t *out_checksum){
    pthread_t *threads = calloc(cfg->requests, sizeof(*threads)); 
    struct reader_arg *args = calloc(cfg->requests, sizeof(*args)); 
    if (!threads || !args) { 
        perror("alloc thread state"); 
        free(threads); 
        free(args); 
        return -1; 
    } 
    struct read_ctx ctx; 
    memset(&ctx, 0, sizeof(ctx)); 
    ctx.cfg = cfg; 
    ctx.store = store; 
    ctx.chunk_bytes = chunk_bytes; 
    //ctx.read_blocks_per_req = (uint64_t)turn * cfg->req_per_blocks; //No mulit-turn
    ctx.read_blocks_per_req = (uint64_t)cfg->req_per_blocks; 

    atomic_init(&ctx.blocks_read, 0); 
    atomic_init(&ctx.verify_fail, 0); 
    atomic_init(&ctx.checksum, 0); 
    gate_init(&ctx.start, cfg->requests);
 
    int ret = 0; 
    uint32_t created = 0; 
    for (uint32_t i = 0; i < cfg->requests; i++) { 
        args[i].ctx = &ctx; 
        args[i].request_id = i; 
        args[i].scratch = cfg->copy_to_dst ? xaligned_alloc(cfg->block_bytes) : NULL; 
        if (cfg->copy_to_dst && !args[i].scratch) { 
            perror("alloc scratch"); 
            gate_open_after_created(&ctx.start, created); 
            ret = -1; 
            goto join_out; 
        } 
 
        //int rc = pthread_create(&threads[i], NULL, reader_thread, &args[i]); 
        int rc = pthread_create(&threads[i], NULL, rdma_reader_thread, &args[i]); 

        if (rc != 0) { 
            errno = rc; 
            perror("pthread_create"); 
            gate_open_after_created(&ctx.start, created); 
            ret = -1; 
            goto join_out; 
        } 
        created++; 
    } 
 
    double t0 = now_sec(); 
    gate_open(&ctx.start); 
    for (uint32_t i = 0; i < cfg->requests; i++) 
        pthread_join(threads[i], NULL); 
    created = 0; 
    double t1 = now_sec(); 
 
    *out_sec = t1 - t0; 
    *out_blocks = atomic_load_explicit(&ctx.blocks_read, memory_order_relaxed); 
    *out_fails = atomic_load_explicit(&ctx.verify_fail, memory_order_relaxed); 
    *out_checksum = atomic_load_explicit(&ctx.checksum, memory_order_relaxed); 
 
join_out: 
    for (uint32_t i = 0; i < created; i++) { 
        if (threads[i]) pthread_join(threads[i], NULL); 
        free(args[i].scratch); 
    } 
    for (uint32_t i = created; i < cfg->requests; i++) 
        free(args[i].scratch); 
    gate_destroy(&ctx.start); 
    free(threads); 
    free(args); 
    return ret; 
}

static int read_cumulative(const struct cfg *cfg, const uint8_t *store, 
                           uint64_t chunk_bytes, uint32_t turn, 
                           double *out_sec, uint64_t *out_blocks, 
                           uint64_t *out_fails, uint64_t *out_checksum) 
{ 
    pthread_t *threads = calloc(cfg->requests, sizeof(*threads)); 
    struct reader_arg *args = calloc(cfg->requests, sizeof(*args)); 
    if (!threads || !args) { 
        perror("alloc thread state"); 
        free(threads); 
        free(args); 
        return -1; 
    } 
 
    struct read_ctx ctx; 
    memset(&ctx, 0, sizeof(ctx)); 
    ctx.cfg = cfg; 
    ctx.store = store; 
    ctx.chunk_bytes = chunk_bytes; 
    ctx.read_blocks_per_req = (uint64_t)turn * cfg->req_per_blocks; 
    atomic_init(&ctx.blocks_read, 0); 
    atomic_init(&ctx.verify_fail, 0); 
    atomic_init(&ctx.checksum, 0); 
    gate_init(&ctx.start, cfg->requests); 
 
    int ret = 0; 
    uint32_t created = 0; 
    for (uint32_t i = 0; i < cfg->requests; i++) { 
        args[i].ctx = &ctx; 
        args[i].request_id = i; 
        args[i].scratch = cfg->copy_to_dst ? xaligned_alloc(cfg->block_bytes) : NULL; 
        if (cfg->copy_to_dst && !args[i].scratch) { 
            perror("alloc scratch"); 
            gate_open_after_created(&ctx.start, created); 
            ret = -1; 
            goto join_out; 
        } 
 
        int rc = pthread_create(&threads[i], NULL, reader_thread, &args[i]); 
        if (rc != 0) { 
            errno = rc; 
            perror("pthread_create"); 
            gate_open_after_created(&ctx.start, created); 
            ret = -1; 
            goto join_out; 
        } 
        created++; 
    } 
 
    double t0 = now_sec(); 
    gate_open(&ctx.start); 
    for (uint32_t i = 0; i < cfg->requests; i++) 
        pthread_join(threads[i], NULL); 
    created = 0; 
    double t1 = now_sec(); 
 
    *out_sec = t1 - t0; 
    *out_blocks = atomic_load_explicit(&ctx.blocks_read, memory_order_relaxed); 
    *out_fails = atomic_load_explicit(&ctx.verify_fail, memory_order_relaxed); 
    *out_checksum = atomic_load_explicit(&ctx.checksum, memory_order_relaxed); 
 
join_out: 
    for (uint32_t i = 0; i < created; i++) { 
        if (threads[i]) pthread_join(threads[i], NULL); 
        free(args[i].scratch); 
    } 
    for (uint32_t i = created; i < cfg->requests; i++) 
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
    cfg.nic_bandwidth_simulation = (struct nic *)calloc( 1, sizeof(struct nic));
    uint64_t chunk_bytes = 0, total_chunks = 0, store_bytes = 0; 
    if (!checked_mul_u64(cfg.requests, cfg.block_bytes, &chunk_bytes) || 
        !checked_mul_u64(cfg.rounds, cfg.req_per_blocks, &total_chunks) || 
        !checked_mul_u64(total_chunks, chunk_bytes, &store_bytes) || 
        store_bytes > (uint64_t)SIZE_MAX) { 
        fprintf(stderr, "configured memory size overflows\n"); 
        return 1; 
    } 
 
    printf("=== memory multi-turn cumulative read ===\n"); 
    printf("requests        : %u pthreads\n", cfg.requests); 
    printf("req_per_blocks  : %u appended blocks/request/turn\n", 
           cfg.req_per_blocks); 
    printf("rounds          : %u turn(s)\n", cfg.rounds); 
    printf("block_bytes     : %u (%.2f MiB)\n", 
           cfg.block_bytes, (double)cfg.block_bytes / (1024.0 * 1024.0)); 
    printf("write chunk     : %u blocks x %u bytes = %.2f MiB\n", 
           cfg.requests, cfg.block_bytes, 
           (double)chunk_bytes / (1024.0 * 1024.0)); 
    printf("write/turn      : %u chunks, %.2f GiB\n", 
           cfg.req_per_blocks, 
           ((double)cfg.req_per_blocks * chunk_bytes) / 
               (1024.0 * 1024.0 * 1024.0)); 
    printf("store_bytes     : %" PRIu64 " (%.2f GiB)\n", 
           store_bytes, (double)store_bytes / (1024.0 * 1024.0 * 1024.0)); 
    printf("read mapping    : one pthread per request (req i -> thread i)\n"); 
    printf("read pattern    : turn t reads prefix t * req_per_blocks\n"); 
    printf("copy_to_dst     : %s\n", cfg.copy_to_dst ? "yes" : "no"); 
    printf("verify          : %s\n", cfg.verify ? "yes" : "no"); 
    printf("===============================\n"); 
 
    uint8_t *store = xaligned_alloc((size_t)store_bytes); 
    if (!store) { 
        perror("alloc store"); 
        return 1; 
    } 
 
    FILE *csv = NULL; 
    if (cfg.csv_path[0]) { 
        csv = fopen(cfg.csv_path, "w"); 
        if (!csv) { 
            perror("open csv"); 
            free(store); 
            return 1; 
        } 
        fprintf(csv, "turn,write_blocks,read_blocks_per_request,total_read_blocks,write_gib,write_seconds,write_gib_per_sec,read_gib,read_seconds,agg_read_gib_per_sec,verify_fail,checksum\n"); 
    } 
 
    printf("\nturn  write_blk  read_blk/req  total_read_blk  write_GiB  write_s  write_GiB/s  read_GiB  read_s  agg_read_GiB/s  fails   checksum\n"); 
    printf("----  ---------  ------------  --------------  ---------  -------  -----------  --------  ------  --------------  ------  ----------\n"); 
 
    for (uint32_t turn = 1; turn <= cfg.rounds; turn++) { 
        double wt0 = now_sec(); 
        if (write_turn(&cfg, store, chunk_bytes, turn) != 0) { 
            if (csv) fclose(csv); 
            free(store); 
            return 1; 
        } 
        double wt1 = now_sec(); 
 
        double read_sec = 0.0; 
        uint64_t read_blocks = 0, fails = 0, checksum = 0; 
        // if (read_cumulative(&cfg, store, chunk_bytes, turn, &read_sec, 
        //                     &read_blocks, &fails, &checksum) != 0) { 
        //     if (csv) fclose(csv); 
        //     free(store); 
        //     return 1; 
        // } 

        if (read_rdma(&cfg, store, chunk_bytes, turn, &read_sec, 
                            &read_blocks, &fails, &checksum) != 0) { 
            if (csv) fclose(csv); 
            free(store); 
            return 1; 
        } 
        uint64_t write_blocks = (uint64_t)cfg.requests * cfg.req_per_blocks; 
        uint64_t read_blocks_per_req = (uint64_t)turn * cfg.req_per_blocks; 
        double write_sec = wt1 - wt0; 
        double write_gib = ((double)write_blocks * cfg.block_bytes) / 
                           (1024.0 * 1024.0 * 1024.0); 
        double read_gib = ((double)read_blocks * cfg.block_bytes) / 
                          (1024.0 * 1024.0 * 1024.0); 
        double write_bw = write_sec > 0.0 ? write_gib / write_sec : 0.0; 
        double read_bw = read_sec > 0.0 ? read_gib / read_sec : 0.0; 
 
        printf("%4u  %9" PRIu64 "  %12" PRIu64 "  %14" PRIu64 
               "  %9.2f  %7.3f  %11.2f  %8.2f  %6.3f  %14.2f  %6" PRIu64 
               "  0x%08" PRIx64 "\n", 
               turn, write_blocks, read_blocks_per_req, read_blocks, 
               write_gib, write_sec, write_bw, read_gib, read_sec, read_bw, 
               fails, checksum); 
 
        if (csv) { 
            fprintf(csv,"%u,%" PRIu64 ",%" PRIu64 ",%" PRIu64 
                    ",%.6f,%.9f,%.6f,%.6f,%.9f,%.6f,%" PRIu64 ",%" PRIu64 "\n", 
                    turn, write_blocks, read_blocks_per_req, read_blocks, 
                    write_gib, write_sec, write_bw, read_gib, read_sec, 
                    read_bw, fails, checksum); 
            fflush(csv); 
        } 
    } 
 
    if (csv) fclose(csv); 
    free(store); 
    return 0; 
}
