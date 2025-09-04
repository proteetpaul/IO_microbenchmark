#include "nvme_spec.h"
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <getopt.h>
#include <spdk/env.h>
#include <spdk/nvme.h>
#include <spdk/nvme_spec.h>
#include <spdk/string.h>
#include <spdk/log.h>
#include <rte_errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <sched.h>
#include <string.h>
#include <vector>
#include <chrono>
#include <pthread.h>
#include <sched.h>
#include <algorithm>
#include <system_error>
#include <cmath>
#include <rte_log.h>
#include <tuple>
#include <atomic>

#define CACHE_LINE_SIZE 64


struct ControllerInfo {
    struct spdk_nvme_ns *ns;

    struct spdk_nvme_ctrlr *controller;
    
    const struct spdk_nvme_ctrlr_data *controller_data;
    
    size_t max_io_xfer_size;
    
    uint32_t mqe; // Maximum queue entries
    
    size_t sector_size;
};

struct IOTrace {
    std::chrono::time_point<std::chrono::system_clock> start;

    std::chrono::time_point<std::chrono::system_clock> end;
    
    uint64_t duration_us;
};


struct Options {
    char *transport_id;
    
    uint32_t num_threads;
    
    uint32_t num_files;
    
    uint64_t file_size_bytes;
    
    uint64_t io_chunk_size_bytes;

    ControllerInfo controller_info;

    uint32_t transfers_per_file() {
        return std::max(1ul, file_size_bytes / io_chunk_size_bytes);
    }

    uint32_t lba_per_transfer() {
        return std::min(io_chunk_size_bytes, file_size_bytes) / controller_info.sector_size;
    }
} __attribute__((aligned(CACHE_LINE_SIZE)));

// struct WorkerCtx {

// } __attribute__((aligned(CACHE_LINE_SIZE)));

struct spdk_mempool *mempool;

static const uint32_t ALIGNMENT = 4096;

void read_complete(void *arg, const struct spdk_nvme_cpl *completion) {
    if (spdk_nvme_cpl_is_error(completion)) {
        printf("Completion error: %s", spdk_nvme_cpl_get_status_string(&completion->status));
    }

	IOTrace *trace = (IOTrace*)arg;
    trace->end = std::chrono::system_clock::now();
	trace->duration_us = (uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
		trace->end - trace->start
	).count();
}

void run_single_threaded_benchmark(Options *opts, uint32_t thread_id, 
        pthread_barrier_t *start_point, IOTrace *traces) {
    int cpuid = sched_getcpu();
    printf("Thread %d running on cpu %d\n", thread_id, cpuid);

    char thread_name[20];
    snprintf(thread_name, 20, "spdk-worker-%d", thread_id);
    pthread_setname_np(pthread_self(), thread_name);

    uint32_t num_files_per_thread = opts->num_files / opts->num_threads;
    uint32_t transfers_per_file = opts->transfers_per_file();
    uint32_t lba_count = opts->lba_per_transfer();

    uint32_t total_submissions = num_files_per_thread * transfers_per_file;
    uint64_t lba_start = thread_id * total_submissions;
    auto controller_info = opts->controller_info;
    // create queue pair
    struct spdk_nvme_io_qpair_opts qpair_opts;
    spdk_nvme_ctrlr_get_default_io_qpair_opts(controller_info.controller, 
        &qpair_opts,
        sizeof(struct spdk_nvme_io_qpair_opts)
    );

    // Set capacity of IO queues to max possible value (default is 1024)
    qpair_opts.io_queue_size = 32;

    struct spdk_nvme_qpair *qpair = spdk_nvme_ctrlr_alloc_io_qpair(
        controller_info.controller,
        &qpair_opts,
        sizeof(struct spdk_nvme_io_qpair_opts)
    );
    if (qpair == NULL) {
        printf("Failed to create queue pair\n");
        exit(1);
    }
    
    uint64_t current_lba = lba_start;
    uint32_t num_submissions = 0;
    uint32_t completed = 0;
    bool should_terminate = false;
    pthread_barrier_wait(start_point);

    printf("Thread %d running on cpu %d\n", thread_id, cpuid);
    auto start_time = std::chrono::system_clock::now();
    bool resubmit = false;
    void *payload = NULL;

    while (!should_terminate) {
        while (num_submissions < total_submissions) {
            if (!resubmit) {
                payload = spdk_mempool_get(mempool);
                if (payload == NULL) {
                    printf("Mempool returned NULL\n");
                    should_terminate = true;
                    break;
                }
            }
            int ret = spdk_nvme_ns_cmd_read(controller_info.ns, qpair, payload, 
                current_lba, lba_count, read_complete, (void*)(&traces[num_submissions]), 0);
            if (ret == -ENOMEM) {
                // Submission queue is full. Poll for completions
                resubmit = true;
                break;
            }
            traces[num_submissions].start = std::chrono::system_clock::now();
            num_submissions++;
            current_lba += lba_count;
        }

        completed += spdk_nvme_qpair_process_completions(qpair, 0);
        should_terminate |= (completed == total_submissions);
    }

    auto end_time = std::chrono::system_clock::now();
    auto duration = (uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
		end_time - start_time
	).count();

    spdk_nvme_ctrlr_free_io_qpair(qpair);

    printf("Thread %d took %ld ms\n", thread_id, duration);
    
    // Calculate and print effective bandwidth
    uint64_t total_bytes_read = (uint64_t)total_submissions * opts->io_chunk_size_bytes;
    double duration_seconds = duration / 1000.0;
    double bandwidth_mbps = (total_bytes_read / (1024.0 * 1024.0)) / duration_seconds;
    printf("Thread %d effective bandwidth: %.2f MB/s (%.2f GB/s)\n", 
           thread_id, bandwidth_mbps, bandwidth_mbps / 1024.0);
    
}

struct CompletionCtx {
    uint64_t *current_lba;
    uint32_t lba_count;
    std::chrono::time_point<std::chrono::system_clock> start;
    uint64_t *max_duration_us;
    uint64_t buffer_idx;
    void **payload_buffers;
    struct spdk_nvme_ns *ns;
    struct spdk_nvme_qpair *qpair;
};

void duration_benchmark_read_complete(void *arg, const struct spdk_nvme_cpl *completion) {
    if (spdk_nvme_cpl_is_error(completion)) {
        printf("Completion error: %s", spdk_nvme_cpl_get_status_string(&completion->status));
    }
    CompletionCtx *ctx = (CompletionCtx *)arg;
    int ret = spdk_nvme_ns_cmd_read(ctx->ns, ctx->qpair, ctx->payload_buffers[ctx->buffer_idx], 
        *(ctx->current_lba), ctx->lba_count, duration_benchmark_read_complete, (void*)ctx, 0); 
    *(ctx->current_lba) += ctx->lba_count;
    if (ret < 0) {
        printf("Failed to submit IO\n");
        exit(1);
    }
}

void run_duration_benchmark(Options *opts, uint32_t thread_id, 
        pthread_barrier_t *start_point) {
    int cpuid = sched_getcpu();
    printf("Thread %d running on cpu %d\n", thread_id, cpuid);
    
    char thread_name[20];
    snprintf(thread_name, 20, "spdk-worker-%d", thread_id);
    pthread_setname_np(pthread_self(), thread_name);

    const uint32_t lba_count = opts->lba_per_transfer();
    uint32_t queue_depth = 128;
    auto controller_info = opts->controller_info;

    uint64_t tick_rate = spdk_get_ticks_hz();

    struct spdk_nvme_io_qpair_opts qpair_opts;
    spdk_nvme_ctrlr_get_default_io_qpair_opts(controller_info.controller, 
        &qpair_opts,
        sizeof(struct spdk_nvme_io_qpair_opts)
    );
    uint32_t print_period_ms = 100;
    qpair_opts.io_queue_size = queue_depth;
    struct spdk_nvme_qpair *qpair = spdk_nvme_ctrlr_alloc_io_qpair(
        controller_info.controller,
        &qpair_opts,
        sizeof(struct spdk_nvme_io_qpair_opts)
    );
    if (qpair == NULL) {
        printf("Failed to create queue pair\n");
        exit(1);
    }
    uint64_t current_lba = 0ll;
    void **payload_buffers = (void **) malloc(sizeof(void*) * queue_depth);
    for (int i=0; i<queue_depth; i++) {
        payload_buffers[i] = spdk_mempool_get(mempool);
    }

    pthread_barrier_wait(start_point);

    // Submit initial IOs
    for (int i=0; i<queue_depth; i++) {
        CompletionCtx *ctx = new CompletionCtx;
        ctx->buffer_idx = i;
        ctx->payload_buffers = payload_buffers;
        ctx->current_lba = &current_lba;
        ctx->lba_count = lba_count;
        ctx->ns = controller_info.ns;
        ctx->qpair = qpair;
        int ret = spdk_nvme_ns_cmd_read(controller_info.ns, qpair, payload_buffers[i], 
            current_lba, lba_count, duration_benchmark_read_complete, (void*)ctx, 0);
        if (ret < 0) {
            printf("Failed to submit IO\n");
            exit(1);
        }
        current_lba += lba_count;
    }
    auto current_tick = spdk_get_ticks();
    uint64_t duration_seconds = 5;
    auto end_tick = current_tick + duration_seconds * tick_rate;
    auto next_print_tick = current_tick + (tick_rate * print_period_ms) / 1000; // Print every 100 ms
    uint64_t completed = 0;
    uint64_t last_period_completed = 0;
    while (current_tick < end_tick) {
        // auto start_tick = spdk_get_ticks();
        completed += spdk_nvme_qpair_process_completions(qpair, 1);
        current_tick = spdk_get_ticks();

        if (current_tick >= next_print_tick) {
            // print
            double iops = (completed - last_period_completed) * 1000.0/print_period_ms;
            double bw_mbps = (iops * opts->io_chunk_size_bytes) / (1024 * 1024);
            printf("IOPS: %0.2f, Bandwidth: %0.2f MBps\n", iops, bw_mbps);
            next_print_tick = current_tick + (tick_rate * print_period_ms) / 1000;
            last_period_completed = completed;
            current_tick = spdk_get_ticks();
        }
        if (current_tick >= end_tick) {
            break;
        }
    }

    uint32_t drained = 0;
    while (drained < queue_depth) {
        drained += spdk_nvme_qpair_process_completions(qpair, 0);
    }
    spdk_nvme_ctrlr_free_io_qpair(qpair);
}

static uint64_t parse_iec_size(const char *text)
{
    if (text == NULL || *text == '\0') {
        return 0;
    }

    char *endptr = NULL;
    unsigned long long value = strtoull(text, &endptr, 10);
    if (value == 0 && endptr == text) {
        return 0;
    }

    uint64_t multiplier = 1;
    if (endptr != NULL) {
        if (strcasecmp(endptr, "B") == 0 || strcasecmp(endptr, "") == 0) {
            multiplier = 1;
        } else if (strcasecmp(endptr, "KiB") == 0 || strcasecmp(endptr, "K") == 0 || strcasecmp(endptr, "KB") == 0) {
            multiplier = 1024ULL;
        } else if (strcasecmp(endptr, "MiB") == 0 || strcasecmp(endptr, "M") == 0 || strcasecmp(endptr, "MB") == 0) {
            multiplier = 1024ULL * 1024ULL;
        } else if (strcasecmp(endptr, "GiB") == 0 || strcasecmp(endptr, "G") == 0 || strcasecmp(endptr, "GB") == 0) {
            multiplier = 1024ULL * 1024ULL * 1024ULL;
        } else if (strcasecmp(endptr, "TiB") == 0 || strcasecmp(endptr, "T") == 0 || strcasecmp(endptr, "TB") == 0) {
            multiplier = 1024ULL * 1024ULL * 1024ULL * 1024ULL;
        } else {
            return 0;
        }
    }

    return (uint64_t)value * multiplier;
}

void usage(const char *program_name) {
    printf("Usage: %s [options]\n", program_name);
    printf("Options:\n");
    printf("  -t, --transport_id <id>  Transport ID for PCIe device (e.g., '0000:01:00.0')\n");
    printf("  -n, --num_threads <n>    Number of threads to use\n");
    printf("  -f, --num_files <n>  Number of files (or targets) to use\n");
    printf("  -s, --file_size <size>   File size in IEC units (e.g., 512KiB, 2MiB, 1GiB)\n");
    printf("  -c, --io_chunk_size <size> IO chunk size in IEC units (e.g., 4KiB, 64KiB, 1MiB)\n");
    printf("  -h                   Show this help message\n");
    printf("\nExample:\n");
    printf("  %s --transport_id 0000:01:00.0 --num_threads 2 --num_files 4 --file_size 1GiB --io_chunk_size 64KiB\n", program_name);
}

Options parse_args(int argc, char **argv) {
    Options opts = {0};
    int opt;
    static struct option long_options[] = {
        {"transport_id", required_argument, 0, 't'},
        {"num_threads", required_argument, 0, 'n'},
        {"num_files", required_argument, 0, 'f'},
        {"file_size", required_argument, 0, 's'},
        {"io_chunk_size", required_argument, 0, 'c'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}
    };

    while ((opt = getopt_long(argc, argv, "t:n:f:s:c:h", long_options, NULL)) != -1) {
        switch (opt) {
        case 't':
            opts.transport_id = optarg;
            break;
        case 'n':
            opts.num_threads = (uint32_t)strtoul(optarg, NULL, 10);
            break;
        case 'f':
            opts.num_files = (uint32_t)strtoul(optarg, NULL, 10);
            break;
        case 's': {
            uint64_t size = parse_iec_size(optarg);
            if (size == 0) {
                fprintf(stderr, "Invalid --file_size value: %s\n", optarg);
                usage(argv[0]);
                exit(1);
            }
            opts.file_size_bytes = size;
            break;
        }
        case 'c': {
            uint64_t size = parse_iec_size(optarg);
            if (size == 0) {
                fprintf(stderr, "Invalid --io_chunk_size value: %s\n", optarg);
                usage(argv[0]);
                exit(1);
            }
            opts.io_chunk_size_bytes = size;
            break;
        }
        case 'h':
            usage(argv[0]);
            exit(0);
        default:
            usage(argv[0]);
            exit(0);
        }
    }
    if (opts.transport_id == NULL) {
        fprintf(stderr, "Error: Transport ID is required. Use -t option.\n");
        usage(argv[0]);
        exit(1);
    }

    if (opts.file_size_bytes % opts.io_chunk_size_bytes != 0) {
        fprintf(stderr, "Error: File size (%lu bytes) must be a multiple of IO chunk size (%lu bytes)\n", opts.file_size_bytes, opts.io_chunk_size_bytes);
        exit(1);
    }
    
    // Set default IO chunk size if not provided
    if (opts.io_chunk_size_bytes == 0) {
        opts.io_chunk_size_bytes = 64 * 1024; // Default to 64KB
    }
    
    return opts;
}

static bool
probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	 struct spdk_nvme_ctrlr_opts *opts)
{
	printf("Attaching to %s\n", trid->traddr);
	return true;
}

void admin_cmd_complete_cb(void *arg, const struct spdk_nvme_cpl *completion) {
    if (spdk_nvme_cpl_is_error(completion)) {
        const char *err_msg = spdk_nvme_cpl_get_status_string(&completion->status);
        printf("Failed to set power mode: %s\n", err_msg);
        exit(1);
    }
}

// void get_power_mode_cb(void *arg, const struct spdk_nvme_cpl *completion) {
//     if (spdk_nvme_cpl_is_error(completion)) {
//         const char *err_msg = spdk_nvme_cpl_get_status_string(&completion->status);
//         printf("Failed to get power mode: %s\n", err_msg);
//         exit(1);
//     }
//     printf("Power mode: %d\n", completion->cdw0);
// }

// void print_power_mode(struct spdk_nvme_ctrlr *ctrlr) {
//     int ret = spdk_nvme_ctrlr_cmd_get_feature(ctrlr, SPDK_NVME_FEAT_POWER_MANAGEMENT, 
//         0, NULL, 0, get_power_mode_cb, NULL);
//     if (ret != 0) {
//         printf("Failed to get power mode\n");
//         exit(1);
//     }
//     int completed = 0;
//     while (completed == 0) {
//         completed = spdk_nvme_ctrlr_process_admin_completions(ctrlr);
//     }
// }

void set_power_mode(struct spdk_nvme_ctrlr *ctrlr) {
    spdk_nvme_feat_power_management cdw11;
    cdw11.bits.ps = 0;   // Maximum performance
    cdw11.bits.wh = 1;
    cdw11.bits.reserved = 0;

    int ret = spdk_nvme_ctrlr_cmd_set_feature(ctrlr, SPDK_NVME_FEAT_POWER_MANAGEMENT, cdw11.raw, 0, NULL, 0, admin_cmd_complete_cb, NULL);
    if (ret != 0) {
        printf("Failed to set power mode\n");
    }
    int completed = 0;
    while (completed == 0) {
        completed = spdk_nvme_ctrlr_process_admin_completions(ctrlr);
    }
}

static void
attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	  struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *spdk_ctrlr_opts)
{
    Options *opts = (Options*) cb_ctx;
    ControllerInfo *controller_info = (ControllerInfo *) aligned_alloc(CACHE_LINE_SIZE, sizeof(ControllerInfo));

	int nsid;
	struct spdk_nvme_ns *ns;
	printf("Attached to %s\n", trid->traddr);

    set_power_mode(ctrlr);

	opts->controller_info.controller_data = spdk_nvme_ctrlr_get_data(ctrlr);
    opts->controller_info.controller = ctrlr;
	for (nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0;
	     nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
		ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
		if (ns == NULL) {
			continue;
		}
		opts->controller_info.ns = ns;
        opts->controller_info.max_io_xfer_size = spdk_nvme_ns_get_max_io_xfer_size(ns);
        opts->controller_info.sector_size = spdk_nvme_ns_get_sector_size(ns);
        union spdk_nvme_cap_register cap_reg = spdk_nvme_ctrlr_get_regs_cap(ctrlr);
        opts->controller_info.mqe = cap_reg.bits.mqes;

        printf("======Controller Information======\n");
        printf("Namespace ID: %d\nSize: %ju GB\nMQE: %d\nMax IO transfer size: %d KB\n", spdk_nvme_ns_get_id(ns),
	        spdk_nvme_ns_get_size(ns) / 1000000000,
            cap_reg.bits.mqes,
            spdk_nvme_ns_get_max_io_xfer_size(ns)/1024);

        // Validate IO chunk size
        if (opts->io_chunk_size_bytes > opts->controller_info.max_io_xfer_size) {
            fprintf(stderr, "Error: IO chunk size (%lu bytes) exceeds maximum transfer size (%zu bytes)\n",
                    opts->io_chunk_size_bytes, opts->controller_info.max_io_xfer_size);
            exit(1);
        }

        printf("=============\n");
        break;
	}
}

void cleanup(Options *opts) {
    spdk_mempool_free(mempool);
    spdk_nvme_detach(opts->controller_info.controller);
}

void print_percentiles(std::vector<uint64_t> &durations) {
    std::sort(durations.begin(), durations.end());
    auto pct = [&](double p) -> uint64_t {
        if (durations.empty()) return 0;
        double rank = (p / 100.0) * static_cast<double>(durations.size());
        size_t idx = static_cast<size_t>(std::ceil(rank)) - 1;
        if (idx >= durations.size()) idx = durations.size() - 1;
        return durations[idx];
    };

    uint64_t p50 = pct(50.0)/1000;
    uint64_t p70 = pct(70.0)/1000;
    uint64_t p90 = pct(90.0)/1000;
    uint64_t p99 = pct(99.0)/1000;

    printf("Latency percentiles (milliseconds):\np50=%lu\n p70=%lu\n p90=%lu\n p99=%lu\n",
           (unsigned long)p50, (unsigned long)p70, (unsigned long)p90, (unsigned long)p99);
}

void run_benchmark(Options *app_opts) {
    uint32_t main_thread_cpu = sched_getcpu();
    uint32_t num_threads = app_opts->num_threads;
    pthread_barrier_t start_barrier;
    pthread_barrier_init(&start_barrier, NULL, num_threads);
    
    // Create CPU masks for each thread to ensure they run on different cores
    std::vector<cpu_set_t> cpu_masks(num_threads-1);
    for (uint32_t i = 0; i < num_threads-1; i++) {
        if (i == main_thread_cpu) continue;
        CPU_ZERO(&cpu_masks[i]);
        CPU_SET(i, &cpu_masks[i]);  // Assign thread i to CPU core i
    }
    
    // uint32_t submissions_per_thread = app_opts->transfers_per_file() * app_opts->num_files / num_threads;
    // uint64_t trace_chunk_size = sizeof(struct IOTrace) * submissions_per_thread;
    // uint64_t padding = CACHE_LINE_SIZE - trace_chunk_size % CACHE_LINE_SIZE;
    // trace_chunk_size += padding;
    // std::vector<IOTrace*> trace_array_ptrs(num_threads);

    // for (int i=0; i<num_threads; i++) {
    //     trace_array_ptrs[i] = (IOTrace *) aligned_alloc(CACHE_LINE_SIZE, trace_chunk_size * num_threads);
    // }

    std::vector<pthread_t> threads(num_threads);
    std::vector<pthread_attr_t> thread_attrs(num_threads);
    
    for (uint32_t i = 0; i < num_threads; i++) {
        pthread_attr_init(&thread_attrs[i]);
        pthread_attr_setaffinity_np(&thread_attrs[i], sizeof(cpu_set_t), &cpu_masks[i]);
        
        pthread_create(&threads[i], &thread_attrs[i], 
            [](void *arg) -> void* {
                auto *args = static_cast<std::tuple<Options*, uint32_t, pthread_barrier_t*>*>(arg);
                // run_single_threaded_benchmark(std::get<0>(*args), std::get<1>(*args), 
                //                             std::get<2>(*args), std::get<3>(*args));
                run_duration_benchmark(std::get<0>(*args), std::get<1>(*args), 
                                            std::get<2>(*args));
                delete args;
                return NULL;
            },
            // new std::tuple<Options*, uint32_t, pthread_barrier_t*, IOTrace*>(app_opts, i, &start_barrier, trace_array_ptrs[i])
            new std::tuple<Options*, uint32_t, pthread_barrier_t*>(app_opts, i, &start_barrier)
        );
    }

    for (uint32_t i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
        pthread_attr_destroy(&thread_attrs[i]);
    }
    
    // Compute percentiles on duration_us
    // std::vector<uint64_t> durations;
    // durations.reserve(submissions_per_thread * num_threads);
    // uint32_t offset = 0;
    // for (int i=0; i<num_threads; i++) {
    //     for (int j=0; j<submissions_per_thread; j++) {
    //         durations.push_back(trace_array_ptrs[i][j].duration_us);
    //     }
    // }

    // print_percentiles(durations);
    // for (int i=0; i<num_threads; i++) {
    //     free(trace_array_ptrs[i]);
    // }
}

int main(int argc, char **argv) {
    int rc;
    struct spdk_env_opts spdk_opts;
    Options app_opts = parse_args(argc, argv);
    
    printf("Using transport ID: %s\n", app_opts.transport_id);
    
    spdk_opts.opts_size = sizeof(spdk_opts);
    spdk_env_opts_init(&spdk_opts);

    if (spdk_env_init(&spdk_opts) < 0) {
		fprintf(stderr, "Unable to initialize SPDK env\n");
		return 1;
	}
    // Enable debug logs
    spdk_log_set_level(SPDK_LOG_DEBUG);
    rte_log_set_global_level(RTE_LOG_DEBUG);

    // Use the transport_id to connect to the specific PCIe device
    struct spdk_nvme_transport_id tid;
    spdk_nvme_trid_populate_transport(&tid, SPDK_NVME_TRANSPORT_PCIE);
	snprintf(tid.subnqn, sizeof(tid.subnqn), "%s", SPDK_NVMF_DISCOVERY_NQN);
    snprintf(tid.traddr, sizeof(tid.traddr), "%s", app_opts.transport_id);

    if (spdk_nvme_probe(&tid, (void*)&app_opts, probe_cb, attach_cb, NULL) < 0) {
        printf("spdk_nvme_probe failed.\n");
        return 1;
    }
    printf("Num files: %d\nFile size: %ld\nNum threads: %d\nIO chunk size: %ld\n", app_opts.num_files, app_opts.file_size_bytes, app_opts.num_threads, app_opts.io_chunk_size_bytes);
    // Pre-allocate memory in a pool
    uint32_t cache_size = 0;
    if (app_opts.num_threads > 1) {
        cache_size = app_opts.num_files / app_opts.num_threads;
    }
    mempool = spdk_mempool_create("benchmarking", app_opts.num_files * app_opts.transfers_per_file(), app_opts.io_chunk_size_bytes, cache_size, SPDK_ENV_NUMA_ID_ANY);
    if (mempool == NULL) {
        std::error_code ec(rte_errno, std::generic_category());
        printf("Failed to create SPDK mempool: %d\n", rte_errno);
        exit(1);
    }

    run_benchmark(&app_opts);

    cleanup(&app_opts);
    return 0;
}