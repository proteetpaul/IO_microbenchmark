#include "nvme_spec.h"
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <getopt.h>
#include <spdk/env.h>
#include <spdk/nvme.h>
#include <spdk/string.h>
#include <spdk/log.h>
#include <rte_errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <vector>
#include <chrono>
#include <pthread.h>
#include <sched.h>
#include <future>
#include <algorithm>
#include <system_error>
#include <cmath>
#include <rte_log.h>

/**
TODO():
- Fix bug where SPDK is unable to create mempool for large number of files
- Investigate why submission queue size is halved
- Investigate why threads are launched on the same core
*/

struct Options {
    char *transport_id;
    uint32_t num_threads;
    uint32_t num_files;
    uint64_t file_size_bytes;
};

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

static ControllerInfo global_controller_info = {
    .ns = NULL, 
    .controller = NULL,
    .controller_data = NULL,
};

struct spdk_mempool *mempool;

static const uint32_t ALIGNMENT = 4096;

void read_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
    if (spdk_nvme_cpl_is_error(completion)) {
        printf("Completion error: %s", spdk_nvme_cpl_get_status_string(&completion->status));
    }

	IOTrace *trace = (IOTrace*)arg;
    trace->end = std::chrono::system_clock::now();
	trace->duration_us = (uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
		trace->end - trace->start
	).count();
}

std::vector<IOTrace> run_single_threaded_benchmark(Options *opts, uint32_t thread_id, pthread_barrier_t *start_point) {
    int cpuid = sched_getcpu();
    printf("Thread %d running on cpu %d\n", thread_id, cpuid);
    // Set thread name (useful for investigating flamegraphs)
    char thread_name[20];
    snprintf(thread_name, 20, "spdk-worker-%d", thread_id);
    pthread_setname_np(pthread_self(), thread_name);

    uint32_t num_files_per_thread = opts->num_files / opts->num_threads;
    uint32_t transfers_per_file = std::max(1ul, opts->file_size_bytes / global_controller_info.max_io_xfer_size);
    uint32_t lba_count = std::min(global_controller_info.max_io_xfer_size, opts->file_size_bytes) / global_controller_info.sector_size;

    uint32_t total_submissions = num_files_per_thread * transfers_per_file;
    uint64_t lba_start = thread_id * total_submissions;
    // create queue pair
    struct spdk_nvme_io_qpair_opts qpair_opts;
    spdk_nvme_ctrlr_get_default_io_qpair_opts(global_controller_info.controller, 
        &qpair_opts,
        sizeof(struct spdk_nvme_io_qpair_opts)
    );

    // Set capacity of IO queues to max possible value (default is 1024)
    qpair_opts.io_queue_size = global_controller_info.mqe + 1;

    struct spdk_nvme_qpair *qpair = spdk_nvme_ctrlr_alloc_io_qpair(
        global_controller_info.controller,
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
    std::vector<IOTrace> traces(total_submissions);
    bool should_terminate = false;
    pthread_barrier_wait(start_point);

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
            int ret = spdk_nvme_ns_cmd_read(global_controller_info.ns, qpair, payload, 
                current_lba, lba_count, read_complete, (void*)(&traces[num_submissions]), 0);
            if (ret == -ENOMEM) {
                // Submission queue is full. Poll for completions
                resubmit = true;
                break;
            }
            traces[num_submissions].start = std::chrono::system_clock::now();
            num_submissions++;
            current_lba += transfers_per_file;
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
    return traces;
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
    printf("  -h                   Show this help message\n");
    printf("\nExample:\n");
    printf("  %s --transport_id 0000:01:00.0 --num_threads 2 --num_files 4 --file_size 1GiB\n", program_name);
}

Options parse_args(int argc, char **argv) {
    Options opts = {0};
    int opt;
    static struct option long_options[] = {
        {"transport_id", required_argument, 0, 't'},
        {"num_threads", required_argument, 0, 'n'},
        {"num_files", required_argument, 0, 'f'},
        {"file_size", required_argument, 0, 's'},
        {"help", no_argument, 0, 'h'},
        {0, 0, 0, 0}
    };

    while ((opt = getopt_long(argc, argv, "t:n:f:s:h", long_options, NULL)) != -1) {
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
    return opts;
}

static bool
probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	 struct spdk_nvme_ctrlr_opts *opts)
{
	printf("Attaching to %s\n", trid->traddr);
	return true;
}

static void
attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	  struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts)
{
	int nsid;
	struct spdk_nvme_ns *ns;
	printf("Attached to %s\n", trid->traddr);

	global_controller_info.controller_data = spdk_nvme_ctrlr_get_data(ctrlr);
    global_controller_info.controller = ctrlr;
	for (nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0;
	     nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
		ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
		if (ns == NULL) {
			continue;
		}
		global_controller_info.ns = ns;
        global_controller_info.max_io_xfer_size = spdk_nvme_ns_get_max_io_xfer_size(ns);
        global_controller_info.sector_size = spdk_nvme_ns_get_sector_size(ns);
        union spdk_nvme_cap_register cap_reg = spdk_nvme_ctrlr_get_regs_cap(ctrlr);
        global_controller_info.mqe = cap_reg.bits.mqes;

        printf("======Controller Information======\n");
        printf("Namespace ID: %d\nSize: %ju GB\nMQE: %d\nMax IO transfer size: %d KB\n", spdk_nvme_ns_get_id(ns),
	        spdk_nvme_ns_get_size(ns) / 1000000000,
            cap_reg.bits.mqes,
            spdk_nvme_ns_get_max_io_xfer_size(ns)/1024);

        printf("=============\n");
        break;
	}
}

void cleanup() {
    spdk_mempool_free(mempool);
    spdk_nvme_detach(global_controller_info.controller);
}

int main(int argc, char **argv) {
    int rc;
    struct spdk_env_opts spdk_opts;
    Options cmd_opts = parse_args(argc, argv);
    
    printf("Using transport ID: %s\n", cmd_opts.transport_id);
    
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
    snprintf(tid.traddr, sizeof(tid.traddr), "%s", cmd_opts.transport_id);

    if (spdk_nvme_probe(&tid, NULL, probe_cb, attach_cb, NULL) < 0) {
        printf("spdk_nvme_probe failed.\n");
        return 1;
    }
    printf("Num files: %d\nFile size: %ld\nNum threads: %d\n", cmd_opts.num_files, cmd_opts.file_size_bytes, cmd_opts.num_threads);
    // Pre-allocate memory in a pool
    uint32_t cache_size = 0;
    if (cmd_opts.num_threads > 1) {
        cache_size = cmd_opts.num_files / cmd_opts.num_threads;
    }
    mempool = spdk_mempool_create("benchmarking", cmd_opts.num_files, cmd_opts.file_size_bytes, cache_size, SPDK_ENV_NUMA_ID_ANY);
    if (mempool == NULL) {
        std::error_code ec(rte_errno, std::generic_category());
        printf("Failed to create SPDK mempool: %d\n", rte_errno);
        exit(1);
    }

    pthread_barrier_t start_barrier;
    pthread_barrier_init(&start_barrier, NULL, cmd_opts.num_threads);

    // Run per-thread benchmarks and collect IO traces
    std::vector<std::future<std::vector<IOTrace>>> futures;
    futures.reserve(cmd_opts.num_threads);
    for (uint32_t i = 0; i < cmd_opts.num_threads; i++) {
        futures.emplace_back(std::async(std::launch::async, run_single_threaded_benchmark, &cmd_opts, i, &start_barrier));
    }

    std::vector<IOTrace> all_traces;
    for (auto &f : futures) {
        std::vector<IOTrace> t = f.get();
        all_traces.insert(all_traces.end(), t.begin(), t.end());
    }
    
    // Compute percentiles on duration_us
    std::vector<uint64_t> durations;
    durations.reserve(all_traces.size());
    for (const auto &tr : all_traces) {
        durations.push_back(tr.duration_us);
    }
    std::sort(durations.begin(), durations.end());
    auto pct = [&](double p) -> uint64_t {
        if (durations.empty()) return 0;
        double rank = (p / 100.0) * static_cast<double>(durations.size());
        size_t idx = static_cast<size_t>(std::ceil(rank)) - 1;
        if (idx >= durations.size()) idx = durations.size() - 1;
        return durations[idx];
    };

    uint64_t p50 = pct(50.0);
    uint64_t p70 = pct(70.0);
    uint64_t p90 = pct(90.0);
    uint64_t p99 = pct(99.0);

    printf("Latency percentiles (microseconds):\np50=%lu\n p70=%lu\n p90=%lu\n p99=%lu\n",
           (unsigned long)p50, (unsigned long)p70, (unsigned long)p90, (unsigned long)p99);

    cleanup();
    return 0;
}