#include "nvme_spec.h"
#include <getopt.h>
#include <spdk/env.h>
#include <spdk/nvme.h>
#include <spdk/string.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <vector>
#include <chrono>
#include <thread>

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
    size_t sector_size;
    uint32_t mqe; // Maximum queue entries
};

struct IOTrace {
    std::chrono::time_point<std::chrono::system_clock> start;
    std::chrono::time_point<std::chrono::system_clock> end;
};

struct spdk_read_cb_args {
    IOTrace *trace;
    uint32_t *completed;
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
	spdk_read_cb_args *args = (spdk_read_cb_args*)arg;
    uint32_t &completed = *args->completed;
    completed++;
    args->trace->end = std::chrono::system_clock::now();
	// if (strcmp(sequence->buf, DATA_BUFFER_STRING)) {
	// 	fprintf(stderr, "Read data doesn't match write data\n");
	// 	exit(1);
	// }
}

void run_single_threaded_benchmark(Options *opts, uint32_t thread_id) {
    std::vector<IOTrace> traces(opts->num_files);
    uint32_t num_files_per_thread = opts->num_files / opts->num_threads;
    uint32_t sectors_per_file = opts->file_size_bytes / global_controller_info.sector_size;

    uint64_t lba_start = thread_id * num_files_per_thread * sectors_per_file;
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
    
    uint64_t current_lba = lba_start;
    uint32_t file_idx = 0;
    uint32_t completed = 0;
    std::vector<spdk_read_cb_args> callback_args;
    while (file_idx < num_files_per_thread) {
        traces[file_idx].start = std::chrono::system_clock::now();
        void *payload = spdk_mempool_get(mempool);
        spdk_read_cb_args cb_args = {
            .trace = &traces[file_idx],
            .completed = &completed,
        };
        callback_args.push_back(cb_args);
        spdk_nvme_ns_cmd_read(global_controller_info.ns, qpair, payload, current_lba, sectors_per_file, read_complete, (void*)(&callback_args[file_idx]), 0);
        file_idx++;
        current_lba += sectors_per_file;
    }

    while (completed < num_files_per_thread) {
        spdk_nvme_qpair_process_completions(qpair, 0);
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
        global_controller_info.sector_size = spdk_nvme_ns_get_sector_size(ns);
        union spdk_nvme_cap_register cap_reg = spdk_nvme_ctrlr_get_regs_cap(ctrlr);
        global_controller_info.mqe = cap_reg.bits.mqes;

        printf("  Namespace ID: %d size: %juGB\n", spdk_nvme_ns_get_id(ns),
	       spdk_nvme_ns_get_size(ns) / 1000000000);
        break;
	}
}

void cleanup() {
    spdk_mempool_free(mempool);
    spdk_nvme_detach(global_controller_info.controller);
}

int main(int argc, char **argv) {
    int rc;
    struct spdk_env_opts opts;
    Options cmd_opts = parse_args(argc, argv);    
    
    printf("Using transport ID: %s\n", cmd_opts.transport_id);
    
    spdk_env_opts_init(&opts);
    
    // Use the transport_id to connect to the specific PCIe device
    struct spdk_nvme_transport_id tid = {};
    tid.trtype = SPDK_NVME_TRANSPORT_PCIE;
    tid.adrfam = SPDK_NVMF_ADRFAM_INTRA_HOST;
    snprintf(tid.traddr, sizeof(tid.traddr), "%s", cmd_opts.transport_id);

    if (spdk_nvme_probe(&tid, NULL, probe_cb, attach_cb, NULL) < 0) {
        printf("spdk_nvme_probe failed.\n");
        return 1;
    }
    // Pre-allocate memory in a pool
    mempool = spdk_mempool_create("benchmarking", cmd_opts.num_files, cmd_opts.file_size_bytes, SPDK_MEMPOOL_DEFAULT_CACHE_SIZE, SPDK_ENV_NUMA_ID_ANY);

    std::vector<std::thread> threads;
    for (int i=0; i<cmd_opts.num_threads; i++) {
        std::thread t(run_single_threaded_benchmark, &cmd_opts, i);
        threads.push_back(std::move(t));
    }
    for (int i=0; i<cmd_opts.num_threads; i++) {
        threads[i].join();
    }

    cleanup();
    return 0;
}