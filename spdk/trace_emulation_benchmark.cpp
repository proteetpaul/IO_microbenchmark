#include "nvme_spec.h"
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <getopt.h>
#include <pthread.h>
#include <rte_errno.h>
#include <rte_log.h>
#include <sched.h>
#include <spdk/env.h>
#include <spdk/log.h>
#include <spdk/nvme.h>
#include <spdk/nvme_spec.h>
#include <spdk/string.h>
#include <sstream>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/types.h>
#include <system_error>
#include <tuple>
#include <unistd.h>
#include <vector>

#define CACHE_LINE_SIZE 64

struct TraceEntry {
  uint64_t sector;
  uint32_t io_size;
  double queued_time_ms;
  double latency_ms;
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

struct Options {
  char *transport_id;
  char *trace_file;
  uint32_t num_threads;
  uint32_t queue_depth;
  ControllerInfo controller_info;
} __attribute__((aligned(CACHE_LINE_SIZE)));

struct spdk_mempool *mempool;

static const uint32_t ALIGNMENT = 4096;

struct CompletionCtx {
  uint32_t current_submission_idx;
  uint32_t *next_submission_ptr;
  TraceEntry *io_uring_traces;
  uint32_t num_traces;
  IOTrace *output_traces;
  uint64_t buffer_idx;
  void **payload_buffers;
  struct spdk_nvme_ns *ns;
  struct spdk_nvme_qpair *qpair;
  uint32_t sector_size;
};

void read_complete(void *arg, const struct spdk_nvme_cpl *completion) {
  if (spdk_nvme_cpl_is_error(completion)) {
    printf("Completion error: %s",
           spdk_nvme_cpl_get_status_string(&completion->status));
  }

  CompletionCtx *ctx = (CompletionCtx *)arg;
  uint32_t *next_submission = ctx->next_submission_ptr;
  uint32_t current_submission = ctx->current_submission_idx;

  ctx->output_traces[current_submission].duration_us =
      (uint64_t)std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now() -
          ctx->output_traces[current_submission].start)
          .count();

  if (*next_submission >= ctx->num_traces) {
    return;
  }
  ctx->current_submission_idx = (*next_submission);
  ctx->output_traces[ctx->current_submission_idx].start = std::chrono::system_clock::now();

  TraceEntry &trace_entry = ctx->io_uring_traces[*next_submission];
  int ret = spdk_nvme_ns_cmd_read(
      ctx->ns, ctx->qpair, ctx->payload_buffers[ctx->buffer_idx],
      trace_entry.sector, trace_entry.io_size/ctx->sector_size, read_complete,
      (void *)ctx, 0);
  if (ret < 0) {
    exit(1);
  }
  (*next_submission)++;
}

void run_trace_replay_benchmark(Options *opts, uint32_t thread_id,
                                pthread_barrier_t *start_point,
                                IOTrace *output_traces,
                                TraceEntry *trace_entries,
                                uint32_t num_trace_entries) {
  int cpuid = sched_getcpu();
  printf("Thread %d running on cpu %d\n", thread_id, cpuid);

  char thread_name[20];
  snprintf(thread_name, 20, "spdk-worker-%d", thread_id);
  pthread_setname_np(pthread_self(), thread_name);

  auto controller_info = opts->controller_info;

  struct spdk_nvme_io_qpair_opts qpair_opts;
  spdk_nvme_ctrlr_get_default_io_qpair_opts(
      controller_info.controller, &qpair_opts,
      sizeof(struct spdk_nvme_io_qpair_opts));
  qpair_opts.io_queue_size = opts->queue_depth;

  struct spdk_nvme_qpair *qpair =
      spdk_nvme_ctrlr_alloc_io_qpair(controller_info.controller, &qpair_opts,
                                     sizeof(struct spdk_nvme_io_qpair_opts));
  if (qpair == NULL) {
    printf("Failed to create queue pair\n");
    exit(1);
  }

  uint32_t next_submission = 0;
  // uint32_t total_submissions = (end_idx - start_idx);
  uint32_t completed = 0;

  void **payload_buffers = (void **)malloc(sizeof(void *) * opts->queue_depth);
  pthread_barrier_wait(start_point);

  for (int i = 0; i < opts->queue_depth; i++) {
    payload_buffers[i] = spdk_mempool_get(mempool);
  }

  // Submit initial IOs
  while (next_submission < opts->queue_depth) {
    CompletionCtx *ctx = new CompletionCtx;
    ctx->current_submission_idx = next_submission;
    ctx->next_submission_ptr = &next_submission;
    ctx->io_uring_traces = trace_entries;
    ctx->num_traces = num_trace_entries;
    ctx->output_traces = output_traces;
    ctx->buffer_idx = next_submission;
    ctx->payload_buffers = payload_buffers;
    ctx->ns = controller_info.ns;
    ctx->qpair = qpair;
    ctx->sector_size = controller_info.sector_size;

    ctx->output_traces[next_submission].start = std::chrono::system_clock::now();
    int ret = spdk_nvme_ns_cmd_read(
        controller_info.ns, qpair, payload_buffers[next_submission],
        trace_entries[next_submission].sector,
        trace_entries[next_submission].io_size/controller_info.sector_size,
        read_complete, (void *)ctx, 0);
    if (ret < 0) {
      printf("Failed to submit IO\n");
      exit(1);
    }
    next_submission++;
  }

  auto start_time = std::chrono::system_clock::now();

  while (true) {
    int ret = spdk_nvme_qpair_process_completions(qpair, 0);
    
    if (ret < 0) {
      printf("Received negative ret: %d\n", ret);
    } else if (ret > 0) {
      completed += ret;
      // printf("Completed: %d\n", completed);
    }
    if (completed >= num_trace_entries) {
      break;
    }
  }

  auto end_time = std::chrono::system_clock::now();
  auto duration =
      (uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
          end_time - start_time)
          .count();

  spdk_nvme_ctrlr_free_io_qpair(qpair);

  printf("Thread %d took %ld ms\n", thread_id, duration);

  // Calculate and print effective bandwidth
  uint64_t total_bytes_read = 0;
  for (uint32_t i = 0; i < num_trace_entries; i++) {
    total_bytes_read += trace_entries[i].io_size;
  }
  double duration_seconds = duration / 1000.0;
  double bandwidth_mbps =
      (total_bytes_read / (1024.0 * 1024.0)) / duration_seconds;
  printf("Thread %d effective bandwidth: %.2f MB/s (%.2f GB/s)\n", thread_id,
         bandwidth_mbps, bandwidth_mbps / 1024.0);

  // Calculate and print IOPs
  double iops = next_submission / duration_seconds;
  printf("Thread %d IOPs: %.2f\n", thread_id, iops);
}

void usage(const char *program_name) {
  printf("Usage: %s [options]\n", program_name);
  printf("Options:\n");
  printf("  -t, --transport_id <id>  Transport ID for PCIe device (e.g., "
         "'0000:01:00.0')\n");
  printf(
      "  -f, --trace_file <file>  Path to trace file containing IO traces\n");
  printf("  -n, --num_threads <n>    Number of threads to use\n");
  printf("  -q, --queue_depth <n>   Queue depth for IO operations (default: "
         "128)\n");
  printf("  -h                       Show this help message\n");
  printf("\nExample:\n");
  printf("  %s --transport_id 0000:01:00.0 --trace_file trace.txt "
         "--num_threads 2 --queue_depth 64\n",
         program_name);
}

Options parse_args(int argc, char **argv) {
  Options opts = {0};
  int opt;
  static struct option long_options[] = {
      {"transport_id", required_argument, 0, 't'},
      {"trace_file", required_argument, 0, 'f'},
      {"num_threads", required_argument, 0, 'n'},
      {"queue_depth", required_argument, 0, 'q'},
      {"help", no_argument, 0, 'h'},
      {0, 0, 0, 0}};

  while ((opt = getopt_long(argc, argv, "t:f:n:q:h", long_options, NULL)) !=
         -1) {
    switch (opt) {
    case 't':
      opts.transport_id = optarg;
      break;
    case 'f':
      opts.trace_file = optarg;
      break;
    case 'n':
      opts.num_threads = (uint32_t)strtoul(optarg, NULL, 10);
      break;
    case 'q':
      opts.queue_depth = (uint32_t)strtoul(optarg, NULL, 10);
      break;
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

  if (opts.trace_file == NULL) {
    fprintf(stderr, "Error: Trace file is required. Use -f option.\n");
    usage(argv[0]);
    exit(1);
  }

  if (opts.num_threads == 0) {
    opts.num_threads = 1; // Default to single thread
  }

  if (opts.queue_depth == 0) {
    opts.queue_depth = 128; // Default queue depth
  }

  return opts;
}

static bool probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                     struct spdk_nvme_ctrlr_opts *opts) {
  printf("Attaching to %s\n", trid->traddr);
  return true;
}

static void attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                      struct spdk_nvme_ctrlr *ctrlr,
                      const struct spdk_nvme_ctrlr_opts *spdk_ctrlr_opts) {
  Options *opts = (Options *)cb_ctx;
  ControllerInfo *controller_info =
      (ControllerInfo *)aligned_alloc(CACHE_LINE_SIZE, sizeof(ControllerInfo));

  int nsid;
  struct spdk_nvme_ns *ns;
  printf("Attached to %s\n", trid->traddr);

  opts->controller_info.controller_data = spdk_nvme_ctrlr_get_data(ctrlr);
  opts->controller_info.controller = ctrlr;
  for (nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0;
       nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
    ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
    if (ns == NULL) {
      continue;
    }
    opts->controller_info.ns = ns;
    opts->controller_info.max_io_xfer_size =
        spdk_nvme_ns_get_max_io_xfer_size(ns);
    opts->controller_info.sector_size = spdk_nvme_ns_get_sector_size(ns);
    union spdk_nvme_cap_register cap_reg = spdk_nvme_ctrlr_get_regs_cap(ctrlr);
    opts->controller_info.mqe = cap_reg.bits.mqes;

    printf("======Controller Information======\n");
    printf("Namespace ID: %d\nSize: %ju GB\nMQE: %d\nMax IO transfer size: %d "
           "KB\n",
           spdk_nvme_ns_get_id(ns), spdk_nvme_ns_get_size(ns) / 1000000000,
           cap_reg.bits.mqes, spdk_nvme_ns_get_max_io_xfer_size(ns) / 1024);
           

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
    if (durations.empty())
      return 0;
    double rank = (p / 100.0) * static_cast<double>(durations.size());
    size_t idx = static_cast<size_t>(std::ceil(rank)) - 1;
    if (idx >= durations.size())
      idx = durations.size() - 1;
    return durations[idx];
  };

  uint64_t p50 = pct(50.0);
  uint64_t p70 = pct(70.0);
  uint64_t p90 = pct(90.0);
  uint64_t p99 = pct(99.0);

  printf("Latency percentiles (microseconds):\np50=%lu\n p70=%lu\n p90=%lu\n "
         "p99=%lu\n",
         (unsigned long)p50, (unsigned long)p70, (unsigned long)p90,
         (unsigned long)p99);
}

void run_benchmark(Options *app_opts,
                   const std::vector<TraceEntry> &trace_entries) {
  uint32_t num_threads = app_opts->num_threads;
  pthread_barrier_t start_barrier;
  pthread_barrier_init(&start_barrier, NULL, num_threads);

  // Create CPU masks for each thread to ensure they run on different cores
  std::vector<cpu_set_t> cpu_masks(num_threads);
  for (uint32_t i = 0; i < num_threads; i++) {
    CPU_ZERO(&cpu_masks[i]);
    CPU_SET(i, &cpu_masks[i]); // Assign thread i to CPU core i
  }

  uint32_t entries_per_thread = trace_entries.size() / num_threads;
  uint64_t trace_chunk_size = sizeof(struct IOTrace) * entries_per_thread;
  uint64_t padding = CACHE_LINE_SIZE - trace_chunk_size % CACHE_LINE_SIZE;
  trace_chunk_size += padding;
  std::vector<IOTrace *> trace_array_ptrs(num_threads);

  for (int i = 0; i < num_threads; i++) {
    trace_array_ptrs[i] =
        (IOTrace *)aligned_alloc(CACHE_LINE_SIZE, trace_chunk_size);
  }

  std::vector<pthread_t> threads(num_threads);
  std::vector<pthread_attr_t> thread_attrs(num_threads);

  for (uint32_t i = 0; i < num_threads; i++) {
    pthread_attr_init(&thread_attrs[i]);
    pthread_attr_setaffinity_np(&thread_attrs[i], sizeof(cpu_set_t),
                                &cpu_masks[i]);

    uint32_t start_idx = i * entries_per_thread;
    uint32_t end_idx = (i == num_threads - 1) ? trace_entries.size()
                                              : start_idx + entries_per_thread;
    uint32_t num_elements = (end_idx - start_idx);
    using TupleType = std::tuple<Options *, uint32_t, pthread_barrier_t *,
                                 IOTrace *, TraceEntry *, uint32_t>;

    pthread_create(
        &threads[i], &thread_attrs[i],
        [](void *arg) -> void * {
          auto *args = static_cast<TupleType *>(arg);
          run_trace_replay_benchmark(std::get<0>(*args), std::get<1>(*args),
                                     std::get<2>(*args), std::get<3>(*args),
                                     std::get<4>(*args), std::get<5>(*args));
          delete args;
          return NULL;
        },
        new TupleType(app_opts, i, &start_barrier, trace_array_ptrs[i],
                      (TraceEntry *)trace_entries.data() + start_idx,
                      num_elements));
  }

  for (uint32_t i = 0; i < num_threads; i++) {
    pthread_join(threads[i], NULL);
    pthread_attr_destroy(&thread_attrs[i]);
  }

  // Compute percentiles on duration_us
  std::vector<uint64_t> durations;
  durations.reserve(entries_per_thread * num_threads);
  for (int i = 0; i < num_threads; i++) {
    for (int j = 0; j < entries_per_thread; j++) {
      durations.push_back(trace_array_ptrs[i][j].duration_us);
    }
  }

  print_percentiles(durations);
  for (int i = 0; i < num_threads; i++) {
    free(trace_array_ptrs[i]);
  }
}

std::vector<TraceEntry> parse_trace_file(const std::string &filename) {
  std::vector<TraceEntry> trace_entries;
  std::ifstream file(filename);

  if (!file.is_open()) {
    fprintf(stderr, "Error: Could not open trace file %s\n", filename.c_str());
    exit(1);
  }

  std::string line;
  while (std::getline(file, line)) {
    if (line.empty())
      continue;

    std::istringstream iss(line);
    TraceEntry entry;

    if (!(iss >> entry.sector >> entry.io_size >> entry.queued_time_ms >>
          entry.latency_ms)) {
      fprintf(stderr, "Warning: Could not parse line: %s\n", line.c_str());
      continue;
    }

    trace_entries.push_back(entry);
  }

  printf("Parsed %zu trace entries from %s\n", trace_entries.size(),
         filename.c_str());
  return trace_entries;
}

int main(int argc, char **argv) {
  printf("Main thread running on cpu %d\n", sched_getcpu());
  int rc;
  struct spdk_env_opts spdk_opts;
  Options app_opts = parse_args(argc, argv);

  printf("Using transport ID: %s\n", app_opts.transport_id);
  printf("Using trace file: %s\n", app_opts.trace_file);

  // Parse trace file
  std::vector<TraceEntry> trace_entries = parse_trace_file(app_opts.trace_file);
  if (trace_entries.empty()) {
    fprintf(stderr, "Error: No valid trace entries found\n");
    exit(1);
  }

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

  if (spdk_nvme_probe(&tid, (void *)&app_opts, probe_cb, attach_cb, NULL) < 0) {
    printf("spdk_nvme_probe failed.\n");
    return 1;
  }

  printf("Num threads: %d\n", app_opts.num_threads);
  printf("Queue depth: %d\n", app_opts.queue_depth);

  // Find maximum IO size in trace to determine mempool size
  uint32_t max_io_size = 0;
  for (const auto &entry : trace_entries) {
    max_io_size = std::max(max_io_size, entry.io_size);
  }

  // Pre-allocate memory in a pool
  uint32_t cache_size = 0;
  if (app_opts.num_threads > 1) {
    cache_size = trace_entries.size() / app_opts.num_threads;
  }
  mempool = spdk_mempool_create("trace_benchmarking", app_opts.queue_depth,
                                max_io_size, cache_size, SPDK_ENV_NUMA_ID_ANY);
  if (mempool == NULL) {
    std::error_code ec(rte_errno, std::generic_category());
    printf("Failed to create SPDK mempool: %d\n", rte_errno);
    exit(1);
  }

  run_benchmark(&app_opts, trace_entries);

  cleanup(&app_opts);
  return 0;
}
