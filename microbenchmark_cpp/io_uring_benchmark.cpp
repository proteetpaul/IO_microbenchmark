#include <algorithm>
#include <bits/types/struct_iovec.h>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <iostream>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <pthread.h>
#include <stdexcept>
#include <string.h>
#include <string>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <thread>
#include <unistd.h>
#include <vector>

const size_t BUFFER_ALIGNMENT = 4096;

enum class IoEngine { IoUring, PosixODirect };

struct MicrobenchConfig {
  std::vector<std::string> files;
  int num_threads;
  size_t file_size;
  size_t chunk_size;
  uint32_t queue_depth;
  bool use_fixed_buffers = false;
  bool use_fixed_files = false;
  IoEngine engine = IoEngine::IoUring;
};

// Splits a list of files among a number of threads in a round-robin fashion.
std::vector<std::vector<std::string>>
split_files(const std::vector<std::string> &files, int num_threads) {
  std::vector<std::vector<std::string>> out(num_threads);
  for (size_t i = 0; i < files.size(); ++i) {
    out[i % num_threads].push_back(files[i]);
  }
  return out;
}

double percentile(const std::vector<double> &sorted_values, double pct) {
  if (sorted_values.empty()) {
    return 0.0;
  }
  size_t idx = static_cast<size_t>(ceil(sorted_values.size() * pct)) - 1;
  idx = std::min(idx, sorted_values.size() - 1);
  return sorted_values[idx];
}

const unsigned long long DURATION_MILLIS = 1000; // Benchmark duration

class UringWorker {
public:
  UringWorker(const std::vector<std::string> &files,
              const MicrobenchConfig &cfg, io_uring ring_params)
      : file_paths(files), config(cfg), ring(std::move(ring_params)) {
    num_files = file_paths.size();
  }

  void run(pthread_barrier_t &start_barrier) {
    open_files_and_alloc_buffers();

    // 2. Wait for all threads to be ready before starting the benchmark.
    pthread_barrier_wait(&start_barrier);
    run_duration_benchmark();

    io_uring_queue_exit(&ring);
    for (void *buf : buffers) {
      free(buf);
    }
    for (int fd : fds) {
      close(fd);
    }
  }

private:
	io_uring ring;

	const std::vector<std::string> &file_paths;

	const MicrobenchConfig &config;

	std::vector<int> fds;

	std::vector<void *> buffers;

	uint32_t num_files;

	void open_files_and_alloc_buffers() {
    const size_t num_chunks =
			(config.file_size + config.chunk_size - 1) / config.chunk_size;
		fds.resize(file_paths.size());
		buffers.resize(file_paths.size() * num_chunks);

		std::vector<iovec> iovecs(num_files * num_chunks);

		for (uint32_t i = 0; i < num_files; ++i) {
			fds[i] = open(file_paths[i].c_str(), O_RDONLY | O_DIRECT);
			if (fds[i] < 0) {
				throw std::runtime_error("Failed to open file for uring: " +
										file_paths[i]);
			}
      for (uint32_t j=0; j<num_chunks; j++) {
        uint32_t buffer_idx = i * num_chunks + j;
        if (posix_memalign(&buffers[buffer_idx], BUFFER_ALIGNMENT, config.file_size) !=
            0) {
          throw std::runtime_error("Failed to allocate aligned memory for uring");
        }
        iovecs[buffer_idx].iov_base = buffers[buffer_idx];
			  iovecs[buffer_idx].iov_len = config.chunk_size;
      }
		}
		if (config.use_fixed_buffers) {
			io_uring_register(ring.ring_fd, IORING_REGISTER_BUFFERS, iovecs.data(),
							num_files * num_chunks);
		}
		if (config.use_fixed_files) {
			int ret = io_uring_register(ring.ring_fd, IORING_REGISTER_FILES, fds.data(),
							num_files);
			if (ret < 0) {
				throw std::runtime_error("Failed to register files\n");
			}
		}
	}

	void run_duration_benchmark() {
		const size_t num_chunks =
			(config.file_size + config.chunk_size - 1) / config.chunk_size;
		if (num_files == 0) {
			return;
		}

		// Submit initial batch of read requests
		size_t file_idx = 0;
		size_t chunk_idx = 0;
		for (unsigned int i = 0; i < config.queue_depth; ++i) {
			submit_single_read(file_idx, chunk_idx);
			chunk_idx++;
			if (chunk_idx == num_chunks) {
				chunk_idx = 0;
				file_idx = (file_idx + 1) % num_files;
			}
		}


		long long reads_submitted = config.queue_depth;
		long long total_completions = 0;

		auto start_time = std::chrono::high_resolution_clock::now();
		while (true) {
			io_uring_submit(&ring);

			struct io_uring_cqe *cqe;
			// Wait for one completion, but then reap all available ones in the loop.
			int ret = io_uring_wait_cqe(&ring, &cqe);
			if (ret < 0) {
				// ETIME means the timeout was reached, which we don't use here.
				if (-ret != ETIME) {
				fprintf(stderr, "io_uring_wait_cqe failed: %s\n", strerror(-ret));
				}
				break;
			}

			unsigned head;
			unsigned completions_this_round = 0;
			io_uring_for_each_cqe(&ring, head, cqe) {
				completions_this_round++;
				if (cqe->res < 0) {
					fprintf(stderr, "Async read failed: %s\n", strerror(-cqe->res));
					throw std::runtime_error("Read failed");
				}
			}

			total_completions += completions_this_round;
			io_uring_cq_advance(&ring, completions_this_round);

			// Re-submit one new read for each one that completed.
			for (unsigned i = 0; i < completions_this_round; ++i) {
				bool ret = submit_single_read(file_idx, chunk_idx);
				if (!ret) {
					printf("Failed to submit\n");
					break;
				}
				reads_submitted++;
				chunk_idx++;
				if (chunk_idx == num_chunks) {
					chunk_idx = 0;
					file_idx = (file_idx + 1) % num_files;
				}
			}

			auto current_time = std::chrono::high_resolution_clock::now();
			auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
									current_time - start_time)
									.count();
			if (elapsed_ms >= DURATION_MILLIS) {
				break;
			}
		}

		auto end_time = std::chrono::high_resolution_clock::now();
		double time_ms =
			std::chrono::duration<double, std::milli>(end_time - start_time)
				.count();
		double total_bytes =
			static_cast<double>(total_completions) * config.chunk_size;
		double throughput_mbps =
			(total_bytes * 1000.0) / (time_ms * 1024.0 * 1024.0);

		std::cout << "Thread finished." << std::endl;
		printf("  Completions: %lld, Submitted: %lld\n", total_completions,
			reads_submitted);
		printf("  Time taken: %.2f ms\n", time_ms);
		printf("  Bandwidth: %.2f MB/s\n", throughput_mbps);
	}

	bool submit_single_read(size_t file_idx, size_t chunk_idx) {
    const size_t num_chunks =
			(config.file_size + config.chunk_size - 1) / config.chunk_size;
		struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
		if (!sqe) {
			// Should not happen if we only submit after a completion
			return false;
		}

		int fd = fds[file_idx];
    uint32_t buf_idx = file_idx * num_chunks + chunk_idx;
		off_t offset = chunk_idx * config.chunk_size;

		if (config.use_fixed_buffers) {
			io_uring_prep_read_fixed(
				sqe, fd,
				buffers[buf_idx],
				config.chunk_size, offset, file_idx);
		} else {
			io_uring_prep_read(sqe, fd,
				buffers[buf_idx],
				config.chunk_size, offset);
		}
		if (config.use_fixed_files) {
			sqe->flags |= IOSQE_FIXED_FILE;
			sqe->fd = file_idx;
		}
		io_uring_submit(&ring);
		return true;
	}
};

struct io_uring *create_and_configure_rings(const MicrobenchConfig &config) {
  struct io_uring *rings =
      (struct io_uring *)malloc(config.num_threads * sizeof(struct io_uring));
  for (int i = 0; i < config.num_threads; i++) {
    struct io_uring_params params;
    memset(&params, 0, sizeof(struct io_uring_params));
    params.flags = IORING_SETUP_IOPOLL | IORING_SETUP_SQPOLL;
    if (i > 0) {
      params.flags |= IORING_SETUP_ATTACH_WQ;
	  params.wq_fd = rings[0].ring_fd;
    }
    int ret = io_uring_queue_init_params(config.queue_depth, &rings[i], &params);
	if (ret < 0) {
		throw std::runtime_error("Failed to create ring\n");
	}

    // int ring_fd = io_uring_setup(config.queue_depth, &params);
    // if (ring_fd < 0) {
    //     throw std::runtime_error("Failed to setup ring");
    // }
    // uint8_t *sq_ring_ptr = (uint8_t *)mmap(
    //     NULL, params.sq_off.array + params.sq_entries * sizeof(__U32_TYPE),
    //     PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd,
    //     IORING_OFF_SQ_RING);
    // uint8_t *sqes_ptr =
    //     (uint8_t *)mmap(NULL, params.sq_entries * sizeof(struct
    //     io_uring_sqe),
    //                     PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE,
    //                     ring_fd, IORING_OFF_SQES);
    // uint8_t *cq_ring_ptr = (uint8_t *)mmap(
    //     NULL,
    //     params.cq_off.cqes + params.cq_entries * sizeof(struct io_uring_cq),
    //     PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ring_fd,
    //     IORING_OFF_CQ_RING);
    // ring_params[i].fd = ring_fd;

    // ring_params[i].sq_params.sq_ring_ptr = sq_ring_ptr + params.sq_off.array;
    // ring_params[i].sq_params.sq_entries_ptr =
    //     sqes_ptr + params.sq_off.ring_entries;
    // ring_params[i].sq_params.sq_head_ptr = sqes_ptr + params.sq_off.head;
    // ring_params[i].sq_params.sq_tail_ptr = sqes_ptr + params.sq_off.tail;
    // ring_params[i].sq_params.sq_flags_ptr = sqes_ptr + params.sq_off.flags;
    // ring_params[i].sq_params.sq_flags_ptr = sqes_ptr +
    // params.sq_off.ring_mask;

    // ring_params[i].cq_params.cq_head_ptr = cq_ring_ptr + params.cq_off.head;
    // ring_params[i].cq_params.cq_tail_ptr = cq_ring_ptr + params.cq_off.tail;
    // ring_params[i].cq_params.cq_ring_ptr =
    //     cq_ring_ptr + params.cq_off.ring_entries;
  }
  return rings;
}

void run_uring_bench(const MicrobenchConfig &config) {
  std::cout << "Running io_uring sequential read benchmark with "
            << config.num_threads << " threads" << std::endl;
  std::cout << "File size: " << config.file_size
            << ", Chunk size: " << config.chunk_size << std::endl;

  auto files_per_thread = split_files(config.files, config.num_threads);
  struct io_uring *rings = create_and_configure_rings(config);
  std::vector<std::thread> workers;

  // Barrier ensures all threads start the timed benchmark loop simultaneously.
  pthread_barrier_t start_barrier;
  pthread_barrier_init(&start_barrier, NULL, config.num_threads);

  for (int i = 0; i < config.num_threads; ++i) {
	printf("Launching worker\n");
    workers.emplace_back([&, i]() {
      try {
        UringWorker worker(files_per_thread[i], config,
                           std::move(rings[i]));
        worker.run(start_barrier);
      } catch (const std::exception &e) {
        std::cerr << "Thread " << i << " failed: " << e.what() << std::endl;
      }
    });
  }

  for (auto &worker : workers) {
    worker.join();
  }

  std::cout << "io_uring benchmark completed" << std::endl;
}

void print_usage() {
  std::cout
      << "Usage: ./microbench [OPTIONS]\n"
      << "Options:\n"
      << "  --engine <string>          IO engine: \"uring\" or \"posix\" "
         "(required)\n"
      << "  --num-threads <int>        Number of threads (required)\n"
      << "  --file-size <bytes>        File size in bytes (required)\n"
      << "  --chunk-size <bytes>       Chunk size in bytes (default: 4096)\n"
      << "  --files \"<path1> <path2>...\" List of files to read (required, in "
         "quotes)\n";
}

int main(int argc, char *argv[]) {
  MicrobenchConfig config;
  std::vector<std::string> args(argv + 1, argv + argc);

  for (size_t i = 0; i < args.size(); ++i) {
    if (args[i] == "--engine") {
      if (args[++i] == "uring")
        config.engine = IoEngine::IoUring;
      else
        config.engine = IoEngine::PosixODirect;
    } else if (args[i] == "--num-threads") {
      config.num_threads = std::stoi(args[++i]);
    } else if (args[i] == "--file-size") {
      config.file_size = std::stoul(args[++i]);
    } else if (args[i] == "--chunk-size") {
      config.chunk_size = std::stoul(args[++i]);
    } else if (args[i] == "--files") {
      std::string file_list = args[++i];
      std::string current_file;
      for (char c : file_list) {
        if (c == ' ') {
          if (!current_file.empty()) {
            config.files.push_back(current_file);
            current_file.clear();
          }
        } else {
          current_file += c;
        }
      }
      if (!current_file.empty()) {
        config.files.push_back(current_file);
      }
    } else if (args[i] == "--use-fixed-buffers") {
      config.use_fixed_buffers = true;
    } else if (args[i] == "--use-fixed-files") {
      config.use_fixed_files = true;
    } else if (args[i] == "--queue-depth") {
      config.queue_depth = std::stoi(args[++i]);
    }
  }
  printf("Num files: %d\n", config.files.size());
  printf("Queue depth: %d\n", config.queue_depth);
  printf("use fixed buffers: %d\n", config.use_fixed_buffers);
  printf("use fixed files: %d\n", config.use_fixed_files);
  

  // Basic validation
  if (config.file_size == 0 || config.num_threads == 0 ||
      config.files.empty()) {
    print_usage();
    return 1;
  }

  try {
    if (config.engine == IoEngine::IoUring) {
      run_uring_bench(config);
    } else {
    }
  } catch (const std::exception &e) {
    std::cerr << "An error occurred: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}
