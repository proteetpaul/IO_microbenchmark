#include <cstdint>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <liburing.h>
#include <pthread.h>
#include <ratio>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <cstdio>
#include <iostream>
#include <chrono>

typedef std::pair<uint32_t, uint32_t> Range;

enum class IoEngine { IoUring, PosixODirect };

struct MicrobenchConfig {
    std::string parent_dir;
	int num_files;
    int num_threads;
    uint32_t queue_depth;
    IoEngine engine = IoEngine::IoUring;
};

std::vector<Range>
split_files(const uint32_t num_files, const int num_threads) {
	std::vector<Range> ranges;
	if (num_threads <= 0) {
		return ranges;
	}
	ranges.resize(static_cast<size_t>(num_threads));

	const uint32_t base = num_threads > 0 ? (num_files / static_cast<uint32_t>(num_threads)) : 0u;
	const uint32_t rem = num_threads > 0 ? (num_files % static_cast<uint32_t>(num_threads)) : 0u;

	uint32_t start = 0;
	for (int i = 0; i < num_threads; ++i) {
		const bool get_extra = static_cast<uint32_t>(i) < rem;
		const uint32_t count = base + (get_extra ? 1u : 0u);
		const uint32_t end = start + count; // half-open [start, end)
		ranges[static_cast<size_t>(i)] = {start, end};
		start = end;
	}

	return ranges;
}

class UringWorker {
public:
	UringWorker(const Range file_range,
				const MicrobenchConfig &cfg, io_uring ring_params, int dirfd)
			: file_idx_range(file_range), config(cfg), ring(std::move(ring_params)), dirfd(dirfd) {
	}

	void run(pthread_barrier_t &start_barrier) {
		printf("Thread running on cpu %d\n", sched_getcpu());
		for (int i=file_idx_range.first; i<file_idx_range.second; i++) {
			std::string file_name = "file_" + std::to_string(i) + ".dat";
			files.push_back(file_name);
		}
		// 2. Wait for all threads to be ready before starting the benchmark.
		pthread_barrier_wait(&start_barrier);
		run_benchmark();

		io_uring_queue_exit(&ring);
		for (int fd : fds) {
			close(fd);
		}
	}
	
private:
	io_uring ring;

	int dirfd;

	const Range file_idx_range;

	const MicrobenchConfig &config;

	std::vector<int> fds;

	std::vector<std::string> files;

	void run_benchmark() {
		int num_files = file_idx_range.second - file_idx_range.first;
		// Submit initial batch of read requests
		size_t file_idx = 0;
		for (unsigned int i = 0; i < config.queue_depth; ++i) {
			bool ret = submit_single_op(file_idx);
			file_idx++;
			if (!ret) {
				printf("Failed to submit\n");
				break;
			}
		}
		long long total_completions = 0;

		auto start_time = std::chrono::high_resolution_clock::now();
		while (total_completions < num_files) {
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
					fprintf(stderr, "Open failed: %s\n", strerror(-cqe->res));
					throw std::runtime_error("Open failed");
				}
			}

			total_completions += completions_this_round;
			io_uring_cq_advance(&ring, completions_this_round);

			if (file_idx >= num_files) continue;

			// Re-submit one new read for each one that completed.
			for (unsigned i = 0; i < completions_this_round; ++i) {
				bool ret = submit_single_op(file_idx);
				file_idx++;
				if (!ret) {
					printf("Failed to submit\n");
					break;
				}
			}
		}

		auto end_time = std::chrono::high_resolution_clock::now();
		double time_us =
			std::chrono::duration<double, std::micro>(end_time - start_time)
				.count();

		printf("Thread finished.");
		printf("  Time taken: %.2f us\n", time_us);
	}

	bool submit_single_op(size_t file_idx) {
		struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
		if (!sqe) {
			// Should not happen if we only submit after a completion
			return false;
		}
		io_uring_prep_openat(sqe, dirfd, files[file_idx].c_str(), O_CREAT | O_RDWR, S_IRWXU);
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
		params.flags = IORING_SETUP_SQPOLL;
		if (i > 0) {
			params.flags |= IORING_SETUP_ATTACH_WQ;
			params.wq_fd = rings[0].ring_fd;
		}
		int ret = io_uring_queue_init_params(config.queue_depth, &rings[i], &params);
		if (ret < 0) {
			throw std::runtime_error("Failed to create ring\n");
		}
	}
	return rings;
}
	
void run_uring_bench(const MicrobenchConfig &config, int dirfd) {
	auto files_per_thread = split_files(config.num_files, config.num_threads);
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
									std::move(rings[i]), dirfd);
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

void run_posix_bench_thread(Range file_idx_range, pthread_barrier_t &start_barrier, int dirfd) {
	printf("Thread running on cpu %d\n", sched_getcpu());

	std::vector<std::string> files;
	for (int i=file_idx_range.first; i<file_idx_range.second; i++) {
		std::string file_name = "file_" + std::to_string(i) + ".dat";
		files.push_back(file_name);
	}
	pthread_barrier_wait(&start_barrier);
	auto start_time = std::chrono::high_resolution_clock::now();

	for (auto& file: files) {
		int fd = openat(dirfd, file.c_str(), O_CREAT | O_RDWR, S_IRWXU);
		if (fd < 0) {
			std::cout << "Error encountered while opening error\n";
			break;
		}
	}
	auto end_time = std::chrono::high_resolution_clock::now();
	double time_us =
		std::chrono::duration<double, std::micro>(end_time - start_time)
			.count();

	printf("Thread finished.");
	printf("  Time taken: %.2f us\n", time_us);
}

void run_posix_bench(const MicrobenchConfig &config, int dirfd) {
	auto files_per_thread = split_files(config.num_files, config.num_threads);
	std::vector<std::thread> workers;

	pthread_barrier_t start_barrier;
	pthread_barrier_init(&start_barrier, NULL, config.num_threads);
	for (int i = 0; i < config.num_threads; ++i) {
		printf("Launching worker\n");
		workers.emplace_back([&, i]() {
			run_posix_bench_thread(files_per_thread[i], start_barrier, dirfd);
		});
	}
	for (auto &worker : workers) {
		worker.join();
	}
	std::cout << "Posix benchmark completed" << std::endl;
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
		} else if (args[i] == "--num-files") {
			config.num_files = std::stoi(args[++i]);
		} else if (args[i] == "--queue-depth") {
			config.queue_depth = std::stoi(args[++i]);
		} else if (args[i] == "--parent-dir") {
			config.parent_dir = args[++i];
		}
	}
    printf("Num files: %d\n", config.num_files);
	printf("Queue depth: %d\n", config.queue_depth);
	printf("Parent dir: %s\n", config.parent_dir.c_str());

	int directory_fd = dirfd(opendir(config.parent_dir.c_str()));

	try {
		if (config.engine == IoEngine::IoUring) {
			run_uring_bench(config, directory_fd);
		} else {
			run_posix_bench(config, directory_fd);
		}
	} catch (const std::exception &e) {
		std::cerr << "An error occurred: " << e.what() << std::endl;
		return 1;
	}
	
	return 0;
  }
  