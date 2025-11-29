//! Microbenchmark for sequential read performance of IO engines: io_uring (polled, kernel polling) and POSIX O_DIRECT.
//! Each benchmark launches N threads to read a set of files sequentially.
//! 

extern crate libc;
extern crate io_uring;
extern crate pprof;
extern crate clap;
extern crate log;

use std::collections::VecDeque;
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::os::raw::c_void;
use std::os::unix::fs::OpenOptionsExt;
use std::os::fd::{AsRawFd, RawFd};
use std::path::PathBuf;
use std::thread;
use std::time::Instant;
use std::alloc::{Layout, alloc};
use io_uring::squeue::Entry;
use io_uring::{cqueue, opcode, squeue, IoUring};
use libc::iovec;
use pprof::ProfilerGuard;
use std::sync::{Arc, Barrier};
use std::sync::mpsc::{self, Receiver};
use clap::Parser;
use log::info;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// IO engine: "uring" or "posix"
    #[arg(long)]
    pub engine: String,
    /// Number of threads
    #[arg(long)]
    pub num_threads: usize,
    /// File size in bytes
    #[arg(long)]
    pub file_size: usize,
    /// Chunk size in bytes
    #[arg(long, default_value_t = 4096)]
    pub chunk_size: usize,
    /// List of files to read
    #[arg(long, required = true)]
    pub files: String,
    #[arg(long, default_value_t = true)]
    pub use_fixed_buffers: bool,
    #[arg(long, default_value_t = true)]
    pub use_fixed_files: bool,
    /// Number of io_uring kernel workers
    #[arg(long, default_value_t = 1)]
    pub kernel_workers: usize,
}

#[allow(dead_code)]
fn start_flamegraph() -> ProfilerGuard<'static> {
    pprof::ProfilerGuardBuilder::default()
                .frequency(1000)
                .build()
                .unwrap()
}

#[allow(dead_code)]
fn stop_flamegraph(profiler: ProfilerGuard, filename: &str) -> Result<(), Box<dyn std::error::Error>> {
    let report = profiler.report().build()?;
    let mut svg_data = Vec::new();
    report.flamegraph(&mut svg_data)?;
    let svg_string = String::from_utf8(svg_data)?;
    std::fs::write(filename, svg_string)?;
    println!("Flamegraph saved to {}", filename);
    Ok(())
}

fn main() {
    println!("Main thread running on cpu {}", get_cpu().unwrap());
    std::thread::sleep(std::time::Duration::from_secs(10));
    let cli = Cli::parse();
    let engine = match cli.engine.as_str() {
        "uring" => IoEngine::IoUring,
        "posix" => IoEngine::PosixODirect,
        _ => panic!("Unknown engine: {}", cli.engine),
    };
    if cli.num_threads % cli.kernel_workers != 0 {
        panic!("Number of kernel workers ({}) must divide number of threads ({})", 
               cli.kernel_workers, cli.num_threads);
    }
    
    let files: Vec<std::path::PathBuf> = cli.files
        .split_whitespace()
        .map(PathBuf::from)
        .collect();
    let config = MicrobenchConfig {
        files,
        num_threads: cli.num_threads,
        file_size: cli.file_size,
        chunk_size: cli.chunk_size,
        engine,
        use_fixed_buffers: cli.use_fixed_buffers,
        use_fixed_files: cli.use_fixed_files,
        kernel_workers: cli.kernel_workers,
    };
    
    run_microbench(config);
}

#[derive(Clone)]
pub struct MicrobenchConfig {
    pub files: Vec<PathBuf>,
    pub num_threads: usize,
    pub file_size: usize,
    pub chunk_size: usize,
    pub engine: IoEngine,
    pub use_fixed_buffers: bool,
    pub use_fixed_files: bool,
    pub kernel_workers: usize,
}

#[derive(Clone)]
pub enum IoEngine {
    IoUring,
    PosixODirect,
}

pub fn run_microbench(config: MicrobenchConfig) {
    match config.engine {
        IoEngine::IoUring => run_uring_bench(config),
        IoEngine::PosixODirect => run_posix_odirect_bench(&config),
    }
}

pub struct IoUringThreadpool {
    pub workers: Vec<thread::JoinHandle<()>>,
    pub rx: Receiver<Vec<(Instant, Option<Instant>)>>,
}

// Check out ebpf -> get familiar with observability tools (bcc)
// Start with one single large file 
impl IoUringThreadpool {
    const NUM_ENTRIES: u32 = 128;
    const DURATION_MILLIS: u64 = 1000;
    pub const BUFFER_ALIGNMENT: usize = 4096;

    pub fn new(files_per_thread: &Vec<Vec<PathBuf>>, config: MicrobenchConfig) -> IoUringThreadpool {
        let (tx, rx) = mpsc::channel();
        let mut workers = Vec::<thread::JoinHandle<()>>::new();
        
        let barrier = Arc::new(Barrier::new(files_per_thread.len()));
        let user_to_kernel_worker_ratio = config.num_threads / config.kernel_workers;
        println!("Number of kernel workers: {}", config.kernel_workers);
        for i in 0..config.kernel_workers {
            let mut builder = IoUring::<squeue::Entry, cqueue::Entry>::builder();
            builder.setup_iopoll();
            builder.setup_sqpoll(50000);
            builder.setup_sqpoll_cpu(get_cpu().unwrap() as u32 + i as u32 + 1);

            let mut first_iter = true;
            for j in 0..user_to_kernel_worker_ratio {
                let thread_idx = i * user_to_kernel_worker_ratio + j;
                let ring = builder
                    .build(Self::NUM_ENTRIES)
                    .expect("Failed to build IoUring instance");
                if first_iter && user_to_kernel_worker_ratio > 1 {
                    builder.setup_attach_wq(ring.as_raw_fd());
                    first_iter = false;
                }
                let files = files_per_thread[thread_idx].clone();
                let tx = tx.clone();
                let config_clone = config.clone();
                let barrier_clone = barrier.clone();
                let worker = thread::spawn(move || {                
                    let mut uring_worker = UringWorker::new(ring, files, config_clone);
                    uring_worker.duration_based_benchmark(barrier_clone);
                    let timings = uring_worker.get_timings().clone();
                    tx.send(timings).unwrap();
                });
                workers.push(worker);
            }
        }
        
        drop(tx);
        IoUringThreadpool { workers, rx}
    }
}

#[derive(Clone)]
pub struct IoTask {
    pub ptr: *mut u8,
    pub num_bytes: usize,
    pub file: RawFd,
    pub buf_id: u16,
    pub offset: usize,
}

struct UringWorker {
    ring: io_uring::IoUring,
    // completions_array: Vec<usize>,
    tasks: Vec<IoTask>,
    file_paths: Vec<PathBuf>,
    timings: Vec<(Instant, Option<Instant>)>, // (submit_time, completion_time), indexed by file_idx
    config: MicrobenchConfig,
    files: Vec<Arc<File>>,
}

fn get_cpu() -> Option<i32> {
    let cpu = unsafe { libc::sched_getcpu() };
    if cpu == -1 {
        None // An error occurred
    } else {
        Some(cpu)
    }
}

impl UringWorker {
    fn new(ring: io_uring::IoUring, file_paths: Vec<PathBuf>, config: MicrobenchConfig) -> UringWorker {
        let mut completions_array = Vec::<usize>::new();
        completions_array.resize(1<<16, 0);
        let mut tasks = Vec::new();
        tasks.reserve(IoUringThreadpool::NUM_ENTRIES as usize);
        let timings = vec![(Instant::now(), None); file_paths.len()];
        info!("Thread running on cpu {}", get_cpu().unwrap());
        
        UringWorker {
            ring,
            tasks,
            file_paths,
            timings,
            config,
            files: Vec::new(),
        }
    }

    fn create_io_task(&mut self, file_path: &PathBuf) -> (File, *mut u8) {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(file_path)
            .expect("Failed to open file with O_DIRECT");
        let layout = Layout::from_size_align(self.config.file_size, IoUringThreadpool::BUFFER_ALIGNMENT).unwrap();
        let base_ptr = unsafe { alloc(layout) };
        (file, base_ptr)
    }

    fn create_io_tasks(&mut self) {
        let file_paths = self.file_paths.clone();
        let mut iovecs = Vec::<libc::iovec>::new();
        let num_chunks = self.config.file_size / self.config.chunk_size;
        let mut buf_id = 0;

        for file_path in file_paths.iter() {
            let (file, base_ptr) = self.create_io_task(file_path);
            let file_arc = Arc::new(file);
            iovecs.push(iovec {
                iov_base: base_ptr as *mut c_void,
                iov_len: self.config.file_size
            });
            for i in 0..num_chunks {
                let task = IoTask {
                    ptr: base_ptr.wrapping_add(i * self.config.chunk_size),
                    num_bytes: self.config.chunk_size,
                    file: file_arc.as_raw_fd(),
                    buf_id,
                    offset: i * self.config.chunk_size,
                };
                self.tasks.push(task);
            }
            self.files.push(file_arc);
            buf_id += 1;
        }
        if self.config.use_fixed_buffers {
            unsafe {
                let _ = self.ring.submitter().register_buffers(&iovecs);
            }
        }
    }

    fn duration_based_benchmark(&mut self, barrier: Arc<Barrier>) {
        let num_chunks = (self.config.file_size + self.config.chunk_size - 1) / self.config.chunk_size;
        let num_files = self.file_paths.len();
        assert!(num_files * num_chunks > IoUringThreadpool::NUM_ENTRIES as usize);

        {
            self.create_io_tasks();
        }
        let mut task_queue = VecDeque::<(IoTask, usize)>::new();
        for i in 0..self.tasks.len() {
            task_queue.push_back((self.tasks[i].clone(), i));
        }
        barrier.wait();

        // Submit initial reads
        let mut reads_submitted = 0;
        let initial_reads_submit = std::cmp::min(num_files * num_chunks, IoUringThreadpool::NUM_ENTRIES as usize);
        while reads_submitted < initial_reads_submit {
            let (task, idx) = task_queue.pop_front().unwrap();
            self.submit_single_read(task, idx);
            reads_submitted += 1;
        }
        
        let mut total_completions = 0;

        let start_time = Instant::now();
        loop {
            let num_completions = self.poll_completions(&mut task_queue);
            total_completions += num_completions;

            while num_completions > 0 {
                let (task, idx) = task_queue.pop_front().unwrap();
                self.submit_single_read(task.clone(), idx);
                reads_submitted += 1;
            }

            let current = Instant::now();
            if (current-start_time).as_millis() as u64 > IoUringThreadpool::DURATION_MILLIS {
                break;
            }
        }

        let end_time = Instant::now();
        let time_ms =  (end_time-start_time).as_millis();
        println!("Completions received: {}", total_completions);
        println!("Reads submitted: {}", reads_submitted);
        println!("Time taken: {}", time_ms);
        println!("BW: {:.2} MBps", (self.config.chunk_size * total_completions) as f64 * 1000.0 / (time_ms * 1024 * 1024) as f64);
    }

    fn poll_completions(&mut self, task_queue: &mut VecDeque<(IoTask, usize)>) -> usize {
        let mut num_completions = 0;
        {
            self.ring.submit_and_wait(1);
        }
        {
            let cq = &mut self.ring.completion();
            loop {
                cq.sync();
                match cq.next() {
                    Some(cqe) => {
                        let task_id = cqe.user_data() as usize;
                        if cqe.result() != self.config.chunk_size as i32 {
                            panic!("Read failed");
                        }
                        task_queue.push_back((self.tasks[task_id].clone(), task_id));
                        num_completions += 1;
                    }
                    None => break,
                }
            }
        }
        num_completions
    }

    fn submit_single_read(&mut self, task: IoTask, idx: usize) {
        let sqe: Entry;
        if self.config.use_fixed_buffers {
            let read_fixed_op = opcode::ReadFixed::new(
                io_uring::types::Fixed(task.file.try_into().expect("Failed to convert")),
                task.ptr,
                self.config.chunk_size as _,
                task.buf_id as u16,
            )
            .offset(task.offset as u64);
            sqe = read_fixed_op.build().user_data(idx as u64);
        } else {
            let read_op = opcode::Read::new(
                io_uring::types::Fd(task.file.as_raw_fd()),
                task.ptr,
                self.config.chunk_size as _,
            );
            sqe = read_op
                .offset(task.offset as u64)
                .build()
                .user_data(idx as u64);
        }
        unsafe {
            let sq = &mut (self.ring.submission());
            while let Err(_) = sq.push(&sqe) {
                sq.sync(); // Submit to kernel to make space
            }
        }
        self.ring.submit().expect("Failed to submit");
    }

    pub fn get_timings(&self) -> &Vec<(Instant, Option<Instant>)> {
        &self.timings
    }
}


/// Benchmark sequential read using io_uring (polled, kernel polling)
pub fn run_uring_bench(config: MicrobenchConfig) {
    println!("Running io_uring sequential read benchmark with {} threads", config.num_threads);
    println!("File size: {}", config.file_size);
    let files_per_thread = split_files(&config.files, config.num_threads);
    let mut threadpool = IoUringThreadpool::new(&files_per_thread, config);
    for worker in threadpool.workers.drain(..) {
        let _ = worker.join();
    }
    // Gather timings
    let mut all_latencies = Vec::new();
    for timings in threadpool.rx.iter() {
        for (start, end_opt) in timings {
            if let Some(end) = end_opt {
                let ms = (end - start).as_secs_f64() * 1000.0;
                all_latencies.push(ms);
            }
        }
    }
    if all_latencies.is_empty() {
        println!("No timings collected");
        return;
    }
    all_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mean = all_latencies.iter().sum::<f64>() / all_latencies.len() as f64;
    let p50 = percentile(&all_latencies, 0.5);
    let p70 = percentile(&all_latencies, 0.7);
    let p90 = percentile(&all_latencies, 0.9);
    println!("io_uring benchmark completed");
    println!("Mean: {:.2} ms, p50: {:.2} ms, p70: {:.2} ms, p90: {:.2} ms", mean, p50, p70, p90);
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    let idx = ((sorted.len() as f64) * pct).ceil() as usize - 1;
    sorted.get(idx).copied().unwrap_or(*sorted.last().unwrap())
}

/// Benchmark sequential read using POSIX O_DIRECT
pub fn run_posix_odirect_bench(config: &MicrobenchConfig) {
    println!("Running POSIX O_DIRECT sequential read benchmark with {} threads", config.num_threads);
    let files_per_thread = split_files(&config.files, config.num_threads);
    use std::sync::mpsc;
    let (tx, rx) = mpsc::channel();
    let mut workers = Vec::new();
    for (_thread_id, files) in files_per_thread.iter().enumerate() {
        let files = files.clone();
        let tx = tx.clone();
        let file_size = config.file_size;
        // let chunk_size = config.chunk_size;
        let worker = thread::spawn(move || {
            // Start per-thread flamegraph
            // let profiler = start_flamegraph();
            
            let mut latencies = Vec::with_capacity(files.len());
            for file_path in files {
                let file = OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_DIRECT)
                    .open(&file_path)
                    .expect("Failed to open file with O_DIRECT");

                let layout = Layout::from_size_align(file_size, 4096).unwrap();
                let buf = unsafe { alloc(layout) };
                let start = Instant::now();
                let ret = unsafe { libc::pread(
                    file.as_raw_fd(),
                    buf as *mut libc::c_void,
                    file_size,
                    0,
                ) };
                if ret < 0 {
                    panic!("pread failed: {}", std::io::Error::last_os_error());
                }
                let end = Instant::now();
                let ms = (end - start).as_secs_f64() * 1000.0;
                latencies.push(ms);
            }
            
            // Stop flamegraph and save
            // let filename = format!("flamegraph_thread_{}.svg", thread_id);
            // if let Err(e) = stop_flamegraph(profiler, &filename) {
            //     eprintln!("Failed to generate flamegraph for thread {}: {}", thread_id, e);
            // }
            
            tx.send(latencies).unwrap();
        });
        workers.push(worker);
    }
    drop(tx);
    for worker in workers.drain(..) {
        let _ = worker.join();
    }
    // Gather timings
    let mut all_latencies = Vec::new();
    for latencies in rx {
        all_latencies.extend(latencies);
    }
    if all_latencies.is_empty() {
        println!("No timings collected");
        return;
    }
    all_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mean = all_latencies.iter().sum::<f64>() / all_latencies.len() as f64;
    let p50 = percentile(&all_latencies, 0.5);
    let p70 = percentile(&all_latencies, 0.7);
    let p90 = percentile(&all_latencies, 0.9);
    println!("POSIX O_DIRECT benchmark completed");
    println!("Mean: {:.2} ms, p50: {:.2} ms, p70: {:.2} ms, p90: {:.2} ms", mean, p50, p70, p90);
}

/// Helper: Split files among threads
fn split_files(files: &[PathBuf], num_threads: usize) -> Vec<Vec<PathBuf>> {
    let mut out = vec![vec![]; num_threads];
    for (i, file) in files.iter().enumerate() {
        out[i % num_threads].push(file.clone());
    }
    out
} 