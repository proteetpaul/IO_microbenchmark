//! Microbenchmark for sequential read performance of IO engines: io_uring (polled, kernel polling) and POSIX O_DIRECT.
//! Each benchmark launches N threads to read a set of files sequentially.
//! 

extern crate libc;
extern crate io_uring;
extern crate pprof;
extern crate clap;

use std::fs::{File, OpenOptions};
use std::os::raw::c_void;
use std::os::unix::fs::OpenOptionsExt;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::thread;
use std::time::Instant;
use std::alloc::{Layout, alloc};
use io_uring::squeue::Entry;
use io_uring::{cqueue, opcode, squeue, IoUring};
use libc::{iovec, posix_fadvise};
use pprof::ProfilerGuard;
use std::sync::mpsc::{self, Receiver};
use clap::Parser;

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
    #[arg(long, default_value_t = false)]
    pub use_fixed_buffers: bool,
}

fn start_flamegraph() -> ProfilerGuard<'static> {
    pprof::ProfilerGuardBuilder::default()
                .frequency(1000)
                .build()
                .unwrap()
}

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
    std::thread::sleep(std::time::Duration::from_secs(10));
    let cli = Cli::parse();
    let engine = match cli.engine.as_str() {
        "uring" => IoEngine::IoUring,
        "posix" => IoEngine::PosixODirect,
        _ => panic!("Unknown engine: {}", cli.engine),
    };
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
    const NUM_ENTRIES: u32 = 4096;
    pub const BUFFER_ALIGNMENT: usize = 4096;

    pub fn new(files_per_thread: &Vec<Vec<PathBuf>>, config: MicrobenchConfig) -> IoUringThreadpool {
        let (tx, rx) = mpsc::channel();
        let mut workers = Vec::<thread::JoinHandle<()>>::new();
        let mut first_iter = true;
        let mut builder = IoUring::<squeue::Entry, cqueue::Entry>::builder();
        // builder.setup_iopoll();
        builder.setup_sqpoll(50000);
        builder.setup_sqpoll_cpu(12);
        for (thread_id, files) in files_per_thread.iter().enumerate() {
            let ring = builder
                .build(Self::NUM_ENTRIES)
                .expect("Failed to build IoUring instance");
            if first_iter {
                builder.setup_attach_wq(ring.as_raw_fd());
                first_iter = false;
            }
            let files = files.clone();
            let tx = tx.clone();
            let config_clone = config.clone();
            let worker = thread::spawn(move || {
                // let mut profiler: Option<ProfilerGuard<'static>> = None;
                // if thread_id == 0 {
                //     // Start per-thread flamegraph
                //     profiler = Some(start_flamegraph());
                // }
                
                let mut uring_worker = UringWorker::new(ring, files, config_clone);
                uring_worker.thread_loop();
                let timings = uring_worker.get_timings().clone();
                
                // if thread_id == 0{
                //     // Stop flamegraph and save
                //     let filename = format!("flamegraph_thread_{}.svg", thread_id);
                //     if let Err(e) = stop_flamegraph(profiler.unwrap(), &filename) {
                //         eprintln!("Failed to generate flamegraph for thread {}: {}", thread_id, e);
                //     }
                // }
                
                tx.send(timings).unwrap();
            });
            workers.push(worker);
        }
        drop(tx);
        IoUringThreadpool { workers, rx}
    }
}

pub struct IoTask {
    pub base_ptr: *mut u8,
    pub num_bytes: usize,
    pub file: File,
}

struct UringWorker {
    ring: io_uring::IoUring,
    completions_array: Vec<usize>,
    tasks: Vec<Option<IoTask>>,
    file_paths: Vec<PathBuf>,
    timings: Vec<(Instant, Option<Instant>)>, // (submit_time, completion_time), indexed by file_idx
    config: MicrobenchConfig,
}

impl UringWorker {
    fn new(ring: io_uring::IoUring, file_paths: Vec<PathBuf>, config: MicrobenchConfig) -> UringWorker {
        let mut completions_array = Vec::<usize>::new();
        completions_array.resize(1<<16, 0);
        let tasks = Vec::<Option<IoTask>>::new();
        let timings = vec![(Instant::now(), None); file_paths.len()];
        
        UringWorker {
            ring,
            completions_array,
            tasks,
            file_paths,
            timings,
            config,
        }
    }

    fn create_io_task(&mut self, file_path: &PathBuf) -> IoTask {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(file_path)
            .expect("Failed to open file with O_DIRECT");
        let layout = Layout::from_size_align(self.config.file_size, IoUringThreadpool::BUFFER_ALIGNMENT).unwrap();
        let base_ptr = unsafe { alloc(layout) };
        unsafe {
            let ret = posix_fadvise(file.as_raw_fd(), 0, self.config.file_size as i64, libc::POSIX_FADV_SEQUENTIAL);
            assert_eq!(ret, 0, "posix_fadvise errored");
        }
        IoTask {
            base_ptr,
            num_bytes: self.config.file_size,
            file,
        }
    }

    fn create_io_tasks(&mut self) {
        let file_paths = self.file_paths.clone();
        let mut iovecs = Vec::<libc::iovec>::new();
        for file_path in file_paths.iter() {
            let task = self.create_io_task(file_path);
            iovecs.push(iovec {
                iov_base: task.base_ptr as *mut c_void, 
                iov_len: self.config.file_size
            });
            self.tasks.push(Some(task));
        }
        if self.config.use_fixed_buffers {
            unsafe {
                let _ = self.ring.submitter().register_buffers(&iovecs);
            }
        }
    }

    fn thread_loop(&mut self) {
        let mut inflight = 0;
        let mut file_idx = 0;
        let num_files = self.file_paths.len();
        self.create_io_tasks();
        while file_idx < num_files || inflight > 0 {
            // Submit next file if available
            if file_idx < num_files {
                let submit_time = Instant::now();
                let num_chunks = self.submit_reads(file_idx.clone());
                if num_chunks > 0 {
                    self.completions_array[file_idx] = num_chunks;
                    self.timings[file_idx].0 = submit_time;
                    inflight += 1;
                    file_idx += 1;
                }
            }
            // Poll completions
            let completed = self.poll_completions();

            inflight -= completed;
        }
    }

    fn submit_reads(&mut self, file_idx: usize) -> usize {
        let task = self.tasks[file_idx].as_ref().unwrap();
        let ring = &mut self.ring;
        let sq = &mut (ring.submission());
        let mut buf_ptr = task.base_ptr;
        let num_chunks = (task.num_bytes + self.config.chunk_size - 1) / self.config.chunk_size;
        sq.sync();
        let remaining = sq.capacity() - sq.len();
        if num_chunks > remaining {
            return 0;
        }
        let user_data = (file_idx as u64)<<48;
        for i in 0..num_chunks {
            let sqe: Entry;
            if self.config.use_fixed_buffers {
                let read_fixed_op = opcode::ReadFixed::new(
                    io_uring::types::Fd(task.file.as_raw_fd()),
                    task.base_ptr,
                    self.config.chunk_size as _,
                    file_idx as u16,
                )
                .offset((i * self.config.chunk_size) as u64);
                sqe = read_fixed_op.build().user_data(user_data + i as u64);
            } else {
                let read_op = opcode::Read::new(
                    io_uring::types::Fd(task.file.as_raw_fd()),
                    buf_ptr,
                    self.config.chunk_size as _,
                );
                sqe = read_op
                    .offset((i * self.config.chunk_size) as u64)
                    .build()
                    .user_data(user_data + i as u64);
            }
            unsafe {
                while let Err(_) = sq.push(&sqe) {
                    sq.sync(); // Submit to kernel to make space
                }
                buf_ptr = buf_ptr.add(self.config.chunk_size);
            }
        }
        sq.sync();
        num_chunks
    }

    /// Returns number of tasks completed in this poll
    fn poll_completions(&mut self) -> usize {
        let cq = &mut self.ring.completion();
        let mut completed = 0;
        loop {
            cq.sync();
            match cq.next() {
                Some(cqe) => {
                    let errno = -cqe.result();
                    let err = std::io::Error::from_raw_os_error(errno);
                    if cqe.result() == 0 {
                        print!("here\n");
                    }
                    assert!(
                        cqe.result() == self.config.chunk_size as i32 || cqe.result() == 0,
                        "Read cqe result error: {err}"
                    );
                    let opcode = (cqe.user_data()>>48) as usize;
                    let remaining = &mut self.completions_array[opcode];
                    *remaining -= 1;
                    if *remaining == 0 {
                        self.timings[opcode].1 = Some(Instant::now());
                        completed += 1;
                        self.tasks[opcode].take();
                    }
                },
                None => {
                    break;
                }
            }
        }
        completed
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
    for (thread_id, files) in files_per_thread.iter().enumerate() {
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