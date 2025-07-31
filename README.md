# IO Microbenchmark

A Rust-based microbenchmark for comparing sequential read performance between different IO engines: io_uring (with io polling and kernel polling) and POSIX (O_DIRECT enabled).

## Prerequisites

### 1. Install BPF Compiler Collection (BCC) Tools

BCC tools are required for profiling block IO's. Install them based on your Linux distribution:

#### Ubuntu/Debian:
```bash
sudo apt-get update
sudo apt-get install -y bpfcc-tools linux-headers-$(uname -r)
```

### 2. Install FIO (Flexible I/O Tester)

FIO is used for comparison benchmarks and file generation:

#### Ubuntu/Debian:
```bash
sudo apt-get install fio
```

## Running the microbenchmark


```bash
cd microbenchmark
./run_read_microbenchmark.sh --engine uring|posix --num-files N --file-size SIZE --num-threads N --chunk-size SIZE --dir DIR
```
