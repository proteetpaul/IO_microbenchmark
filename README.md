# IO Microbenchmark

A Rust-based microbenchmark for comparing sequential read performance between different IO engines: io_uring (with kernel polling) and POSIX O_DIRECT.

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

## Building the Microbenchmark

Navigate to the microbenchmark directory and build the project:

```bash
cd microbenchmark
cargo build --release --bin microbench_sequential_read
```

## Usage

### Basic Usage

The microbenchmark supports two IO engines:
- `uring`: io_uring with kernel polling
- `posix`: POSIX O_DIRECT

### Command Line Options

```bash
./target/release/microbench_sequential_read \
    --engine <uring|posix> \
    --num-threads <number> \
    --file-size <bytes> \
    --chunk-size <bytes> \
    --files "<file1> <file2> ..."
```

### Examples

#### 1. Generate Test Files

First, create some test files using FIO:

```bash
# Create a 1GB test file
fio --name=testfile --size=1G --filename=testfile.dat --create_only=1

# Create multiple test files
for i in {1..4}; do
    fio --name=testfile$i --size=1G --filename=testfile$i.dat --create_only=1
done
```

#### 2. Running the microbenchmark

```bash
cd microbenchmark
./run_read_microbenchmark.sh --engine uring|posix --num-files N --file-size SIZE --num-threads N --chunk-size SIZE --dir DIR
```