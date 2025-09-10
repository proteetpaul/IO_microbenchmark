#!/bin/bash

# set -e

# Usage message
usage() {
  echo "Usage: $0 --engine uring|posix --num-files N --file-size SIZE --num-threads N --chunk-size SIZE --dir DIR [--profile] [--extra-args '...']"
  echo "Example: $0 --engine uring --num-files 4 --file-size 1G --num-threads 4 --chunk-size 4096 --dir tmp/bench --profile"
  exit 1
}

# Default values
CHUNK_SIZE=4096
DIR="/mnt/ext4_fs/work_dir"
EXTRA_ARGS=""
PROFILE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --engine) ENGINE="$2"; shift 2 ;;
    --num-files) NUM_FILES="$2"; shift 2 ;;
    --file-size) FILE_SIZE="$2"; shift 2 ;;
    --num-threads) NUM_THREADS="$2"; shift 2 ;;
    --chunk-size) CHUNK_SIZE="$2"; shift 2 ;;
    --dir) DIR="$2"; shift 2 ;;
    --profile) PROFILE=true; shift ;;
    --extra-args) EXTRA_ARGS="$2"; shift 2 ;;
    *) usage ;;
  esac
done

# Check required args
if [[ -z "$ENGINE" || -z "$NUM_FILES" || -z "$FILE_SIZE" || -z "$NUM_THREADS" ]]; then
  usage
fi

# Create directory for files
mkdir -p "$DIR"

# Create files using fio
echo "Creating $NUM_FILES files of size $FILE_SIZE in $DIR ..."
for i in $(seq 0 $((NUM_FILES-1))); do
  FILE="$DIR/file${i}_${FILE_SIZE}.dat"
  if [[ ! -f "$FILE" ]]; then
    echo "  Creating $FILE ..."
    fio --name=prep --filename="$FILE" --size="$FILE_SIZE" --rw=write --direct=1 --iodepth=1 --refill_buffers --randrepeat=0 --output=/dev/null &
  fi
done

wait
sync

# Build file list for microbenchmark
FILE_LIST=""
for i in $(seq 0 $((NUM_FILES-1))); do
  FILE_LIST="$FILE_LIST $DIR/file${i}_${FILE_SIZE}.dat"
done

sudo /usr/sbin/biosnoop-bpfcc -Q > biosnoop.txt 2>&1 &
BIO_PID=$!
# Wait for 10 seconds to let the bio latency tool jit compilation finish
sleep 10

echo "Building microbenchmark"
cargo build --release --bin microbench_sequential_read

# Run the microbenchmark
echo "Running microbenchmark..."
# python3 parse_interrupts.py
env RUST_LOG=info RUST_BACKTRACE=full cargo run --release --bin microbench_sequential_read -- \
  --engine "$ENGINE" \
  --num-threads "$NUM_THREADS" \
  --file-size "$(( $(numfmt --from=iec $FILE_SIZE) ))" \
  --chunk-size "$(( $(numfmt --from=iec $CHUNK_SIZE) ))" \
  --files "$FILE_LIST" \
  $EXTRA_ARGS &
MICROBENCH_PID=$!

sudo /usr/sbin/funclatency-bpfcc nvme_pci_complete_batch --microseconds --pid $MICROBENCH_PID --duration 20 &
FUNC_LATENCY_PID=$!

# Only benchmark kernel code, 1000 Hz, generate profile output in folded format for flamegraph generation
if [[ "$PROFILE" == "true" ]]; then
  echo "Starting profiler for PID $MICROBENCH_PID..."
  sudo /usr/sbin/profile-bpfcc -F 10000 --pid $MICROBENCH_PID -K -f -d 15 --stack-storage-size=40000 > profile_output.txt &
  # profile-cache-misses-bpfcc uses the same logic as profile-bpfcc, except that it samples cache misses instead of the software timer
  # --count determines the sampling period (count=x means sample 1 out of x cache misses)
  # sudo ./profile-cache-misses-bpfcc --count 1000 --pid $MICROBENCH_PID -K -f -d 15 --stack-storage-size=40000 > cache_miss_output.txt &
  PROFILE_PID=$!  
else
  PROFILE_PID=""
fi

wait $MICROBENCH_PID
# python3 parse_interrupts.py

echo "Microbenchmark completed... Stopping biosnoop..."
sudo kill $BIO_PID

if [[ "$PROFILE" == "true" ]]; then
  echo "Stopping profiler..."
  sudo kill -SIGINT $PROFILE_PID
  wait $PROFILE_PID

  ../FlameGraph/flamegraph.pl --title="Flame Graph for IO" profile_output.txt > flamegraph_128K.svg
  # ../FlameGraph/flamegraph.pl --title="Flame Graph for IO" cache_miss_output.txt > flamegraph_128K_cache.svg
fi

sync biosnoop.txt
TRACE_FILE_NAME="traces/trace_file_${FILE_SIZE}_chunk_${CHUNK_SIZE}.txt"
python3 parse_bpfcc.py --pid $MICROBENCH_PID --engine $ENGINE --file biosnoop.txt --timing-type block_device --trace-output $TRACE_FILE_NAME

wait $FUNC_LATENCY_PID
echo "Done."
# rm -rf "$DIR"/
