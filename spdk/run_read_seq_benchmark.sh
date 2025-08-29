#!/usr/bin/env bash

set -euo pipefail

print_usage() {
    echo "Usage: $0 -t <transport_id> -n <num_threads> -f <num_files> -s <file_size> [options]" >&2
    echo "Options:" >&2
    echo "  -C <cmake_args>     Extra CMake args (e.g., -DSPDK_DIR=/opt/spdk)" >&2
    echo "  -p <profiler>       Profiler: stat|record (default: record)" >&2
    echo "  -o <perf_output>    Output file for perf record (default: perf.data)" >&2
    echo "  -G <flamegraph_dir> Path to Flamegraph tools dir (default: ./Flamegraph)" >&2
    echo "  -O <svg_output>     Output SVG path (default: flamegraph.svg)" >&2
    echo "  -h                  Show this help" >&2
}

CMAKE_ARGS=""
BIN="./build/spdk_microbenchmark"
TRANSPORT_ID=""
NUM_THREADS=""
NUM_FILES=""
FILE_SIZE=""
PROFILER="record"
PERF_OUT="perf.data"
FLAMEGRAPH_DIR="../FlameGraph"
FLAMEGRAPH_OUT="flamegraph.svg"

while getopts ":C:t:n:f:s:p:o:G:O:h" opt; do
  case ${opt} in
    C) CMAKE_ARGS=${OPTARG} ;;
    t) TRANSPORT_ID=${OPTARG} ;;
    n) NUM_THREADS=${OPTARG} ;;
    f) NUM_FILES=${OPTARG} ;;
    s) FILE_SIZE=${OPTARG} ;;
    p) PROFILER=${OPTARG} ;;
    o) PERF_OUT=${OPTARG} ;;
    G) FLAMEGRAPH_DIR=${OPTARG} ;;
    O) FLAMEGRAPH_OUT=${OPTARG} ;;
    h) print_usage; exit 0 ;;
    :) echo "Error: -${OPTARG} requires an argument" >&2; print_usage; exit 1 ;;
    \?) echo "Error: invalid option -${OPTARG}" >&2; print_usage; exit 1 ;;
  esac
done

# Configure and build with CMake in ./build
mkdir -p build
cmake -S . -B build ${CMAKE_ARGS}
cmake --build build -j"$(nproc)"

if [[ -z "$TRANSPORT_ID" || -z "$NUM_THREADS" || -z "$NUM_FILES" || -z "$FILE_SIZE" ]]; then
  echo "Error: -t, -n, -f, -s are required" >&2
  print_usage
  exit 1
fi

CMD=("$BIN" \
  --transport_id "$TRANSPORT_ID" \
  --num_threads "$NUM_THREADS" \
  --num_files "$NUM_FILES" \
  --file_size "$FILE_SIZE")

echo "Running: ${CMD[*]}" >&2

case "$PROFILER" in
  stat)
    exec perf stat -d -d -d -- "${CMD[@]}"
    ;;
  record)
    perf record -F 10000 --call-graph dwarf -o "$PERF_OUT" -- "${CMD[@]}"
    status=$?

    if [[ ! -f "$PERF_OUT" ]]; then
      echo "Warning: perf output '$PERF_OUT' not found; skipping flamegraph." >&2
      exit 0
    fi
    if [[ ! -x "$FLAMEGRAPH_DIR/flamegraph.pl" ]] || [[ ! -x "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" ]]; then
      echo "Warning: Flamegraph tools not found in '$FLAMEGRAPH_DIR'. Skipping SVG generation." >&2
      exit 0
    fi
    echo "Generating Flamegraph SVG -> $FLAMEGRAPH_OUT" >&2
    perf script | perl "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" | perl "$FLAMEGRAPH_DIR/flamegraph.pl" > "$FLAMEGRAPH_OUT"
    ;;
  *)
    echo "Error: unknown profiler '$PROFILER' (use 'stat' or 'record')" >&2
    exit 1
    ;;
esac

