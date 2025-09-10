import argparse
import sys
import numpy as np

"""
Biosnoop output is in the following format:
TIME(s)     COMM           PID     DISK      T SECTOR     BYTES  QUE(ms) LAT(ms)
"""
def parse_biosnoop(file, pid, engine, timing_type):
    latencies = []
    io_sizes = []
    trace_data = []  # Store trace data: (sector, io_size, queued_time, latency)
    
    for line in file:
        line = line.strip()
        if not line or line.startswith("TIME(") or line.startswith("--"):
            continue
        parts = line.split()
        if len(parts) < 8:
            continue
        try:
            line_pid = int(parts[2])
            comm = parts[1]
            que_ms = float(parts[-2])  # QUE column
            lat_ms = float(parts[-1])  # LAT column
            io_size = int(parts[-3])
            # Extract sector number - typically in the 6th column
            sector = int(parts[5])
        except (ValueError, IndexError):
            continue
        if line_pid == pid:
            cond = comm.startswith("iou-sqp") if engine == "uring" else comm.startswith("microbench_seq")
            if cond:
                if timing_type == "queued":
                    latencies.append(que_ms)
                elif timing_type == "block_device":
                    latencies.append(lat_ms)
                elif timing_type == "total":
                    latencies.append(lat_ms + que_ms)
                io_sizes.append(io_size)
                # Store trace data: sector, io_size, queued_time, latency
                trace_data.append((sector, io_size, que_ms, lat_ms))
    return latencies, io_sizes, trace_data

def print_percentiles(latencies):
    if not latencies:
        print(f"No I/O events found.")
        return
    percentiles = [50, 70, 90, 99]
    values = np.percentile(latencies, percentiles)
    
    for p, v in zip(percentiles, values):
        print(f"  p{p}: {v:.3f}")
    print(f"Mean: {np.mean(latencies)}")
    print(f"  count: {len(latencies)} events")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse biosnoop-bpfcc output and print latency percentiles.")
    parser.add_argument("--pid", type=int, required=True, help="PID to filter")
    parser.add_argument("--engine", type=str, required=True, choices=["posix", "uring"], help="Engine type")
    parser.add_argument("--timing-type", type=str, required=False, default="block_device", choices=["queued", "block_device", "total"], 
                       help="Timing type to report: queued (QUE), block_device (LAT+QUE), or total (LAT)")
    parser.add_argument("--file", type=str, help="Path to biosnoop output file (default: stdin)")
    parser.add_argument("--trace-output", type=str, help="Path to output trace file (optional)")
    args = parser.parse_args()

    if args.file:
        with open(args.file, "r") as f:
            latencies, io_sizes, trace_data = parse_biosnoop(f, args.pid, args.engine, args.timing_type)
    else:
        latencies, io_sizes, trace_data = parse_biosnoop(sys.stdin, args.pid, args.engine, args.timing_type)

    print(f"{args.timing_type.replace('_', ' ').title()} latency percentiles (ms):")
    print_percentiles(latencies)

    print(f"IO sizes percentiles:")
    print_percentiles(io_sizes)
    
    # Write trace data to file if specified
    if args.trace_output:
        with open(args.trace_output, "w") as f:
            for sector, io_size, queued_time, latency in trace_data:
                f.write(f"{sector} {io_size} {queued_time} {latency}\n")
        print(f"Trace data written to {args.trace_output} ({len(trace_data)} I/O events)")
