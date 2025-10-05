WORKDIR=/users/proteet/IO_microbenchmark/microbenchmark_cpp/workdir
NUM_THREADS=1
ENGINE=uring
QUEUE_DEPTH=32

rm -f $WORKDIR/*
# Clear inodes and dentry cache (ref: https://www.kernel.org/doc/Documentation/sysctl/vm.txt)
echo 2 | sudo tee /proc/sys/vm/drop_caches

build/open_microbenchmark --engine $ENGINE --num-threads $NUM_THREADS --num-files 2000 --queue-depth $QUEUE_DEPTH --parent-dir ~/IO_microbenchmark/microbenchmark_cpp/workdir