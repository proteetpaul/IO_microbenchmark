FIO_PATH="${HOME}/fio/fio"

numactl --cpunodebind=0 $FIO_PATH --thread fio_test.ini &
FIO_PID=$!

echo $FIO_PID

sudo /usr/sbin/funclatency-bpfcc io_issue_sqe --microseconds --pid $FIO_PID --duration 20 &
FUNC_LATENCY_PID=$!

sudo /usr/sbin/profile-bpfcc -F 10000 --pid $FIO_PID -K -f -d 15 --stack-storage-size=40000 > profile_output.txt &
PROFILE_PID=$!

wait $FIO_PID
wait $FUNC_LATENCY_PID

echo "Stopping profiler..."
sudo kill -SIGINT $PROFILE_PID
wait $PROFILE_PID

../FlameGraph/flamegraph.pl --title="Flame Graph for fio" profile_output.txt > flamegraph.svg