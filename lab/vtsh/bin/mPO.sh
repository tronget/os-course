#!/bin/sh
NUM_PROCESSES=$1
TEST_TYPE=$2

for i in $(seq 1 $NUM_PROCESSES); do
    if [ "$TEST_TYPE" = "sequential_read" ]; then
        ./ioload rw=read block_size=131072 block_count=184320 file=/tmp/testfile.bin range=0-0 direct=on type=sequence &
    elif [ "$TEST_TYPE" = "random_read" ]; then
        ./ioload rw=read block_size=4096 block_count=150000 file=/tmp/testfile.bin range=0-0 direct=on type=random &
    fi
done

echo "Started $NUM_PROCESSES processes of type $TEST_TYPE"
wait
echo "All processes completed"
