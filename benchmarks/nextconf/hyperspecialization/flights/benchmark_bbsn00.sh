#!/usr/bin/env bash

INPUT_FILE=/hot/data/flights/flights_on_time_performance_2019_01.csv
# make results directory
RESDIR=/hot/scratch/hyperspecial/flights
mkdir -p ${RESDIR}


# build benchmark
mkdir -p build && pushd build &&
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)

mkdir -p process_row && mv *.so process_row
# run query using runner
NUM_RUNS="${NUM_RUNS:-11}"

BENCHDIR=benchmark_results
mkdir -p ${BENCHDIR}

for ((r=1; r <= NUM_RUNS; r++)); do
LOG=${BENCHDIR}/flights-orig-run-$r.txt
./runner_orig --path $INPUT_FILE --output_path $RESDIR 2>&1 | tee $LOG
done

for ((r=1; r <= NUM_RUNS; r++)); do
LOG=${BENCHDIR}/flights-constant-run-$r.txt
./runner_constant --path $INPUT_FILE --output_path $RESDIR 2>&1 | tee $LOG
done

for ((r=1; r <= NUM_RUNS; r++)); do
LOG=${BENCHDIR}/flights-narrow-run-$r.txt
./runner_narrow --path $INPUT_FILE --output_path $RESDIR 2>&1 | tee $LOG
done

popd
