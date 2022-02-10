#!/usr/bin/env bash

INPUT_FILE=/hot/data/flights/flights_on_time_performance_2019_01.csv
# make results directory
RESDIR=/hot/scratch/hyperspecial/flights
mkdir -p ${RESDIR}


# build benchmark
mkdir -p build && pushd build &&
cmake -DCMAKE_BUILD_TYPE=Release ..

mkdir -p process_row && mv *.so process_row
# run query using runner
./build/runner_orig --path $INPUT_FILE --output_path $RESDIR
./build/runner_constant --path $INPUT_FILE --output_path $RESDIR
./build/runner_narrow --path $INPUT_FILE --output_path $RESDIR

popd
