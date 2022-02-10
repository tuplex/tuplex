#!/usr/bin/env bash

INPUT_FILE=/hot/data/flights/flights_on_time_performance_2019_01.csv
# make results directory
RESDIR=/hot/scratch/hyperspecial/flights
mkdir -p ${RESDIR}


# build benchmark
mkdir -p build && cd build &&
cmake -DCMAKE_BUILD_TYPE=Release .. &&
cd ..


# run query using runner
./build/runner --path $INPUT_FILE --