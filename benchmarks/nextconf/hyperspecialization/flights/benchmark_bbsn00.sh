#!/usr/bin/env bash

INPUT_FILE=/hot/data/flights/flights_on_time_performance_2019_01.csv
# make results directory
RESDIR=/hot/scratch/hyperspecial/flights
mkdir -p ${RESDIR}


# build benchmark
mkdir -p build && cd build &&


cd ..
cmake --build . --target MyExe --config Debug

# run query using runner
./