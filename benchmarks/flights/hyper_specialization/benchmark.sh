#!/usr/bin/env bash

# Parse HWLOC settings
HWLOC=""
if [ $# -ne 0 ] && [ $# -ne 1 ]; then # check nmber of inputs
  echo "usage: ./benchmark.sh [-hwloc]"
  exit 1
fi

if [ $# -eq 1 ]; then # check if hwloc
  if [ "$1" != "-hwloc" ]; then # check flag
    echo -e "invalid flag: $1\nusage: ./benchmark.sh [-hwloc]"
    exit 1
  fi
  HWLOC="hwloc-bind --cpubind node:1 --membind node:1 --cpubind node:2 --membind node:2"
fi

# use 11 runs (in case of cold start) and a timeout after 60min
NUM_RUNS=5
TIMEOUT=3600
INPUT_FILE='2019.csv' #'/disk/data/zillow/large10GB.csv'

# make results directory
RESDIR=results_hyperspecial
mkdir -p ${RESDIR}

# basic 
# compile cpp
#echo "compiling basic cpp"
#COMPILE_LOG="${RESDIR}/basic-compile.txt"
#(time ./build-cpp.sh) &>$COMPILE_LOG

#echo "benchmarking basic"
#for ((r = 1; r <= NUM_RUNS; r++)); do
  #LOG="${RESDIR}/basic-run-$r.txt"
  #timeout $TIMEOUT ${HWLOC} ./basic --path $INPUT_FILE --output_path basic_output >$LOG 2>$LOG.stderr
#done

# constants 
# compile cpp
echo "compiling constant cpp"
COMPILE_LOG="${RESDIR}/constant-compile.txt"
(time ./build-cpp.sh) &>$COMPILE_LOG

echo "benchmarking constant"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/constant-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ./basic --path $INPUT_FILE --output_path constant_output >$LOG 2>$LOG.stderr
done

