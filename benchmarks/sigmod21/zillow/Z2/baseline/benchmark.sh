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
NUM_RUNS=11
TIMEOUT=3600
INPUT_FILE='/disk/data/zillow/large10GB.csv'

# make results directory
RESDIR=results_zillow-baseline
mkdir -p ${RESDIR}

# tuplex
echo "benchmarking tuplex (without cache)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 ../runtuplex.py --output-path tuplex_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

echo "benchmarking tuplex (with cache)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex_io-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 ../runtuplex-io.py --output-path tuplex_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
done

# compile cpp
echo "compiling cpp"
COMPILE_LOG="${RESDIR}/cpp_baseline-compile.txt"
(time ./build-cpp.sh) &>$COMPILE_LOG

# cpp
echo "benchmarking cpp (without preload)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/cpp_no_preload-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ./tester --path $INPUT_FILE --output_path no_preload_output >$LOG 2>$LOG.stderr
done

echo "benchmarking cpp (with preload)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/cpp_preload-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ./tester --path $INPUT_FILE --output_path preload_output --preload  >$LOG 2>$LOG.stderr
done
