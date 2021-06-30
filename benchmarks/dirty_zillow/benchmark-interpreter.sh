#!/usr/bin/env bash
# (c) L.Spiegelberg 2017-2021
# run benchmark of all baselines (C++, Scala, PySparksQL-Scala)...

# Parse HWLOC settings
HWLOC=""
if [ $# -ne 0 ] && [ $# -ne 1 ]; then # check nmber of inputs
  echo "usage: ./benchmark_baselines_bbsn00.sh [-hwloc]"
  exit 1
fi

if [ $# -eq 1 ]; then # check if hwloc
  if [ "$1" != "-hwloc" ]; then # check flag
    echo -e "invalid flag: $1\nusage: ././benchmark_baselines_bbsn00.sh [-hwloc]"
    exit 1
  fi
  HWLOC="hwloc-bind --cpubind node:1 --membind node:1 --cpubind node:2 --membind node:2"
fi

# bbsn00
export PATH=/opt/pypy3.6/bin/:$PATH

INPUT_FILE=data/zillow_dirty@10G.csv
RESDIR=benchmark_results/pyperf
OUTPUT_DIR=benchmark_output/pyperf
NUM_RUNS=11
TIMEOUT=8h
mkdir -p ${RESDIR}
mkdir -p ${OUTPUT_DIR}


echo "running tuplex in pure interpreter resolve mode"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-resolve-interpreter-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --resolve-with-interpreter --mode resolve --path $INPUT_FILE --output-path $OUTPUT_DIR >$LOG 2>$LOG.stderr
done


