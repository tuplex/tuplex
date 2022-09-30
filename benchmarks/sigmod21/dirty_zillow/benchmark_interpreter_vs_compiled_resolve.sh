#!/usr/bin/env bash
# (c) 2021 L.Spiegelberg
# this file compares how fast in a single-threaded setting resolving via
# 1. interpreter only vs. 2. compiled code path is

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

# use 10 runs (3 for very long jobs) and a timeout after 180min/3h
NUM_RUNS=11
TIMEOUT=14400

# for 50G use ./replicate-data.py -s 7500 -o data/zillow_dirty@50G.csv
DATA_PATH='data/zillow_dirty@10G.csv'

RESDIR=results_dirty_zillow_interpreter_vs_compiled

mkdir -p ${RESDIR}

# warmup cache by touching file
# vmtouch -dl <dir> => needs sudo rights I assume...


# create tuplex_config.json
#python3 create_conf.py --opt-null --opt-pushdown --opt-filter --opt-llvm > tuplex_config.json
#cp tuplex_config.json ${RESDIR}

echo "running tuplex in compiled resolve mode"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-resolve-compiled-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --single-threaded --mode resolve --path $DATA_PATH >$LOG 2>$LOG.stderr
done

echo "running tuplex in pure interpreter resolve mode"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-resolve-interpreter-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --single-threaded --resolve-with-interpreter --mode resolve --path $DATA_PATH >$LOG 2>$LOG.stderr
done

