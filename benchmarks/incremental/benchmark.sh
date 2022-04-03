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

# use 10 runs (3 for very long jobs) and a timeout after 180min/3h
NUM_RUNS=1
TIMEOUT=14400

RESDIR='results_dirty_zillow@100G'
DATA_PATH_SSD='/hot/scratch/bgivertz/data/zillow_dirty@100G.csv'
INCREMENTAL_OUT_PATH_SSD='/hot/scratch/bgivertz/output/incremental'
INCREMENTAL_COMMIT_OUT_PATH_SSD='/hot/scratch/bgivertz/output/commit'
PLAIN_OUT_PATH_SSD='/hot/scratch/bgivertz/output/plain'

rm -rf $RESDIR
rm -rf $INCREMENTAL_OUT_PATH_SSD
rm -rf $PLAIN_OUT_PATH_SSD
rm -rf $INCREMENTAL_COMMIT_OUT_PATH_SSD

# does file exist?
if [[ ! -f "$DATA_PATH_SSD" ]]; then
	echo "file $DATA_PATH_SSD not found, abort."
	exit 1
fi

mkdir -p ${RESDIR}

# create tuplex_config.json
python3 create_conf.py --opt-pushdown --opt-filter --opt-llvm > tuplex_config.json

echo "running out-of-order ssd experiments"
for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "Running trial ($r/$NUM_RUNS)"
  LOG="${RESDIR}/tuplex-plain-out-of-order-ssd-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --clear-cache --path $DATA_PATH_SSD --output-path $PLAIN_OUT_PATH_SSD >$LOG 2>$LOG.stderr

  LOG="${RESDIR}/tuplex-incremental-out-of-order-ssd-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --clear-cache --incremental-resolution --path $DATA_PATH_SSD --output-path $INCREMENTAL_OUT_PATH_SSD >$LOG 2>$LOG.stderr

  LOG="${RESDIR}/tuplex-compare-out-of-order-ssd-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 compare_folders.py $PLAIN_OUT_PATH_SSD $INCREMENTAL_OUT_PATH_SSD >$LOG 2>$LOG.stderr
done

echo "running in-order ssd experiments"
for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "Running trial ($r/$NUM_RUNS)"
  LOG="${RESDIR}/tuplex-plain-in-order-ssd-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --clear-cache --resolve-in-order --path $DATA_PATH_SSD --output-path $PLAIN_OUT_PATH_SSD >$LOG 2>$LOG.stderr

  LOG="${RESDIR}/tuplex-incremental-in-order-ssd-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --clear-cache --resolve-in-order --incremental-resolution --path $DATA_PATH_SSD --output-path $INCREMENTAL_OUT_PATH_SSD >$LOG 2>$LOG.stderr

  LOG="${RESDIR}/tuplex-incremental-in-order-commit-ssd-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --clear-cache --resolve-in-order --incremental-resolution --commit --path $DATA_PATH_SSD --output-path $INCREMENTAL_COMMIT_OUT_PATH_SSD >$LOG 2>$LOG.stderr

  LOG="${RESDIR}/tuplex-compare-in-order-ssd-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 compare_folders.py --in-order $PLAIN_OUT_PATH_SSD $INCREMENTAL_OUT_PATH_SSD >$LOG 2>$LOG.stderr

  LOG="${RESDIR}/tuplex-compare-in-order-commit-ssd-$r.txt"
    timeout $TIMEOUT ${HWLOC} python3 compare_folders.py --in-order $INCREMENTAL_COMMIT_OUT_PATH_SSD $INCREMENTAL_OUT_PATH_SSD >$LOG 2>$LOG.stderr
done

python3 graph.py --results-path $RESDIR --num-trials $NUM_RUNS --num-steps 7

rm -rf $INCREMENTAL_OUT_PATH_SSD
rm -rf $PLAIN_OUT_PATH_SSD
rm -rf $INCREMENTAL_COMMIT_OUT_PATH_SSD
