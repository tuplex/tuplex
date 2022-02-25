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
NUM_RUNS=11
TIMEOUT=14400

DATA_PATH='data/zillow_dirty@10G.csv'
RESDIR=results_dirty_zillow@10G

# does file exist?
if [[ ! -f "$DATA_PATH" ]]; then
	echo "file $DATA_PATH not found, abort."
	exit 1
fi

mkdir -p ${RESDIR}

# warmup cache by touching file
# vmtouch -dl <dir> => needs sudo rights I assume...

# create tuplex_config.json
#python3 create_conf.py --opt-null --opt-pushdown --opt-filter --opt-llvm > tuplex_config.json
python3 create_conf.py --opt-pushdown --opt-filter --opt-llvm > tuplex_config.json
#cp tuplex_config.json ${RESDIR}

# Multi-threaded experiments
# Without order
echo "running no-merge experiments using 16x parallelism"
echo "running tuplex without incremental resolution"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-plain-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --path $DATA_PATH >$LOG 2>$LOG.stderr
done

echo "running tuplex with incremental resolution"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-incremental-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --incremental-resolution --path $DATA_PATH >$LOG 2>$LOG.stderr
done

# With order
echo "running in-order experiments using 16x parallelism"
echo "running tuplex without incremental resolution"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-plain-in-order-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --resolve-in-order --path $DATA_PATH >$LOG 2>$LOG.stderr
done

echo "running tuplex with incremental resolution"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-incremental-in-order-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --resolve-in-order --incremental-resolution --path $DATA_PATH >$LOG 2>$LOG.stderr
done
