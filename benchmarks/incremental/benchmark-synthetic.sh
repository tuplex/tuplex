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
NUM_STEPS=5
TIMEOUT=14400

RESDIR='results_synthetic'
DATA_PATH='/hot/scratch/bgivertz/data/synth'
INCREMENTAL_OUT_PATH='/hot/scratch/bgivertz/output/incremental'
INCREMENTAL_COMMIT_OUT_PATH='/hot/scratch/bgivertz/output/commit'
PLAIN_OUT_PATH='/hot/scratch/bgivertz/output/plain'

rm -rf $RESDIR
rm -rf $INCREMENTAL_OUT_PATH
rm -rf $PLAIN_OUT_PATH
rm -rf $INCREMENTAL_COMMIT_OUT_PATH

mkdir -p ${RESDIR}

# create tuplex_config.json
python3 create_conf.py --opt-pushdown --opt-filter --opt-llvm --executor-count 63 --executor-memory "6G" > tuplex_config.json

echo "running out-of-order ssd experiments"
for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "trial ($r/$NUM_RUNS)"

  for ((s = 0; s <= 10; s++)) do
    echo "running plain"
    LOG="${RESDIR}/plain-out-of-order-e$s-t$r.txt"
    timeout $TIMEOUT ${HWLOC} python3 runsynthetic.py --num-steps $NUM_STEPS --clear-cache --input-path "$DATA_PATH$s.csv" --output-path $PLAIN_OUT_PATH >$LOG 2>$LOG.stderr

    echo "running incremental"
    LOG="${RESDIR}/incremental-out-of-order-e$s-t$r.txt"
    timeout $TIMEOUT ${HWLOC} python3 runsynthetic.py --num-steps $NUM_STEPS --clear-cache --incremental-resolution --input-path "$DATA_PATH$s.csv" --output-path $INCREMENTAL_OUT_PATH >$LOG 2>$LOG.stderr
  done
done

#echo "running in-order ssd experiments"
#for ((r = 1; r <= NUM_RUNS; r++)); do
#  echo "trial ($r/$NUM_RUNS)"
#
#  echo "running plain"
#  LOG="${RESDIR}/tuplex-plain-in-order-ssd-$r.txt"
#  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --clear-cache --resolve-in-order --path $DATA_PATH_SSD --output-path $PLAIN_OUT_PATH_SSD >$LOG 2>$LOG.stderr
#
#  echo "running incremental"
#  LOG="${RESDIR}/tuplex-incremental-in-order-ssd-$r.txt"
#  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --clear-cache --resolve-in-order --incremental-resolution --path $DATA_PATH_SSD --output-path $INCREMENTAL_OUT_PATH_SSD >$LOG 2>$LOG.stderr
#
#  echo "running commit"
#  LOG="${RESDIR}/tuplex-incremental-in-order-commit-ssd-$r.txt"
#  timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --clear-cache --resolve-in-order --incremental-resolution --commit --path $DATA_PATH_SSD --output-path $INCREMENTAL_COMMIT_OUT_PATH_SSD >$LOG 2>$LOG.stderr
#
#done

#echo "graphing results"
#python3 graph.py --results-path $RESDIR --num-trials $NUM_RUNS --num-steps 7

rm -rf $INCREMENTAL_OUT_PATH
rm -rf $PLAIN_OUT_PATH
rm -rf $INCREMENTAL_COMMIT_OUT_PATH
