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
DATA_PATH='/hot/scratch/bgivertz/data/synthetic/synth'
INCREMENTAL_OUT_PATH='/hot/scratch/bgivertz/output/incremental'
COMMIT_OUT_PATH='/hot/scratch/bgivertz/output/commit'
PLAIN_OUT_PATH='/hot/scratch/bgivertz/output/plain'

rm -rf $RESDIR
rm -rf $INCREMENTAL_OUT_PATH
rm -rf $PLAIN_OUT_PATH
rm -rf $COMMIT_OUT_PATH

mkdir -p ${RESDIR}

# create tuplex_config.json
python3 create_conf.py --opt-pushdown --opt-filter --opt-llvm --executor-count 63 --executor-memory "6G" > tuplex_config.json

echo "running out of order experiments"
for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "trial ($r/$NUM_RUNS)"

  for ((s = 0; s <= 10; s++)) do
    echo "running plain ($s/10)"
    LOG="${RESDIR}/plain-out-of-order-e$s-t$r.txt"
    timeout $TIMEOUT ${HWLOC} python3 runsynthetic.py --num-steps $NUM_STEPS --clear-cache --input-path "$DATA_PATH$s.csv" --output-path $PLAIN_OUT_PATH >$LOG 2>$LOG.stderr

    echo "running incremental ($s/10)"
    LOG="${RESDIR}/incremental-out-of-order-e$s-t$r.txt"
    timeout $TIMEOUT ${HWLOC} python3 runsynthetic.py --num-steps $NUM_STEPS --clear-cache --incremental-resolution --input-path "$DATA_PATH$s.csv" --output-path $INCREMENTAL_OUT_PATH >$LOG 2>$LOG.stderr
  done
done

echo "running in order experiments"
for ((r = 1; r < NUM_RUNS; r++)); do
  echo "trial ($r/$NUM_RUNS)"

  for ((s = 0; s <= 10; s++)) do
    echo "running plain ($s/10)"
    LOG="${RESDIR}/plain-in-order-e$s-t$r.txt"
    timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --clear-cache --resolve-in-order --input-path "$DATA_PATH$s.csv" --output-path $PLAIN_OUT_PATH >$LOG 2>$LOG.stderr

    echo "running incremental ($s/10)"
    LOG="${RESDIR}/incremental-in-order-e$s-t$r.txt"
    timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --clear-cache --resolve-in-order --incremental-resolution --input-path "$DATA_PATH$s.csv" --output-path $INCREMENTAL_OUT_PATH >$LOG 2>$LOG.stderr


    echo "running commit ($s/10)"
    LOG="${RESDIR}/commit-in-order-e$s-t$r.txt"
    timeout $TIMEOUT ${HWLOC} python3 runtuplex.py --clear-cache --resolve-in-order --incremental-resolution --commit --input-path "$DATA_PATH$s.csv" --output-path $COMMIT_OUT_PATH >$LOG 2>$LOG.stderr
  done
done


rm -rf $INCREMENTAL_OUT_PATH
rm -rf $PLAIN_OUT_PATH
rm -rf $COMMIT_OUT_PATH
