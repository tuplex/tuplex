#!/usr/bin/env bash
# this file benchmarks hyper only

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

# use 10 runs (first one ignore, it's warmup)
NUM_RUNS=11
TIMEOUT=14400

LINEITEM_DATA_PATH=/hot/data/tpch_q19/integers/sf-10/lineitem.tbl
PART_DATA_PATH=/hot/data/tpch_q19/integers/sf-10/part.tbl

BASE_RESDIR=benchmark_results/hyper
mkdir -p ${BASE_RESDIR}/sf10

RESDIR=${BASE_RESDIR}/sf10
echo "Hyper SF-10 preprocessed, integers only"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/hyper-preprocessed-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runhyper.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --preprocessed >$LOG 2>$LOG.stderr
done


echo "Hyper SF-10 preprocessed single-threaded, integers only"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/hyper-preprocessed-single-threaded-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runhyper.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --preprocessed --single-threaded >$LOG 2>$LOG.stderr
done


