#!/usr/bin/env bash
# this script benchmarks only tuplex, but uses the tpch data containing only the 4 necessary columns
# launch via nohup perflock bash benchmark-tuplex.sh -hwloc &

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

BASE_RESDIR=results_tpcq19

mkdir -p ${BASE_RESDIR}
mkdir -p ${BASE_RESDIR}/sf1
mkdir -p ${BASE_RESDIR}/sf10

python3 create_conf.py --opt-filter --opt-llvm --opt-parser | tee tuplex_config.json

LINEITEM_DATA_PATH=/hot/data/tpch/sf-1/lineitem.tbl
PART_DATA_PATH=/hot/data/tpch/sf-1/part.tbl
RESDIR=${BASE_RESDIR}/sf1
echo "Tuplex SF1 E2E"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH >$LOG 2>$LOG.stderr
done

echo "Tuplex SF1 Cached"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-cached-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --cache >$LOG 2>$LOG.stderr
done

LINEITEM_DATA_PATH=/hot/data/tpch/sf-10/lineitem.tbl
PART_DATA_PATH=/hot/data/tpch/sf-10/part.tbl
RESDIR=${BASE_RESDIR}/sf10
echo "Tuplex SF10 E2E"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH >$LOG 2>$LOG.stderr
done

echo "Tuplex SF10 Cached"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-cached-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --cache >$LOG 2>$LOG.stderr
done
