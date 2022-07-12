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

BASE_RESDIR=results_tpcq19_weld

mkdir -p ${BASE_RESDIR}
mkdir -p ${BASE_RESDIR}/sf1
mkdir -p ${BASE_RESDIR}/sf10

# with pushdown
python3 create_conf.py --executor-count 0 --executor-memory 256G --opt-filter --opt-llvm --opt-parser | tee tuplex_config.json

LINEITEM_DATA_PATH=/hot/data/tpch_q19/integers/sf-1/lineitem.tbl
PART_DATA_PATH=/hot/data/tpch_q19/integers/sf-1/part.tbl
RESDIR=${BASE_RESDIR}/sf1
echo "Tuplex SF1 E2E"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-weldmode-pushdown-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --preprocessed --weld >$LOG 2>$LOG.stderr
done

echo "Tuplex SF1 Cached"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-cached-weldmode-pushdown-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --cache --preprocessed --weld >$LOG 2>$LOG.stderr
done

#echo "Tuplex SF1 Optimal Cached"
#for ((r = 1; r <= NUM_RUNS; r++)); do
  #LOG="${RESDIR}/tuplex-cached-optimal-weldmode-run-$r.txt"
  #timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --cache --preprocessed --weld --mode optimal >$LOG 2>$LOG.stderr
#done

LINEITEM_DATA_PATH=/hot/data/tpch_q19/integers/sf-10/lineitem.tbl
PART_DATA_PATH=/hot/data/tpch_q19/integers/sf-10/part.tbl
RESDIR=${BASE_RESDIR}/sf10
echo "Tuplex SF10 E2E"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-weldmode-pushdown-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --preprocessed --weld >$LOG 2>$LOG.stderr
done

echo "Tuplex SF10 Cached"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-cached-weldmode-pushdown-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --cache --preprocessed --weld >$LOG 2>$LOG.stderr
done

#echo "Tuplex SF10 Optimal Cached"
#for ((r = 1; r <= NUM_RUNS; r++)); do
  #LOG="${RESDIR}/tuplex-cached-optimal-weldmode-run-$r.txt"
  #timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --cache --preprocessed --weld --mode optimal >$LOG 2>$LOG.stderr
#done

# without pushdown
python3 create_conf.py --executor-count 0 --executor-memory 256G --opt-llvm --opt-parser | tee tuplex_config.json

LINEITEM_DATA_PATH=/hot/data/tpch_q19/integers/sf-1/lineitem.tbl
PART_DATA_PATH=/hot/data/tpch_q19/integers/sf-1/part.tbl
RESDIR=${BASE_RESDIR}/sf1
echo "Tuplex SF1 E2E"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-weldmode-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --preprocessed --weld >$LOG 2>$LOG.stderr
done

echo "Tuplex SF1 Cached"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-cached-weldmode-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --cache --preprocessed --weld >$LOG 2>$LOG.stderr
done

#echo "Tuplex SF1 Optimal Cached"
#for ((r = 1; r <= NUM_RUNS; r++)); do
  #LOG="${RESDIR}/tuplex-cached-optimal-weldmode-run-$r.txt"
  #timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --cache --preprocessed --weld --mode optimal >$LOG 2>$LOG.stderr
#done

LINEITEM_DATA_PATH=/hot/data/tpch_q19/integers/sf-10/lineitem.tbl
PART_DATA_PATH=/hot/data/tpch_q19/integers/sf-10/part.tbl
RESDIR=${BASE_RESDIR}/sf10
echo "Tuplex SF10 E2E"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-weldmode-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --preprocessed --weld >$LOG 2>$LOG.stderr
done

echo "Tuplex SF10 Cached"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-cached-weldmode-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --cache --preprocessed --weld >$LOG 2>$LOG.stderr
done

#echo "Tuplex SF10 Optimal Cached"
#for ((r = 1; r <= NUM_RUNS; r++)); do
  #LOG="${RESDIR}/tuplex-cached-optimal-weldmode-run-$r.txt"
  #timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --cache --preprocessed --weld --mode optimal >$LOG 2>$LOG.stderr
#done

