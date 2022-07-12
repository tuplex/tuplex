#!/usr/bin/env bash
# launch via nohup perflock bash benchmark.sh -hwloc &

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
RESDIR=/results/tpch_q19
NUM_RUNS=11
TIMEOUT=14400
PYTHON=python3.6

mkdir -p ${RESDIR}

# INTEGER RUNS -------------------------------------------------------------------------------------
echo "INTEGER DATA"
LINEITEM_DATA_PATH=/data/tpch/q19/integer/lineitem.tbl
PART_DATA_PATH=/data/tpch/q19/integer/part.tbl

# with pushdown
${PYTHON} create_conf.py --executor-count 0 --driver-memory 100G --opt-filter --opt-llvm --opt-parser | tee tuplex_config.json
cp tuplex_config.json "${RESDIR}/tuplex_config_weld_pushdown.json"

echo "Tuplex SF10 E2E Weld-mode + Pushdown"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-weldmode-pushdown-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --preprocessed --weld >$LOG 2>$LOG.stderr
done

echo "Tuplex SF10 Cached Weld-mode + Pushdown"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-cached-weldmode-pushdown-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --cache --preprocessed --weld >$LOG 2>$LOG.stderr
done

# without pushdown
${PYTHON} create_conf.py --executor-count 0 --driver-memory 100G --opt-llvm --opt-parser | tee tuplex_config.json
cp tuplex_config.json "${RESDIR}/tuplex_config_weld_no_pushdown.json"

echo "Tuplex SF10 E2E Weld-mode"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-weldmode-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --preprocessed --weld >$LOG 2>$LOG.stderr
done

echo "Tuplex SF10 Cached Weld-mode"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-cached-weldmode-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --cache --preprocessed --weld >$LOG 2>$LOG.stderr
done

# weld runs
echo "Weld SF10 E2E + Cached"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/weld-run-$r.txt"
  ${HWLOC} ./weld/build/weldq19 --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --preprocessed >$LOG 2>$LOG.stderr
done

# hyper runs
echo "Hyper SF10 Preprocessed"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/hyper-preprocessed-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runhyper.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --preprocessed >$LOG 2>$LOG.stderr
done

#echo "Hyper SF-10 preprocessed single-threaded, integers only"
#for ((r = 1; r <= NUM_RUNS; r++)); do
  #LOG="${RESDIR}/hyper-preprocessed-single-threaded-run-$r.txt"
  #timeout $TIMEOUT ${HWLOC} python3.7 runhyper.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --preprocessed --single-threaded >$LOG 2>$LOG.stderr
#done

# STRING RUNS --------------------------------------------------------------------------------------
echo "STRING DATA"
# with 16 threads
${PYTHON} create_conf.py --opt-filter --opt-llvm --opt-parser --executor-memory 6G --driver-memory 10G | tee tuplex_config.json
cp tuplex_config.json "${RESDIR}/tuplex_config_mt.json"

LINEITEM_DATA_PATH=/data/tpch/q19/string/lineitem.tbl
PART_DATA_PATH=/data/tpch/q19/string/part.tbl
echo "Tuplex SF10 E2E Preprocessed"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --preprocessed >$LOG 2>$LOG.stderr
done

echo "Tuplex SF10 Cached Preprocessed"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-cached-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --cache --preprocessed >$LOG 2>$LOG.stderr
done

export PYSPARK_PYTHON=${PYTHON}
export PYSPARK_DRIVER_PYTHON=${PYTHON}
echo "Pyspark RDD SF10"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --mode nosql --preprocessed >$LOG 2>$LOG.stderr
done

echo "Pyspark RDD SF10 Cached"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-cached-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --mode nosql --cache --preprocessed >$LOG 2>$LOG.stderr
done

echo "Pyspark SQL SF10"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-sql-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --mode sql --preprocessed >$LOG 2>$LOG.stderr
done

echo "Pyspark SQL SF10 Cached"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-sql-cached-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --lineitem_path $LINEITEM_DATA_PATH --part_path $PART_DATA_PATH --mode sql --cache --preprocessed >$LOG 2>$LOG.stderr
done


