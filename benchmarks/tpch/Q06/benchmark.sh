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
RESDIR=/results/tpch_q06
NUM_RUNS=11
TIMEOUT=14400
PYTHON=python3.6
DATA_PATH=/data/tpch/q6/lineitem.tbl

mkdir -p ${RESDIR}

# Single threaded
echo "Single-threaded"
${PYTHON} create_conf.py --executor-count 0 --driver-memory 100G --opt-filter --opt-llvm --opt-parser | tee tuplex_config.json
cp tuplex_config.json "${RESDIR}/tuplex_config_st.json"

echo "Tuplex (E2E)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-st-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --path $DATA_PATH --preprocessed >$LOG 2>$LOG.stderr
done

echo "Tuplex (Cached)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-st-cached-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --path $DATA_PATH --preprocessed --cache >$LOG 2>$LOG.stderr
done

echo "Weld"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/weld-run-$r.txt"
  ${HWLOC} ./weld/build/weldq6 --path $DATA_PATH --preprocessed 2>$LOG.stderr | tee $LOG
done

# Multithreaded
echo "Multi-threaded"
echo "Hyper"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/hyper-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runhyper.py --path $DATA_PATH --preprocessed >$LOG 2>$LOG.stderr
done

${PYTHON} create_conf.py --opt-filter --opt-llvm --opt-parser --executor-memory 6G --driver-memory 10G | tee tuplex_config.json
cp tuplex_config.json "${RESDIR}/tuplex_config_mt.json"

echo "Tuplex (E2E)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --path $DATA_PATH --preprocessed >$LOG 2>$LOG.stderr
done

echo "Tuplex (Cached)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-cached-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --path $DATA_PATH --preprocessed --cache >$LOG 2>$LOG.stderr
done

export PYSPARK_PYTHON=${PYTHON}
export PYSPARK_DRIVER_PYTHON=${PYTHON}
echo "Pyspark RDD (no cache)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --path $DATA_PATH --mode rdd --preprocessed >$LOG 2>$LOG.stderr
done
echo "Pyspark SQL (no cache)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-sql-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --path $DATA_PATH --mode sql --preprocessed >$LOG 2>$LOG.stderr
done

echo "Pyspark RDD (cache)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-cached-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --path $DATA_PATH --mode rdd --cache --preprocessed >$LOG 2>$LOG.stderr
done
echo "Pyspark SQL (cache)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-sql-cached-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --path $DATA_PATH --mode sql --cache --preprocessed >$LOG 2>$LOG.stderr
done
