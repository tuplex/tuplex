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

# use 5 runs (3 for very long jobs) and a timeout after 180min/3h
DATA_PATH='/data/311/311_preprocessed.csv'
RESDIR='/results/311'
OUTPUT_DIR='/results/output/311'
NUM_RUNS=11
TIMEOUT=14400
PYTHON=python3.6

mkdir -p ${RESDIR}

cp tuplex_config_mt.json tuplex_config.json
echo "running mt tuplex e2e"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-e2e-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/tuplex_e2e >$LOG 2>$LOG.stderr
done

echo "running mt tuplex cached"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-weld-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/tuplex_cached --weld-mode >$LOG 2>$LOG.stderr
done

cp tuplex_config_st.json tuplex_config.json
echo "running st tuplex e2e"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/sttuplex-run-e2e-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/sttuplex_e2e >$LOG 2>$LOG.stderr
done

echo "running st tuplex cached"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/sttuplex-run-weld-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/sttuplex_cached --weld-mode >$LOG 2>$LOG.stderr
done

# Weld
echo "running weld"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/weld-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} python2 rungrizzly.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/weld >$LOG 2>$LOG.stderr
done

# spark
export PYSPARK_PYTHON=${PYTHON}
export PYSPARK_DRIVER_PYTHON=${PYTHON}
echo "benchmarking pyspark e2e"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-run-e2e-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/pyspark_e2e >$LOG 2>$LOG.stderr
done

echo "benchmarking pyspark cached"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-run-weld-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/pyspark_cached --weld-mode >$LOG 2>$LOG.stderr
done

echo "benchmarking pyspark sql e2e"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pysparksql-run-e2e-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/pysparksql_e2e --sql-mode >$LOG 2>$LOG.stderr
done

echo "benchmarking pyspark sql cached"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pysparksql-run-weld-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/pysparksql_cached --sql-mode --weld-mode >$LOG 2>$LOG.stderr
done

# Dask
echo "benchmarking dask e2e"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/dask-run-e2e-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} rundask.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/dask_e2e >$LOG 2>$LOG.stderr
done

echo "benchmarking dask cached"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/dask-run-weld-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} rundask.py --path $DATA_PATH --weld-mode --output-path ${OUTPUT_DIR}/dask_cached >$LOG 2>$LOG.stderr
done

