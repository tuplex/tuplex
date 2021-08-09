#!/usr/bin/env bash

# use 5 runs (3 for very long jobs) and a timeout after 180min/3h
NUM_RUNS=1
TIMEOUT=60
DATA_PATH='../../tuplex/test/resources/pipelines/flights/sortsample.csv'
RESDIR=./results/logs
OUTPUT_DIR=./results/output
PYTHON=python3.9

mkdir -p ${RESDIR}

# create tuplex_config.json
python3 create_conf.py --opt-null --opt-pushdown --opt-filter --opt-llvm > tuplex_config.json
# nvo making provlems...
python3 create_conf.py --opt-pushdown --opt-filter --opt-llvm > tuplex_config.json
cp tuplex_config.json ${RESDIR}


echo "running tuplex"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-e2e-$r.txt"
  gtimeout $TIMEOUT ${PYTHON} runtuplex.py --path $DATA_PATH --output-path "${OUTPUT_DIR}/tuplex_output" >$LOG 2>$LOG.stderr
done

# spark
export PYSPARK_PYTHON=${PYTHON}
export PYSPARK_DRIVER_PYTHON=${PYTHON}
echo "benchmarking pyspark"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-run-e2e-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[4]" --driver-memory 8g runpyspark.py --path $DATA_PATH --output-path "${OUTPUT_DIR}/spark_output" >$LOG 2>$LOG.stderr
done
#for ((r = 1; r <= NUM_RUNS; r++)); do
#  LOG="${RESDIR}/pyspark-run-weld-$r.txt"
#  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --path $DATA_PATH --weld-mode --output-path "${OUTPUT_DIR}/spark_output" >$LOG 2>$LOG.stderr
#done
#for ((r = 1; r <= NUM_RUNS; r++)); do
#  LOG="${RESDIR}/pysparksql-run-e2e-$r.txt"
#  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --path $DATA_PATH --sql-mode --output-path "${OUTPUT_DIR}/spark_output" >$LOG 2>$LOG.stderr
#done
#for ((r = 1; r <= NUM_RUNS; r++)); do
#  LOG="${RESDIR}/pysparksql-run-weld-$r.txt"
#  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --path $DATA_PATH --sql-mode --weld-mode --output-path "${OUTPUT_DIR}/spark_output" >$LOG 2>$LOG.stderr
#done


# Dask
#echo "benchmarking dask"
#for ((r = 1; r <= NUM_RUNS; r++)); do
#  LOG="${RESDIR}/dask-run-e2e-$r.txt"
#  timeout $TIMEOUT ${PYTHON} rundask.py --path $DATA_PATH --output-path "${OUTPUT_DIR}/dask_output" >$LOG 2>$LOG.stderr
#done
#for ((r = 1; r <= NUM_RUNS; r++)); do
#  LOG="${RESDIR}/dask-run-weld-$r.txt"
#  timeout $TIMEOUT ${PYTHON} rundask.py --path $DATA_PATH --weld-mode --output-path "${OUTPUT_DIR}/dask_output" >$LOG 2>$LOG.stderr
#done
