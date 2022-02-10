#!/usr/bin/env bash
# (c) 2021 L.Spiegelberg
# runs Tuplex benchmark on AWS setup

# use 11 runs and a timeout after 60min
NUM_RUNS="${NUM_RUNS:-11}"
TIMEOUT=3600

LG_INPUT_PATH='/data/flights'
SM_INPUT_PATH='/data/flights_small'
LG_RESDIR=/results/flights/flights
SM_RESDIR=/results/flights/flights_small
OUTPUT_DIR=/results/output/flights_output
PYTHON=python3.6

mkdir -p ${LG_RESDIR}
mkdir -p ${SM_RESDIR}

echo "running on large flight data"
echo "running tuplex"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${LG_RESDIR}/tuplex-run-$r.txt"
  rm -rf $OUTPUT_DIR/tuplex
  timeout $TIMEOUT ${PYTHON} runtuplex.py --path $LG_INPUT_PATH --output-path $OUTPUT_DIR/tuplex >$LOG 2>$LOG.stderr
done

echo "running on small flight data"
echo "running tuplex"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${SM_RESDIR}/tuplex-run-$r.txt"
  rm -rf $OUTPUT_DIR/tuplex
  timeout $TIMEOUT ${PYTHON} runtuplex.py --path $SM_INPUT_PATH --output-path $OUTPUT_DIR/tuplex  >$LOG 2>$LOG.stderr
done


# spark
echo "benchmarking pyspark"
export PYSPARK_PYTHON=${PYTHON}
export PYSPARK_DRIVER_PYTHON=${PYTHON}
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${LG_RESDIR}/pyspark-sql-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --path $LG_INPUT_PATH --output-path $OUTPUT_DIR/spark >$LOG 2>$LOG.stderr
done

# Dask
echo "benchmarking dask"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${LG_RESDIR}/dask-run-$r.txt"
    timeout $TIMEOUT ${PYTHON} rundask.py --path $LG_INPUT_PATH --output-path $OUTPUT_DIR/dask >$LOG 2>$LOG.stderr
done


# spark
echo "benchmarking pyspark"
export PYSPARK_PYTHON=${PYTHON}
export PYSPARK_DRIVER_PYTHON=${PYTHON}
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${SM_RESDIR}/pyspark-sql-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --path $SM_INPUT_PATH --output-path $OUTPUT_DIR/spark >$LOG 2>$LOG.stderr
done

# Dask
echo "benchmarking dask"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${SM_RESDIR}/dask-run-$r.txt"
    timeout $TIMEOUT ${PYTHON} rundask.py --path $SM_INPUT_PATH --output-path $OUTPUT_DIR/dask >$LOG 2>$LOG.stderr
done
