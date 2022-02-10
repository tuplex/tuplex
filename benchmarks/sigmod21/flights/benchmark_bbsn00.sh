#!/usr/bin/env bash

# use 5 runs and a timeout after 60min
NUM_RUNS=5
TIMEOUT=3600

DATA_PATH='/disk/data/flights'

RESDIR=results_flights

mkdir -p ${RESDIR}

echo "running tuplex"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-$r.txt"
  timeout $TIMEOUT python3 runtuplex.py --path $DATA_PATH  >$LOG 2>$LOG.stderr
done


# spark
echo "benchmarking pyspark"
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-sql-run-$r.txt"
  timeout $TIMEOUT spark-submit --master "local[16]" --driver-memory 256g runpyspark.py --path $DATA_PATH >$LOG 2>$LOG.stderr
done

# Dask
echo "benchmarking dask"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/dask-run-$r.txt"
    timeout $TIMEOUT python3 rundask.py --path /disk/data/flights >$LOG 2>$LOG.stderr
done