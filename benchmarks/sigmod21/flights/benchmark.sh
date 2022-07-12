#!/usr/bin/env bash

# use 5 runs and a timeout after 15min
NUM_RUNS=5
TIMEOUT=900

for i in $(seq 11); do
  v=$(printf %02d $i)
  for ((r = 1; r <= NUM_RUNS; r++)); do
    LOG="pyspark-01-$v-run-$r.txt"
    timeout $TIMEOUT spark-submit --master "local[*]" --driver-memory 100g runpyspark.py --path 'data-'$v'/*.csv' >$LOG 2>$LOG.stderr
    LOG="dask-01-$v-run-$r.txt"
    timeout $TIMEOUT python3 rundask.py --path 'data-'$v'/*.csv' >$LOG 2>$LOG.stderr
  done

done
