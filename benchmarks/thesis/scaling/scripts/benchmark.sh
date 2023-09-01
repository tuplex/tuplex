#!/usr/bin/env bash

NUM_RUNS="${NUM_RUNS:-5}"
RESULT_DIR="${RESULT_DIR:-/results/pyscaling}"

# add pyenv versions
export PATH=$HOME/.pyenv/versions/3.10.11/bin/:$HOME/.pyenv/versions/3.11.3/bin/:$HOME/.pyenv/versions/3.12.0a7/bin/:$PATH

# smaller file so each run takes ~1-2s
# ZILLOW_IN_PATH=/data/zillow/large100MB.csv
# takes 10-20s
ZILLOW_IN_PATH=/data/zillow/large1GB.csv
ZILLOW_OUT_PATH=/scratch/zillow
MAX_PARALLELISM=${MAX_PARALLELISM:-32}
mkdir -p ${ZILLOW_OUT_PATH}
mkdir -p ${RESULT_DIR}

PYTHON=python3.9

echo "benchmarking using $PYTHON"

# step 1: benchmark zillow
echo "Benchmark  I/II: Zillow over ${ZILLOW_IN_PATH}"

for ((p = 1; p <= MAX_PARALLELISM; p++)); do
  OUT_DIR=${RESULT_DIR}/zillow/${p}
  mkdir -p "${OUT_DIR}"
  echo "-- running with parallelism=${p}"

  for ((r = 1; r <= NUM_RUNS; r++)); do
    LOG=$OUT_DIR/"$PYTHON-process-run-$r.txt"
    $PYTHON /code/scripts/z2_query.py --path ${ZILLOW_IN_PATH} --mode process -p ${p} --output-path ${ZILLOW_OUT_PATH} >$LOG 2>$LOG.stderr
  done

  for ((r = 1; r <= NUM_RUNS; r++)); do
    LOG=$OUT_DIR/"$PYTHON-thread-run-$r.txt"
    $PYTHON /code/scripts/z2_query.py --path ${ZILLOW_IN_PATH} --mode thread -p ${p} --output-path ${ZILLOW_OUT_PATH} >$LOG 2>$LOG.stderr
  done
done


echo "Benchmark II/II: Countprimes"
for ((p = 1; p <= MAX_PARALLELISM; p++)); do

  OUT_DIR=${RESULT_DIR}/countprimes/${p}
  mkdir -p "${OUT_DIR}"
  echo "-- running with parallelism=${p}"

  for ((r = 1; r <= NUM_RUNS; r++)); do
    LOG=$OUT_DIR/"$PYTHON-process-run-$r.txt"
    $PYTHON /code/scripts/countprimes.py --mode process -p ${p} >$LOG 2>$LOG.stderr
  done

    for ((r = 1; r <= NUM_RUNS; r++)); do
    LOG=$OUT_DIR/"$PYTHON-thread-run-$r.txt"
    $PYTHON /code/scripts/countprimes.py --mode thread -p ${p} >$LOG 2>$LOG.stderr
  done

done

echo "Done!"
