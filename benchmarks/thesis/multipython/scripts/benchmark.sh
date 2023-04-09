#!/usr/bin/env bash

NUM_RUNS="${NUM_RUNS:-5}"
RESULT_DIR="${RESULT_DIR:-/results/pyversions}"

# add pyenv versions
export PATH=$HOME/.pyenv/versions/3.10.11/bin/:$HOME/.pyenv/versions/3.11.3/bin/:$HOME/.pyenv/versions/3.12.0a7/bin/:$PATH

# smaller file so each run takes ~1-2s
# ZILLOW_IN_PATH=/data/zillow/large100MB.csv
# takes 10-20s
ZILLOW_IN_PATH=/data/zillow/large1GB.csv
ZILLOW_OUT_PATH=/scratch/zillow
mkdir -p ${ZILLOW_OUT_PATH}
mkdir -p ${RESULT_DIR}

PYTHON_VERSIONS=(python3.2 python3.3 python3.4 python3.5 python3.6 python3.7 python3.8 python3.9 python3.10 python3.11 python3.12)

for PYTHON in "${PYTHON_VERSIONS[@]}"; do
 echo "benchmarking $PYTHON"

  # step 1: benchmark zillow
  echo "Benchmark  I/II: Zillow over ${ZILLOW_IN_PATH}"
  OUT_DIR=${RESULT_DIR}/zillow
  mkdir -p "${OUT_DIR}"

  for ((r = 1; r <= NUM_RUNS; r++)); do
    LOG=$OUT_DIR/"$PYTHON-run-$r.txt"
    $PYTHON /code/scripts/z2_query.py --path ${ZILLOW_IN_PATH} --output-path ${ZILLOW_OUT_PATH} >$LOG 2>$LOG.stderr
  done

  echo "Benchmark II/II: Countprimes"
  OUT_DIR=${RESULT_DIR}/countprimes
    mkdir -p "${OUT_DIR}"

    for ((r = 1; r <= NUM_RUNS; r++)); do
      LOG=$OUT_DIR/"$PYTHON-run-$r.txt"
      $PYTHON /code/scripts/countprimes.py >$LOG 2>$LOG.stderr
    done

done