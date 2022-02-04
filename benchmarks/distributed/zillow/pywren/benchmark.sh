#!/usr/bin/env bash
# (c) L.Spiegelberg 2017-2022
# runs zillow Z1 benchmark via PyWren

NUM_RUNS="${NUM_RUNS:-11}"
TIMEOUT=1500
PYTHON=python3.7
RESDIR=/results/zillow/Z1
mkdir -p ${RESDIR}

# experiment variables
INPUT_PATH="s3://tuplex-public/data/100GB/"
OUTPUT_PATH="s3://pywren-leonhard/pywren-100GB-zillow-output"

# make comparable to AWS EMR (i.e., use only 4 threads, 10G, 120 concurrency)
LAMBDA_MEMORY=10000
LAMBDA_CONCURRENCY=120

echo "benchmarking Zillow (Z1) over 100G with PyWren"
for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "Run $r/${NUM_RUNS}"
  LOG="${RESDIR}/pywren-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runpywren.py --input-uri $INPUT_PATH --output-uri $OUTPUT_PATH \
                                          --lambda-memory $LAMBDA_MEMORY \
                                          --lambda-concurrency $LAMBDA_CONCURRENCY  >$LOG 2>$LOG.stderr
  cp experiment.log "${RESDIR}/pywren-run-$r.log.txt"
done

echo "Done!"