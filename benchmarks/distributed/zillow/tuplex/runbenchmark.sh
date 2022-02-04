#!/usr/bin/env bash
# (c) L.Spiegelberg 2017-2022
# runs zillow Z1 benchmark via Tuplex

NUM_RUNS="${NUM_RUNS:-11}"
TIMEOUT=1500
PYTHON=python3.8
RESDIR=/results/zillow/Z1
mkdir -p ${RESDIR}

# experiment variables
INPUT_PATH="s3://tuplex-public/data/100GB/*.csv"
OUTPUT_PATH="s3://tuplex-leonhard/experiments/Zillow/Z1/output_compiled"
SCRACTH_DIR="s3://tuplex-leonhard/scratch"
LAMBDA_MEMORY=10000
LAMBDA_CONCURRENCY=120


echo "benchmarking Zillow (Z1) over 100G with Tuplex (compiled)"
OUTPUT_PATH="s3://tuplex-leonhard/experiments/Zillow/Z1/output_compiled"
for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "Run $r/${NUM_RUNS}"
  LOG="${RESDIR}/tuplex-compiled-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runtuplex.py -m "compiled" --path $INPUT_PATH --output-path $OUTPUT_PATH \
                                          --scratch-dir $SCRATCH_DIR --lambda-memory $LAMBDA_MEMORY \
                                          --lambda-concurrency $LAMBDA_CONCURRENCY >$LOG 2>$LOG.stderr
  cp experiment.log "${RESDIR}/tuplex-compiled-run-$r.log.txt"
done

echo "benchmarking Zillow (Z1) over 100G with Tuplex (interpreted)"
OUTPUT_PATH="s3://tuplex-leonhard/experiments/Zillow/Z1/output_interpreted"
for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "Run $r/${NUM_RUNS}"
  LOG="${RESDIR}/tuplex-compiled-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runtuplex.py -m "compiled" --path $INPUT_PATH --output-path $OUTPUT_PATH \
                                          --scratch-dir $SCRATCH_DIR --lambda-memory $LAMBDA_MEMORY \
                                          --lambda-concurrency $LAMBDA_CONCURRENCY >$LOG 2>$LOG.stderr
  cp experiment.log "${RESDIR}/tuplex-compiled-run-$r.log.txt"
done

echo "Done!"