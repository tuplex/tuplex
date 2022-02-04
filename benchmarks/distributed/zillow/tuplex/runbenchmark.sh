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
SCRATCH_DIR="s3://tuplex-leonhard/scratch"

# make comparable to AWS EMR (i.e., use only 4 threads)
LAMBDA_MEMORY=10000
LAMBDA_CONCURRENCY=120
LAMBDA_THREADS=4

echo "benchmarking Zillow (Z1) over 100G with Tuplex (compiled, 1 thread)"
OUTPUT_PATH="s3://tuplex-leonhard/experiments/Zillow/Z1/output_compiled_1x"
LAMBDA_THREADS=1
for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "Run $r/${NUM_RUNS}"
  LOG="${RESDIR}/tuplex-compiled-1x-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runtuplex.py -m "compiled" --path $INPUT_PATH --output-path $OUTPUT_PATH \
                                          --scratch-dir $SCRATCH_DIR --lambda-memory $LAMBDA_MEMORY \
                                          --lambda-concurrency $LAMBDA_CONCURRENCY \
                                          --lambda-threads $LAMBDA_THREADS >$LOG 2>$LOG.stderr
  cp experiment.log "${RESDIR}/tuplex-compiled-1x-run-$r.log.txt"
done

echo "benchmarking Zillow (Z1) over 100G with Tuplex (compiled, 2 threads)"
OUTPUT_PATH="s3://tuplex-leonhard/experiments/Zillow/Z1/output_compiled_2x"
LAMBDA_THREADS=1
for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "Run $r/${NUM_RUNS}"
  LOG="${RESDIR}/tuplex-compiled-2x-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runtuplex.py -m "compiled" --path $INPUT_PATH --output-path $OUTPUT_PATH \
                                          --scratch-dir $SCRATCH_DIR --lambda-memory $LAMBDA_MEMORY \
                                          --lambda-concurrency $LAMBDA_CONCURRENCY \
                                          --lambda-threads $LAMBDA_THREADS >$LOG 2>$LOG.stderr
  cp experiment.log "${RESDIR}/tuplex-compiled-2x-run-$r.log.txt"
done

echo "benchmarking Zillow (Z1) over 100G with Tuplex (compiled, 4 threads)"
OUTPUT_PATH="s3://tuplex-leonhard/experiments/Zillow/Z1/output_compiled_4x"
LAMBDA_THREADS=4
for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "Run $r/${NUM_RUNS}"
  LOG="${RESDIR}/tuplex-compiled-4x-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runtuplex.py -m "compiled" --path $INPUT_PATH --output-path $OUTPUT_PATH \
                                          --scratch-dir $SCRATCH_DIR --lambda-memory $LAMBDA_MEMORY \
                                          --lambda-concurrency $LAMBDA_CONCURRENCY \
                                          --lambda-threads $LAMBDA_THREADS >$LOG 2>$LOG.stderr
  cp experiment.log "${RESDIR}/tuplex-compiled-4x-run-$r.log.txt"
done

echo "benchmarking Zillow (Z1) over 100G with Tuplex (compiled, 6 threads)"
OUTPUT_PATH="s3://tuplex-leonhard/experiments/Zillow/Z1/output_compiled_6x"
LAMBDA_THREADS=6
for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "Run $r/${NUM_RUNS}"
  LOG="${RESDIR}/tuplex-compiled-6x-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runtuplex.py -m "compiled" --path $INPUT_PATH --output-path $OUTPUT_PATH \
                                          --scratch-dir $SCRATCH_DIR --lambda-memory $LAMBDA_MEMORY \
                                          --lambda-concurrency $LAMBDA_CONCURRENCY \
                                          --lambda-threads $LAMBDA_THREADS >$LOG 2>$LOG.stderr
  cp experiment.log "${RESDIR}/tuplex-compiled-6x-run-$r.log.txt"
done

echo "benchmarking Zillow (Z1) over 100G with Tuplex (interpreted)"
OUTPUT_PATH="s3://tuplex-leonhard/experiments/Zillow/Z1/output_interpreted"
for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "Run $r/${NUM_RUNS}"
  LOG="${RESDIR}/tuplex-interpreted-run-$r.txt"
  # use single-thread here
  timeout $TIMEOUT ${PYTHON} runtuplex.py -m "interpreted" --path $INPUT_PATH --output-path $OUTPUT_PATH \
                                          --scratch-dir $SCRATCH_DIR --lambda-memory $LAMBDA_MEMORY \
                                          --lambda-concurrency $LAMBDA_CONCURRENCY \
                                          --lambda-threads 1 >$LOG 2>$LOG.stderr
  cp experiment.log "${RESDIR}/tuplex-interpreted-run-$r.log.txt"
done

echo "Done!"