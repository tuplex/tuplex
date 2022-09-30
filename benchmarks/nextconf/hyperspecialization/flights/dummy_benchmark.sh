#!/usr/bin/env bash
# benchmark to use the test to check out whether hyper-specialization works

# use 11 runs and a timeout after 60min
NUM_RUNS="${NUM_RUNS:-11}"
TIMEOUT=3600

RESULT_DIR=./results/
PYTHON=python3.6

mkdir -p $RESULT_DIR

echo "running ctest benchmark"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESULT_DIR}/tuplex-hyperexp-run-$r.txt"
  #timeout $TIMEOUT ${PYTHON} runtuplex.py --path $LG_INPUT_PATH --output-path $OUTPUT_DIR/tuplex >$LOG 2>$LOG.stderr
  ctest -R "SamplingTest.FlightsLambdaVersion" --verbose >$LOG 2>$LOG.stderr
done
