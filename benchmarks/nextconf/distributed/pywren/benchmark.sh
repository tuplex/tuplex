#!/usr/bin/env bash

# this is the benchmark file to run from within the docker container

# test for 3GB, 5GB, 10GB (1GB doesn't work for PyWren...)
# !!!do not use 128MB!!! it's used for colding the lambda
MEMORY_SIZES=(3000 5000 10000)

# how often to repeat each setup
NRUNS=1
LAMBDA_NAME=pywren_runner
RESULT_DIR=/results
# set to true of false
COLD_START=true

INPUT_URI=s3://tuplex-public/data/100GB/
OUTPUT_URI=s3://pywren-leonhard/pywren-100GB-zillow-output

mkdir -p $RESULT_DIR

for MEMORY_SIZE in "${MEMORY_SIZES[@]}"; do

  echo "configuring Lambda to use ${MEMORY_SIZE}mb"
  aws lambda update-function-configuration --function-name ${LAMBDA_NAME} --memory-size ${MEMORY_SIZE}
  aws lambda wait function-updated --function-name ${LAMBDA_NAME}
  echo "Lambda ${LAMBDA_NAME} updated"

  # create runs (cold-start!)
  for r in $(seq $NRUNS); do
    OUTPUT_FILE="$RESULT_DIR/run-$r-pywren-${MEMORY_SIZE}mb"

    if [ "$COLD_START" = true ] ; then
      OUTPUT_FILE="$OUTPUT_FILE-cold"
    else
      OUTPUT_FILE="$OUTPUT_FILE-warm"
    fi

    echo "run $r / ${NRUNS} (saving to ${OUTPUT_FILE})"

    # dummy update to cold start function
    if [ "$COLD_START" = true ] ; then
      echo "cold starting function..."
      # random update environment variable to force cold-start
      aws lambda update-function-configuration --function-name ${LAMBDA_NAME} --memory-size ${MEMORY_SIZE} --environment "Variables={RANDOM_VAR=$(date)}"

      aws lambda wait function-updated --function-name ${LAMBDA_NAME}
      echo "reconfigured"
    fi

    echo "running benchmark:::"
    python3.7 /work/run_pywren.py --input-uri ${INPUT_URI} --output-uri ${OUTPUT_URI} --output-stats $OUTPUT_FILE.json > $OUTPUT_FILE.txt 2>&1
    echo "run $r / ${NRUNS} done"
  done

done
echo "benchmark done"
