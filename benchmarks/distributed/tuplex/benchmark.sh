#!/usr/bin/env bash
# (c) L.Spiegelberg 2020, this file runs the benchmark for Tuplex on AWS Lambda

NUM_RUNS=1
RESULT_DIR=results-aws-lambda
mkdir -p ${RESULT_DIR}
mkdir -p ${RESULT_DIR}/100G
mkdir -p ${RESULT_DIR}/1000G

echo "running over 100GB"

PYTHON=python3.7


# cold start, reset?
# test for 3GB, 5GB, 10GB (1GB doesn't work for PyWren...)
# !!!do not use 128MB!!! it's used for colding the lambda
MEMORY_SIZES=(3000 5000 10000)
LAMBDA_NAME=tuplex-lambda-runner

for MEMORY_SIZE in "${MEMORY_SIZES[@]}"; do

  echo "configuring Lambda to use ${MEMORY_SIZE}mb"
  aws lambda update-function-configuration --function-name ${LAMBDA_NAME} --memory-size ${MEMORY_SIZE}
  aws lambda wait function-updated --function-name ${LAMBDA_NAME}
  echo "Lambda ${LAMBDA_NAME} updated"

  # create runs (cold-start!)
  for r in $(seq $NRUNS); do
    OUTPUT_FILE="$RESULT_DIR/run-$r-tuplex-${MEMORY_SIZE}mb"

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
		${PYTHON} run_tuplex.py --path 's3://tuplex/data/100GB/*.csv' >$OUTPUT_FILE.txt 2>$OUTPUT_FILE.txt.stderr
    echo "run $r / ${NRUNS} done"
  done

done
echo "benchmark done"

for ((r = 1; r <= NUM_RUNS; r++)); do
	LOG=${RESULT_DIR}/100G/tuplex-lambda-run-$r.txt
	${PYTHON} run_tuplex.py --path 's3://tuplex/data/100GB/*.csv' >$LOG 2>$LOG.stderr
done







exit 0


echo "running over 1TB"
for ((r = 1; r <= NUM_RUNS; r++)); do
        LOG=${RESULT_DIR}/1000G/tuplex-lambda-run-$r.txt
        ${PYTHON} run_tuplex.py --path 's3://tuplex/data/1000GB/*.csv' >$LOG 2>$LOG.stderr
done
