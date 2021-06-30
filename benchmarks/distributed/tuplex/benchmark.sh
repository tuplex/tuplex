#!/usr/bin/bash
# (c) L.Spiegelberg 2020, this file runs the benchmark for Tuplex on AWS Lambda

NUM_RUNS=1
RESULT_DIR=results-aws-lambda
mkdir -p ${RESULT_DIR}
mkdir -p ${RESULT_DIR}/100G
mkdir -p ${RESULT_DIR}/1000G

echo "running over 100GB"

for ((r = 1; r <= NUM_RUNS; r++)); do
	LOG=${RESULT_DIR}/100G/tuplex-lambda-run-$r.txt
	python3.7 runtuplex.py --path 's3://tuplex/data/100GB/*.csv' >$LOG 2>$LOG.stderr
done

echo "running over 1TB"
for ((r = 1; r <= NUM_RUNS; r++)); do
        LOG=${RESULT_DIR}/1000G/tuplex-lambda-run-$r.txt
        python3.7 runtuplex.py --path 's3://tuplex/data/1000GB/*.csv' >$LOG 2>$LOG.stderr
done