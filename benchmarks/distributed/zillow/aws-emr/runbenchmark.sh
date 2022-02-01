#!/usr/bin/env bash
# (c) L.Spiegelberg 2017-2022
# runs zillow Z1 benchmark via AWS EMR Serverless
NUM_RUNS="${NUM_RUNS:-11}"
TIMEOUT=9000
PYTHON=python3.68
RESDIR=/results/zillow/Z1
mkdir -p ${RESDIR}

echo "benchmarking Zillow (Z1) over 100G on AWS EMR"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/aws-emr-run-$r.txt"
  timeout $TIMEOUT ${PYTHON} runzillow.py >$LOG 2>$LOG.stderr
  cp experiment.log "${RESDIR}/aws-emr-run-$r.log.txt"
done