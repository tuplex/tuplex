#!/usr/bin/env bash
# (c) L.Spiegelberg 2022
# this file runs the central hyperspecialization vs. no hyperspecialization experiment

# use 11 runs (in case of cold start) and a timeout after 60min
NUM_RUNS=10
TIMEOUT=3600

# make results directory
RESDIR=results_hyper
mkdir -p ${RESDIR}
PYTHON=python3.9
echo "benchmarking nohyper (hot)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/flights-nohyper-run-$r.txt"
  echo "running $r/${NUM_RUNS}"
  timeout $TIMEOUT $PYTHON runtuplex.py --no-hyper >$LOG 2>$LOG.stderr
  # copy temp aws_job.json result for analysis
  cp aws_job.json ${RESDIR}/"flights-nohyper-run-$r.json"
done
# hyper-specialized
echo "benchmarking hyper (hot)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/flights-hyper-run-$r.txt"
  echo "running $r/${NUM_RUNS}"
  timeout $TIMEOUT $PYTHON runtuplex.py >$LOG 2>$LOG.stderr
  # copy temp aws_job.json result for analysis
  cp aws_job.json ${RESDIR}/"flights-hyper-run-$r.json"
done
echo "done!"

