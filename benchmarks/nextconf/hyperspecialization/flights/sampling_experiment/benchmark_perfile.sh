#!/usr/bin/env bash
# (c) L.Spiegelberg 2022
# this file runs a simple sampling benchmark to show that sampling times increase with number of files etc.

# use 11 runs (in case of cold start) and a timeout after 60min
NUM_RUNS=10
TIMEOUT=3600

# make results directory
RESDIR=results_sampling/perfile-badparse-fmt
mkdir -p ${RESDIR}
PYTHON=python3.9
echo "benchmarking sampling times"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/perfile-sampling-run-$r.txt"
  echo "running $r/${NUM_RUNS}"
  timeout $TIMEOUT $PYTHON runtuplex_perfile.py >$LOG 2>$LOG.stderr
done
echo "done!"

