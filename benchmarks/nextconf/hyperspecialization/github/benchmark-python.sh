#!/usr/bin/env bash
# (c) L.Spiegelberg 2022
# this file runs the pure python mode for the github query (to establish a baseline!)

# use 11 runs (in case of cold start) and a timeout after 60min
NUM_RUNS=NUM_RUNS="${NUM_RUNS:-1}"
TIMEOUT=3600

#-------------------------------------------------------------------------------------------------
# set 1: internal format
# make results directory
RESDIR=experimental_results/github/baseline
echo "Storing experimental result in ${RESDIR}, using ${NUM_RUNS} runs"
mkdir -p ${RESDIR}
PYTHON=python3.9

# rm job folder if it exists...
[ -d job ] && rm -rf job

echo "Benchmark github in pure python mode (baseline)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/github-python-run-$r.txt"
  echo "running $r/${NUM_RUNS}"
  timeout $TIMEOUT $PYTHON runtuplex-new.py --python-mode >$LOG 2>$LOG.stderr
  # copy temp aws_job.json result for analysis
  cp aws_job.json ${RESDIR}/"github-python-run-$r.json"
done
# mv job folder
mkdir -p $RESDIR/github-python
mv job/*.json $RESDIR/github-python
rm -rf job


echo "done!"

