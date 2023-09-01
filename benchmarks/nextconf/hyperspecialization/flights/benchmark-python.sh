#!/usr/bin/env bash
# (c) L.Spiegelberg 2023
# this file runs the pure python mode for the filter query (to establish a baseline!)

# use 11 runs (in case of cold start) and a timeout after 60min
NUM_RUNS=NUM_RUNS="${NUM_RUNS:-1}"
TIMEOUT=3600

#-------------------------------------------------------------------------------------------------
# set 1: internal format
# make results directory
RESDIR=experimental_results/flights/baseline
echo "Storing experimental result in ${RESDIR}, using ${NUM_RUNS} runs"
mkdir -p ${RESDIR}
PYTHON=python3.9

# rm job folder if it exists...
[ -d job ] && rm -rf job

echo "Benchmark flights in pure python mode (baseline)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/flights-python-run-$r.txt"
  echo "running $r/${NUM_RUNS}"
  timeout $TIMEOUT $PYTHON runtuplex-filter.py --python-mode >$LOG 2>$LOG.stderr
  # copy temp aws_job.json result for analysis
  cp aws_job.json ${RESDIR}/"flights-python-run-$r.json"
done
# mv job folder
mkdir -p $RESDIR/flights-python
mv job/*.json $RESDIR/flights-python
rm -rf job


echo "done!"

