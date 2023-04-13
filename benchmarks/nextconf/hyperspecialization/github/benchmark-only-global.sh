#!/usr/bin/env bash
# (c) L.Spiegelberg 2022
# this file runs the central hyperspecialization vs. no hyperspecialization experiment
# for the Github query

# use 11 runs (in case of cold start) and a timeout after 60min
NUM_RUNS=NUM_RUNS="${NUM_RUNS:-1}"
TIMEOUT=3600

#-------------------------------------------------------------------------------------------------
# set 1: internal format
# make results directory
RESDIR=experimental_results/github-global
echo "Storing experimental result in ${RESDIR}, using ${NUM_RUNS} runs"
echo "benchmarking only global runs"
mkdir -p ${RESDIR}
PYTHON=python3.9

# rm job folder if it exists...
[ -d job ] && rm -rf job

# -----------------------------
### 2a. NoHyper w. no filter promo ###
echo "Benchmarking nohyper, no filter promotion"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/github-nohyper-nopromo-run-$r.txt"
  echo "running $r/${NUM_RUNS}"
  timeout $TIMEOUT $PYTHON runtuplex-new.py --no-promo --no-hyper  >$LOG 2>$LOG.stderr
  # copy temp aws_job.json result for analysis
  cp aws_job.json ${RESDIR}/"github-nohyper-nopromo-run-$r.json"
done
# mv job folder
mkdir -p $RESDIR/github-nohyper-nopromo
mv job/*.json $RESDIR/github-nohyper-nopromo
rm -rf job
### 2b. NoHyper w. promo ###
echo "Benchmarking nohyper w. filter promotion"
[ -d job ] && rm -rf job
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/github-nohyper-run-$r.txt"
  echo "running $r/${NUM_RUNS}"
  timeout $TIMEOUT $PYTHON runtuplex-new.py --no-hyper >$LOG 2>$LOG.stderr
  # copy temp aws_job.json result for analysis
  cp aws_job.json ${RESDIR}/"github-nohyper-run-$r.json"
done
# mv job folder
mkdir -p $RESDIR/github-nohyper
mv job/*.json $RESDIR/github-nohyper
rm -rf job

echo "done!"

