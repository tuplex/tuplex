#!/usr/bin/env bash
# (c) L.Spiegelberg 2022
# this file runs explores how stratified sampling
# influences hyperspecialization

# use 11 runs (in case of cold start) and a timeout after 60min
NUM_RUNS=NUM_RUNS="${NUM_RUNS:-1}"
TIMEOUT=3600

#-------------------------------------------------------------------------------------------------
# set 1: internal format
# make results directory
RESDIR=experimental_results/stratified-sampling
echo "Storing experimental result in ${RESDIR}, using ${NUM_RUNS} runs"
mkdir -p ${RESDIR}
PYTHON=python3.9

SAMPLES_ARR=(1 2 4 8 16 32 64 128 256 512 1024)
for samples_per_strata in "${SAMPLES_ARR[@]}"; do
  echo "Running with ${samples_per_strata} samples per strata"
  RESDIR=experimental_results/stratified-sampling/${samples_per_strata}
  mkdir -p ${RESDIR}
  echo "Storing results in ${RESDIR}"

  # rm job folder if it exists...
  [ -d job ] && rm -rf job

  ### 1a. Hyper w. cf ###
  echo "Benchmark hyper w. cf"
  # hyper-specialized
  echo "benchmarking hyper (hot)"
  for ((r = 1; r <= NUM_RUNS; r++)); do
    LOG="${RESDIR}/flights-hyper-run-$r.txt"
    echo "running $r/${NUM_RUNS}"
    timeout $TIMEOUT $PYTHON runtuplex-filter.py --samples_per_strata=${samples_per_strata} >$LOG 2>$LOG.stderr
    # copy temp aws_job.json result for analysis
    cp aws_job.json ${RESDIR}/"flights-hyper-run-$r.json"
  done
  # mv job folder
  mkdir -p $RESDIR/flights-hyper
  mv job/*.json $RESDIR/flights-hyper
  rm -rf job

  ### 1b. Hyper w. cf ###
  # hyper-specialized
  echo "Benchmark hyper without cf"
  for ((r = 1; r <= NUM_RUNS; r++)); do
    LOG="${RESDIR}/flights-hyper-nocf-run-$r.txt"
    echo "running $r/${NUM_RUNS}"
    timeout $TIMEOUT $PYTHON runtuplex-filter.py --no-cf --samples_per_strata=${samples_per_strata} >$LOG 2>$LOG.stderr
    # copy temp aws_job.json result for analysis
    cp aws_job.json ${RESDIR}/"flights-hyper-nocf-run-$r.json"
  done
  # mv job folder
  mkdir -p $RESDIR/flights-hyper-nocf
  mv job/*.json $RESDIR/flights-hyper-nocf
  rm -rf job
done

echo "done!"

