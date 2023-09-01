#!/usr/bin/env bash
# (c) L.Spiegelberg 2023
# this file runs the sampling experiment using Tuplex's original settings but with viton for the two different datasets

# use 11 runs (in case of cold start) and a timeout after 60min
NUM_RUNS=NUM_RUNS="${NUM_RUNS:-1}"
TIMEOUT=3600

#-------------------------------------------------------------------------------------------------
# set 1: internal format
# make results directory
RESDIR=experimental_results/filter-sampling-viton
echo "Storing experimental result in ${RESDIR}, using ${NUM_RUNS} runs"
mkdir -p ${RESDIR}
PYTHON=python3.9

# rm job folder if it exists...
[ -d job ] && rm -rf job

# sampling modes to use when invoking viton
MODES=(A B C D)

### Invoke script in sampling experiment mode ###
echo "Benchmark different sampling modes using tuplex on small dataset (2002-2005)"
for mode in "${MODES[@]}"; do
  echo "-- experiment run for mode ${mode}"

  for ((r = 1; r <= NUM_RUNS; r++)); do
    LOG="${RESDIR}/flights-sampling-viton-small-${mode}-run-$r.txt"
    echo "running $r/${NUM_RUNS}"
    timeout $TIMEOUT $PYTHON runtuplex-filter.py --fast-client-sampling --dataset=small --sampling-mode=${mode} >$LOG 2>$LOG.stderr
    # do not care about job file.
    # copy temp aws_job.json result for analysis
    # cp aws_job.json ${RESDIR}/"flights-hyper-run-$r.json"
  done
done


### Invoke script in sampling experiment mode ###
echo "Benchmark different sampling modes using tuplex on full dataset (1987-2021)"
for mode in "${MODES[@]}"; do
  echo "-- experiment run for mode ${mode}"

  for ((r = 1; r <= NUM_RUNS; r++)); do
    LOG="${RESDIR}/flights-sampling-viton-full-${mode}-run-$r.txt"
    echo "running $r/${NUM_RUNS}"
    timeout $TIMEOUT $PYTHON runtuplex-filter.py --fast-client-sampling --dataset=full --sampling-mode=${mode} >$LOG 2>$LOG.stderr
    # do not care about job file.
    # copy temp aws_job.json result for analysis
    # cp aws_job.json ${RESDIR}/"flights-hyper-run-$r.json"
  done
done
# mv job folder
mkdir -p $RESDIR/flights-hyper
mv job/*.json $RESDIR/flights-hyper
rm -rf job

echo "done!"
