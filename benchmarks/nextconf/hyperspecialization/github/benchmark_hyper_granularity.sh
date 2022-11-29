#!/usr/bin/env bash
# (c) 2022 L. Spiegelberg
# benchmarks the different sampling modes for the Github query

RESDIR=results/granularity
NUM_RUNS=3
PYTHON=python3.9
TIMEOUT=300 # 5 min timeout per run
echo ">> Running hyper granularity benchmark using ${NUM_RUNS} runs"
mkdir -p $RESDIR

echo ">> Clearing job folder if it exists"
[ -d job ] && rm -rf job


# run hyper mode
echo ">> running with hyper mode, saving results to $RESDIR/hyper"
SPLIT_SIZES=(16MB 32MB 64MB 100MB 128MB 160MB 200MB 256MB 300MB 400MB 512MB 600MB 700MB 900MB 1000MB 1024MB 2048MB)
for size in "${SPLIT_SIZES[@]}"; do
  echo ">> running for size $size"
  mkdir -p job
  mkdir -p $RESDIR/$size/
  for ((r = 1; r <= NUM_RUNS; r++)); do
    echo "-- run $r/$NUM_RUNS"
    LOG=$RESDIR/$size/log-run-$r.txt
    timeout $TIMEOUT ${PYTHON} runtuplex.py --split-size $size > $LOG 2> $LOG.stderr
  done
  # move job folder
  mv job $RESDIR/$size/
  echo "-- moved job folder to $RESDIR/$size/"
  [ -d job ] && rm -rf job
done

echo "-----"
echo "done!"

