#!/usr/bin/env bash
# (c) 2022 L. Spiegelberg
# benchmarks the different sampling modes for the Github query

RESDIR=results/sampling
NUM_RUNS=11
PYTHON=python3.9
TIMEOUT=300 # 5 min timeout per run
echo ">> Running sampling benchmark (A - F) using ${NUM_RUNS} runs"
mkdir -p $RESDIR

echo ">> Clearing job folder if it exists"
[ -d job ] && rm -rf job

# no hyper with different sampling modes
SAMPLING_MODES=(A B C D E F)
for sm in "${SAMPLING_MODES[@]}"; do
  echo ">> running with sampling mode $sm, saving results to $RESDIR/$sm"
  mkdir -p $RESDIR/$sm
  for ((r = 1; r <= NUM_RUNS; r++)); do
    echo "-- run $r/$NUM_RUNS"
    LOG=$RESDIR/$sm/log-run-$r.txt
    timeout $TIMEOUT ${PYTHON} runtuplex.py --no-hyper --sampling-mode $sm > $LOG 2> $LOG.stderr
  done
  # move job folder
  mv job $RESDIR/$sm/
  echo "-- moved job folder"
  [ -d job ] && rm -rf job
done

# run hyper mode
echo ">> running with hyper mode, saving results to $RESDIR/hyper"
mkdir -p $RESDIR/hyper
for ((r = 1; r <= NUM_RUNS; r++)); do
  echo "-- run $r/$NUM_RUNS"
  LOG=$RESDIR/hyper/log-run-$r.txt
  timeout $TIMEOUT ${PYTHON} runtuplex.py > $LOG 2> $LOG.stderr
done
# move job folder
mv job $RESDIR/hyper/
echo "-- moved job folder"
[ -d job ] && rm -rf job

echo "-----"
echo "done!"

