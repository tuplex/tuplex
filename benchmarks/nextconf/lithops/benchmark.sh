#!/usr/bin/env bash
# (c) L.Spiegelberg 2022
# this file runs the pure python mode for the filter query (to establish a baseline!)

# use 11 runs (in case of cold start) and a timeout after 60min
NUM_RUNS=NUM_RUNS="${NUM_RUNS:-1}"
TIMEOUT=3600

#-------------------------------------------------------------------------------------------------
# set 1: internal format
# make results directory
RESDIR=experimental_results/lithops/flights
echo "Storing experimental result in ${RESDIR}, using ${NUM_RUNS} runs"
mkdir -p ${RESDIR}
PYTHON=python3.9

echo "Benchmark flights in custom mode (Lithops)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/flights-lithops-custom-run-$r.txt"
  echo "running $r/${NUM_RUNS}"
  timeout $TIMEOUT $PYTHON flights_query.py --mode custom >$LOG 2>$LOG.stderr
done

echo "Benchmark flights in Viton mode (Lithops)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/flights-lithops-viton-run-$r.txt"
  echo "running $r/${NUM_RUNS}"
  timeout $TIMEOUT $PYTHON flights_query.py --mode viton >$LOG 2>$LOG.stderr
done

# move the lithops plots
mv plots ${RESDIR}

echo "done!"

