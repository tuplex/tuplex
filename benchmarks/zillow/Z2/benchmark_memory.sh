#!/usr/bin/env bash

HWLOC="hwloc-bind --cpubind node:1 --membind node:1 --cpubind node:2 --membind node:2"

INPUT_FILE=data/zillow_clean@10G.csv

RES_DIR=benchmark_results/memory_analysis
mkdir -p $RES_DIR
python3.7 -m memory_profiler rundask.py --mode pandas --output-path pandas_output --path $INPUT_FILE

exit 1


MEMLOG="$RES_DIR/pandas_mem.txt"
LOG="$RES_DIR/pandas_run.txt"
date > $MEMLOG
free -s 1 -b >> $MEMLOG &
FREEPID=$!
/usr/bin/time -v ${HWLOC} python3.7 rundask.py --mode pandas --output-path pandas_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
date >> $MEMLOG
kill $FREEPID

MEMLOG="$RES_DIR/dask_mem.txt"
LOG="$RES_DIR/dask_run.txt"
date > $MEMLOG
free -s 1 -b >> $MEMLOG &
FREEPID=$!
/usr/bin/time -v ${HWLOC} python3.7 rundask.py --mode dask --output-path pandas_output --path $INPUT_FILE >$LOG 2>$LOG.stderr
date >> $MEMLOG
kill $FREEPID
