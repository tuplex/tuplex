#!/usr/bin/env bash

# Parse HWLOC settings
HWLOC=""
if [ $# -ne 0 ] && [ $# -ne 1 ]; then # check nmber of inputs
  echo "usage: ./benchmark_baselines_bbsn00.sh [-hwloc]"
  exit 1
fi

if [ $# -eq 1 ]; then # check if hwloc
  if [ "$1" != "-hwloc" ]; then # check flag
    echo -e "invalid flag: $1\nusage: ././benchmark_baselines_bbsn00.sh [-hwloc]"
    exit 1
  fi
  HWLOC="hwloc-bind --cpubind node:1 --membind node:1 --cpubind node:2 --membind node:2"
fi

echo "Z1 trace"
cd Z1
LOG=../Z1-pandas-trace.txt
${HWLOC} python3.7 rundask.py --mode trace-pandas --path /hot/data/zillow/large10GB.csv #| tee $LOG
cd ..

echo "Z2 trace"
cd Z2
LOG=../Z2-pandas-trace.txt
${HWLOC} python3.7 rundask.py --mode trace-pandas --path data/zillow_clean@10G.csv #| tee $LOG
cd ..

echo "done"
