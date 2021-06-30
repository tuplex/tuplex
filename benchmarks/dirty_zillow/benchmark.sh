#!/usr/bin/env bash

# Parse HWLOC settings
HWLOC=""
if [ $# -ne 0 ] && [ $# -ne 1 ]; then # check nmber of inputs
  echo "usage: ./benchmark.sh [-hwloc]"
  exit 1
fi

if [ $# -eq 1 ]; then # check if hwloc
  if [ "$1" != "-hwloc" ]; then # check flag
    echo -e "invalid flag: $1\nusage: ./benchmark.sh [-hwloc]"
    exit 1
  fi
  HWLOC="hwloc-bind --cpubind node:1 --membind node:1 --cpubind node:2 --membind node:2"
fi

# use 10 runs (3 for very long jobs) and a timeout after 180min/3h
NUM_RUNS=11
TIMEOUT=14400

DATA_PATH='data/zillow_dirty@10G.csv'
DATA_SYNTH_PATH='data/zillow_dirty_synthetic@10G.csv'
RESDIR=results_dirty_zillow@10G

# does file exist?
if [[ ! -f "$DATA_PATH" ]]; then
	echo "file $DATA_PATH not found, abort."
	exit 1
fi

mkdir -p ${RESDIR}

# warmup cache by touching file
# vmtouch -dl <dir> => needs sudo rights I assume...

# create tuplex_config.json
#python3 create_conf.py --opt-null --opt-pushdown --opt-filter --opt-llvm > tuplex_config.json
#cp tuplex_config.json ${RESDIR}

echo "running experiments using 16x parallelism"
echo "running tuplex in plain mode"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-plain-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --mode plain --path $DATA_PATH >$LOG 2>$LOG.stderr
done

echo "running tuplex in resolve mode"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-resolve-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --mode resolve --path $DATA_PATH >$LOG 2>$LOG.stderr
done

echo "running tuplex in custom mode"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-custom-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --mode custom --path $DATA_PATH >$LOG 2>$LOG.stderr
done


echo "running synthetic benchmark w. 16x parallelism"

echo "running tuplex in plain mode"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-plain-synth-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --mode plain --path $DATA_SYNTH_PATH >$LOG 2>$LOG.stderr
done

echo "running tuplex in resolve mode"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-resolve-synth-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --mode resolve --path $DATA_SYNTH_PATH >$LOG 2>$LOG.stderr
done

echo "running tuplex in custom mode"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-custom-synth-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --mode custom --path $DATA_SYNTH_PATH >$LOG 2>$LOG.stderr
done

echo "running experiments in single-thread mode"
echo "running tuplex in plain mode"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-plain-st-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --single-threaded --mode plain --path $DATA_PATH >$LOG 2>$LOG.stderr
done

echo "running tuplex in resolve mode"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-resolve-st-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --single-threaded --mode resolve --path $DATA_PATH >$LOG 2>$LOG.stderr
done

echo "running tuplex in custom mode"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-custom-st-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --single-threaded --mode custom --path $DATA_PATH >$LOG 2>$LOG.stderr
done

echo "running tuplex in resolve(interpreter)  mode"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-resolve-interpreter-st-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --single-threaded --mode resolve --resolve-with-interpreter --path $DATA_PATH >$LOG 2>$LOG.stderr
done

echo "running in-order resolve 16x parallelism experiments"
echo "running tuplex in plain (in order)  mode"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-plain-in-order-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --mode plain --resolve-in-order  --path $DATA_PATH >$LOG 2>$LOG.stderr
done

echo "running tuplex in resolve (in order)  mode"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-resolve-in-order-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --mode resolve --resolve-in-order  --path $DATA_PATH >$LOG 2>$LOG.stderr
done

echo "running tuplex in custom (in order)  mode"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-run-custom-in-order-$r.txt"
  timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --mode custom --resolve-in-order  --path $DATA_PATH >$LOG 2>$LOG.stderr
done
