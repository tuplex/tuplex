#!/usr/bin/env bash
# (c) 2020 L.Spiegelberg
# run pyspark + tuplex with artitifical optimization barriers

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

# use 5 runs (3 for very long jobs) and a timeout after 180min/3h
NUM_RUNS=5
TIMEOUT=10800
DATA_PATH=/disk/data/flights_small
RESDIR=results_caching

mkdir -p ${RESDIR}

# (1) run normal tuplex
python3 create_conf.py --opt-pushdown --opt-filter --opt-llvm > tuplex_config.json
cp tuplex_config.json ${RESDIR}


echo "running normal tuplex"
for ((r = 1; r <= NUM_RUNS; r++)); do
	LOG="${RESDIR}/tuplex-run-$r.txt"
	timeout $TIMEOUT ${HWLOC} python3.7 runtuplex.py --path $DATA_PATH  >$LOG 2>$LOG.stderr
done

echo "running cached tuplex"
for ((r = 1; r <= NUM_RUNS; r++)); do
        LOG="${RESDIR}/tuplex-cached-run-$r.txt"
        timeout $TIMEOUT ${HWLOC} python3.7 runtuplex-io.py --path $DATA_PATH  >$LOG 2>$LOG.stderr
done

echo "running pyspark-sim tuplex"
for ((r = 1; r <= NUM_RUNS; r++)); do
        LOG="${RESDIR}/tuplex-sim-run-$r.txt"
        timeout $TIMEOUT ${HWLOC} python3.7 runtuplex-io.py --simulate-spark --path $DATA_PATH  >$LOG 2>$LOG.stderr
done

echo "running pysparksql"
export PYSPARK_PYTHON=python3.7
export PYSPARK_DRIVER_PYTHON=python3.7
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-sql-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 256g runpyspark.py --path $DATA_PATH >$LOG 2>$LOG.stderr
done