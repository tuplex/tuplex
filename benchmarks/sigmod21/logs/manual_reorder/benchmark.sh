#!/usr/bin/env bash
# runs weblogs manually reordered benchmarks for docker

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

DATA_PATH=/data/logs
IP_PATH=/data/logs/ip_blacklist.csv
RESDIR=/results/weblogs
OUTPUT_DIR=/results/output/weblogs
NUM_RUNS=11
REDUCED_NUM_RUNS=4
TIMEOUT=14400
PYTHON=python3.6

mkdir -p ${RESDIR}

echo "running tuplex (reordered split mode)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-reordered-split-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/tuplex_reordered_split_output --ip_blacklist_path $IP_PATH --pipeline_type split >$LOG 2>$LOG.stderr
done

echo "running tuplex (reordered strip mode)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-reordered-strip-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/tuplex_reordered_strip_output --ip_blacklist_path $IP_PATH --pipeline_type strip >$LOG 2>$LOG.stderr
done

echo "running tuplex (reordered regex mode)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-reordered-regex-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/tuplex_reordered_regex_output --ip_blacklist_path $IP_PATH --pipeline_type regex >$LOG 2>$LOG.stderr
done

echo "running tuplex (reordered split regex mode)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-reordered-split-regex-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/tuplex_reordered_splitregex_output --ip_blacklist_path $IP_PATH --pipeline_type split_regex >$LOG 2>$LOG.stderr
done

# spark
export PYSPARK_PYTHON=${PYTHON}
export PYSPARK_DRIVER_PYTHON=${PYTHON}
echo "benchmarking pyspark (reordered sparksql regex)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pysparksql-reordered-regex-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 256g runpyspark.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/sparksql_reordered_regex_output --ip_blacklist_path $IP_PATH --pipeline_type 'spark_regex' >$LOG 2>$LOG.stderr
done

echo "benchmarking pyspark (reordered fast spark split)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pysparksql-reordered-split-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 256g runpyspark.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/sparksql_reordered_split_output --ip_blacklist_path $IP_PATH --pipeline_type 'spark_split' >$LOG 2>$LOG.stderr
done
