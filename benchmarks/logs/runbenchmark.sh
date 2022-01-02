#!/usr/bin/env bash
# runs weblogs benchmark for docker

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
NUM_RUNS="${NUM_RUNS:-11}"
REDUCED_NUM_RUNS=4
TIMEOUT=14400
PYTHON=python3.6

mkdir -p ${RESDIR}

# Tuplex
echo "running tuplex (split mode)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-split-run-$r.txt"
  rm -rf $OUTPUT_DIR
  mkdir -p $OUTPUT_DIR
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/tuplex_split_output --ip_blacklist_path $IP_PATH --pipeline_type split >$LOG 2>$LOG.stderr
  rm -rf /tmp/*
done

echo "running tuplex (strip mode)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-strip-run-$r.txt"
  rm -rf $OUTPUT_DIR
  mkdir -p $OUTPUT_DIR
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/tuplex_strip_output --ip_blacklist_path $IP_PATH --pipeline_type strip >$LOG 2>$LOG.stderr
  rm -rf /tmp/*
done

echo "running tuplex (regex mode)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-regex-run-$r.txt"
  rm -rf $OUTPUT_DIR
  mkdir -p $OUTPUT_DIR
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/tuplex_regex_output --ip_blacklist_path $IP_PATH --pipeline_type regex >$LOG 2>$LOG.stderr
  rm -rf /tmp/*
done

echo "running tuplex (split regex mode)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/tuplex-split-regex-run-$r.txt"
  rm -rf $OUTPUT_DIR
  mkdir -p $OUTPUT_DIR
  timeout $TIMEOUT ${HWLOC} ${PYTHON} runtuplex.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/tuplex_splitregex_output --ip_blacklist_path $IP_PATH --pipeline_type split_regex >$LOG 2>$LOG.stderr
  rm -rf /tmp/*
done

# spark
export PYSPARK_PYTHON=${PYTHON}
export PYSPARK_DRIVER_PYTHON=${PYTHON}
echo "benchmarking pyspark (sparksql regex)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pysparksql-regex-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/sparksql_regex_output --ip_blacklist_path $IP_PATH --pipeline_type 'spark_regex' >$LOG 2>$LOG.stderr
  rm -rf /tmp/*
done

echo "benchmarking pyspark (sparksql split)"
for ((r = 1; r <= NUM_RUNS; r++)); do
  LOG="${RESDIR}/pysparksql-split-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/sparksql_split_output --ip_blacklist_path $IP_PATH --pipeline_type 'spark_split' >$LOG 2>$LOG.stderr
  rm -rf /tmp/*
done

echo "benchmarking pyspark (python regex)"
for ((r = 1; r <= REDUCED_NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-python-regex-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/pyspark_regex_output --ip_blacklist_path $IP_PATH --pipeline_type 'regex' >$LOG 2>$LOG.stderr
  rm -rf /tmp/*
done

echo "benchmarking pyspark (python strip)"
for ((r = 1; r <= REDUCED_NUM_RUNS; r++)); do
  LOG="${RESDIR}/pyspark-python-strip-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} spark-submit --master "local[16]" --driver-memory 100g runpyspark.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/pyspark_strip_output --ip_blacklist_path $IP_PATH --pipeline_type 'strip' >$LOG 2>$LOG.stderr
  rm -rf /tmp/*
done

# Dask
#echo "benchmarking dask (python split)"
#for ((r = 1; r <= REDUCED_NUM_RUNS; r++)); do
  #LOG="${RESDIR}/dask-split-run-$r.txt"
  #timeout $TIMEOUT ${HWLOC} ${PYTHON} rundask.py --path $DATA_PATH --ip_blacklist_path $IP_PATH --pipeline_type 'split' >$LOG 2>$LOG.stderr
  #printf "*Dask End Time: " >>$LOG
  #date +"%s.%7N" >>$LOG
  ##date +"%F %T.%6N" >>$LOG
#done

#echo "benchmarking dask (python split regex)"
#for ((r = 1; r <= REDUCED_NUM_RUNS; r++)); do
  #LOG="${RESDIR}/dask-split-regex-run-$r.txt"
  #timeout $TIMEOUT ${HWLOC} ${PYTHON} rundask.py --path $DATA_PATH --ip_blacklist_path $IP_PATH --pipeline_type 'split_regex' >$LOG 2>$LOG.stderr
  #printf "*Dask End Time: " >>$LOG
  #date +"%s.%7N" >>$LOG
  ##date +"%F %T.%6N" >>$LOG
#done

echo "benchmarking dask (python regex)"
for ((r = 1; r <= REDUCED_NUM_RUNS; r++)); do
  LOG="${RESDIR}/dask-regex-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} rundask.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/dask_regex_output --ip_blacklist_path $IP_PATH --pipeline_type 'regex' >$LOG 2>$LOG.stderr
  printf "*Dask End Time: " >>$LOG
  date +"%s.%7N" >>$LOG
  #date +"%F %T.%6N" >>$LOG
  rm -rf /tmp/*
done

echo "benchmarking dask (python strip)"
for ((r = 1; r <= REDUCED_NUM_RUNS; r++)); do
  LOG="${RESDIR}/dask-strip-run-$r.txt"
  timeout $TIMEOUT ${HWLOC} ${PYTHON} rundask.py --path $DATA_PATH --output-path ${OUTPUT_DIR}/dask_strip_output --ip_blacklist_path $IP_PATH --pipeline_type 'strip' >$LOG 2>$LOG.stderr
  printf "*Dask End Time: " >>$LOG
  date +"%s.%7N" >>$LOG
  #date +"%F %T.%6N" >>$LOG
  rm -rf /tmp/*
done
