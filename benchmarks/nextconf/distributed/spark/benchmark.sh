#!/usr/bin/env bash
# (c) L.Spiegelberg 2020
# this runs Spark benchmark over zillow for 100G and 1TB

SPARK_MASTER=spark://ip-172-31-29-237.ec2.internal:7077

NUM_RUNS=5
RESULT_DIR=results-pyspark
mkdir -p ${RESULT_DIR}
mkdir -p ${RESULT_DIR}/100G
mkdir -p ${RESULT_DIR}/1000G

# S3 has eventual consistency, Spark jobs fail, i.e. S3Guard has to be used
OUTPUT_PATH='pyspark_output'

echo "running over 100G"
for ((r = 1; r <= NUM_RUNS; r++)); do

  echo "running pyspark tuple"

  export PYSPARK_PYTHON=python3.7
  export PYSPARK_DRIVER_PYTHON=python3.7
  LOG=${RESULT_DIR}/100G/pyspark-tuple-run-$r.txt
  spark-submit --executor-memory 4G --driver-memory 4G --deploy-mode client --master ${SPARK_MASTER} runpyspark.py --path 's3n://tuplex/data/100GB/*.csv' --output-path ${OUTPUT_PATH}_100G --mode tuple >$LOG 2>$LOG.stderr

  echo "running pyspark sql"
  export PYSPARK_PYTHON=python3.7
  export PYSPARK_DRIVER_PYTHON=python3.7
  LOG=${RESULT_DIR}/100G/pyspark-sql-run-$r.txt
  spark-submit --executor-memory 4G --driver-memory 4G --deploy-mode client --master ${SPARK_MASTER} runpyspark.py --path 's3n://tuplex/data/100GB/*.csv' --output-path ${OUTPUT_PATH}_100G --mode sql >$LOG 2>$LOG.stderr

   echo "running pyspark dict"
  export PYSPARK_PYTHON=python3.7
  export PYSPARK_DRIVER_PYTHON=python3.7
  LOG=${RESULT_DIR}/100G/pyspark-dict-run-$r.txt
  spark-submit --executor-memory 4G --driver-memory 4G --deploy-mode client --master ${SPARK_MASTER} runpyspark.py --path 's3n://tuplex/data/100GB/*.csv' --output-path ${OUTPUT_PATH}_100G --mode dict >$LOG 2>$LOG.stderr
done

echo "running over 1000G"
for ((r = 1; r <= NUM_RUNS; r++)); do

  echo "running pyspark tuple"

  export PYSPARK_PYTHON=python3.7
  export PYSPARK_DRIVER_PYTHON=python3.7
  LOG=${RESULT_DIR}/1000G/pyspark-tuple-run-$r.txt
  spark-submit --executor-memory 4G --driver-memory 4G --deploy-mode client --master ${SPARK_MASTER} runpyspark.py --path 's3n://tuplex/data/1000GB/*.csv' --output-path ${OUTPUT_PATH}_1000G --mode tuple >$LOG 2>$LOG.stderr

  echo "running pyspark sql"
  export PYSPARK_PYTHON=python3.7
  export PYSPARK_DRIVER_PYTHON=python3.7
  LOG=${RESULT_DIR}/1000G/pyspark-sql-run-$r.txt
  spark-submit --executor-memory 4G --driver-memory 4G --deploy-mode client --master ${SPARK_MASTER} runpyspark.py --path 's3n://tuplex/data/1000GB/*.csv' --output-path ${OUTPUT_PATH}_1000G --mode sql >$LOG 2>$LOG.stderr

   echo "running pyspark dict"
  export PYSPARK_PYTHON=python3.7
  export PYSPARK_DRIVER_PYTHON=python3.7
  LOG=${RESULT_DIR}/1000G/pyspark-dict-run-$r.txt
  spark-submit --executor-memory 4G --driver-memory 4G --deploy-mode client --master ${SPARK_MASTER} runpyspark.py --path 's3n://tuplex/data/1000GB/*.csv' --output-path ${OUTPUT_PATH}_1000G --mode dict >$LOG 2>$LOG.stderr
done