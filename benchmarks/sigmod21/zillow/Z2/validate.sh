#!/usr/bin/env bash
# (c) L.Spiegelberg 2017-2020
# validate that output is the same across all frameworks!

INPUT_FILE=data/zillow_clean.csv

OUTPUT_DIR=validation_output
if [ -d "$OUTPUT_DIR" ]; then
  rm -rf $OUTPUT_DIR
fi
mkdir -p OUTPUT_DIR

## Tuplex
echo "running Tuplex"
python3 runtuplex.py --output-path $OUTPUT_DIR/tuplex --path $INPUT_FILE

# pure python runs with/without pypy
echo "running pure python versions with cpython and pypy"
python3 runpython.py --mode tuple --output-path $OUTPUT_DIR/python3_tuple --path $INPUT_FILE
python3 runpython.py --mode dict --output-path $OUTPUT_DIR/python3_dict --path $INPUT_FILE
pypy3 runpython.py --mode tuple --output-path $OUTPUT_DIR/pypy3_tuple --path $INPUT_FILE
pypy3 runpython.py --mode dict --output-path $OUTPUT_DIR/pypy3_dict --path $INPUT_FILE

# cython + nuitka
echo "running compiled python via cython and nuitka"
python3 runpython.py --mode tuple --compiler cython --output-path $OUTPUT_DIR/cython3_tuple --path $INPUT_FILE
python3 runpython.py --mode dict --compiler cython --output-path $OUTPUT_DIR/cython3_dict --path $INPUT_FILE
python3 runpython.py --mode tuple --compiler nuitka --output-path $OUTPUT_DIR/nuitka_tuple --path $INPUT_FILE
python3 runpython.py --mode dict --compiler nuitka --output-path $OUTPUT_DIR/nuitka_dict --path $INPUT_FILE

# Pyspark runs with/without pypy
echo "running pyspark using cpython and pypy as workers"
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
spark-submit runpyspark.py --mode rdd_tuple --output-path $OUTPUT_DIR/pyspark_python3_tuple --path $INPUT_FILE
spark-submit runpyspark.py --mode rdd_dict --output-path $OUTPUT_DIR/pyspark_python3_dict --path $INPUT_FILE
spark-submit runpyspark.py --mode sql --output-path $OUTPUT_DIR/pyspark_python3_df --path $INPUT_FILE
export PYSPARK_PYTHON=pypy3
export PYSPARK_DRIVER_PYTHON=pypy3
spark-submit runpyspark.py --mode rdd_tuple --output-path $OUTPUT_DIR/pyspark_pypy3_tuple --path $INPUT_FILE
spark-submit runpyspark.py --mode rdd_dict --output-path $OUTPUT_DIR/pyspark_pypy3_dict --path $INPUT_FILE
spark-submit runpyspark.py --mode sql --output-path $OUTPUT_DIR/pyspark_pypy3_df --path $INPUT_FILE

# Dask + Pandas
echo "running dask and pandas"
python3 rundask.py --mode dask --output-path $OUTPUT_DIR/dask_output --path $INPUT_FILE
python3 rundask.py --mode pandas --output-path $OUTPUT_DIR/pandas_output --path $INPUT_FILE

# run C++ baseline
echo "Running C++ baseline"
cd baseline
./build-cpp.sh
cp tester ../cc_baseline
cd ..
mkdir -p $OUTPUT_DIR/cc_no_preload_output
./cc_baseline --path $INPUT_FILE --output_path $OUTPUT_DIR/cc_no_preload_output
mkdir -p $OUTPUT_DIR/cc_preload_output
./cc_baseline --path $INPUT_FILE --output_path $OUTPUT_DIR/cc_preload_output --preload
rm cc_baseline

# run Scala baseline + PySparkSQLScala

echo "Running pure Scala baseline"
cd scala
./build.sh
cd ..
mkdir -p $OUTPUT_DIR/scala
java -jar scala/target/scala-2.11/ZillowScala-assembly-0.1.jar $INPUT_FILE $OUTPUT_DIR/scala/zillow_out.csv

echo "Running SparkSQL Scala baseline"
cd sparksql-scala
./build.sh
cd ..
spark-submit --class tuplex.exp.spark.ZillowClean sparksql-scala/target/scala-2.11/zillowscala_2.11-0.1.jar $INPUT_FILE $OUTPUT_DIR/sparksql-scala

## Ray/Modin?
## requires pip3 install modin[ray]
## export MODIN_ENGINE=ray  # Modin will use Ray
##export MODIN_ENGINE=dask  # Modin will use Dask
#
# run validation script
python3 validate_all.py
