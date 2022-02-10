#!/usr/bin/env bash

export PYSPARK_PYTHON=python3.6
export PYSPARK_DRIVER_PYTHON=python3.6

/usr/bin/time --verbose spark-submit --master 'local[64]' --driver-memory 256g  runpyspark.py --path /hot/data/311/311_preprocessed.csv --sql-mode

echo "Input file stats:"
du -sh /hot/data/311/311_preprocessed.csv

# cached, b.c. wc is so slow
echo "lines: 197,628,001 /hot/data/311/311_preprocessed.csv"
#echo "#lines: $(wc -l /hot/data/311/311_preprocessed.csv)" 
