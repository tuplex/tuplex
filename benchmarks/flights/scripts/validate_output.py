#!/usr/bin/env python3
# (c) L.Spiegelberg 2020
# leonhard_spiegelberg@brown.edu

# this script uses Spark to validate the output of two folders against each other
# run via spark-submit
import sys
import os
import time

if len(sys.argv) != 3:
    print("usage: {} folderA folderB".format(sys.argv[0]))

# e.g. folderA = 'tuplex_output'
#      folderB = 'pyspark_output'
folderA = sys.argv[1]
folderB = sys.argv[2]
diff_output_path = 'diff_rows.csv'


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, column, coalesce, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DoubleType

tstart = time.time()

spark = SparkSession.builder.appName("flight_query_output_validator").getOrCreate()
df = spark.read.csv(os.path.join(folderA, '*.csv'), header=True).cache()
df_spark = spark.read.csv(os.path.join(folderB, '*.csv'), header=True).cache()

# compute diff in count and actual rows
countdiff = abs(df_spark.count() - df.count())
diff_rows = df.union(df_spark).subtract(df.intersect(df_spark)).cache()
diff_row_cnt = diff_rows.count()
if diff_row_cnt > 0:
    diff_rows.coalesce(1).write.csv(diff_output_path, mode='overwrite', sep=',', header=True, escape='"', nullValue='\u0000',
               emptyValue='\u0000')

print('validation took: {}s'.format(time.time() - tstart))
print('number of differing rows: {} ({})'.format(countdiff, diff_row_cnt))
print('saved diff result to: {}'.format(countdiff))


