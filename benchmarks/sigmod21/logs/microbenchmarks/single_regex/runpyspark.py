import time
import argparse
import json
import os
import glob
import sys
import re

parser = argparse.ArgumentParser(description="Apache data cleaning + join")
parser.add_argument(
    "--path",
    type=str,
    dest="data_path",
    default="../../test/resources/2000.01.01.txt",
    help="path or pattern to log data",
)
parser.add_argument(
    "--ip_blacklist_path",
    type=str,
    dest="ip_blacklist_path",
    default="../../test/resources/bad_ips_all.txt",
)
parser.add_argument(
    "--pipeline_type",
    type=str,
    dest="pipeline_type",
    choices=["spark_regex"],
    default="spark_regex",
    help="whether to use the regex clean function or the string strip based one",
)

args = parser.parse_args()

# save the run configuration
output_path = f"spark_output_{args.pipeline_type}"

# get the input files
perf_paths = [args.data_path]
if not os.path.isfile(args.data_path):
    file_paths = sorted(glob.glob(os.path.join(args.data_path, "*.*.*.txt")))
    perf_paths = file_paths

if not perf_paths:
    print("found no log data to process, abort.")
    sys.exit(1)

# import spark
startup_time = 0

tstart = time.time()
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, col, column, regexp_extract
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
    BooleanType,
)

spark = SparkSession.builder.appName("apacheLogs").getOrCreate()

startup_time = time.time() - tstart
print("PySpark startup time: {}".format(startup_time))

# open file
tstart = time.time()
df = (
    spark.read.text(perf_paths)
    .select(
        regexp_extract("value", r'^(\S+) \S+ \S+ \[[\w:/]+\s[+\-]\d{4}\] "\S+ \S+\s*\S*\s*" \d{3} \S+', 1).alias("ip"),
    )
)

df.select(
    ["ip"]
).write.csv(
    output_path,
    mode="overwrite",
    sep=",",
    header=True,
    escape='"',
    nullValue="\u0000",
    # emptyValue="\u0000",
)

job_time = time.time() - tstart
print("Pyspark job time: {} s".format(job_time))

print(json.dumps({"startupTime": startup_time, "jobTime": job_time}))
