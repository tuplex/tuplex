import time
import argparse
import json
import os
import glob
import sys

parser = argparse.ArgumentParser(description="311 data cleaning")
parser.add_argument(
    "--weld-mode",
    dest="weld_mode",
    help="if active, add cache statements like weld does; else, do end-to-end",
    action="store_true",
)
parser.add_argument(
    "--sql-mode",
    dest="sql_mode",
    help="if active, use PySparkSQL rather than UDFs",
    action="store_true",
)
parser.add_argument(
    "--path",
    type=str,
    dest="data_path",
    default="../../test/resources/pipelines/flights/sortsample.csv",
    help="path or pattern to flights data",
)
parser.add_argument('--output-path', type=str, dest='output_path', default='spark_output',
                    help='specify path where to save output data files')

args = parser.parse_args()


def fix_zip_codes(zips):
    if not zips:
        return None
    # Truncate everything to length 5
    s = zips[:5]

    # Set 00000 zip codes to nan
    if s == "00000":
        return None
    else:
        return s


# save the run configuration
output_path = args.output_path

# get the input files
perf_paths = [args.data_path]
if not os.path.isfile(args.data_path):
    file_paths = sorted(glob.glob(os.path.join(args.data_path, "*.*.*.txt")))
    perf_paths = file_paths

if not perf_paths:
    print("found no 311 data to process, abort.")
    sys.exit(1)

# import spark
startup_time = 0

tstart = time.time()
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
    BooleanType,
)

spark = SparkSession.builder.appName("Sorting").getOrCreate()

startup_time = time.time() - tstart
print("PySpark startup time: {}".format(startup_time))

tstart = time.time()
df = spark.read.csv(
    perf_paths,
    inferSchema=True,
    header=True,
    mode="PERMISSIVE",
    multiLine=True,
    escape='"',
)
df = (
    df.sort("DAY_OF_MONTH")
    .write.csv(
        output_path,
        mode="overwrite",
        sep=",",
        header=True,
        escape='"',
        nullValue="",
        emptyValue="",
    )
)
job_time = time.time() - tstart
print("PySpark job time: {} s".format(job_time))
print(json.dumps({"startupTime": startup_time, "jobTime": job_time}))
