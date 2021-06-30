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
    default="../../test/resources/pipelines/311/311-service-requests-sf=1.csv",
    help="path or pattern to 311 data",
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

spark = SparkSession.builder.appName("311Cleaning").getOrCreate()

startup_time = time.time() - tstart
print("PySpark startup time: {}".format(startup_time))

if args.sql_mode:
    # @Rahul: please integrate below code with the benchmark and create a hyper version similar to tplx199/TPC-H Q6.
    # df.createOrReplaceTempView('requests')
    # df.withColumnRenamed('Incident Zip', 'zip').createOrReplaceTempView('requests')
    # spark.sql("""SELECT DISTINCT CASE WHEN r.zip = 'N/A' THEN NULL
    #  WHEN r.zip = 'NA' THEN NULL
    #   WHEN r.zip = 'NO CLUE' THEN NULL
    #   WHEN ISNULL(r.zip) THEN NULL
    #   WHEN SUBSTR(r.zip, 0, 5) = '00000' THEN NULL
    #   ELSE SUBSTR(r.zip, 0, 5) END as res FROM requests r""").show(1000)
    if args.weld_mode:
        tstart = time.time()
        df = spark.read.csv(
            perf_paths,
            inferSchema=True,
            header=True,
            mode="PERMISSIVE",
            multiLine=True,
            escape='"',
        ).cache()
        df.count()
        read_time = time.time() - tstart
        # compute
        tstart = time.time()
        df.createOrReplaceTempView("requests")
        df.withColumnRenamed("Incident Zip", "zip").createOrReplaceTempView("requests")
        df = spark.sql(
            """SELECT DISTINCT CASE WHEN r.zip = 'N/A' THEN NULL
         WHEN r.zip = 'NA' THEN NULL
          WHEN r.zip = 'NO CLUE' THEN NULL
          WHEN ISNULL(r.zip) THEN NULL
          WHEN SUBSTR(r.zip, 0, 5) = '00000' THEN NULL
          ELSE SUBSTR(r.zip, 0, 5) END as res FROM requests r"""
        )
        # output
        df.write.csv(
            output_path,
            mode="overwrite",
            sep=",",
            header=True,
            escape='"',
            nullValue="",
            emptyValue="",
        )

        job_time = time.time() - tstart
        print(
            json.dumps(
                {
                    "startupTime": startup_time,
                    "readTime": read_time,
                    "jobTime": job_time,
                }
            )
        )
    else:
        tstart = time.time()
        df = spark.read.csv(
            perf_paths,
            inferSchema=True,
            header=True,
            mode="PERMISSIVE",
            multiLine=False,
            escape='"',
        )
        # compute
        df.createOrReplaceTempView("requests")
        df.withColumnRenamed("Incident Zip", "zip").createOrReplaceTempView("requests")
        df = spark.sql(
            """SELECT DISTINCT CASE WHEN r.zip = 'N/A' THEN NULL
         WHEN r.zip = 'NA' THEN NULL
          WHEN r.zip = 'NO CLUE' THEN NULL
          WHEN ISNULL(r.zip) THEN NULL
          WHEN SUBSTR(r.zip, 0, 5) = '00000' THEN NULL
          ELSE SUBSTR(r.zip, 0, 5) END as res FROM requests r"""
        )
        # output
        df.write.csv(
            output_path,
            mode="overwrite",
            sep=",",
            header=True,
            escape='"',
            nullValue="",
            emptyValue="",
        )

        job_time = time.time() - tstart
        print("PySpark job time: {} s".format(job_time))
        print(json.dumps({"startupTime": startup_time, "jobTime": job_time}))
else:
    if args.weld_mode:
        # input
        tstart = time.time()
        df = spark.read.csv(
            perf_paths,
            inferSchema=True,
            header=True,
            mode="PERMISSIVE",
            multiLine=True,
            escape='"',
        ).cache()
        df.count()
        read_time = time.time() - tstart

        # compute
        tstart = time.time()
        df = df.withColumn(
            "Incident Zip", udf(fix_zip_codes, StringType())("Incident Zip")
        ).distinct()
        # output
        df.write.csv(
            output_path,
            mode="overwrite",
            sep=",",
            header=True,
            escape='"',
            nullValue="",
            emptyValue="",
        )

        job_time = time.time() - tstart
        print(
            json.dumps(
                {
                    "startupTime": startup_time,
                    "readTime": read_time,
                    "jobTime": job_time,
                }
            )
        )
    else:
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
            df.withColumn(
                "Incident Zip", udf(fix_zip_codes, StringType())("Incident Zip")
            )
            .distinct()
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
