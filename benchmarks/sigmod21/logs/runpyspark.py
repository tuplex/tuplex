import time
import argparse
import json
import os
import glob
import sys
import re
import random
import string

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
    choices=["regex", "strip", "spark_regex", "spark_split"],
    default="regex",
    help="whether to use the regex clean function or the string strip based one",
)
parser.add_argument(
    "--output-path", 
    type=str,
    dest="output_path",
    default="spark_output/",
    help='specify path where to save output data files',
)
args = parser.parse_args()


def ParseWithRegex(logline):
    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0
    """
    try:
        match = re.search(
            '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)',
            logline,
        )
        if match is None:
            return Row(
                "ip",
                "client_id",
                "user_id",
                "date",
                "method",
                "endpoint",
                "protocol",
                "response_code",
                "content_size",
            )("", "", "", "", "", "", "", -1, -1)
        size_field = match.group(9)
        if size_field == "-":
            size = 0
        else:
            size = int(match.group(9))
        return Row(
            "ip",
            "client_id",
            "user_id",
            "date",
            "method",
            "endpoint",
            "protocol",
            "response_code",
            "content_size",
        )(
            match.group(1),
            match.group(2),
            match.group(3),
            match.group(4),
            match.group(5),
            match.group(6),
            match.group(7),
            int(match.group(8)),
            size,
        )
    except:
        return ("", "", "", "", "", "", "", -1, -1)


def ParseWithStrip(x):
    try:
        y = x

        i = y.find(" ")
        ip = y[:i]
        y = y[i + 1 :]

        i = y.find(" ")
        client_id = y[:i]
        y = y[i + 1 :]

        i = y.find(" ")
        user_id = y[:i]
        y = y[i + 1 :]

        i = y.find("]")
        date = y[:i][1:]
        y = y[i + 2 :]

        y = y[y.find('"') + 1 :]

        method = ""
        endpoint = ""
        protocol = ""
        failed = False
        if y.find(" ") < y.rfind('"'):
            i = y.find(" ")
            method = y[:i]
            y = y[i + 1 :]

            i = y.find(" ")  # needs to be any whitespace
            endpoint = y[:i]
            y = y[i + 1 :]

            i = y.rfind('"')
            protocol = y[:i]
            protocol = protocol[protocol.rfind(" ") + 1 :]
            y = y[i + 2 :]
        else:
            failed = True
            i = y.rfind('"')
            y = y[i + 2 :]

        i = y.find(" ")
        response_code = y[:i]
        content_size = y[i + 1 :]

        if not failed:
            return Row(
                "ip",
                "client_id",
                "user_id",
                "date",
                "method",
                "endpoint",
                "protocol",
                "response_code",
                "content_size",
            )(
                ip,
                client_id,
                user_id,
                date,
                method,
                endpoint,
                protocol,
                int(response_code),
                0 if content_size == "-" else int(content_size),
            )
        else:
            return Row(
                "ip",
                "client_id",
                "user_id",
                "date",
                "method",
                "endpoint",
                "protocol",
                "response_code",
                "content_size",
            )("", "", "", "", "", "", "", -1, -1)
    except:
        return Row(
            "ip",
            "client_id",
            "user_id",
            "date",
            "method",
            "endpoint",
            "protocol",
            "response_code",
            "content_size",
        )("", "", "", "", "", "", "", -1, -1)


def RandomizeEndpointUDF(x):
    return re.sub(
        "^/~[^/]+",
        "/~" + "".join([random.choice(string.ascii_uppercase) for _ in range(10)]),
        x,
    )


def CreateRandomID():
    return "/~" + "".join([random.choice(string.ascii_uppercase) for _ in range(10)])


# save the run configuration
output_path = args.output_path
clean_function = ParseWithRegex if args.pipeline_type == "regex" else ParseWithStrip

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
from pyspark.sql.functions import udf, col, column, regexp_extract, expr, regexp_replace, split, trim, lit, length, substring, coalesce, concat, size
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
if args.pipeline_type == "regex" or args.pipeline_type == "strip":
    tstart = time.time()
    RandomizeEndpointUDF = udf(RandomizeEndpointUDF, StringType())
    clean_schema = StructType(
        [
            StructField("ip", StringType(), False),
            StructField("client_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("date", StringType(), False),
            StructField("method", StringType(), False),
            StructField("endpoint", StringType(), False),
            StructField("protocol", StringType(), False),
            StructField("response_code", IntegerType(), False),
            StructField("content_size", IntegerType(), False),
        ]
    )
    clean_function = udf(clean_function, clean_schema)
    df = spark.read.csv(
        perf_paths,
        sep="\n",
        schema="col0 STRING",
        header=False,
        mode="PERMISSIVE",
        multiLine=True,
        escape="",
        encoding="utf-8",
    )
    # df.show(truncate=False)
    # parse the rows
    df = df.withColumn("col1", clean_function(df["col0"]))
    df = df.select("col1.*")
    df = df.withColumn("endpoint", RandomizeEndpointUDF(df["endpoint"]))
    # df.show(truncate=False)
elif args.pipeline_type == "spark_regex":
    tstart = time.time()
    CreateRandomID = udf(CreateRandomID, StringType())
    df = (
        spark.read.text(perf_paths)
        .select(
            regexp_extract("value", r"(^\S+) ", 1).alias("ip"),
            regexp_extract("value", r"^\S+ (\S+) ", 1).alias("client_id"),
            regexp_extract("value", r"^\S+ \S+ (\S+) ", 1).alias("user_id"),
            regexp_extract("value", r"^.*\[([\w:/]+\s[+\-]\d{4})\]", 1).alias("date"),
            regexp_extract("value", r'^.*"(\S+) \S+\s*\S*\s*"', 1).alias("method"),
            regexp_extract("value", r'^.*"\S+ (\S+)\s*\S*\s*"', 1).alias("endpoint"),
            regexp_extract("value", r'^.*"\S+ \S+\s*(\S*)\s*"', 1).alias("protocol"),
            regexp_extract("value", r'^.*" (\d{3}) ', 1)
            .cast("integer")
            .alias("response_code"),
            regexp_extract("value", r'^.*" \d{3} (\S+)', 1)
            .cast("integer")
            .alias("content_size"),
        )
        .na.fill({"content_size": 0})
        .filter("endpoint != ''")
    )
    df = df.withColumn("randomstr", CreateRandomID())
    df = df.withColumn(
        "endpoint", expr("regexp_replace(endpoint, '^/~[^/]+', randomstr)")
    )
    # df.select(["ip", "date", "method", "endpoint", "protocol", "response_code", "content_size"]).show(truncate=False)
else:
    tstart = time.time()
    CreateRandomID = udf(CreateRandomID, StringType())
    df = (
        spark.read.text(perf_paths)
        .withColumn("cols", split(col("value"), " "))
        .withColumn("ip", trim(col("cols")[0]))
        .withColumn("client_id", trim(col("cols")[1]))
        .withColumn("user_id", trim(col("cols")[2]))
        .withColumn("date", concat(col("cols")[3], lit(" "), col("cols")[4]))
        .withColumn("date", trim(col("date")))
        .withColumn("date", col("date").substr(lit(0), length(col("date")) - 1))
        .withColumn("date", col("date").substr(lit(2), length(col("date")) - 1))
        .withColumn("method", trim(col("cols")[5]))
        .withColumn("method", substring(col("method"), 2, 1000))
        .withColumn("endpoint", trim(col("cols")[6]))
        .withColumn("protocol", trim(col("cols")[7]))
        .withColumn(
            "protocol", col("protocol").substr(lit(0), length(col("protocol")) - 1)
        )
        .withColumn("response_code", trim(col("cols")[8]).cast("int"))
        .withColumn("content_size", coalesce(trim(col("cols")[9]).cast("int"), lit(0)))
        .na.fill({"content_size": 0})
        .filter(col("response_code").isNotNull())
        .filter(length(col("endpoint")) > 0)
        # .filter(length(col("method")) > 0)
        # .filter(length(col("protocol")) > 0)
        .withColumn("randomstr", CreateRandomID())
        .withColumn("endpoint", expr("regexp_replace(endpoint, '^/~[^/]+', randomstr)"))
        .drop("text")
        .drop("cols")
    )
    # df.select(["ip", "date", "method", "endpoint", "protocol", "response_code", "content_size"]).show(truncate=False)

# df.filter(col('content_size') < 0).show()

# join on bad ips
bad_ip_df = spark.read.csv(
    [args.ip_blacklist_path],
    inferSchema=True,
    header=True,
    mode="PERMISSIVE",
    multiLine=True,
    escape='"',
)

df_malicious_requests = df.join(bad_ip_df, df["ip"] == bad_ip_df["BadIPs"])

df_malicious_requests.select(
    ["ip", "date", "method", "endpoint", "protocol", "response_code", "content_size"]
).write.csv(
    output_path,
    mode="overwrite",
    sep=",",
    header=True,
    escape='"',
    nullValue="",
    emptyValue="",
)
job_time = time.time() - tstart
print("Pyspark job time: {} s".format(job_time))

# print spark plan
df_malicious_requests.explain(extended=True)

# print stats as last line
print(json.dumps({"startupTime": startup_time, "jobTime": job_time}))
