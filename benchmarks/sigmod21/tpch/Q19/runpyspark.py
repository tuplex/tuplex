#!/usr/bin/env python3
# TPC-H Q19

import time
import argparse
import os
import json

parser = argparse.ArgumentParser(description="TPC-H Q19 using PySpark (SQL)")
parser.add_argument(
    "--lineitem_path",
    type=str,
    dest="lineitem_path",
    default="data/lineitem.tbl",
    help="path to lineitem.tbl",
)
parser.add_argument(
    "--part_path",
    type=str,
    dest="part_path",
    default="data/part.tbl",
    help="path to part.tbl",
)
parser.add_argument(
    "--cache",
    action="store_true",
    help="use this flag to measure separate load/compute times via .cache",
)
parser.add_argument("--mode", type=str, default="sql")
parser.add_argument(
    "--preprocessed",
    action="store_true",
    help="whether the input file is preprocessed (eg. pushdown applied)",
)


args = parser.parse_args()

lineitem_columns = [
    "l_orderkey",
    "l_partkey",
    "l_suppkey",
    "l_linenumber",
    "l_quantity",
    "l_extendedprice",
    "l_discount",
    "l_tax",
    "l_returnflag",
    "l_linestatus",
    "l_shipdate",
    "l_commitdate",
    "l_receiptdate",
    "l_shipinstruct",
    "l_shipmode",
    "l_comment",
]
part_columns = [
    "p_partkey",
    "p_name",
    "p_mfgr",
    "p_brand",
    "p_type",
    "p_size",
    "p_container",
    "p_retailprice",
    "p_comment",
]

if args.preprocessed:
    lineitem_columns = [
        "l_partkey",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_shipinstruct",
        "l_shipmode",
    ]
    part_columns = ["p_partkey", "p_brand", "p_size", "p_container"]


tstart = time.time()
import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    DateType,
)

spark = SparkSession.builder.appName("TPC-H Q19").getOrCreate()

startup_time = time.time() - tstart
print("PySpark startup took: {}s".format(startup_time))
tstart = time.time()

load_time = 0
query_time = 0

lineitem_schema = StructType(
    [
        StructField("l_orderkey", IntegerType(), True),
        StructField("l_partkey", IntegerType(), True),
        StructField("l_suppkey", IntegerType(), True),
        StructField("l_linenumber", IntegerType(), True),
        StructField("l_quantity", DoubleType(), True),
        StructField("l_extendedprice", DoubleType(), True),
        StructField("l_discount", DoubleType(), True),
        StructField("l_tax", DoubleType(), True),
        StructField("l_returnflag", StringType(), True),
        StructField("l_linestatus", StringType(), True),
        StructField("l_shipdate", DateType(), True),
        StructField("l_commitdate", DateType(), True),
        StructField("l_receiptdate", DateType(), True),
        StructField("l_shipinstruct", StringType(), True),
        StructField("l_shipmode", StringType(), True),
        StructField("l_comment", StringType(), True),
    ]
)

part_schema = StructType(
    [
        StructField("p_partkey", IntegerType(), True),
        StructField("p_name", StringType(), True),
        StructField("p_mfgr", StringType(), True),
        StructField("p_brand", StringType(), True),
        StructField("p_type", StringType(), True),
        StructField("p_size", IntegerType(), True),
        StructField("p_container", StringType(), True),
        StructField("p_retailprice", DoubleType(), True),
        StructField("p_comment", StringType(), True),
    ]
)
if args.preprocessed:
    lineitem_schema = StructType(
        [
            StructField("l_partkey", IntegerType(), True),
            StructField("l_quantity", DoubleType(), True),
            StructField("l_extendedprice", DoubleType(), True),
            StructField("l_discount", DoubleType(), True),
            StructField("l_shipinstruct", StringType(), True),
            StructField("l_shipmode", StringType(), True),
        ]
    )
    part_schema = StructType(
        [
            StructField("p_partkey", IntegerType(), True),
            StructField("p_brand", StringType(), True),
            StructField("p_size", IntegerType(), True),
            StructField("p_container", StringType(), True),
        ]
    )


res = None

if args.mode == "sql":
    if args.cache:
        # cache input
        lineitem_df = spark.read.csv(
            args.lineitem_path,
            sep="|",
            header=False,
            mode="PERMISSIVE",
            schema=lineitem_schema,
        ).cache()
        part_df = spark.read.csv(
            args.part_path,
            sep="|",
            header=False,
            mode="PERMISSIVE",
            schema=part_schema,
        ).cache()
        lineitem_df.createOrReplaceTempView("lineitem")
        part_df.createOrReplaceTempView("part")
        print("lineitem rows: {}".format(lineitem_df.count()))
        print("part rows: {}".format(part_df.count()))
        load_time = time.time() - tstart

        # run query
        tstart = time.time()
        res = spark.sql(
            """SELECT
	    sum(l_extendedprice* (1 - l_discount)) as revenue
	FROM
	    lineitem,
	    part
	WHERE
	    (
		p_partkey = l_partkey
		AND p_brand = 'Brand#12'
		AND p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		AND l_quantity >= 1 AND l_quantity <= 1 + 10
		AND p_size between 1 AND 5
		AND l_shipmode in ('AIR', 'AIR REG')
		AND l_shipinstruct = 'DELIVER IN PERSON'
	    )
	    OR
	    (
		p_partkey = l_partkey
		AND p_brand = 'Brand#23'
		AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		AND l_quantity >= 10 AND l_quantity <= 10 + 10
		AND p_size between 1 AND 10
		AND l_shipmode in ('AIR', 'AIR REG')
		AND l_shipinstruct = 'DELIVER IN PERSON'
	    )
	    OR
	    (
		p_partkey = l_partkey
		AND p_brand = 'Brand#34'
		AND p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		AND l_quantity >= 20 AND l_quantity <= 20 + 10
		AND p_size between 1 AND 15
		AND l_shipmode in ('AIR', 'AIR REG')
		AND l_shipinstruct = 'DELIVER IN PERSON'
	    )"""
        ).collect()
        query_time = time.time() - tstart
    else:
        # end-to-end time
        lineitem_df = spark.read.csv(
            args.lineitem_path,
            sep="|",
            header=False,
            mode="PERMISSIVE",
            schema=lineitem_schema,
        )
        part_df = spark.read.csv(
            args.part_path,
            sep="|",
            header=False,
            mode="PERMISSIVE",
            schema=part_schema,
        )
        lineitem_df.createOrReplaceTempView("lineitem")
        part_df.createOrReplaceTempView("part")

        res = spark.sql(
            """SELECT
	    sum(l_extendedprice* (1 - l_discount)) as revenue
	FROM
	    lineitem,
	    part
	WHERE
	    (
		p_partkey = l_partkey
		AND p_brand = 'Brand#12'
		AND p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		AND l_quantity >= 1 AND l_quantity <= 1 + 10
		AND p_size between 1 AND 5
		AND l_shipmode in ('AIR', 'AIR REG')
		AND l_shipinstruct = 'DELIVER IN PERSON'
	    )
	    OR
	    (
		p_partkey = l_partkey
		AND p_brand = 'Brand#23'
		AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		AND l_quantity >= 10 AND l_quantity <= 10 + 10
		AND p_size between 1 AND 10
		AND l_shipmode in ('AIR', 'AIR REG')
		AND l_shipinstruct = 'DELIVER IN PERSON'
	    )
	    OR
	    (
		p_partkey = l_partkey
		AND p_brand = 'Brand#34'
		AND p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		AND l_quantity >= 20 AND l_quantity <= 20 + 10
		AND p_size between 1 AND 15
		AND l_shipmode in ('AIR', 'AIR REG')
		AND l_shipinstruct = 'DELIVER IN PERSON'
	    )"""
        ).collect()
        query_time = time.time() - tstart
else:

    @F.udf(returnType=DoubleType())
    def extract_revenue(l_extendedprice, l_discount):
        return l_extendedprice * (1 - l_discount)

    @F.udf(returnType=BooleanType())
    def Q19_filter(
        p_brand, p_container, l_quantity, p_size, l_shipmode, l_shipinstruct
    ):
        return (
            (
                p_brand == "Brand#12"
                and p_container in ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]
                and 1 <= l_quantity <= 1 + 10
                and 1 <= p_size <= 5
                and l_shipmode in ["AIR", "AIR REG"]
                and l_shipinstruct == "DELIVER IN PERSON"
            )
            or (
                p_brand == "Brand#23"
                and p_container in ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]
                and 10 <= l_quantity <= 10 + 10
                and 1 <= p_size <= 10
                and l_shipmode in ["AIR", "AIR REG"]
                and l_shipinstruct == "DELIVER IN PERSON"
            )
            or (
                p_brand == "Brand#34"
                and p_container in ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]
                and 20 <= l_quantity <= 20 + 10
                and 1 <= p_size <= 15
                and l_shipmode in ["AIR", "AIR REG"]
                and l_shipinstruct == "DELIVER IN PERSON"
            )
        )

    lineitem_df = spark.read.csv(
        args.lineitem_path,
        sep="|",
        header=False,
        mode="PERMISSIVE",
        schema=lineitem_schema,
    )
    part_df = spark.read.csv(
        args.part_path, sep="|", header=False, mode="PERMISSIVE", schema=part_schema
    )

    if args.cache:
        lineitem_df = lineitem_df.cache()
        part_df = part_df.cache()
        lineitem_df.count()
        part_df.count()
        load_time = time.time() - tstart
        tstart = time.time()

    res = (
        part_df.join(lineitem_df, part_df["p_partkey"] == lineitem_df["l_partkey"])
        .filter(
            Q19_filter(
                "p_brand",
                "p_container",
                "l_quantity",
                "p_size",
                "l_shipmode",
                "l_shipinstruct",
            )
        )
        .withColumn("revenue", extract_revenue("l_extendedprice", "l_discount"))
        .select(F.sum("revenue"))
        .collect()
    )
    query_time = time.time() - tstart

# Print Results
print("Result::\n{}".format(res))
fw = "pyspark"
if args.mode == "sql":
    fw += "-sql"
print(
    "framework,version,load,query,result\n{},{},{},{},{}".format(
        fw, pyspark.__version__, load_time, query_time, str(res)
    )
)
