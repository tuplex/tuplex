#!/usr/bin/env python3
# (c) L.Spiegelberg
# performs Q06 of TPC-H (as generated via dbgen)

import time
import argparse
import os
import json

parser = argparse.ArgumentParser(description="TPC-H query 06 using PySpark (SQL)")
parser.add_argument(
    "--path",
    type=str,
    dest="data_path",
    default="data/lineitem.tbl",
    help="path to lineitem.tbl",
)
parser.add_argument("--cache", action="store_true",
                    help="use this flag to measure separate load/compute times via .cache")
parser.add_argument("--mode", type=str, default="sql")
parser.add_argument("--preprocessed", action="store_true",
                    help="whether the input file is quantity,extended_price,discount,shipdate with shipdate being an integer")
args = parser.parse_args()

tstart = time.time()
import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

spark = SparkSession.builder.appName("TPC-H Q06").getOrCreate()

startup_time = time.time() - tstart
print("PySpark startup took: {}s".format(startup_time))
tstart = time.time()

load_time = 0
query_time = 0

listitem_columns = ['l_orderkey', 'l_partkey', 'l_suppkey',
                    'l_linenumber', 'l_quantity', 'l_extendedprice',
                    'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus',
                    'l_shipdate', 'l_commitdate', 'l_receiptdate',
                    'l_shipinstruct', 'l_shipmode', 'l_comment']

schema = StructType([
    StructField("l_orderkey", IntegerType(), True),
    StructField("l_partkey", IntegerType(), True),
    StructField("l_suppkey", IntegerType(), True),
    StructField('l_linenumber', IntegerType(), True),
    StructField('l_quantity', DoubleType(), True),
    StructField('l_extendedprice', DoubleType(), True),
    StructField('l_discount', DoubleType(), True),
    StructField('l_tax', DoubleType(), True),
    StructField('l_returnflag', StringType(), True),
    StructField('l_linestatus', StringType(), True),
    StructField('l_shipdate', DateType(), True),
    StructField('l_commitdate', DateType(), True),
    StructField('l_receiptdate', DateType(), True),
    StructField('l_shipinstruct', StringType(), True),
    StructField('l_shipmode', StringType(), True),
    StructField('l_comment', StringType(), True)
])

preprocessed_schema = StructType([
    StructField('l_quantity', DoubleType(), True),
    StructField('l_extendedprice', DoubleType(), True),
    StructField('l_discount', DoubleType(), True),
    StructField('l_shipdate', IntegerType(), True),
])

res = None

if args.mode == 'sql':
    if args.cache:
        # time with caching!
        df = spark.read.csv(args.data_path, sep='|', header=False, mode='PERMISSIVE',
                            schema=preprocessed_schema).cache() if args.preprocessed else spark.read.csv(args.data_path,
                                                                                                         sep='|',
                                                                                                         header=False,
                                                                                                         mode='PERMISSIVE',
                                                                                                         schema=schema).cache()
        df.createOrReplaceTempView("lineitem")

        print('rows: {}'.format(df.count()))
        load_time = time.time() - tstart
        tstart = time.time()
        if args.preprocessed:
            res = spark.sql("""SELECT
                sum(l_extendedprice * l_discount) as revenue
            FROM
                lineitem
            WHERE
                l_shipdate >= 19940101
                AND l_shipdate < 19950101
                AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
                AND l_quantity < 24""").collect()
        else:
            res = spark.sql("""SELECT
                sum(l_extendedprice * l_discount) as revenue
            FROM
                lineitem
            WHERE
                l_shipdate >= date '1994-01-01'
                AND l_shipdate < date '1994-01-01' + interval '1' year
                AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
                AND l_quantity < 24""").collect()
        query_time = time.time() - tstart
    else:
        # end-to-end time
        df = spark.read.csv(args.data_path, sep='|', header=False, mode='PERMISSIVE',
                            schema=preprocessed_schema) if args.preprocessed else spark.read.csv(args.data_path,
                                                                                                 sep='|', header=False,
                                                                                                 mode='PERMISSIVE',
                                                                                                 schema=schema)
        df.createOrReplaceTempView("lineitem")

        if args.preprocessed:
            res = spark.sql("""SELECT
                sum(l_extendedprice * l_discount) as revenue
            FROM
                lineitem
            WHERE
                l_shipdate >= 19940101
                AND l_shipdate < 19950101
                AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
                AND l_quantity < 24""").collect()
        else:
            res = spark.sql("""SELECT
                        sum(l_extendedprice * l_discount) as revenue
                    FROM
                        lineitem
                    WHERE
                        l_shipdate >= date '1994-01-01'
                        AND l_shipdate < date '1994-01-01' + interval '1' year
                        AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
                        AND l_quantity < 24""").collect()
        query_time = time.time() - tstart
else:
    sc = spark.sparkContext

    l_shipdateidx = listitem_columns.index('l_shipdate')
    l_extendedpriceidx = listitem_columns.index('l_extendedprice')
    l_discountidx = listitem_columns.index('l_discount')
    l_quantityidx = listitem_columns.index('l_quantity')


    def filter_tuples(row):
        l_shipdate = int(row[l_shipdateidx].replace('-', ''))
        l_discount = float(row[l_discountidx])
        l_quantity = int(row[l_quantityidx])
        return 19940101 <= l_shipdate < 19950101 and 0.05 <= l_discount <= 0.07 and l_quantity < 24


    def extract_pricediscount(row):
        l_extendedprice = float(row[l_extendedpriceidx])
        l_discount = float(row[l_discountidx])
        return l_discount * l_extendedprice


    # preprocessed order is 0->quantity, 1->extended, 2->discount, 3->shipdate
    def extract_pricediscount_p(row):
        l_extendedprice = float(row[1])
        l_discount = float(row[2])
        return l_discount * l_extendedprice


    def filter_tuples_p(row):
        l_shipdate = int(row[3])
        l_discount = float(row[2])
        l_quantity = int(row[0])
        return 19940101 <= l_shipdate < 19950101 and 0.05 <= l_discount <= 0.07 and l_quantity < 24

    rdd = sc.textFile(args.data_path).map(lambda line: line.split('|'))

    if args.cache:
        rdd = rdd.cache()
        rdd.count()
        load_time = time.time() - tstart
        tstart = time.time()

    if args.preprocessed:
        res = rdd.filter(filter_tuples_p).map(extract_pricediscount_p).sum()
    else:
        res = rdd.filter(filter_tuples).map(extract_pricediscount).sum()
    query_time = time.time() - tstart
print('Result::\n{}'.format(res))
fw = 'pyspark'
if args.mode == 'sql':
    fw += '-sql'
print('framework,version,load,query,result\n{},{},{},{},{}'.format(fw, pyspark.__version__, load_time, query_time,
                                                                   str(res)))
