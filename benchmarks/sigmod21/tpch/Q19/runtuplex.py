#!/usr/bin/env python3
# TPC-H Q19

import time
import argparse
import tuplex
import os
import json

parser = argparse.ArgumentParser(description="TPC-H Q19 using Tuplex")
parser.add_argument(
    "--lineitem_path",
    type=str,
    dest="lineitem_path",
    default="data/lineitem_preprocessed.tbl",
    help="path to lineitem.tbl",
)
parser.add_argument(
    "--part_path",
    type=str,
    dest="part_path",
    default="data/part_preprocessed.tbl",
    help="path to part.tbl",
)
parser.add_argument(
    "--cache",
    action="store_true",
    help="use this flag to measure separate load/compute times via .cache",
)
parser.add_argument(
    "--preprocessed",
    action="store_true",
    help="whether the input file is preprocessed (eg. pushdown applied)",
)
parser.add_argument(
    "--mode",
    type=str,
    default="tuplex",
    choices=["tuplex", "pysparksql", "optimal"],
    help="different options for manual filter pushdown",
)
parser.add_argument(
    "--weld",
    action="store_true",
    help="whether to use integer preprocessed data"
)
parser.add_argument(
    "--bitwise",
    action="store_true",
    help="whether the filter uses bitwise operations, rather than logical"
)

args = parser.parse_args()

if(args.weld):
    assert(args.preprocessed)

conf = {}
if os.path.exists("tuplex_config.json"):
    with open("tuplex_config.json") as fp:
        conf = json.load(fp)
else:
    # configuration, make sure to give enough runtime memory to the executors!
    conf = {
        "webui.enable": False,
        "executorMemory": "2G",
        "driverMemory": "2G",
        "partitionSize": "64MB",
        "runTimeMemory": "128MB",
        "useLLVMOptimizer": True,
        "nullValueOptimization": True,
        "csv.selectionPushdown": True,
        "optimizer.generateParser": True,
    }

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

lineitem_type_hints = {}
part_type_hints = {}

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

    if args.weld:
        lineitem_type_hints = {
                0: int,
                1: float,
                2: float,
                3: float,
                4: int,
                5: int,
        }
        part_type_hints = {
                0: int, 1: int, 2: int, 3:int}


def Q19_filter_logical(x):
    return (
        (
            x["p_brand"] == "Brand#12"
            and x["p_container"] in ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]
            and 1 <= x["l_quantity"] <= 1 + 10
            and 1 <= x["p_size"] <= 5
            and x["l_shipmode"] in ["AIR", "AIR REG"]
            and x["l_shipinstruct"] == "DELIVER IN PERSON")
        or (
            x["p_brand"] == "Brand#23"
            and x["p_container"] in ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]
            and 10 <= x["l_quantity"] <= 10 + 10
            and 1 <= x["p_size"] <= 10
            and x["l_shipmode"] in ["AIR", "AIR REG"]
            and x["l_shipinstruct"] == "DELIVER IN PERSON"
        )
        or (
            x["p_brand"] == "Brand#34"
            and x["p_container"] in ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]
            and 20 <= x["l_quantity"] <= 20 + 10
            and 1 <= x["p_size"] <= 15
            and x["l_shipmode"] in ["AIR", "AIR REG"]
            and x["l_shipinstruct"] == "DELIVER IN PERSON"))

def Q19_filter_weld(x):
    return (
        (
            x["p_brand"] == 12
            and x["p_container"] in [18, 31, 25, 4]
            and 1 <= x["l_quantity"] <= 1 + 10
            and 1 <= x["p_size"] <= 5
            and x["l_shipmode"] in [3, 7]
            and x["l_shipinstruct"] == 0)
        or (
            x["p_brand"] == 23
            and x["p_container"] in [5, 38, 19, 13]
            and 10 <= x["l_quantity"] <= 10 + 10
            and 1 <= x["p_size"] <= 10
            and x["l_shipmode"] in [3, 7]
            and x["l_shipinstruct"] == 0 
        )
        or (
            x["p_brand"] == 34
            and x["p_container"] in [1, 14, 29, 21]
            and 20 <= x["l_quantity"] <= 20 + 10
            and 1 <= x["p_size"] <= 15
            and x["l_shipmode"] in [3, 7]
            and x["l_shipinstruct"] == 0))

def Q19_filter_bitwise(x):
    return (
        (
            (x["p_brand"] == "Brand#12")
            & (x["p_container"] in ["SM CASE", "SM BOX", "SM PACK", "SM PKG"])
            & (1 <= x["l_quantity"] <= 1 + 10)
            & (1 <= x["p_size"] <= 5)
            & (x["l_shipmode"] in ["AIR", "AIR REG"])
            & (x["l_shipinstruct"] == "DELIVER IN PERSON"))
        | (
            (x["p_brand"] == "Brand#23")
            & (x["p_container"] in ["MED BAG", "MED BOX", "MED PKG", "MED PACK"])
            & (10 <= x["l_quantity"] <= 10 + 10)
            & (1 <= x["p_size"] <= 10)
            & (x["l_shipmode"] in ["AIR", "AIR REG"])
            & (x["l_shipinstruct"] == "DELIVER IN PERSON")
        )
        | (
            (x["p_brand"] == "Brand#34")
            & (x["p_container"] in ["LG CASE", "LG BOX", "LG PACK", "LG PKG"])
            & (20 <= x["l_quantity"] <= 20 + 10)
            & (1 <= x["p_size"] <= 15)
            & (x["l_shipmode"] in ["AIR", "AIR REG"])
            & (x["l_shipinstruct"] == "DELIVER IN PERSON")))

Q19_filter = Q19_filter_bitwise if args.bitwise else Q19_filter_logical
if args.weld:
    Q19_filter = Q19_filter_weld

def Q19_pysparksql_lfilter(x):
    return (
        x["l_shipmode"] in ["AIR", "AIR REG"]
        and x["l_shipinstruct"] == "DELIVER IN PERSON")


def Q19_optimal_lfilter_strings(x):
    return (
        x["l_shipmode"] in ["AIR", "AIR REG"]
        and x["l_shipinstruct"] == "DELIVER IN PERSON"
        and 1 <= x["l_quantity"] <= 20 + 10)

def Q19_optimal_lfilter_weld(x):
    return (
        x["l_shipmode"] in [3, 7]
        and x["l_shipinstruct"] == 0
        and 1 <= x["l_quantity"] <= 20 + 10)

Q19_optimal_lfilter = Q19_optimal_lfilter_weld if args.weld else Q19_optimal_lfilter_strings

def Q19_pysparksql_pfilter(x):
    return 1 <= x["p_size"]


def Q19_optimal_pfilter_strings(x):
    return 1 <= x["p_size"] <= 15 and x["p_container"] in [
        "SM CASE",
        "SM BOX",
        "SM PACK",
        "SM PKG",
        "MED BAG",
        "MED BOX",
        "MED PKG",
        "MED PACK",
        "LG CASE",
        "LG BOX",
        "LG PACK",
        "LG PKG"]

def Q19_optimal_pfilter_weld(x):
    return 1 <= x["p_size"] <= 15 and x["p_container"] in [18, 31, 25, 4, 5, 38, 19, 13, 1, 14, 29, 21]

Q19_optimal_pfilter = Q19_optimal_pfilter_weld if args.weld else Q19_optimal_pfilter_strings

def Q19_reduced_filter_strings(x):
    return (
        (
            x["p_brand"] == "Brand#12"
            and x["p_container"] in ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]
            and 1 <= x["l_quantity"] <= 1 + 10
            and 1 <= x["p_size"] <= 5)
        or (
            x["p_brand"] == "Brand#23"
            and x["p_container"] in ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]
            and 10 <= x["l_quantity"] <= 10 + 10
            and 1 <= x["p_size"] <= 10
        )
        or (
            x["p_brand"] == "Brand#34"
            and x["p_container"] in ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]
            and 20 <= x["l_quantity"] <= 20 + 10
            and 1 <= x["p_size"] <= 15))

def Q19_reduced_filter_weld(x):
    return (
        (
            x["p_brand"] == 12
            and x["p_container"] in [18, 31, 25, 4]
            and 1 <= x["l_quantity"] <= 1 + 10
            and 1 <= x["p_size"] <= 5)
        or (
            x["p_brand"] == 23
            and x["p_container"] in [5, 38, 19, 13]
            and 10 <= x["l_quantity"] <= 10 + 10
            and 1 <= x["p_size"] <= 10
        )
        or (
            x["p_brand"] == 34
            and x["p_container"] in [1, 14, 29, 21]
            and 20 <= x["l_quantity"] <= 20 + 10
            and 1 <= x["p_size"] <= 15))

Q19_reduced_filter = Q19_reduced_filter_weld if args.weld else Q19_reduced_filter_strings


load_time = 0
query_time = 0
res = 0.0
if args.cache:
    # cache
    tstart = time.time()
    c = tuplex.Context(conf=conf)
    lineitem_ds = c.csv(
        args.lineitem_path, delimiter="|", columns=lineitem_columns
    ).cache()
    part_ds = c.csv(args.part_path, delimiter="|", columns=part_columns).cache()
    load_time = time.time() - tstart

    # run query
    if args.mode == "tuplex":
        tstart = time.time()
        res = (
            part_ds.join(lineitem_ds, "p_partkey", "l_partkey")
            .filter(Q19_filter)
            .aggregate(
                lambda a, b: a + b,
                lambda a, x: a + x["l_extendedprice"] * (1 - x["l_discount"]),
                0.0,
            )
            .collect()
        )
        query_time = time.time() - tstart
    elif args.mode == "pysparksql":
        tstart = time.time()
        lineitem_ds = lineitem_ds.filter(Q19_pysparksql_lfilter)
        part_ds = part_ds.filter(Q19_pysparksql_pfilter)
        res = (
            part_ds.join(lineitem_ds, "p_partkey", "l_partkey")
            .filter(Q19_reduced_filter)
            .aggregate(
                lambda a, b: a + b,
                lambda a, x: a + x["l_extendedprice"] * (1 - x["l_discount"]),
                0.0,
            )
            .collect()
        )
        query_time = time.time() - tstart
    else:  # optimal
        tstart = time.time()
        lineitem_ds = lineitem_ds.filter(Q19_optimal_lfilter)
        part_ds = part_ds.filter(Q19_optimal_pfilter)
        res = (
            part_ds.join(lineitem_ds, "p_partkey", "l_partkey")
            .filter(Q19_reduced_filter)
            .aggregate(
                lambda a, b: a + b,
                lambda a, x: a + x["l_extendedprice"] * (1 - x["l_discount"]),
                0.0,
            )
            .collect()
        )
        query_time = time.time() - tstart
else:
    # end-to-end
    if args.mode == "tuplex":
        tstart = time.time()

        c = tuplex.Context(conf=conf)
        lineitem_ds = c.csv(args.lineitem_path, delimiter="|", columns=lineitem_columns)
        res = (
            c.csv(args.part_path, delimiter="|", columns=part_columns)
            .join(lineitem_ds, "p_partkey", "l_partkey")
            .filter(Q19_filter)
            .aggregate(
                lambda a, b: a + b,
                lambda a, x: a + x["l_extendedprice"] * (1 - x["l_discount"]),
                0.0,
            )
            .collect()
        )
        query_time = time.time() - tstart
    elif args.mode == "pysparksql":
        tstart = time.time()

        c = tuplex.Context(conf=conf)
        lineitem_ds = c.csv(
            args.lineitem_path, delimiter="|", columns=lineitem_columns
        ).filter(Q19_pysparksql_lfilter)
        res = (
            c.csv(args.part_path, delimiter="|", columns=part_columns)
            .filter(Q19_pysparksql_pfilter)
            .join(lineitem_ds, "p_partkey", "l_partkey")
            .filter(Q19_reduced_filter)
            .aggregate(
                lambda a, b: a + b,
                lambda a, x: a + x["l_extendedprice"] * (1 - x["l_discount"]),
                0.0,
            )
            .collect()
        )
        query_time = time.time() - tstart
    else:  # optimal
        tstart = time.time()

        c = tuplex.Context(conf=conf)
        lineitem_ds = c.csv(
            args.lineitem_path, delimiter="|", columns=lineitem_columns
        ).filter(Q19_optimal_lfilter)
        res = (
            c.csv(args.part_path, delimiter="|", columns=part_columns)
            .filter(Q19_optimal_pfilter)
            .join(lineitem_ds, "p_partkey", "l_partkey")
            .filter(Q19_reduced_filter)
            .aggregate(
                lambda a, b: a + b,
                lambda a, x: a + x["l_extendedprice"] * (1 - x["l_discount"]),
                0.0,
            )
            .collect()
        )
        query_time = time.time() - tstart

    print("end-to-end took: {}s".format(query_time))
print("Result::\n{}".format(res))
print(
    "framework,version,load,query,result\n{},{},{},{},{}".format(
        "tuplex", "0.1.6", load_time, query_time, str(res)
    )
)
