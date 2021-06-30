#!/usr/bin/env python3
# (c) L.Spiegelberg
# performs Q6 of TPC-H (as generated via dbgen)

import time
import argparse
import tuplex
import os
import json

parser = argparse.ArgumentParser(description="TPC-H query using Tuplex")
parser.add_argument(
    "--path",
    type=str,
    dest="data_path",
    default="data/lineitem.tbl",
    help="path to lineitem.tbl",
)
parser.add_argument("--cache", action="store_true",
                    help="use this flag to measure separate load/compute times via .cache")
parser.add_argument("--preprocessed", action="store_true",
                    help="whether the input file is quantity,extended_price,discount,shipdate with shipdate being an integer")
args = parser.parse_args()

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

listitem_columns = ['l_orderkey', 'l_partkey', 'l_suppkey',
                    'l_linenumber', 'l_quantity', 'l_extendedprice',
                    'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus',
                    'l_shipdate', 'l_commitdate', 'l_receiptdate',
                    'l_shipinstruct', 'l_shipmode', 'l_comment']

if args.preprocessed:
    listitem_columns = ['l_quantity', 'l_extended_price', 'l_discount', 'l_shipdate']

load_time = 0
query_time = 0
res = 0.0
if args.cache:
    if args.preprocessed:
        # cache first
        tstart = time.time()
        c = tuplex.Context(conf=conf)
        ds = c.csv(args.data_path, delimiter='|', columns=listitem_columns, type_hints={0:int,1:float,2:float,3:int}) \
            .cache()
        load_time = time.time() - tstart

        # now run query (debatable whether this is right)
        tstart = time.time()
        res = ds.filter(lambda x: 19940101 <= x['l_shipdate'] < 19950101) \
            .filter(lambda x: 0.05 <= x['l_discount'] <= 0.07) \
            .filter(lambda x: x['l_quantity'] < 24) \
            .aggregate(lambda a, b: a + b, lambda a, x: a + x[1] * x[2], 0.0) \
            .collect()
        query_time = time.time() - tstart
    else:
        # cache first
        tstart = time.time()
        c = tuplex.Context(conf=conf)
        ds = c.csv(args.data_path, delimiter='|', columns=listitem_columns) \
            .mapColumn("l_shipdate", lambda x: int(x.replace('-', ''))) \
            .cache()
        load_time = time.time() - tstart

        # now run query (debatable whether this is right)
        tstart = time.time()
        res = ds.filter(lambda x: 19940101 <= x['l_shipdate'] < 19940101 + 10000) \
            .filter(lambda x: 0.06 - 0.01 <= x['l_discount'] <= 0.06 + 0.01) \
            .filter(lambda x: x['l_quantity'] < 24) \
            .aggregate(lambda a, b: a + b, lambda a, x: a + x[5] * x[6], 0.0) \
            .collect()
        query_time = time.time() - tstart
else:
    # 1. measure end-to-end time
    tstart = time.time()

    c = tuplex.Context(conf=conf)
    if args.preprocessed:
        res = c.csv(args.data_path, delimiter='|', columns=listitem_columns) \
            .filter(lambda x: 19940101 <= x['l_shipdate'] < 19950101) \
            .filter(lambda x: 0.05 <= x['l_discount'] <= 0.07) \
            .filter(lambda x: x['l_quantity'] < 24) \
            .aggregate(lambda a, b: a + b, lambda a, x: a + x[1] * x[2], 0.0) \
            .collect()
    else:
        res = c.csv(args.data_path, delimiter='|', columns=listitem_columns) \
            .mapColumn("l_shipdate", lambda x: int(x.replace('-', ''))) \
            .filter(lambda x: 19940101 <= x['l_shipdate'] < 19940101 + 10000) \
            .filter(lambda x: 0.06 - 0.01 <= x['l_discount'] <= 0.06 + 0.01) \
            .filter(lambda x: x['l_quantity'] < 24) \
            .aggregate(lambda a, b: a + b, lambda a, x: a + x[5] * x[6], 0.0) \
            .collect()
    query_time = time.time() - tstart

    print('end-to-end took: {}s'.format(query_time))
print('Result::\n{}'.format(res))
print('framework,version,load,query,result\n{},{},{},{},{}'.format('tuplex', '0.1.6',load_time, query_time, str(res)))
