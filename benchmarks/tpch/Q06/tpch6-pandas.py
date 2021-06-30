import pandas as pd
import numpy as np
import time
import datetime
import argparse


parser = argparse.ArgumentParser(description="TPC-H query using Hyper API")
parser.add_argument(
    "--path",
    type=str,
    dest="data_path",
    default="data/lineitem.tbl",
    help="path to lineitem.tbl",
)

args = parser.parse_args()

#@TODO: Column selection pushdown, only use what's required

load_time = -1
query_time = -1

tstart = time.time()
columns = ['l_orderkey', 'l_partkey', 'l_suppkey',
    'l_linenumber', 'l_quantity', 'l_extendedprice',
    'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus',
    'l_shipdate', 'l_commitdate', 'l_receiptdate',
    'l_shipinstruct', 'l_shipmode', 'l_comment']

date_cols = ['l_shipdate', 'l_commitdate', 'l_receiptdate']
df = pd.read_csv(args.data_path, delimiter='|', header=None,
                 names=columns, parse_dates=date_cols,
                 dtype={'l_discount': np.float})
load_time = time.time()-tstart
print('read took: {}s'.format(load_time))
tstart = time.time()

# need to compute numbers directly in expressions b.c. of bugs in pandas...
# low = 0.06 - 0.01
# high = 0.06 + 0.01
# based on TPC-H 6
# SELECT
#     sum(l_extendedprice * l_discount) as revenue
# FROM
#     lineitem
# WHERE
#     l_shipdate >= date '1994-01-01'
#     AND l_shipdate < date '1994-01-01' + interval '1' year
#     AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
#     AND l_quantity < 24;
subdf = df[(df['l_shipdate'] >= np.datetime64(datetime.date(1994, 1, 1))) &
           (df['l_shipdate'] < np.datetime64(datetime.date(1994, 1, 1) + datetime.timedelta(days=365))) &
           (0.05 <= df['l_discount']) & (df['l_discount'] <= 0.07) &
           (df['l_quantity'] < 24)]

res = (subdf['l_extendedprice'] * subdf['l_discount']).sum()
print('Result::\n{}'.format(res))
#print(len(subdf))
query_time = time.time() - tstart
print('query took: {}s'.format(query_time))
print('framework,load,query\n{},{},{}'.format('pandas ' + str(pd.__version__), load_time, query_time))