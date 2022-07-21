#!/usr/bin/env python3
# (c) L.Spiegelberg 2021
# conduct dirty Zillow data experiment as described in README.md

import time
import sys
import json
import os
import glob
import argparse
import math
import re
import shutil
import subprocess
import random
import csv

try:
    tstart = time.time()
    from pyspark.sql import *
    from pyspark.sql.types import *
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import *
    from pyspark.sql.functions import udf
    from pyspark.sql.types import *
    pyspark_import_time = time.time() - tstart
except:
    raise ModuleNotFoundError('PySpark not installed')

# # UDFs for pipeline
# def extractBd(x):
#     val = x['facts and features']
#     max_idx = val.find(' bd')
#     if max_idx < 0:
#         max_idx = len(val)
#     s = val[:max_idx]
#
#     # find comma before
#     split_idx = s.rfind(',')
#     if split_idx < 0:
#         split_idx = 0
#     else:
#         split_idx += 2
#     r = s[split_idx:]
#     return int(r)
#
# def extractBa(x):
#     val = x['facts and features']
#     max_idx = val.find(' ba')
#     if max_idx < 0:
#         max_idx = len(val)
#     s = val[:max_idx]
#
#     # find comma before
#     split_idx = s.rfind(',')
#     if split_idx < 0:
#         split_idx = 0
#     else:
#         split_idx += 2
#     r = s[split_idx:]
#     ba = math.ceil(2.0 * float(r)) / 2.0
#     return ba
#
# def extractSqft(x):
#     val = x['facts and features']
#     max_idx = val.find(' sqft')
#     if max_idx < 0:
#         max_idx = len(val)
#     s = val[:max_idx]
#
#     split_idx = s.rfind('ba ,')
#     if split_idx < 0:
#         split_idx = 0
#     else:
#         split_idx += 5
#     r = s[split_idx:]
#     r = r.replace(',', '')
#     return int(r)
#
# def extractOffer(x):
#     offer = x['title'].lower()
#     if 'sale' in offer:
#         return 'sale'
#     if 'rent' in offer:
#         return 'rent'
#     if 'sold' in offer:
#         return 'sold'
#     if 'foreclose' in offer.lower():
#         return 'foreclosed'
#     return offer
#
# def extractType(x):
#     t = x['title'].lower()
#     type = 'unknown'
#     if 'condo' in t or 'apartment' in t:
#         type = 'condo'
#     if 'house' in t:
#         type = 'house'
#     return type
#
# def extractPrice(x):
#     price = x['price']
#     p = 0
#     if x['offer'] == 'sold':
#         # price is to be calculated using price/sqft * sqft
#         val = x['facts and features']
#         s = val[val.find('Price/sqft:') + len('Price/sqft:') + 1:]
#         r = s[s.find('$')+1:s.find(', ') - 1]
#         price_per_sqft = int(r)
#         p = price_per_sqft * x['sqft']
#     elif x['offer'] == 'rent':
#         max_idx = price.rfind('/')
#         p = int(price[1:max_idx].replace(',', ''))
#     else:
#         # take price from price column
#         p = int(price[1:].replace(',', ''))
#
#     return p
#
# def resolveBd(x):
#     if 'Studio' in x['facts and features']:
#         return 1
#     raise ValueError

# #compare types and contents
# def dirty_zillow_pipeline(ctx, path, output_path, step, commit):
#
#     # Increases write times to highlight differences
#
#     # ds = ctx.csv(path)
#     #
#     # ds = ds.withColumn('bedrooms', extractBd)
#     # if step > 0:
#     #     ds = ds.resolve(ValueError, resolveBd)
#     # if step > 1:
#     #     ds = ds.ignore(ValueError)
#     #
#     # ds = ds.withColumn('bathrooms', extractBa)
#     # if step > 2:
#     #     ds = ds.resolve(ValueError, resolveBa)
#     # if step > 3:
#     #     ds = ds.ignore(ValueError)
#     #
#     # ds = ds.withColumn('sqft', extractSqft)
#     # if step > 3:
#     #     ds = ds.ignore(ValueError)
#     #
#     # ds = ds.withColumn('offer', extractOffer)
#     # ds = ds.withColumn('price', extractPrice)
#     # if step > 4:
#     #     ds = ds.resolve(ValueError, lambda x: int(re.sub('[^0-9.]*', '', x['price'])))
#     # if step > 5:
#     #     ds = ds.ignore(TypeError)
#     #     ds = ds.ignore(ValueError)
#     # ds = ds.selectColumns(["address", "bedrooms", "bathrooms", "sqft", "price"])
#
# # Original pipeline, most realistic, taken from previous paper to run benchmark on
#
#     # ds = ds.withColumn("bedrooms", extractBd)
#     # if step > 0:
#     #     ds = ds.resolve(ValueError, resolveBd)
#     # if step > 1:
#     #     ds = ds.ignore(ValueError)
#     # ds = ds.withColumn("type", extractType)
#     # ds = ds.withColumn("zipcode", lambda x: '%05d' % int(x['postal_code']))
#     # if step > 2:
#     #     ds = ds.ignore(TypeError)
#     # ds = ds.mapColumn("city", lambda x: x[0].upper() + x[1:].lower())
#     # ds = ds.withColumn("bathrooms", extractBa)
#     # if step > 3:
#     #     ds = ds.ignore(ValueError)
#     # ds = ds.withColumn("sqft", extractSqft)
#     # if step > 4:
#     #     ds = ds.ignore(ValueError)
#     # ds = ds.withColumn("offer", extractOffer)
#     # ds = ds.withColumn("price", extractPrice)
#     # if step > 5:
#     #     ds = ds.resolve(ValueError, lambda x: int(re.sub('[^0-9.]*', '', x['price'])))
#     # ds = ds.filter(lambda x: 100000 < x['price'] < 2e7 and x['offer'] == 'sale')
#     # ds = ds.selectColumns(["url", "zipcode", "address", "city", "state",
#     #                         "bedrooms", "bathrooms", "sqft", "offer", "type", "price"])
#     # ds.tocsv(output_path, commit=commit)
#
#     ds = ctx.csv(path)
#     ds = ds.withColumn('bedrooms', extractBd)
#     if step > 0:
#         ds = ds.resolve(ValueError, resolveBd)
#     if step > 1:
#         ds = ds.ignore(ValueError)
#     ds = ds.filter(lambda x: x['bedrooms'] < 10)
#     ds = ds.withColumn('type', extractType)
#     ds = ds.filter(lambda x: x['type'] == 'condo')
#     ds = ds.withColumn('zipcode', lambda x: '%05d' % int(x['postal_code']))
#     if step > 2:
#         ds = ds.ignore(TypeError)
#     ds = ds.mapColumn("city", lambda x: x[0].upper() + x[1:].lower())
#     ds = ds.withColumn("bathrooms", extractBa)
#     if step > 3:
#         ds = ds.ignore(ValueError)
#     ds = ds.withColumn('sqft', extractSqft)
#     if step > 4:
#         ds = ds.ignore(ValueError)
#     ds = ds.withColumn('offer', extractOffer)
#     ds = ds.withColumn('price', extractPrice)
#     if step > 5:
#         ds = ds.resolve(ValueError, lambda x: int(re.sub('[^0-9.]*', '', x['price'])))
#     ds = ds.filter(lambda x: 100000 < x['price'] < 2e7 and x['offer'] == 'sale')
#     ds = ds.selectColumns(["url", "zipcode", "address", "city", "state",
#                            "bedrooms", "bathrooms", "sqft", "offer", "type", "price"])
#     ds.tocsv(output_path, commit=commit)
#     return ctx.metrics

def spark_rdd_pipeline(sc):
    # Spark pipeline

    def extractZipcode(x):
        x = tuple(x)
        try:
            return x + ('{:05}'.format(int(float(x[4]))),)
        except:
            return x + (None,)

    def cleanCity(x):
        x = tuple(x)
        try:
            return x[:2] + (x[2][0].upper() + x[2][1:].lower(),) + x[3:]
        except:
            return x[:2] + (None,) + x[3:]

    def extractBd(x):
        x = tuple(x)
        try:
            val = x[6]
            max_idx = val.find(' bd')
            if max_idx < 0:
                max_idx = len(val)
            s = val[:max_idx]

            # find comma before
            split_idx = s.rfind(',')
            if split_idx < 0:
                split_idx = 0
            else:
                split_idx += 2
            r = s[split_idx:]
            return x + (int(r),)
        except:
            return x + (None,)

    def extractBa(x):
        x = tuple(x)
        try:
            val = x[6]
            max_idx = val.find(' ba')
            if max_idx < 0:
                max_idx = len(val)
            s = val[:max_idx]

            # find comma before
            split_idx = s.rfind(',')
            if split_idx < 0:
                split_idx = 0
            else:
                split_idx += 2
            r = s[split_idx:]
            ba = math.ceil(2.0 * float(r)) / 2.0
            return x + (ba,)
        except:
            return x + (None,)

    def extractSqft(x):
        x = tuple(x)
        try:
            val = x[6]
            max_idx = val.find(' sqft')
            if max_idx < 0:
                max_idx = len(val)
            s = val[:max_idx]

            split_idx = s.rfind('ba ,')
            if split_idx < 0:
                split_idx = 0
            else:
                split_idx += 5
            r = s[split_idx:]
            r = r.replace(',', '')
            return x + (int(r),)
        except:
            return x + (None,)

    def extractOffer(x):
        x = tuple(x)
        try:
            offer = x[0].lower()
            if 'sale' in offer:
                return x + ('sale',)
            if 'rent' in offer:
                return x + ('rent',)
            if 'sold' in offer:
                return x + ('sold',)
            if 'foreclose' in offer.lower():
                return x + ('foreclosed',)
            return x + (offer,)
        except:
            return x + (None,)

    def extractType(x):
        x = tuple(x)
        try:
            t = x[0].lower()
            type = 'unknown'
            if 'condo' in t or 'apartment' in t:
                type = 'condo'
            if 'house' in t:
                type = 'house'
            return x + (type,)
        except:
            return x + (None,)

    def extractPrice(x):
        x = tuple(x)
        try:
            price = x[5]
            if x[15] == 'sold':
                # price is to be calculated using price/sqft * sqft
                val = x[6]
                s = val[val.find('Price/sqft:') + len('Price/sqft:') + 1:]
                r = s[s.find('$')+1:s.find(', ') - 1]
                price_per_sqft = int(r)
                price = price_per_sqft * x['sqft']
            elif x[15] == 'rent':
                max_idx = price.rfind('/')
                price = int(price[1:max_idx].replace(',', ''))
            else:
                # take price from price column
                price = int(price[1:].replace(',', ''))

            return x[:5] + (price,) + x[6:]
        except:
            return x[:5] + (None,) + x[6:]

    def selectCols(x):
        x = tuple(x)
        return (x[8],x[12],x[1],x[2],x[3],x[10],x[13],x[14],x[15],x[11],x[5],)

    def filterPrice(x):
        x = tuple(x)
        try:
            # in original 6th column is price
            return 100000 < x[5] <= 2e7 and x[15] == 'sale'
        except:
            return False

    def filterType(x):
        x = tuple(x)
        try:
            return x[-1] == 'condo'
        except:
            return False

    def filterBd(x):
        x = tuple(x)
        try:
            return x[-1] < 10
        except:
            return False


    # note the ignore header to have the contents ready
    rdd1 = sc.textFile(path)
    header = rdd1.first()

    # tuple based approach
    rdd2 = rdd1.zipWithIndex().filter(lambda x: x[1] > 0) \
        .map(lambda x: x[0]) \
        .map(lambda t: tuple(csv.reader([t]))[0])

    # pipeline will be done here
    rddFinal = rdd2.map(extractBd) \
        .filter(filterBd) \
        .map(extractType) \
        .filter(filterType) \
        .map(extractZipcode) \
        .map(cleanCity) \
        .map(extractBa) \
        .map(extractSqft) \
        .map(extractOffer) \
        .map(extractPrice) \
        .filter(filterPrice) \
        .map(selectCols)

    # write to text file
    columns = ['url', 'zipcode', 'address', 'city', 'state', 'bedrooms', 'bathrooms', 'sqft', 'offer', 'type', 'price']

    def tocsvstr(a):
        # escape strings if necessary
        def escapecsv(s):
            s = s.replace('"', '""')  # quote quote
            if ',' in s or '\n' in s:
                s = '"' + s + '"'  # quote if contains , or newline
            return s

        a = [escapecsv(str(x)) for x in a]
        return ','.join(a)

    rddHeader = sc.parallelize([tuple(columns)])

    # experiment is to save to ONE file
    rddHeader.union(rddFinal).map(tocsvstr).saveAsTextFile(output_path)

if __name__ == '__main__':
    user = os.getlogin()
    parser = argparse.ArgumentParser(description='Incremental resolution (Pyspark modes)')
    #parser.add_argument('--mode')
    parser.add_argument('--path', type=str, dest='data_path', default='/hot/scratch/bgivertz/data/zillow_dirty.csv', help='path or pattern to zillow data')
    parser.add_argument('--output-path', type=str, dest='output_path', default='/hot/scratch/{}/output/'.format(user), help='specify path where to save output data files')
    parser.add_argument('--resolve-in-order', dest='resolve_in_order', action="store_true", help="whether to resolve exceptions in order")
    parser.add_argument('--num-steps', dest='num_steps', type=int, default=7)
    parser.add_argument('--clear-cache', dest='clear_cache', action='store_true', help='whether to clear the cache or not')
    args = parser.parse_args()

    assert args.data_path, 'need to set data path!'

    # config vars
    path = args.data_path
    output_path = args.output_path

    if not path:
        print('found no zillow data to process, abort.')
        sys.exit(1)

    print('>>> running {} on {}'.format('tuplex', path))

    # load data
    tstart = time.time()

    # configuration, make sure to give enough runtime memory to the executors!
    conf = {"webui.enable" : False,
            "executorCount" : 16,
            "executorMemory" : "12G",
            "driverMemory" : "12G",
            "partitionSize" : "32MB",
            "runTimeMemory" : "128MB",
            "useLLVMOptimizer" : True,
            "optimizer.nullValueOptimization" : False,
            "csv.selectionPushdown" : True,
            "optimizer.generateParser" : False} # bug when using generated parser. Need to fix that.

    if os.path.exists('tuplex_config.json'):
        with open('tuplex_config.json') as fp:
            conf = json.load(fp)

    # create spark context with similar config like tuplex ctx

    tstart = time.time()
    spark = SparkSession.builder \
        .master("local[{}]".format(conf['executorCount'])) \
        .config("spark.hadoop.validateOutputSpecs", "false") \
        .config('spark.executor.memory', conf['executorMemory']) \
        .config('spark.driver.memory', conf['driverMemory']) \
        .appName("zillow dirty") \
        .getOrCreate()

    sc = spark.sparkContext
    tstart = time.time()
    spark_rdd_pipeline(sc)
    logging.info('completed pipeline in {}s'.format(time.time() - tstart))

    shutil.rmtree(output_path, ignore_errors=True)

    if args.clear_cache:
        subprocess.run(["clearcache"])

    # tstart = time.time()
    # # decide which pipeline to run based on argparse arg!
    # num_steps = args.num_steps
    # metrics = []
    # for step in range(num_steps):
    #     print(f'>>> running pipeline with {step} resolver(s) enabled...')
    #     jobstart = time.time()
    #     m = dirty_zillow_pipeline(ctx, path, output_path, step, not args.commit_mode or step == num_steps - 1)
    #     m = m.as_dict()
    #     m["numResolvers"] = step
    #     m["jobTime"] = time.time() - jobstart
    #     metrics.append(m)
    #
    # runtime = time.time() - tstart
    #
    # print("EXPERIMENTAL RESULTS")
    # print(json.dumps({"startupTime": pyspark_import_time,
    #                   "totalRunTime": runtime,
    #                   "mergeExceptionsInOrder": conf["optimizer.mergeExceptionsInOrder"],
    #                   "incrementalResolution": conf["optimizer.incrementalResolution"]}))
    #
    # for metric in metrics:
    #     print(json.dumps(metric))
