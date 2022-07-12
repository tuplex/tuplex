#!/bin/usr/env python3
# (c) 2017 - 2020 L.Spiegelberg
# following is a simple baseline experiment
import time
import sys
import json
import argparse
import os
import glob

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import csv

def runRDDTuple(path, output_path):

    """
    runs spark using RDDs and tuples as internal data format
    :param path:
    :param output_path:
    :return:
    """
    jstart = time.time()

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
            return x + (int(r),)
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
            if 'condo' in t or 'appartment' in t:
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
            return 100000 < x[5] <= 2e7
        except:
            return False

    def filterType(x):
        x = tuple(x)
        try:
            return x[-1] == 'house'
        except:
            return False

    def filterBd(x):
        x = tuple(x)
        try:
            return x[-1] < 10
        except:
            return False

    tstart = time.time()

    spark = SparkSession.builder \
        .master("local[16]") \
        .config("spark.hadoop.validateOutputSpecs", "false") \
        .appName("read csv") \
        .getOrCreate()

    sc = spark.sparkContext

    tparse_start = time.time()

    # Spark pipeline
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

    tend = time.time()

    sc = spark.sparkContext
    framework = 'Spark {} {}'.format(sc.version, sc.master)

    stats = {'type': 'RDD tuple',
             'framework': framework,
             'run_time': tend - tstart,
             'pipeline_time': tend - tparse_start,
             'startup_time': tparse_start - tstart,
             'job_time': tend - jstart}

    print(stats)


def runRDDDict(path, output_path):

    """
    runs spark using RDDs and dicts as internal data format
    :param path:
    :param output_path:
    :return:
    """
    jstart = time.time()

    def extractZipcode(x):
        try:
            return {'zipcode': '%05d' % int(float(x['postal_code'])), **x}
        except:
            return {'zipcode': None, **x}

    def cleanCity(x):
        try:
            return {**x, 'city': x['city'][0].upper() + x['city'][1:].lower()}
        except:
            return {**x, 'city': None}

    def extractBd(x):
        try:
            val = x['facts and features']
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
            return {**x, 'bedrooms': int(r)}
        except:
            return {**x, 'bedrooms': None}

    def extractBa(x):
        try:
            val = x['facts and features']
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
            return {**x, 'bathrooms': int(r)}
        except:
            return {**x, 'bathrooms': None}

    def extractSqft(x):
        try:
            val = x['facts and features']
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
            return {**x, 'sqft': int(r)}
        except:
            return {**x, 'sqft': None}

    def extractOffer(x):
        try:
            offer = x['title'].lower()

            if 'sale' in offer:
                offer = 'sale'
            if 'rent' in offer:
                offer = 'rent'
            if 'sold' in offer:
                offer = 'sold'
            if 'foreclose' in offer:
                offer = 'foreclosed'

            return {**x, 'offer': offer}
        except:
            return {**x, 'offer': None}

    def extractType(x):
        try:
            t = x['title'].lower()
            type = 'unknown'
            if 'condo' in t or 'apartment' in t:
                type = 'condo'
            if 'house' in t:
                type = 'house'
            return {**x, 'type': type}
        except:
            return {**x, 'type': None}

    def extractPrice(x):
        try:
            price = x['price']

            if x['offer'] == 'sold':
                # price is to be calculated using price/sqft * sqft
                val = x['facts and features']
                s = val[val.find('Price/sqft:') + len('Price/sqft:') + 1:]
                r = s[s.find('$') + 1:s.find(', ') - 1]
                price_per_sqft = int(r)
                price = price_per_sqft * x['sqft']
            elif x['offer'] == 'rent':
                max_idx = price.rfind('/')
                price = int(price[1:max_idx].replace(',', ''))
            else:
                # take price from price column
                price = int(price[1:].replace(',', ''))

            return {**x, 'price': price}
        except:
            return {**x, 'price': None}

    def selectCols(x):
        columns = ['url', 'zipcode', 'address', 'city', 'state', 'bedrooms', 'bathrooms', 'sqft', 'offer', 'type',
                   'price']
        return {key: x[key] for key in x.keys() if key in columns}

    def filterPrice(x):
        try:
            return 100000 < x['price'] <= 2e7
        except:
            return False

    def filterType(x):
        try:
            return x['type'] == 'house'
        except:
            return False

    def filterBd(x):
        try:
            return x['bedrooms'] < 10
        except:
            return False

    tstart = time.time()

    spark = SparkSession.builder \
        .master("local[16]") \
        .config("spark.hadoop.validateOutputSpecs", "false") \
        .appName("read csv") \
        .getOrCreate()

    sc = spark.sparkContext

    tparse_start = time.time()

    # Spark pipeline
    # note the ignore header to have the contents ready
    rdd1 = sc.textFile(path)
    header = rdd1.take(1)[0]
    initialCols = header.split(',')

    # spark has some trouble making correct closure, hence here again columns explicitly
    initialCols = 'title,address,city,state,postal_code,price,facts and features,real estate provider,url,sales_date'.split(
        ',')
    # dict based approach
    rdd2 = rdd1.zipWithIndex().filter(lambda x: x[1] > 0) \
        .map(lambda x: x[0]) \
        .map(lambda t: tuple(csv.reader([t]))[0]) \
        .map(lambda t: dict(zip(initialCols, t)))

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

    def tocsvstr(d):
        # escape strings if necessary
        def escapecsv(s):
            s = s.replace('"', '""')  # quote quote
            if ',' in s or '\n' in s:
                s = '"' + s + '"'  # quote if contains , or newline
            return s

        a = []
        for c in columns:
            a.append(escapecsv(str(d[c])))
        return ','.join(a)

    rddHeader = sc.parallelize([','.join(columns)])

    # experiment is to save to ONE file
    rddHeader.union(rddFinal.map(tocsvstr)).saveAsTextFile(output_path)

    tend = time.time()

    sc = spark.sparkContext
    framework = 'Spark {} {}'.format(sc.version, sc.master)

    stats = {'type': 'RDD dict',
             'framework': framework,
             'run_time': tend - tstart,
             'pipeline_time': tend - tparse_start,
             'startup_time': tparse_start - tstart,
             'job_time': tend - jstart}

    print(stats)


def runDF(path, output_path):

    """
    runs spark using Spark 2.x Dataframe API
    :param path:
    :param output_path:
    :return:
    """

    jstart = time.time()

    def extractZipcode(x):
        try:
            return '%05d' % int(float(x))
        except:
            return None

    def cleanCity(city):
        try:
            return city[0].upper() + city[1:].lower()
        except:
            return None

    def extractBd(val):
        try:
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
            return int(r)
        except:
            return None

    def extractBa(val):
        try:
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
            return int(r)
        except:
            return None

    def extractSqft(val):
        try:
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
            return int(r)
        except:
            return None

    def extractOffer(title):
        try:
            offer = title.lower()

            if 'sale' in offer:
                return 'sale'
            if 'rent' in offer:
                return 'rent'
            if 'sold' in offer:
                return 'sold'
            if 'foreclose' in offer:
                return 'foreclosed'
            return offer
        except:
            return None

    def extractType(title):
        try:
            t = title.lower()
            type = 'unknown'
            if 'condo' in t or 'apartment' in t:
                type = 'condo'
            if 'house' in t:
                type = 'house'
            return type
        except:
            return None

    def extractPrice(price, offer, facts, sqft):
        try:

            if offer == 'sold':
                # price is to be calculated using price/sqft * sqft
                val = facts
                s = val[val.find('Price/sqft:') + len('Price/sqft:') + 1:]
                r = s[s.find('$') + 1:s.find(', ') - 1]
                price_per_sqft = int(r)
                price = price_per_sqft * sqft
            elif offer == 'rent':
                max_idx = price.rfind('/')
                price = int(price[1:max_idx].replace(',', ''))
            else:
                # take price from price column
                price = int(price[1:].replace(',', ''))

            return price
        except:
            return None

    def selectCols(x):
        columns = ['url', 'zipcode', 'address', 'city', 'state', 'bedrooms', 'bathrooms', 'sqft', 'offer', 'type',
                   'price']
        return {key: x[key] for key in x.keys() if key in columns}

    def filterPrice(x):
        try:
            return 100000 < x['price'] <= 2e7
        except:
            return False

    # spark DataFrame udfs
    udfBd = udf(extractBd, IntegerType())
    udfType = udf(extractType, StringType())
    udfZipcode = udf(extractZipcode, StringType())
    udfCity = udf(cleanCity, StringType())
    udfBa = udf(extractBa, IntegerType())
    udfSqft = udf(extractSqft, IntegerType())
    udfOffer = udf(extractOffer, StringType())
    udfPrice = udf(extractPrice, IntegerType())

    def init_spark(app_name, master_config):
        """
        :params app_name: Name of the app
        :params master_config: eg. local[4]
        :returns SparkContext, SQLContext, SparkSession:
        """
        conf = (SparkConf().setAppName(app_name).setMaster(master_config))

        sc = SparkContext(conf=conf)
        #sc.setLogLevel("ERROR")
        sql_ctx = SQLContext(sc)
        spark = SparkSession(sc)

        return (sc, sql_ctx, spark)

    tstart = time.time()
    sc, sql_ctx, spark = init_spark("read csv", "local[16]")

    tparse_start = time.time()

    #df = spark.read.options(header=True, mode='PERMISSIVE', multiLine=True, escape='"') \
    df = spark.read.options(header=True, mode='PERMISSIVE', escape='"') \
        .csv(path) \
        .withColumn('bedrooms', udfBd("facts and features")) \
        .filter("bedrooms  < 10") \
        .withColumn("type", udfType("title")) \
        .filter("type == 'house'") \
        .withColumn("zipcode", udfZipcode("postal_code")) \
        .withColumn("city", udfCity("city")) \
        .withColumn('bathrooms', udfBa("facts and features")) \
        .withColumn('sqft', udfSqft("facts and features")) \
        .withColumn('offer', udfOffer("title")) \
        .withColumn('price', udfPrice("price", "offer", "facts and features", "sqft")) \
        .filter("100000 < price") \
        .filter("price <= 2e7") \
        .select('url', 'zipcode', 'address', 'city', 'state', 'bedrooms', 'bathrooms', 'sqft', 'offer', 'type', 'price')

    # write to file (single partition)
    df.write.csv(output_path, mode='overwrite', header=True)

    tend = time.time()

    framework = 'Spark {} {}'.format(sc.version, sc.master)

    stats = {'type': 'DataFrame',
             'framework': framework,
             'run_time': tend - tstart,
             'pipeline_time': tend - tparse_start,
             'startup_time': tparse_start - tstart,
             'job_time' : tend - jstart}

    print(stats)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Zillow cleaning')
    parser.add_argument('--path', type=str, dest='data_path', default='data/large100MB.csv',
                        help='path or pattern to zillow data')
    parser.add_argument('--mode', type=str, dest='mode', default='tuple',
                        help='specify tuple or dict or sql for internal data representation')
    parser.add_argument('--output-path', type=str, dest='output_path', default='pyspark_output',
                        help='specify path where to save output data files')
    args = parser.parse_args()

    assert args.data_path, 'need to set data path!'

    # config vars
    paths = args.data_path
    if not paths.endswith('.csv'):
        paths = args.data_path + '/*.csv' # because AWS EMR does not allow *
    output_path = args.output_path

    print('>>> running pyspark on {}'.format(paths))
    print('>>> data mode: {}'.format(args.mode))

    # run specific pipeline (either tuples or dicts)
    if 'tuple' in args.mode :
        runRDDTuple(paths, output_path)
    elif 'dict' in args.mode:
        runRDDDict(paths, output_path)
    elif args.mode == 'sql' or args.mode == 'df' or args.mode == 'dataframe':
        runDF(paths, output_path)
    else:
        print('invalid mode {}, abort.'.format(args.mode))
        sys.exit(1)
