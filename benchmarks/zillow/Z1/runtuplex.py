#!/usr/bin/env python3

# Tuplex based cleaning script
import tuplex
import time
import sys
import json
import os
import glob
import argparse

def extractBd(x):
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
    return int(r)

def extractBa(x):
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
    return int(r)

def extractSqft(x):
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
    return int(r)

def extractOffer(x):
    offer = x['title'].lower()
    if 'sale' in offer:
        return 'sale'
    if 'rent' in offer:
        return 'rent'
    if 'sold' in offer:
        return 'sold'
    if 'foreclose' in offer.lower():
        return 'foreclosed'
    return offer

def extractType(x):
    t = x['title'].lower()
    type = 'unknown'
    if 'condo' in t or 'apartment' in t:
        type = 'condo'
    if 'house' in t:
        type = 'house'
    return type

def extractPrice(x):
    price = x['price']
    p = 0
    if x['offer'] == 'sold':
        # price is to be calculated using price/sqft * sqft
        val = x['facts and features']
        s = val[val.find('Price/sqft:') + len('Price/sqft:') + 1:]
        r = s[s.find('$')+1:s.find(', ') - 1]
        price_per_sqft = int(r)
        p = price_per_sqft * x['sqft']
    elif x['offer'] == 'rent':
        max_idx = price.rfind('/')
        p = int(price[1:max_idx].replace(',', ''))
    else:
        # take price from price column
        p = int(price[1:].replace(',', ''))

    return p

# def selectCols(x):
#     columns = ['url', 'zipcode', 'address', 'city', 'state', 'bedrooms', 'bathrooms', 'sqft', 'offer', 'type', 'price']
#     return {key: x[key] for key in x.keys() if key in columns}

def filterPrice(x):
    return 100000 < x['price'] <= 2e7

def filterType(x):
    return x['type'] == 'house'

def filterBd(x):
    return x['bedrooms'] < 10


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Zillow cleaning')
    parser.add_argument('--path', type=str, dest='data_path', default='data/large100MB.csv',
                        help='path or pattern to zillow data')
    parser.add_argument('--output-path', type=str, dest='output_path', default='tuplex_output/',
                        help='specify path where to save output data files')
    parser.add_argument('--single-threaded', dest='single_threaded', action="store_true",
                        help="whether to use a single thread for execution")
    parser.add_argument('--preload', dest='preload', action="store_true",
                        help="whether to add a cache statement after the csv operator to separate IO costs out.")
    parser.add_argument('--no-nvo', dest='no_nvo', action="store_true",
                        help="deactivate null value optimization explicitly.")
    args = parser.parse_args()

    assert args.data_path, 'need to set data path!'

    # config vars
    paths = [args.data_path]
    output_path = args.output_path

    # explicit globbing because dask can't handle patterns well...
    if not os.path.isfile(args.data_path):
        paths = sorted(glob.glob(os.path.join(args.data_path, '*.csv')))
    else:
        paths = [args.data_path]

    if not paths:
        print('found no zillow data to process, abort.')
        sys.exit(1)

    print('>>> running {} on {}'.format('tuplex', paths))

    # load data
    tstart = time.time()

    # configuration, make sure to give enough runtime memory to the executors!
    conf = {"webui.enable" : False,
            "executorCount" : 16,
            "executorMemory" : "2G",
            "driverMemory" : "2G",
            "partitionSize" : "32MB",
            "runTimeMemory" : "128MB",
            "useLLVMOptimizer" : True,
            "optimizer.nullValueOptimization" : False,
            "csv.selectionPushdown" : True}

    # # conf for r5d.4xlarge (in total: 100GB, same as spark)
    # conf = {"webui.enable" : False,
    #         "executorMemory" : "6G",
    #         "driverMemory" : "10G",
    #         "partitionSize" : "32MB",
    #         "runTimeMemory" : "128MB"}

    if os.path.exists('tuplex_config.json'):
        with open('tuplex_config.json') as fp:
            conf = json.load(fp)

    if args.single_threaded:
        conf['executorCount'] = 0
        conf['driverMemory'] = '32G'
    if args.no_nvo:
        conf["optimizer.nullValueOptimization"] = False

    tstart = time.time()
    import tuplex
    ctx = tuplex.Context(conf)

    startup_time = time.time() - tstart
    print('Tuplex startup time: {}'.format(startup_time))
    tstart = time.time()

    # Tuplex pipeline with optional preloading
    ds = ctx.csv(','.join(paths))
    if args.preload:
        ds = ds.cache()
        print('Caching took {}s'.format(time.time() - tstart))
        tstart = time.time()

    ds.withColumn("bedrooms", extractBd) \
        .filter(lambda x: x['bedrooms'] < 10) \
        .withColumn("type", extractType) \
        .filter(lambda x: x['type'] == 'house') \
        .withColumn("zipcode", lambda x: '%05d' % int(x['postal_code'])) \
        .mapColumn("city", lambda x: x[0].upper() + x[1:].lower()) \
        .withColumn("bathrooms", extractBa) \
        .withColumn("sqft", extractSqft) \
        .withColumn("offer", extractOffer) \
        .withColumn("price", extractPrice) \
        .filter(lambda x: 100000 < x['price'] < 2e7) \
        .selectColumns(["url", "zipcode", "address", "city", "state",
                        "bedrooms", "bathrooms", "sqft", "offer", "type", "price"]) \
        .tocsv(output_path)

    run_time = time.time() - tstart

    job_time = time.time() - tstart
    print('Tuplex job time: {} s'.format(job_time))
    m = ctx.metrics
    print(ctx.options())
    print(m.as_json())
    # print stats as last line
    print(json.dumps({"startupTime" : startup_time, "jobTime" : job_time}))
