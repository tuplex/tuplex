#!/usr/bin/env python3
# (c) L.Spiegelberg 2021
# conduct dirty Zillow data experiment as described in README.md

import tuplex
import time
import sys
import json
import os
import glob
import argparse
import math
import re

def dirty_zillow_pipeline(paths, output_path):

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

        ba = math.ceil(2.0 * float(r)) / 2.0
        return ba

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


    # Tuplex pipeline
    ctx.csv(','.join(paths)) \
        .withColumn("bedrooms", extractBd) \
        .filter(lambda x: x['bedrooms'] < 10) \
        .withColumn("type", extractType) \
        .filter(lambda x: x['type'] == 'condo') \
        .withColumn("zipcode", lambda x: '%05d' % int(x['postal_code'])) \
        .mapColumn("city", lambda x: x[0].upper() + x[1:].lower()) \
        .withColumn("bathrooms", extractBa) \
        .withColumn("sqft", extractSqft) \
        .withColumn("offer", extractOffer) \
        .withColumn("price", extractPrice) \
        .filter(lambda x: 100000 < x['price'] < 2e7 and x['offer'] == 'sale') \
        .selectColumns(["url", "zipcode", "address", "city", "state",
                        "bedrooms", "bathrooms", "sqft", "offer", "type", "price"]) \
        .tocsv(output_path)

    return ctx.metrics


def dirty_zillow_pipeline_with_resolvers(paths, output_path):

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
        ba = math.ceil(2.0 * float(r)) / 2.0
        return ba

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

    def resolveBd(x):
        if 'Studio' in x['facts and features']:
            return 1
        raise ValueError

    def resolveBa(x):
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
        return math.ceil(float(r))

    # Tuplex pipeline
    ctx.csv(','.join(paths)) \
        .withColumn("bedrooms", extractBd) \
        .resolve(ValueError, resolveBd) \
        .ignore(ValueError) \
        .filter(lambda x: x['bedrooms'] < 10) \
        .withColumn("type", extractType) \
        .filter(lambda x: x['type'] == 'condo') \
        .withColumn("zipcode", lambda x: '%05d' % int(x['postal_code'])) \
        .ignore(TypeError) \
        .mapColumn("city", lambda x: x[0].upper() + x[1:].lower()) \
        .withColumn("bathrooms", extractBa) \
        .ignore(ValueError) \
        .withColumn("sqft", extractSqft) \
        .ignore(ValueError) \
        .withColumn("offer", extractOffer) \
        .withColumn("price", extractPrice) \
        .resolve(ValueError, lambda x: int(re.sub('[^0-9.]*', '', x['price']))) \
        .filter(lambda x: 100000 < x['price'] < 2e7 and x['offer'] == 'sale') \
        .selectColumns(["url", "zipcode", "address", "city", "state",
                        "bedrooms", "bathrooms", "sqft", "offer", "type", "price"]) \
        .tocsv(output_path)

    return ctx.metrics

# (3) logic is built into pipeline to handle all edge cases directly.
def dirty_zillow_pipeline_full_logic(paths, output_path):

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

        # regex check whether we can cast
        m = re.search(r'^\d+$', r)
        if m:
            return int(r)
        elif 'studio' in val.lower():
            return 1
        else:
            return None

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

        # check if valid float string by using regex matching
        m = re.search(r'^\d+.?\d*$', r)
        if m:
            ba = math.ceil(2.0 * float(r)) / 2.0
            return ba
        else:
            return None

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

        m = re.search(r'^\d+$', r)
        if m:
            return int(r)
        else:
            return None

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
            price_str = price[1:max_idx]
            # Note: could replace this with full regex, but that's slower than just using direct string manipulation
            #       logic
            if price_str.endswith('+'):
                p = int(re.sub('[^0-9.]*', '', price_str))
            else:
                p = int(price_str.replace(',', ''))
        else:
            # take price from price column
            p = int(price[1:].replace(',', ''))

        return p

    def extractZipcode(x):
        return '%05d' % int(x['postal_code'])

    # Tuplex pipeline
    ctx.csv(','.join(paths)) \
        .withColumn("bedrooms", extractBd) \
        .filter(lambda x: x['bedrooms'] != None and x['bedrooms'] < 10) \
        .withColumn("type", extractType) \
        .filter(lambda x: x['type'] == 'condo') \
        .filter(lambda x: x['postal_code'] != None) \
        .withColumn("zipcode", extractZipcode) \
        .mapColumn('zipcode', lambda x: str(x)) \
        .mapColumn("city", lambda x: x[0].upper() + x[1:].lower()) \
        .withColumn("bathrooms", extractBa) \
        .filter(lambda x: x['bathrooms'] != None) \
        .mapColumn('bathrooms', lambda x: float(x)) \
        .withColumn("sqft", extractSqft) \
        .filter(lambda x: x['sqft'] != None) \
        .mapColumn('sqft', lambda x: int(x)) \
        .withColumn("offer", extractOffer) \
        .withColumn("price", extractPrice) \
        .filter(lambda x: 100000 < x['price'] < 2e7 and x['offer'] == 'sale') \
        .selectColumns(["url", "zipcode", "address", "city", "state",
                        "bedrooms", "bathrooms", "sqft", "offer", "type", "price"]) \
        .tocsv(output_path)
    # ctx.csv(','.join(paths)) \
    #     .withColumn("bedrooms", extractBd) \
    #     .filter(lambda x: x['bedrooms'] < 10) \
    #     .withColumn("type", extractType) \
    #     .filter(lambda x: x['type'] == 'condo') \
    #     .filter(lambda x: x['postal_code'] != None) \
    #     .withColumn("zipcode", extractZipcode) \
    #     .mapColumn('zipcode', lambda x: str(x)) \
    #     .mapColumn("city", lambda x: x[0].upper() + x[1:].lower()) \
    #     .withColumn("bathrooms", extractBa) \
    #     .filter(lambda x: x['bathrooms'] != None) \
    #     .mapColumn('bathrooms', lambda x: float(x)) \
    #     .withColumn("sqft", extractSqft) \
    #     .filter(lambda x: x['sqft'] != None) \
    #     .mapColumn('sqft', lambda x: int(x)) \
    #     .withColumn("offer", extractOffer) \
    #     .withColumn("price", extractPrice) \
    #     .filter(lambda x: 100000 < x['price'] < 2e7 and x['offer'] == 'sale') \
    #     .selectColumns(["url", "zipcode", "address", "city", "state",
    #                     "bedrooms", "bathrooms", "sqft", "offer", "type", "price"]) \
    #     .tocsv(output_path)

    return ctx.metrics

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Zillow cleaning')
    parser.add_argument('--path', type=str, dest='data_path', default='data/zillow_dirty.csv',
                        help='path or pattern to zillow data')
    parser.add_argument('--output-path', type=str, dest='output_path', default='tuplex_output/',
                        help='specify path where to save output data files')
    parser.add_argument('--resolve-in-order', dest='resolve_in_order', action="store_true", help="whether to resolve exceptions in order")
    parser.add_argument('-m', '--mode', choices=["plain", "resolve", "custom"],
                                             default="plain", help='whether to run pipeline without resolvers, with Tuplex resolvers, or with custom logic to deal with bad data.')
    parser.add_argument('--single-threaded', dest='single_threaded', action="store_true", help="whether to use a single thread for execution")
    parser.add_argument('--resolve-with-interpreter', dest='resolve_with_interpreter', action='store_true', help='whether to use interpreter only')
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
            "executorMemory" : "6G",
            "driverMemory" : "6G",
            "partitionSize" : "32MB",
            "runTimeMemory" : "128MB",
            "useLLVMOptimizer" : True,
            "optimizer.nullValueOptimization" : False,
            "csv.selectionPushdown" : True,
            "optimizer.generateParser" : False} # bug when using generated parser. Need to fix that.

    if os.path.exists('tuplex_config.json'):
        with open('tuplex_config.json') as fp:
            conf = json.load(fp)

    # override with custom defined order argument.
    if args.resolve_in_order:
        conf['optimizer.mergeExceptionsInOrder'] = True
    else:
        conf['optimizer.mergeExceptionsInOrder'] = False

    if args.single_threaded:
        conf['executorCount'] = 0
        conf['driverMemory'] = '32G'
    if args.resolve_with_interpreter:
        conf['resolveWithInterpreterOnly'] = True
    else:
        conf['resolveWithInterpreterOnly'] = False


    # Note: there's a bug in the merge in order mode here -.-
    # force to false version
    print('>>> bug in generated parser, force to false')
    conf["optimizer.generateParser"]  = False

    tstart = time.time()
    import tuplex
    ctx = tuplex.Context(conf)

    startup_time = time.time() - tstart
    print('Tuplex startup time: {}'.format(startup_time))
    tstart = time.time()

    # decide which pipeline to run based on argparse arg!
    m = None
    if args.mode == 'plain':
        print('>>> running pipeline without any resolvers enabled...')
        # this is the pipeline without resolvers...
        m = dirty_zillow_pipeline(paths, output_path)
    elif args.mode == 'resolve':
        print('>>> running pipeline with Tuplex resolvers...')
        # the pipeline with resolvers
        m = dirty_zillow_pipeline_with_resolvers(paths, output_path)
    elif args.mode == 'custom':
        print('>>> running pipeline with custom resolve logic...')
        # the pipeline with logic builtin customly.
        m = dirty_zillow_pipeline_full_logic(paths, output_path)
    else:
        print('unknown mode ' + str(args.mode))

    run_time = time.time() - tstart
    job_time = time.time() - tstart
    print('Tuplex job time: {} s'.format(job_time))

    # print metrics object as json
    m = ctx.metrics
    print(ctx.options())
    print(m.as_json())
    print(json.dumps(conf))

    # print stats as last line
    print(json.dumps({"startupTime" : startup_time, "jobTime" : job_time,
                      'mode' : args.mode, 'mergeExceptionsInOrder': conf['optimizer.mergeExceptionsInOrder']}))
