#!/usr/bin/env python3

# Dask based cleaning script (can be easily modified to pandas/modin/...)
import time
import sys
import json
import os
import glob
import argparse
import psutil

tstart = time.time()
import pandas as pd
import math
pandas_startup_time = time.time() - tstart

tstart = time.time()
# fix for python3.9 https://github.com/dask/distributed/issues/4168
import multiprocessing.popen_spawn_posix
import dask
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from dask.distributed import Client
import dask.multiprocessing
dask_startup_time = time.time() - tstart

def extractBd(val):
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


def extractType(title):
    t = title.lower()
    type = 'unknown'
    if 'condo' in t or 'apartment' in t:
        type = 'condo'
    if 'house' in t:
        type = 'house'
    return type


def extractBa(val):
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


def extractSqft(val):
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


def extractOffer(offer):
    try:
        offer = offer.lower()
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


def extractPrice(x):
    price, offer, val, sqft = x['price'], x['offer'], x['facts and features'], x['sqft']

    if offer == 'sold':
        # price is to be calculated using price/sqft * sqft
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


def runDask(paths, output_path):
    """
    runs path pipeline using dask dataframes / pandas dataframes
    :param paths:
    :param output_path:
    :return:
    """

    tstart = time.time()

    client = Client(n_workers=16, threads_per_worker=1, processes=True, memory_limit='8GB')
    print(client)

    startup_time = time.time() - tstart

    df = dd.read_csv(paths, low_memory=False)

    load_time = time.time() - tstart

    df['bedrooms'] = df['facts and features'].apply(extractBd, meta=('facts and features', 'str')).astype(int)
    df = df[df['bedrooms'] < 10]
    df['type'] = df['title'].apply(extractType)
    df = df[df['type'] == 'condo']

    df['zipcode'] = df['postal_code'].apply(lambda zc: '%05d' % int(zc), meta=('postal_code', 'str'))

    df['city'] = df['city'].apply(lambda x: x[0].upper() + x[1:].lower(), meta=('city', 'str'))
    df['bathrooms'] = df['facts and features'].apply(extractBa, meta=('facts and features', 'str')).astype(int)
    df['sqft'] = df['facts and features'].apply(extractSqft, meta=('facts and features', 'str')).astype(int)
    df['offer'] = df['title'].apply(extractOffer)

    df['price'] = df[['price', 'offer', 'facts and features', 'sqft']].apply(extractPrice, axis=1, meta=('facts and features', 'str')).astype(int)

    df = df[(100000 < df['price']) & (df['price'] < 2e7) & (df['offer'] == 'sale')]

    df = df[["url", "zipcode", "address", "city", "state",
             "bedrooms", "bathrooms", "sqft", "offer", "type", "price"]]

    save_start = time.time()
    df.to_csv(output_path, index=None)

    run_time = time.time() - tstart
    write_time = time.time() - save_start

    stats = {'type': 'dask',
             'framework': 'dask {}'.format(dask.__version__),
             'input_files': paths,
             'load_time': load_time,
             'run_time': run_time,
             'write_time': write_time,
             'job_time': run_time}
    print(stats)

def runPandas(paths, output_path):

    tstart = time.time()

    df = pd.DataFrame()
    for path in paths:
        df = pd.concat((df, pd.read_csv(path)))

    load_time = time.time() - tstart

    df['bedrooms'] = df['facts and features'].apply(extractBd)
    df = df[df['bedrooms'] < 10]
    df['type'] = df['title'].apply(extractType)
    df = df[df['type'] == 'condo']

    df['zipcode'] = df['postal_code'].apply(lambda zc: '%05d' % int(zc))

    df['city'] = df['city'].apply(lambda x: x[0].upper() + x[1:].lower())
    df['bathrooms'] = df['facts and features'].apply(extractBa)
    df['sqft'] = df['facts and features'].apply(extractSqft)
    df['offer'] = df['title'].apply(extractOffer)

    df['price'] = df[['price', 'offer', 'facts and features', 'sqft']].apply(extractPrice, axis=1)

    df = df[(100000 < df['price']) & (df['price'] < 2e7) & (df['offer'] == 'sale')]

    df = df[["url", "zipcode", "address", "city", "state",
             "bedrooms", "bathrooms", "sqft", "offer", "type", "price"]]

    save_start = time.time()
    df.to_csv(output_path, index=None)

    run_time = time.time() - tstart
    write_time = time.time() - save_start

    stats = {'type': 'pandas',
             'framework': 'pandas {}'.format(pd.__version__),
             'input_files': paths,
             'load_time': load_time,
             'run_time': run_time,
             'write_time': write_time,
             'job_time': run_time}
    print(stats)


def meminfo():
    p = psutil.Process()
    return str(p.memory_info())

# https://psutil.readthedocs.io/en/latest/#psutil.Process.memory_info
def tracePandas(paths, output_path):

    tstart = time.time()

    print('STEP 00: memory usage: {}'.format(meminfo()))
    ts = time.time()
    df = pd.DataFrame()
    for path in paths:
        df = pd.concat((df, pd.read_csv(path)))
    load_time = time.time() - tstart
    print('STEP 01: Load took {}s. {} rows, memory usage: {}'.format(load_time, len(df), meminfo()))
    ts = time.time()
    df['bedrooms'] = df['facts and features'].apply(extractBd)
    print('STEP 02: extractBd took {}s. {} rows, memory usage: {}'.format(time.time() - ts, len(df), meminfo()))
    ts = time.time()
    df = df[df['bedrooms'] < 10]
    print('STEP 03: filterBd took {}s. {} rows, memory usage: {}'.format(time.time() - ts, len(df), meminfo()))
    ts = time.time()
    df['type'] = df['title'].apply(extractType)
    print('STEP 04: extractType took {}s. {} rows, memory usage: {}'.format(time.time() - ts, len(df), meminfo()))
    ts = time.time()
    df = df[df['type'] == 'condo']
    print('STEP 05: filterType took {}s. {} rows, memory usage: {}'.format(time.time() - ts, len(df), meminfo()))
    ts = time.time()
    df['zipcode'] = df['postal_code'].apply(lambda zc: '%05d' % int(zc))
    print('STEP 06: extractZipcode took {}s. {} rows, memory usage: {}'.format(time.time() - ts, len(df), meminfo()))
    ts = time.time()
    df['city'] = df['city'].apply(lambda x: x[0].upper() + x[1:].lower())
    print('STEP 07: mapCity took {}s. {} rows, memory usage: {}'.format(time.time() - ts, len(df), meminfo()))
    ts = time.time()
    df['bathrooms'] = df['facts and features'].apply(extractBa)
    print('STEP 08: extractBa took {}s. {} rows, memory usage: {}'.format(time.time() - ts, len(df), meminfo()))
    ts = time.time()
    df['sqft'] = df['facts and features'].apply(extractSqft)
    print('STEP 09: extractSqft took {}s. {} rows, memory usage: {}'.format(time.time() - ts, len(df), meminfo()))
    ts = time.time()
    df['offer'] = df['title'].apply(extractOffer)
    print('STEP 10: extractOffer took {}s. {} rows, memory usage: {}'.format(time.time() - ts, len(df), meminfo()))
    ts = time.time()
    df['price'] = df[['price', 'offer', 'facts and features', 'sqft']].apply(extractPrice, axis=1)
    print('STEP 11: extractPrice took {}s. {} rows, memory usage: {}'.format(time.time() - ts, len(df), meminfo()))
    ts = time.time()
    df = df[(100000 < df['price']) & (df['price'] < 2e7) & (df['offer'] == 'sale')]
    print('STEP 12: filterPriceAndOffer took {}s. {} rows, memory usage: {}'.format(time.time() - ts, len(df), meminfo()))
    ts = time.time()
    df = df[["url", "zipcode", "address", "city", "state",
             "bedrooms", "bathrooms", "sqft", "offer", "type", "price"]]
    print('STEP 13: selectColumns took {}s. {} rows, memory usage: {}'.format(time.time() - ts, len(df), meminfo()))
    ts = time.time()
    save_start = time.time()
    df.to_csv(output_path, index=None)
    print('STEP 14: saveToCSV took {}s. {} rows, memory usage: {}'.format(time.time() - ts, len(df), meminfo()))
    ts = time.time()
    run_time = time.time() - tstart
    write_time = time.time() - save_start

    stats = {'type': 'pandas',
             'framework': 'pandas {}'.format(pd.__version__),
             'input_files': paths,
             'load_time': load_time,
             'run_time': run_time,
             'write_time': write_time,
             'job_time': run_time}
    print(stats)

def runModinOnRay(paths, output_path):

    tstart = time.time()

    import ray
    ray.init(num_cpus=16)


    import modin.pandas as md

    startup_time = time.time() - tstart
    tstart = time.time()
    df = md.DataFrame()
    for path in paths:
        df = md.concat((df, pd.read_csv(path)))

    load_time = time.time() - tstart

    df['bedrooms'] = df['facts and features'].apply(extractBd)
    df = df[df['bedrooms'] < 10]
    df['type'] = df['title'].apply(extractType)
    df = df[df['type'] == 'condo']

    df['zipcode'] = df['postal_code'].apply(lambda zc: '%05d' % int(zc))

    df['city'] = df['city'].apply(lambda x: x[0].upper() + x[1:].lower())
    df['bathrooms'] = df['facts and features'].apply(extractBa)
    df['sqft'] = df['facts and features'].apply(extractSqft)
    df['offer'] = df['title'].apply(extractOffer)

    df['price'] = df[['price', 'offer', 'facts and features', 'sqft']].apply(extractPrice, axis=1)

    df = df[(100000 < df['price']) & (df['price'] < 2e7) & (df['offer'] == 'sale')]

    df = df[["url", "zipcode", "address", "city", "state",
             "bedrooms", "bathrooms", "sqft", "offer", "type", "price"]]

    save_start = time.time()
    df.to_csv(output_path, index=None)

    run_time = time.time() - tstart
    write_time = time.time() - save_start

    stats = {'type': 'ray',
             'framework': 'modin {}'.format(md.__version__),
             'input_files': paths,
             'startup_time' : startup_time,
             'load_time': load_time,
             'run_time': run_time,
             'write_time': write_time,
             'job_time': run_time}
    print(stats)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Zillow cleaning')
    parser.add_argument('--path', type=str, dest='data_path', default='data/large100MB.csv',
                        help='path or pattern to zillow data')
    parser.add_argument('--mode', type=str, dest='mode', default='dask',
                        help='specify dask or pandas or trace-pandas')
    parser.add_argument('--output-path', type=str, dest='output_path', default=None,
                        help='specify path where to save output data files')
    args = parser.parse_args()

    assert args.data_path, 'need to set data path!'

    # config vars
    paths = [args.data_path]
    output_path = args.output_path

    if not output_path:
        output_path = args.mode + '_output'


    # explicit globbing because dask can't handle patterns well...
    if not os.path.isfile(args.data_path):
        paths = sorted(glob.glob(os.path.join(args.data_path, '*.csv')))
    else:
        paths = [args.data_path]

    if not paths:
        print('found no zillow data to process, abort.')
        sys.exit(1)

        # create folder and alter output path
    if not os.path.isfile(output_path) and (args.mode == 'pandas' or args.mode == 'trace-pandas' or args.mode == 'ray'):
        os.makedirs(output_path, exist_ok=True)
        output_path = os.path.join(output_path, 'zillow_out.csv')

    print('>>> running {} on {}'.format(args.mode, paths))
    # run specific pipeline (either tuples or dicts)
    if 'dask' == args.mode:
        runDask(paths, output_path)
    elif 'trace-pandas' in args.mode:
        tracePandas(paths, output_path)
    elif 'pandas' in args.mode:
        runPandas(paths, output_path)
    elif 'ray' in args.mode:
        runModinOnRay(paths, output_path)
    else:
        print('invalid mode {}, abort.'.format(args.mode))
        sys.exit(1)

