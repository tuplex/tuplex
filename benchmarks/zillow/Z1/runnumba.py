#!/usr/bin/env python3
# (c) L.Spiegelberg 2017-2020
# Benchmark for Zillow data using basic python. May be run in Pypy or CPython
# this version uses loops instead of iterators!

import time
import argparse
import json
import os
import glob
import sys
import csv

#  numba
from numba import njit, prange


# loop versions
# def map(f, arr): # inplace
#     for i in prange(len(arr)):
#         arr[i] = f(arr[i])
#     return arr

# def filter(f, arr):
#     res = []
#     for a in arr:
#         if f(a):
#             res.append(a)
#     return res

def tupleMode(paths, output_path):
    """
    run pipeline using tuple mode
    :param path:
    :param output_path:
    :return:
    """

    ### pipeline functions
    jstart = time.time()

    @njit
    def extractZipcode(x):
        try:
            return x + ('{:05}'.format(int(float(x[4]))),)
        except:
            return x + (None,)

    @njit
    def cleanCity(x):
        try:
            return x[:2] + (x[2][0].upper() + x[2][1:].lower(),) + x[3:]
        except:
            return x[:2] + (None,) + x[3:]

    @njit
    def extractBd(x):
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

    @njit
    def extractBa(x):
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

    @njit
    def extractSqft(x):
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

    @njit
    def extractOffer(x):
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

    @njit
    def extractType(x):
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

    @njit
    def extractPrice(x):
        try:
            price = x[5]
            if x[15] == 'sold':
                # price is to be calculated using price/sqft * sqft
                val = x[6]
                s = val[val.find('Price/sqft:') + len('Price/sqft:') + 1:]
                r = s[s.find('$') + 1:s.find(', ') - 1]
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

    @njit
    def selectCols(x):
        return (x[8], x[12], x[1], x[2], x[3], x[10], x[13], x[14], x[15], x[11], x[5])

    @njit
    def filterPrice(x):
        try:
            return 100000 < x[5] <= 2e7
        except:
            return False

    @njit
    def filterType(x):
        try:
            return x[-1] == 'house'
        except:
            return False

    @njit
    def filterBd(x):
        try:
            return x[-1] < 10
        except:
            return False

    @njit
    def runTuplePipeline(records):
        # trafo steps
        step_1 = map(extractBd, records)
        return step_1
        # step_2 = filter(filterBd, step_1)
        # step_3 = map(extractType, step_2)
        # step_4 = filter(filterType, step_3)
        # step_5 = map(extractZipcode, step_4)
        # step_6 = map(cleanCity, step_5)
        # step_7 = map(extractBa, step_6)
        # step_8 = map(extractSqft, step_7)
        # step_9 = map(extractOffer, step_8)
        # step_10 = map(extractPrice, step_9)
        # step_11 = filter(filterPrice, step_10)
        # step_12 = map(selectCols, step_11)
        # return step_12

    def csv2tuples(path, header=True):
        with open(path, 'r') as f:
            reader = csv.reader(f, delimiter=',', quotechar='"')
            rows = [tuple(row) for row in reader]
            if header:
                return rows[1:]
            else:
                return rows

    ### pipeline end


    # builtin Python3 load
    tstart = time.time()

    # loading using builtin csv module
    row_list = []
    for path in paths:
        row_list += csv2tuples(path)
    load_time = time.time() - tstart

    res = runTuplePipeline(row_list)
    columns = ['url', 'zipcode', 'address', 'city', 'state', 'bedrooms', 'bathrooms', 'sqft', 'offer', 'type', 'price']

    def tocsvstr(a):
        a = [str(x) for x in a]
        return ','.join(a) + '\n'

    save_start = time.time()

    if os.path.exists(output_path):
        if os.path.isfile(output_path):
            os.remove(output_path)
        else:
            os.rmdir(output_path)

    # write to file
    with open(output_path, 'w') as fp:
        fp.write(','.join(columns) + '\n')
        for row in map(tocsvstr, res):
            fp.write(row)

    run_time = time.time() - tstart
    write_time = time.time() - save_start
    job_time = time.time() - jstart

    stats = {'type': 'tuple',
             'framework': 'python3',
             'py_executable' : sys.executable,
             'input_files': paths,
             'load_time': load_time,
             'run_time': run_time,
             'write_time': write_time,
             'job_time' : job_time}
    print(stats)

def dictMode(paths, output_path):
    """
    run pipeline using tuple mode
    :param path:
    :param output_path:
    :return:
    """

    ### pipeline functions
    jstart = time.time()

    @njit
    def extractZipcode(x):
        try:
            return {'zipcode': '%05d' % int(float(x['postal_code'])), **x}
        except:
            return {'zipcode': None, **x}

    @njit
    def cleanCity(x):
        try:
            return {**x, 'city': x['city'][0].upper() + x['city'][1:].lower()}
        except:
            return {**x, 'city': None}

    @njit
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
            offer = x['title']
            assert 'Sale' in offer or 'Rent' in offer or 'SOLD' in offer

            if 'Sale' in offer:
                offer = 'sale'
            if 'Rent' in offer:
                offer = 'rent'
            if 'SOLD' in offer:
                offer = 'sold'
            if 'foreclose' in offer.lower():
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

    def runDictPipeline(records):

        # trafo steps
        step_1 = map(extractBd, records)
        step_2 = filter(filterBd, step_1)
        step_3 = map(extractType, step_2)
        step_4 = filter(filterType, step_3)
        step_5 = map(extractZipcode, step_4)
        step_6 = map(cleanCity, step_5)
        step_7 = map(extractBa, step_6)
        step_8 = map(extractSqft, step_7)
        step_9 = map(extractOffer, step_8)
        step_10 = map(extractPrice, step_9)
        step_11 = filter(filterPrice, step_10)
        step_12 = map(selectCols, step_11)
        return step_12

    # Python3 internals based reader
    def csv2dicts(path):
        with open(path, 'r') as f:
            reader = csv.DictReader(f, delimiter=',', quotechar='"')
            return [dict(row) for row in reader]

    ### pipeline end

    # builtin Python3 load
    tstart = time.time()

    # loading using builtin csv module
    row_list = []
    for path in paths:
        row_list += csv2dicts(path)
    load_time = time.time() - tstart

    res = runDictPipeline(row_list)
    columns = ['url', 'zipcode', 'address', 'city', 'state', 'bedrooms', 'bathrooms', 'sqft', 'offer', 'type', 'price']

    def tocsvstr(d):
        a = []
        for c in columns:
            a.append(str(d[c]))
        return ','.join(a) + '\n'

    save_start = time.time()

    # write to file
    with open(output_path, 'w') as fp:
        fp.write(','.join(columns) + '\n')
        for row in map(tocsvstr, res):
            fp.write(row)


    run_time = time.time() - tstart
    write_time = time.time() - save_start
    job_time = time.time() - jstart

    stats = {'type': 'dict',
             'framework': 'python3',
             'py_executable': sys.executable,
             'input_files': paths,
             'load_time': load_time,
             'run_time': run_time,
             'write_time': write_time,
             'job_time': job_time}
    print(stats)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Zillow cleaning')
    parser.add_argument('--path', type=str, dest='data_path', default='data/large100MB.csv',
                        help='path or pattern to zillow data')
    parser.add_argument('--mode', type=str, dest='mode', default='tuple', help='specify tuple or dict for internal data representation')
    parser.add_argument('--output-path', type=str, dest='output_path', default='python_output', help='specify path where to save output data files')
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

    print('>>> running {} on {}'.format(os.path.basename(sys.executable), paths))
    print('>>> data mode: {}'.format(args.mode))


    # create folder and alter output path
    if not os.path.isfile(output_path):
        os.makedirs(output_path, exist_ok=True)
        output_path = os.path.join(output_path, 'zillow_out.csv')

    # run specific pipeline (either tuples or dicts)
    if args.mode == 'tuple':
        tupleMode(paths, output_path)
    elif args.mode == 'dict':
        dictMode(paths, output_path)
    else:
        print('invalid mode {}, abort.'.format(args.mode))
        sys.exit(1)
