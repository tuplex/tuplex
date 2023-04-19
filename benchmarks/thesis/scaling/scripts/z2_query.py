#!/usr/bin/env python3
# (c) L.Spiegelberg 2017-2020
# Benchmark for Zillow data using basic python. May be run in Pypy or CPython
# use concurrent futures ThreadPoolExecutor or ProcessPoolExecutor.
# could also use multiprocessing module
import concurrent.futures

import io
import pathlib
import shutil
import time
import argparse
import json
import os
import glob
import sys
import csv
import subprocess
import math

def tupleMode(paths, output_path):
    """
    run pipeline using tuple mode
    :param path:
    :param output_path:
    :return:
    """

    ### pipeline functions
    jstart = time.time()

    def extractZipcode(x):
        try:
            return x + ('{:05}'.format(int(float(x[4]))),)
        except:
            return x + (None,)

    def cleanCity(x):
        try:
            return x[:2] + (x[2][0].upper() + x[2][1:].lower(),) + x[3:]
        except:
            return x[:2] + (None,) + x[3:]

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
            ba = math.ceil(2.0 * float(r)) / 2.0
            return x + (ba,)
        except:
            return x + (None,)

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

    def extractType(x):
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

    def selectCols(x):
        return (x[8], x[12], x[1], x[2], x[3], x[10], x[13], x[14], x[15], x[11], x[5])

    def filterPriceAndOffer(x):
        try:
            return 100000 < x[5] <= 2e7 and x[15] == 'sale'
        except:
            return False

    def filterType(x):
        try:
            return x[-1] == 'condo'
        except:
            return False

    def filterBd(x):
        try:
            return x[-1] < 10
        except:
            return False

    def runTuplePipeline(records):
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
        step_11 = filter(filterPriceAndOffer, step_10)
        step_12 = map(selectCols, step_11)
        return step_12

    def csv2tuples(path, header=True):
        with open(path, 'r', encoding='utf-8') as f:
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
    return stats


def tupleModeOnChunk(chunk_info, output_path):
    """
    run pipeline using tuple mode
    :param path:
    :param output_path:
    :return:
    """

    ### pipeline functions
    jstart = time.time()

    def extractZipcode(x):
        try:
            return x + ('{:05}'.format(int(float(x[4]))),)
        except:
            return x + (None,)

    def cleanCity(x):
        try:
            return x[:2] + (x[2][0].upper() + x[2][1:].lower(),) + x[3:]
        except:
            return x[:2] + (None,) + x[3:]

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
            ba = math.ceil(2.0 * float(r)) / 2.0
            return x + (ba,)
        except:
            return x + (None,)

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

    def extractType(x):
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

    def selectCols(x):
        return (x[8], x[12], x[1], x[2], x[3], x[10], x[13], x[14], x[15], x[11], x[5])

    def filterPriceAndOffer(x):
        try:
            return 100000 < x[5] <= 2e7 and x[15] == 'sale'
        except:
            return False

    def filterType(x):
        try:
            return x[-1] == 'condo'
        except:
            return False

    def filterBd(x):
        try:
            return x[-1] < 10
        except:
            return False

    def runTuplePipeline(records):
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
        step_11 = filter(filterPriceAndOffer, step_10)
        step_12 = map(selectCols, step_11)
        return step_12

    def csv2tuples(header=True):
        # read the full chunk in and parse using csv reader!
        path = chunk_info[0]
        chunk_start = chunk_info[1]
        chunk_end = chunk_info[2]

        header = chunk_start == 0
        with open(path, 'r', encoding='utf-8') as f:
            f.seek(chunk_start)
            buffer = f.read(chunk_end - chunk_start + 1)

        # convert buffer to csv tuples via stringio

        reader = csv.reader(io.StringIO(buffer), delimiter=',', quotechar='"')
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
    row_list += csv2tuples()
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
             'input_path': '{}:{}-{}'.format(*chunk_info),
             'load_time': load_time,
             'run_time': run_time,
             'write_time': write_time,
             'job_time': job_time}
    return stats


def dictMode(paths, output_path):
    """
    run pipeline using tuple mode
    :param path:
    :param output_path:
    :return:
    """

    ### pipeline functions
    jstart = time.time()

    def extractZipcode(x):
        try:
            # this only works for python 3.5+, use old version here, see https://peps.python.org/pep-0448/
            # return {'zipcode': '%05d' % int(float(x['postal_code'])), **x}
            ans = x
            ans['zipcode'] = '%05d' % int(float(x['postal_code']))
            return ans
        except:
            ans = x
            ans['zipcode'] = None
            return ans
            #return {'zipcode': None, **x}

    def cleanCity(x):
        try:
            ans = x
            ans['city'] = x['city'][0].upper() + x['city'][1:].lower()
            return ans

            #return {**x, 'city': x['city'][0].upper() + x['city'][1:].lower()}
        except:
            ans = x
            ans['city'] = None
            return ans

            #return {**x, 'city': None}

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

            ans = x
            ans['bedrooms'] = int(r)
            return ans
            #return {**x, 'bedrooms': int(r)}
        except:
            ans = x
            ans['bedrooms'] = None
            return ans
            #return {**x, 'bedrooms': None}

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
            ba = math.ceil(2.0 * float(r)) / 2.0

            ans = x
            ans['bathrooms'] = ba
            return ans
            #return {**x, 'bathrooms': ba}
        except:
            ans = x
            ans['bathrooms'] = None
            return ans
            #return {**x, 'bathrooms': None}

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

            ans = x
            ans['sqft'] = int(r)
            return ans
            # return {**x, 'sqft': int(r)}
        except:
            ans = x
            ans['sqft'] = None
            return ans
            #return {**x, 'sqft': None}

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

            ans = x
            ans['offer'] = offer
            return ans

            # return {**x, 'offer': offer}
        except:
            ans = x
            ans['offer'] = None
            return ans

            # return {**x, 'offer': None}

    def extractType(x):
        try:
            t = x['title'].lower()
            type = 'unknown'
            if 'condo' in t or 'apartment' in t:
                type = 'condo'
            if 'house' in t:
                type = 'house'

            ans = x
            ans['type'] = type
            return ans

            #return {**x, 'type': type}
        except:
            ans = x
            ans['type'] = None
            return ans

            # return {**x, 'type': None}

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

            ans = x
            ans['price'] =  price
            return ans

            # return {**x, 'price': price}
        except:
            ans = x
            ans['price'] =  None
            return ans

            # return {**x, 'price': None}

    def selectCols(x):
        columns = ['url', 'zipcode', 'address', 'city', 'state', 'bedrooms', 'bathrooms', 'sqft', 'offer', 'type',
                   'price']
        return {key: x[key] for key in x.keys() if key in columns}

    def filterPriceAndOffer(x):
        try:
            return 100000 < x['price'] <= 2e7 and x['offer'] == 'sale'
        except:
            return False

    def filterType(x):
        try:
            return x['type'] == 'condo'
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
        step_11 = filter(filterPriceAndOffer, step_10)
        step_12 = map(selectCols, step_11)
        return step_12

    # Python3 internals based reader
    def csv2dicts(path):
        with open(path, 'r', encoding='utf-8') as f:
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
    return stats

# run using parallel read/chunking from https://nurdabolatov.com/parallel-processing-large-file-in-python
def generate_chunks(path, nchunks):

    file_size = os.path.getsize(path)
    chunk_size = file_size // nchunks

    # Arguments for each chunk (eg. [('input.txt', 0, 32), ('input.txt', 32, 64)])
    chunk_args = []
    with open(path, 'r') as f:
        def is_start_of_line(position):
            if position == 0:
                return True
            # Check whether the previous character is EOL
            f.seek(position - 1)
            return f.read(1) == '\n'

        def get_next_line_position(position):
            # Read the current line till the end
            f.seek(position)
            f.readline()
            # Return a position after reading the line
            return f.tell()

        chunk_start = 0
        # Iterate over all chunks and construct arguments for `process_chunk`
        while chunk_start < file_size:
            chunk_end = min(file_size, chunk_start + chunk_size)

            # Make sure the chunk ends at the beginning of the next line
            while not is_start_of_line(chunk_end):
                chunk_end -= 1

            # Handle the case when a line is too long to fit the chunk size
            if chunk_start == chunk_end:
                chunk_end = get_next_line_position(chunk_end)

            # Save `process_chunk` arguments
            args = (path, chunk_start, chunk_end)
            chunk_args.append(args)

            # Move to the next chunk
            chunk_start = chunk_end

    return chunk_args

    # with mp.Pool(cpu_count) as p:
    #     # Run chunks in parallel
    #     chunk_results = p.starmap(process_chunk, chunk_args)
    #
    # results = []
    # # Combine chunk results into `results`
    # for chunk_result in chunk_results:
    #     for result in chunk_result:
    #         results.append(result)
    # return results

# def process_chunk(file_name, chunk_start, chunk_end):
#     chunk_results = []
#     with open(file_name, 'r') as f:
#         # Moving stream position to `chunk_start`
#         f.seek(chunk_start)
#
#         # Read and process lines until `chunk_end`
#         for line in f:
#             chunk_start += len(line)
#             if chunk_start > chunk_end:
#                 break
#             chunk_results.append(process_line(line))
#     return chunk_results

def run_pipeline_in_parallel_tuple_mode(path: str, output_path: str, parallelism:int =0, mode: str='thread'):

    # remove dir
    if os.path.isfile(output_path):
        os.remove(output_path)
    elif os.path.isdir(output_path):
        shutil.rmtree(output_path)

    os.makedirs(pathlib.Path(output_path).parent, exist_ok=True)

    job_time = 0.0
    # perform single-threaded baseline
    if 1 >= parallelism:
        print('-- running baseline (single CPython main-thread)')
        tstart = time.time()
        result = tupleMode([path], output_path)
        print(result)
        job_time = time.time() - tstart
    else:
        assert parallelism > 0, 'negative numbers not accepted'
        print(f'-- running pipeline with {parallelism}-way parallelism using {mode}')
        if 'thread' == mode:
            tstart = time.time()
            result = []

            # chunk data
            chunks = generate_chunks(path, parallelism)
            os.makedirs(output_path, exist_ok=True)

            with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as exec:
                futures = [exec.submit(tupleModeOnChunk, chunk, os.path.join(output_path, 'chunk{:04d}.csv'.format(i))) for i, chunk in enumerate(chunks)]
                for f in concurrent.futures.as_completed(futures):
                    partial_res = f.result()
                    result.append(partial_res)
            print(result)
            job_time = time.time() - tstart
        elif 'process' == mode:
            tstart = time.time()
            result = []
            # chunk data
            chunks = generate_chunks(path, parallelism)
            os.makedirs(output_path, exist_ok=True)

            with concurrent.futures.ProcessPoolExecutor(max_workers=parallelism) as exec:
                futures = [exec.submit(tupleModeOnChunk, chunk, os.path.join(output_path, 'chunk{:04d}.csv'.format(i))) for i, chunk in enumerate(chunks)]
                for f in concurrent.futures.as_completed(futures):
                    partial_res = f.result()
                    result.append(partial_res)
            print(result)
            job_time = time.time() - tstart
        else:
            raise Exception(f"unknown {mode}")

    stats = {'framework': 'python3',
             'py_executable': sys.executable,
             'job_time': job_time,
             'query': 'z2_query',
             'mode': mode,
             'parallelism': parallelism,
             'version_info': str(sys.version_info)}
    return stats

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Zillow cleaning Z2 query, scaling experiment')
    parser.add_argument('--path', type=str, dest='data_path', default='../.cache/large1GB.csv',
                        help='path or pattern to zillow data')
    parser.add_argument('-m', '--mode', choices=["thread", "process"],
                        default="process", help='select scale mode for python.')
    parser.add_argument('-p', '--parallelism', default=0, type=int, help='select scale mode for python.')
    #parser.add_argument('--mode', type=str, dest='mode', default='tuple', help='specify tuple or dict for internal data representation')
    #parser.add_argument('--compiler', type=str, dest='compiler', default=None, help='specify whether to use a python compiler (cython or nuitka)')

    parser.add_argument('--output-path', type=str, dest='output_path', default='python_output', help='specify path where to save output data files')
    args = parser.parse_args()

    assert args.data_path, 'need to set data path!'

    # config vars
    paths = [args.data_path]
    output_path = args.output_path

    # explicit globbing because dask can't handle patterns well...
    assert os.path.isfile(args.data_path), 'Only single file in this script supported'

    # # if compiler is specified, compile to extension module and record time
    # compile_time = 0.0
    # if args.compiler:
    #
    #     # copy functions from this file (till main)
    #     # and compile
    #     # => then import!
    #     with open(sys.argv[0], 'r') as fp:
    #         with open('funcs.py', 'w') as wp:
    #             content = fp.read()
    #             content = content[:content.find("if __name__ == '__main__':")]
    #             wp.write(content)
    #
    #     tstart = time.time()
    #
    #     cmd = None
    #     if args.compiler == 'cython':
    #         # run subprocess
    #         cmd = 'cythonize -3 -f -b funcs.py'
    #     elif args.compiler == 'nuitka':
    #         # run subprocess
    #         # recent nuitka is buggy on lto, disable
    #         # cmd = '{} -m nuitka --module --lto funcs.py'.format(sys.executable)
    #         cmd = '{} -m nuitka --module funcs.py'.format(sys.executable)
    #     else:
    #         raise Exception('compiler {} not supported'.format(args.compiler))
    #
    #     p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #     out, err = p.communicate()
    #     rc = p.returncode
    #     out = out.decode()
    #     err = err.decode()
    #
    #     if rc != 0:
    #         print("Error, return code = {}\n{}".format(rc, err), file=sys.stderr)
    #         sys.exit(1)
    #
    #     # remove file & load
    #     os.remove('funcs.py')
    #
    #     # import & replace functions above!
    #     import funcs
    #     tupleMode = funcs.tupleMode
    #     dictMode = funcs.dictMode
    #     compile_time = time.time() - tstart
    #
    #     print('compilation via {} took: {}s'.format(args.compiler, compile_time))

    print('>>> running {} on {}'.format(os.path.basename(sys.executable), paths))
    print('>>> scaling mode: {}'.format(args.mode))

    # create folder and alter output path
    if not os.path.isfile(output_path):
        os.makedirs(output_path, exist_ok=True)
        output_path = os.path.join(output_path, 'zillow')

    # run specific pipeline (either tuples or dicts)
    stats = {}
    tstart = time.time()
    # run in tuple mode
    stats = run_pipeline_in_parallel_tuple_mode(args.data_path, output_path, int(args.parallelism), args.mode)
    stats['job_time'] = time.time() - tstart
    print(f'-- end-to-end time: {time.time() - tstart}s')
    # if args.mode == 'tuple':
    #     stats = tupleMode(paths, output_path)
    # elif args.mode == 'dict':
    #     stats = dictMode(paths, output_path)
    # else:
    #     print('invalid mode {}, abort.'.format(args.mode))
    #     sys.exit(1)
    #
    # py_executable = sys.executable
    # if args.compiler:
    #     stats['compile_time'] = compile_time
    #     if args.compiler == 'cython':
    #         import cython
    #         stats['version'] = cython.__version__
    #         stats['program'] = 'cython ({})'.format(py_executable)
    #     if args.compiler == 'nuitka':
    #         # run python3 -m nuitka --version to get the nuitka version
    #         p = subprocess.Popen(['{} -m nuitka --version'.format(py_executable)], shell=True,
    #                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #         out, err = p.communicate()
    #         rc = p.returncode
    #         out = out.decode()
    #         err = err.decode()
    #         nuitka_version = out.split()[0] # first line is version...
    #         stats['version'] = nuitka_version
    #         stats['program'] = 'nuitka ({})'.format(py_executable)
    # else:
    #     stats['program'] = py_executable

    stats['program'] = sys.executable
    stats['version_info'] = str(sys.version_info)
    print(stats)
    print(json.dumps(stats))
