#!/usr/bin/env python3
# (c) L.Spiegelberg 2017-2020
# Benchmark for Zillow data using basic python. May be run in Pypy or CPython

import time
import argparse
import json
import os
import glob
import sys
import csv
import subprocess
import math

# test function from https://towardsdatascience.com/how-to-speed-up-python-data-pipelines-up-to-91x-80d7accfe7ec
def count_primes(max_num):
    """This function counts of prime numbers below the input value.
    Input values are in thousands, ie. 40, is 40,000.
    """
    count = 0
    for num in range(max_num * 1000 + 1):
        if num > 1:
            for i in range(2, num):
                if num % i == 0:
                    break
                else:
                    count += 1
    return count

def loop_expression(data):
    return [count_primes(num) for num in data]

def loop_map(data):
    res = [0] * len(data)
    for i in range(len(data)):
        res[i] = count_primes(data[i])
    return res

def run_pipeline():

    data = [10, 20, 30, 40]

    tstart = time.time()
    result = list(map(count_primes, data))
    map_time = time.time() - tstart

    # some compiler may have bad support for map, hence change it up
    tstart = time.time()
    result = loop_map(data)
    for_time = time.time() - tstart

    # some compiler may have bad support for map, hence change it up
    tstart = time.time()
    result = loop_expression(data)
    exp_time = time.time() - tstart

    stats = {'framework': 'python3',
             'py_executable': sys.executable,
             'job_time_map' : map_time,
             'job_time_for' : for_time,
             'job_time_[]' :  exp_time,
             'version_info': str(sys.version_info)}
    return stats

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='CountPrimes')
    args = parser.parse_args()

    print('>>> running {}'.format(os.path.basename(sys.executable)))
    stats = run_pipeline()
    print(stats)
