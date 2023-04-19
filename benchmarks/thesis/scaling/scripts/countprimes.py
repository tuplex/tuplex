#!/usr/bin/env python3
# (c) L.Spiegelberg 2017-2020
# Benchmark for Zillow data using basic python. May be run in Pypy or CPython
# here the idea is to use scaling from 1x to 32x for prime numbers
import time
import argparse
import json
import os
import glob
import sys
import csv
import subprocess
import math

# use concurrent futures ThreadPoolExecutor or ProcessPoolExecutor.
# could also use multiprocessing module
import concurrent.futures

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

def run_pipeline(parallelism=0, mode='thread'):

    data = [10, 20, 30, 40] * 8

    job_time = 0.0
    # perform single-threaded baseline
    if 1 >= parallelism:
        print('-- running baseline (single CPython main-thread)')
        tstart = time.time()
        result = list(map(count_primes, data))
        print(result)
        job_time = time.time() - tstart
    else:
        print(f'-- running pipeline with {parallelism}-way parallelism using {mode}')
        if 'thread' == mode:
            tstart = time.time()
            result = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=parallelism) as exec:
                futures = [exec.submit(count_primes, n) for n in data]
                for f in concurrent.futures.as_completed(futures):
                    partial_res = f.result()
                    result.append(partial_res)
            print(result)
            job_time = time.time() - tstart
        elif 'process' == mode:
            tstart = time.time()
            result = []
            with concurrent.futures.ProcessPoolExecutor(max_workers=parallelism) as exec:
                futures = [exec.submit(count_primes, n) for n in data]
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
             'query': 'countprimes',
             'mode': mode,
             'parallelism': parallelism,
             'version_info': str(sys.version_info)}
    return stats

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='CountPrimes')
    parser.add_argument('-m', '--mode', choices=["thread", "process"],
                        default="process", help='select scale mode for python.')
    parser.add_argument('-p', '--parallelism', default=0, help='select scale mode for python.')
    args = parser.parse_args()

    print('>>> running {}'.format(os.path.basename(sys.executable)))
    stats = run_pipeline(args.parallelism, args.mode)
    print(stats)
    print(json.dumps(stats))
