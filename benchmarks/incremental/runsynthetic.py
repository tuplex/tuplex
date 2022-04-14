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
import shutil
import subprocess
import random

def synthetic_pipeline(ctx, path, output_path, num_steps, current_step, commit):
    ds = ctx.csv(path, header=True)
    ds = ds.withColumn("c", lambda x: 1 // x["a"] if x["a"] == 0 else x["a"])
    for step in range(num_steps):
            if current_step > step:
                ds = ds.resolve(ZeroDivisionError, lambda x: 1 // x["a"] if random.choice([True, False]) else 0)

    ds = ds.selectColumns(['a'])
    ds.tocsv(output_path)

    return ctx.metrics

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Incremental resolution')
    parser.add_argument('--input-path', type=str, dest='data_path', default='synth0.csv', help='path or pattern to synthetic data')
    parser.add_argument('--output-path', type=str, dest='output_path', default='synthetic-output/', help='specify path where to save output data files')
    parser.add_argument('--num-steps', type=int, dest='num_steps', default=5)
    parser.add_argument('--resolve-in-order', dest='resolve_in_order', action="store_true", help="whether to resolve exceptions in order")
    parser.add_argument('--incremental-resolution', dest='incremental_resolution', action="store_true", help="whether to use incremental resolution")
    parser.add_argument('--commit-mode', dest='commit_mode', action='store_true', help='whether to use commit mode')
    parser.add_argument('--clear-cache', dest='clear_cache', action='store_true', help='whether to clear the cache or not')
    args = parser.parse_args()

    assert args.data_path, 'need to set data path!'

    # config vars
    path = args.data_path
    output_path = args.output_path
    num_steps = args.num_steps

    if not path:
        print('found no zillow data to process, abort.')
        sys.exit(1)

    print('>>> running {} on {}'.format('tuplex', path))

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

    if args.incremental_resolution:
        conf["optimizer.incrementalResolution"] = True
    else:
        conf["optimizer.incrementalResolution"] = False

    if args.resolve_in_order:
        conf['optimizer.mergeExceptionsInOrder'] = True
    else:
        conf['optimizer.mergeExceptionsInOrder'] = False

    # Note: there's a bug in the merge in order mode here -.-
    # force to false version
    conf["optimizer.generateParser"] = False
    conf["tuplex.optimizer.sharedObjectPropagation"] = False
    conf["resolveWithInterpreterOnly"] = False

    tstart = time.time()
    import tuplex
    ctx = tuplex.Context(conf)

    print(json.dumps(ctx.options()))

    startup_time = time.time() - tstart
    print('Tuplex startup time: {}'.format(startup_time))

    shutil.rmtree(output_path, ignore_errors=True)

    if args.clear_cache:
        subprocess.run(["clearcache"])

    tstart = time.time()
    metrics = []
    for step in range(num_steps):
        print(f'>>> running pipeline with {step} resolver(s) enabled...')
        jobstart = time.time()
        m = synthetic_pipeline(ctx, path, output_path, num_steps, step, not args.commit_mode or step == num_steps - 1)
        m = m.as_dict()
        m["numResolvers"] = step
        m["jobTime"] = time.time() - jobstart
        metrics.append(m)

    runtime = time.time() - tstart

    print("EXPERIMENTAL RESULTS")
    print(json.dumps({"startupTime": startup_time,
                      "totalRunTime": runtime,
                      "mergeExceptionsInOrder": conf["optimizer.mergeExceptionsInOrder"],
                      "incrementalResolution": conf["optimizer.incrementalResolution"]}))

    for metric in metrics:
        print(json.dumps(metric))
