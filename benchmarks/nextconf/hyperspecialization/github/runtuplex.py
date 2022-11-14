#!/usr/bin/env python3
# this script holds a (dummy) pipeline for working with Lambda/S3/Github data

# Tuplex based cleaning script
import tuplex
import tuplex.dataset
import time
import sys
import json
import os
import glob
import argparse
import logging

def extract_repo_id_code(row):
    if 2012 <= row['year'] <= 2014:
        return row['repository']['id']
    else:
        return row['repo']['id']

def fork_event_pipeline(ctx, input_pattern, s3_output_path, sm = None):
    """test pipeline to extract fork evenets across years"""
    if sm is None:
        sm = tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS
    #sm = tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS
    #sm = tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS
    #sm = tuplex.dataset.SamplingMode.LAST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS

    ctx.json(input_pattern, sampling_mode=sm) \
        .withColumn('year', lambda x: int(x['created_at'].split('-')[0])) \
        .withColumn('repo_id', extract_repo_id_code) \
        .filter(lambda x: x['type'] == 'ForkEvent') \
        .selectColumns(['type', 'repo_id', 'year']) \
        .tocsv(s3_output_path)

def push_event_pipeline(ctx, input_pattern, s3_output_path, sm = None):
    """test pipeline to extract push events across years"""
    if sm is None:
        sm = tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS
    #sm = tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS
    #sm = tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS
    #sm = tuplex.dataset.SamplingMode.LAST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS

    ctx.json(input_pattern, sampling_mode=sm) \
        .withColumn('year', lambda x: int(x['created_at'].split('-')[0])) \
        .withColumn('repo_id', extract_repo_id_code) \
        .filter(lambda x: x['type'] == 'PushEvent') \
        .selectColumns(['type', 'repo_id', 'year']) \
        .tocsv(s3_output_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Github hyper specialization query')
    # parser.add_argument('--path', type=str, dest='data_path', default='data/large100MB.csv',
    #                     help='path or pattern to zillow data')
    # parser.add_argument('--output-path', type=str, dest='output_path', default='tuplex_output/',
    #                     help='specify path where to save output data files')
    # parser.add_argument('--single-threaded', dest='single_threaded', action="store_true",
    #                     help="whether to use a single thread for execution")
    # parser.add_argument('--preload', dest='preload', action="store_true",
    #                     help="whether to add a cache statement after the csv operator to separate IO costs out.")
    parser.add_argument('--no-hyper', dest='no_hyper', action="store_true",
                        help="deactivate hyperspecialization optimization explicitly.")
    parser.add_argument('--no-cf', dest='no_cf', action="store_true",
                        help="deactivate constant-folding optimization explicitly.")
    parser.add_argument('--sampling-mode', dest='sampling_mode', choices=['A', 'B', 'C', 'D', 'E', 'F'], default='A')
    parser.add_argument('--no-filter-pushdown', dest='no_fpd', action="store_true",
                        help="deactivate filter pushdown.")
    parser.add_argument('--no-nvo', dest='no_nvo', action="store_true",
                        help="deactivate null value optimization explicitly.")
    parser.add_argument('--no-llvm-opt', dest='no_opt', action="store_true",
                        help="deactivate llvm optimization explicitly.")
    args = parser.parse_args()

    #if not 'AWS_ACCESS_KEY_ID' in os.environ or 'AWS_SECRET_ACCESS_KEY' not in os.environ:
    #    raise Exception('Did not find AWS credentials in environment, please set.')


    sm_map = {'A' : tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS,
            'B': tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.FIRST_ROWS,
            'C':tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_FILE,
            'D':tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_FILE,
            'E':tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.ALL_FILES,
            'F':tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.ALL_FILES
            }

    sm = sm_map.get(args.sampling_mode, None)

    # when hyper is active use sampling mode D to make general path better.
    if not args.no_hyper:
        sm = sm_map.get('B', None)

    logging.info('Using sampling mode: {}'.format(sm))

    lambda_size = "10000"
    lambda_threads = 3 # 3 seems to be working the best?
    s3_scratch_dir = "s3://tuplex-leonhard/scratch/github-exp"
    use_hyper_specialization = not args.no_hyper
    use_constant_folding = not args.no_cf
    s3_output_path = 's3://tuplex-leonhard/experiments/github'

    # two options: full dataset or tiny sample
    input_pattern = 's3://tuplex-public/data/github_daily/*.json' # <-- full dataset

    # small sample
    #input_pattern = 's3://tuplex-public/data/github_daily_sample/*.sample'

   # input_pattern = 's3://tuplex-public/data/github_daily/2011*.json,s3://tuplex-public/data/github_daily/2013*.json' # <-- single file   


    if use_hyper_specialization:
        s3_output_path += '/hyper'
    else:
        s3_output_path += '/general'

    print('>>> running {} on {} -> {}'.format('tuplex', input_pattern, s3_output_path))

    print('    hyperspecialization: {}'.format(use_hyper_specialization))
    # deactivate, b.c. not supported yet...
    # print('    constant-folding: {}'.format(use_constant_folding))
    # print('    null-value optimization: {}'.format(not args.no_nvo))

    # load data
    tstart = time.time()

    # configuration, make sure to give enough runtime memory to the executors!
    # run on Lambda
    conf = {"webui.enable" : False,
            "backend": "lambda",
            "aws.lambdaMemory": lambda_size,
            "aws.lambdaThreads": lambda_threads,
            "aws.httpThreadCount": 410,
            "aws.maxConcurrency": 410,
            'tuplex.aws.lambdaThreads': 0,
            'tuplex.aws.verboseLogging':True,
            'tuplex.csv.maxDetectionMemory': '256KB',
            "aws.scratchDir": s3_scratch_dir,
            "experimental.hyperspecialization": use_hyper_specialization,
            "executorCount": 0,
            "executorMemory": "2G",
            "driverMemory": "2G",
            "partitionSize": "32MB",
            "runTimeMemory": "128MB",
            "useLLVMOptimizer": True,
            "optimizer.nullValueOptimization": True,
            "resolveWithInterpreterOnly": False,
            "optimizer.constantFoldingOptimization": use_constant_folding,
            "csv.selectionPushdown" : True}

    if os.path.exists('tuplex_config.json'):
        with open('tuplex_config.json') as fp:
            conf = json.load(fp)

    if args.no_nvo:
        conf["optimizer.nullValueOptimization"] = False
    else:
        conf["optimizer.nullValueOptimization"] = True

    # for github deactivate all the optimizations, so stuff runs
    conf["optimizer.constantFoldingOptimization"] = False
    conf["optimizer.filterPushdown"] = not args.no_fpd
    conf["optimizer.selectionPushdown"] = False # <-- does not work yet

    # use this flage here to activate general path to make everything faster
    conf["resolveWithInterpreterOnly"] = False # <-- False means general path is activated 
    
    conf["useLLVMOptimizer"] = not args.no_opt # <-- disable llvm optimizers

    tstart = time.time()
    import tuplex
    ctx = tuplex.Context(conf)

    startup_time = time.time() - tstart
    print('Tuplex startup time: {}'.format(startup_time))
    tstart = time.time()
    ### QUERY HERE ###

    fork_event_pipeline(ctx, input_pattern, s3_output_path, sm)
    #push_event_pipeline(ctx, input_pattern, s3_output_path, sm)

    ### END QUERY ###
    run_time = time.time() - tstart

    job_time = time.time() - tstart
    print('Tuplex job time: {} s'.format(job_time))
    m = ctx.metrics
    print(ctx.options())
    print(m.as_json())
    # print stats as last line
    print(json.dumps({"startupTime": startup_time, "jobTime": job_time}))
