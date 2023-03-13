#!/usr/bin/env python3

# Tuplex based cleaning script
import tuplex
import time
import sys
import json
import os
import glob
import argparse
from .udfs import *

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Flights hyper specialization query')
    parser.add_argument('--no-hyper', dest='no_hyper', action="store_true",
                        help="deactivate hyperspecialization optimization explicitly.")
    parser.add_argument('--no-cf', dest='no_cf', action="store_true",
                        help="deactivate constant-folding optimization explicitly.")
    parser.add_argument('--no-nvo', dest='no_nvo', action="store_true",
                        help="deactivate null value optimization explicitly.")
    parser.add_argument('--internal-fmt', dest='use_internal_fmt',
                        help='if active, use the internal tuplec storage format for exceptions, no CSV format optimization',
                        action='store_true')
    args = parser.parse_args()

    if not 'AWS_ACCESS_KEY_ID' in os.environ or 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        raise Exception('Did not find AWS credentials in environment, please set.')

    lambda_size = "10000"
    lambda_threads = 3
    s3_scratch_dir = "s3://tuplex-leonhard/scratch/flights-exp"
    use_hyper_specialization = not args.no_hyper
    use_constant_folding = not args.no_cf
    input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_2003_*.csv'
    s3_output_path = 's3://tuplex-leonhard/experiments/flights_hyper'

    # full dataset here (oO)
    input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_*.csv'
    
    # use following as debug pattern
    #input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_1987_10.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2000_10.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2021_11.csv'
    #input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_2002*.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2003*.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2004*.csv'
    sm_map = {'A' : tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS,
            'B': tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.FIRST_ROWS,
            'C':tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_FILE,
            'D':tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_FILE,
            'E':tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.ALL_FILES,
            'F':tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.ALL_FILES
            }

    sm = sm_map['D'] #ism_map.get(args.sampling_mode, None)
    sm = sm_map['B']

    if use_hyper_specialization:
        sm = sm_map['D']
        #sm = tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.RANDOM_ROWS
        #sm = sm_map['A'] 
        #sm = sm | tuplex.dataset.SamplingMode.RANDOM_ROWS
    else:
        sm = sm_map['D']
        #sm = sm_map['A']

    #sm = sm | tuplex.dataset.SamplingMode.RANDOM_ROWS

    if use_hyper_specialization:
        s3_output_path += '/hyper'
    else:
        s3_output_path += '/general'

    print('>>> running {} on {} -> {}'.format('tuplex', input_pattern, s3_output_path))

    print('    hyperspecialization: {}'.format(use_hyper_specialization))
    print('    constant-folding: {}'.format(use_constant_folding))
    print('    null-value optimization: {}'.format(not args.no_nvo))

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
            'tuplex.sample.maxDetectionMemory': '256KB',
            "aws.scratchDir": s3_scratch_dir,
            "autoUpcast":True,
            "experimental.hyperspecialization": use_hyper_specialization,
            "executorCount": 0,
            "executorMemory": "2G",
            "driverMemory": "2G",
            "partitionSize": "32MB",
            "runTimeMemory": "128MB",
            "useLLVMOptimizer": True,
            "optimizer.generateParser":False, # not supported on lambda yet
            "optimizer.nullValueOptimization": True,
            "resolveWithInterpreterOnly": False,
            "optimizer.constantFoldingOptimization": use_constant_folding,
            "optimizer.selectionPushdown" : True,
            "experimental.forceBadParseExceptFormat": not args.use_internal_fmt}

    if os.path.exists('tuplex_config.json'):
        with open('tuplex_config.json') as fp:
            conf = json.load(fp)

    conf['inputSplitSize'] = '2GB' #'256MB' #'128MB'
    conf["tuplex.experimental.opportuneCompilation"] = True #False #True

    if args.no_nvo:
        conf["optimizer.nullValueOptimization"] = False
    else:
        conf["optimizer.nullValueOptimization"] = True

    tstart = time.time()
    import tuplex
    ctx = tuplex.Context(conf)

    startup_time = time.time() - tstart
    print('Tuplex startup time: {}'.format(startup_time))
    tstart = time.time()
    ### QUERY HERE ###

    #debug
    #input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_1999_05.csv'
    # input_pattern = "s3://tuplex-public/data/flights_all/flights_on_time_performance_1987_10.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2000_02.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2021_11.csv"

    ctx.csv(input_pattern, sampling_mode=sm) \
        .withColumn("features", extract_feature_vector) \
        .map(fill_in_delays) \
        .filter(lambda x: 2000 <= x['year'] <= 2005) \
        .tocsv(s3_output_path)

    ### END QUERY ###
    run_time = time.time() - tstart

    job_time = time.time() - tstart
    print('Tuplex job time: {} s'.format(job_time))
    m = ctx.metrics
    print(ctx.options())
    print(m.as_json())
    # print stats as last line
    print(json.dumps({"startupTime" : startup_time, "jobTime" : job_time}))
