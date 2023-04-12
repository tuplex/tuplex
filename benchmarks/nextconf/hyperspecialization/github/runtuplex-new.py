#!/usr/bin/env python3
# Github query for Viton paper

# Tuplex based cleaning script
import tuplex
import time
import sys
import json
import os
import glob
import argparse


def extract_repo_id(row):
    if 2012 <= row['year'] <= 2014:

        if row['type'] == 'FollowEvent':
            return row['payload']['target']['id']

        if row['type'] == 'GistEvent':
            return row['payload']['id']

        # this here doesn't work, because no fancy typed row object yet
        # repo = row.get('repository')
        repo = row['repository']

        if repo is None:
            return None
        return repo.get('id')
    else:
        return row['repo'].get('id')

def github_pipeline(ctx, input_pattern, s3_output_path, sm):

    ctx.json(input_pattern, True, True, sm) \
       .filter(lambda x: x['type'] == 'ForkEvent') \
       .withColumn('year', lambda x: int(x['created_at'].split('-')[0])) \
       .withColumn('repo_id', extract_repo_id) \
       .withColumn('commits', lambda row: row['payload'].get('commits')) \
       .withColumn('number_of_commits', lambda row: len(row['commits']) if row['commits'] else 0) \
       .selectColumns(['type', 'repo_id', 'year', 'number_of_commits']) \
       .tocsv(s3_output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Github hyper specialization query')
    parser.add_argument('--no-hyper', dest='no_hyper', action="store_true",
                        help="deactivate hyperspecialization optimization explicitly.")
    parser.add_argument('--no-promo', dest='no_promo', action="store_true",
                        help="deactivate filter-promotion optimization explicitly.")
    # constant-folding for now always deactivated.
    # parser.add_argument('--no-cf', dest='no_cf', action="store_true",
    #                     help="deactivate constant-folding optimization explicitly.")
    parser.add_argument('--no-nvo', dest='no_nvo', action="store_true",
                        help="deactivate null value optimization explicitly.")
    parser.add_argument('--internal-fmt', dest='use_internal_fmt',
                        help='if active, use the internal tuplex storage format for exceptions, no CSV/JSON format optimization',
                        action='store_true')
    parser.add_argument('--samples-per-strata', dest='samples_per_strata', default=10,
                        help='how many samples to use per strata')
    parser.add_argument('--strata-size', dest='strata_size', default=1024,
                        help='how many samples to use per strata')
    args = parser.parse_args()

    if not 'AWS_ACCESS_KEY_ID' in os.environ or 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        raise Exception('Did not find AWS credentials in environment, please set.')

    lambda_size = "10000"
    lambda_threads = 3
    s3_scratch_dir = "s3://tuplex-leonhard/scratch/github-exp"
    use_hyper_specialization = not args.no_hyper
    use_filter_promotion = not args.no_promo
    use_constant_folding = False # deactivate explicitly
    input_pattern = 's3://tuplex-public/data/github_daily/*.json'
    s3_output_path = 's3://tuplex-leonhard/experiments/github'
    strata_size = args.strata_size
    samples_per_strata = args.samples_per_strata
    input_split_size = "2GB"

    # use following as debug pattern
    sm_map = {'A': tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS,
              'B': tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.FIRST_ROWS,
              'C': tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_FILE,
              'D': tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_FILE,
              'E': tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.ALL_FILES,
              'F': tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.ALL_FILES
              }

    sm = sm_map['D']  # ism_map.get(args.sampling_mode, None)
    sm = sm_map['B']

    if use_hyper_specialization:
        sm = sm_map['D']
    else:
        sm = sm_map['D']
    # manipulate output path

    if use_hyper_specialization:
        s3_output_path += '/hyper'
    else:
        s3_output_path += '/general'

    print('>>> running {} on {} -> {}'.format('tuplex', input_pattern, s3_output_path))

    print('    hyperspecialization: {}'.format(use_hyper_specialization))
    print('    constant-folding: {}'.format(use_constant_folding))
    print('    filter-promotion: {}'.format(use_filter_promotion))
    print('    null-value optimization: {}'.format(not args.no_nvo))
    print('    strata: {} per {}'.format(samples_per_strata, strata_size))
    # load data
    tstart = time.time()

    # configuration, make sure to give enough runtime memory to the executors!
    # run on Lambda
    conf = {"webui.enable": False,
            "backend": "lambda",
            "aws.lambdaMemory": lambda_size,
            "aws.lambdaThreads": lambda_threads,
            "aws.httpThreadCount": 410,
            "aws.maxConcurrency": 410,
            'sample.maxDetectionMemory': '32MB',
            'sample.strataSize': strata_size,
            'sample.samplesPerStrata': samples_per_strata,
            "aws.scratchDir": s3_scratch_dir,
            "autoUpcast": True,
            "experimental.hyperspecialization": use_hyper_specialization,
            "executorCount": 0,
            "executorMemory": "2G",
            "driverMemory": "2G",
            "partitionSize": "32MB",
            "runTimeMemory": "128MB",
            "useLLVMOptimizer": True,
            "optimizer.generateParser": False,  # not supported on lambda yet
            "optimizer.nullValueOptimization": True,
            "resolveWithInterpreterOnly": False,
            "optimizer.constantFoldingOptimization": use_constant_folding,
            "optimizer.filterPromotion": use_filter_promotion,
            "optimizer.selectionPushdown": True,
            "experimental.forceBadParseExceptFormat": not args.use_internal_fmt}

    if os.path.exists('tuplex_config.json'):
        with open('tuplex_config.json') as fp:
            conf = json.load(fp)

    conf['inputSplitSize'] = '2GB'  # '256MB' #'128MB'
    conf["tuplex.experimental.opportuneCompilation"] = True  # False #True #False #True

    if args.no_nvo:
        conf["optimizer.nullValueOptimization"] = False
    else:
        conf["optimizer.nullValueOptimization"] = True

    conf["inputSplitSize"] = input_split_size

    tstart = time.time()
    import tuplex

    ctx = tuplex.Context(conf)

    startup_time = time.time() - tstart
    print('Tuplex startup time: {}'.format(startup_time))
    tstart = time.time()
    ### QUERY HERE ###

    github_pipeline(ctx, input_pattern, s3_output_path, sm)

    ### END QUERY ###
    run_time = time.time() - tstart

    job_time = time.time() - tstart
    print('Tuplex job time: {} s'.format(job_time))
    m = ctx.metrics
    print(ctx.options())
    print(m.as_json())
    # print stats as last line
    print(json.dumps({"startupTime": startup_time, "jobTime": job_time}))
