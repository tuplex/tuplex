#!/usr/bin/env python3
# Github query for Viton paper
import logging
import pathlib
from typing import Optional

# Tuplex based cleaning script
# import tuplex
import time
import sys
import json
import os
import glob
import argparse
import logging
import csv


# default parameters to use for paths, scratch dirs
S3_DEFAULT_INPUT_PATTERN='s3://tuplex-public/data/github_daily/*.json'
S3_DEFAULT_OUTPUT_PATH='s3://tuplex-leonhard/experiments/github'
S3_DEFAULT_SCRATCH_DIR="s3://tuplex-leonhard/scratch/github-exp"

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

def human_readable_size(size, decimal_places=2):
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']:
        if size < 1024.0 or unit == 'PiB':
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"


def process_path_with_python(input_path, dest_output_path):

    # handwritten pipeline (optimized)
    # this resembles the following pipeline to process a JSON file to a CSV file
    #     ctx.json(input_pattern, True, True, sm) \
    #        .filter(lambda x: x['type'] == 'ForkEvent') \
    #        .withColumn('year', lambda x: int(x['created_at'].split('-')[0])) \
    #        .withColumn('repo_id', extract_repo_id) \
    #        .withColumn('commits', lambda row: row['payload'].get('commits')) \
    #        .withColumn('number_of_commits', lambda row: len(row['commits']) if row['commits'] else 0) \
    #        .selectColumns(['type', 'repo_id', 'year', 'number_of_commits']) \
    #        .tocsv(s3_output_path)
    tstart = time.time()
    rows = []
    num_input_rows = 0
    with open(input_path, 'r') as fp:
        for line in fp.readlines():
            row = json.loads(line.strip())
            num_input_rows += 1

            # .filter(lambda x: x['type'] == 'ForkEvent')
            if row['type'] != 'ForkEvent':
                continue

            # .withColumn('year', lambda x: int(x['created_at'].split('-')[0])) \
            row['year'] = int(row['created_at'].split('-')[0])

            # .withColumn('repo_id', extract_repo_id)
            row['repo_id'] = extract_repo_id(row)

            # .withColumn('commits', lambda row: row['payload'].get('commits'))
            row['commits'] = row['payload'].get('commits')

            # .withColumn('number_of_commits', lambda row: len(row['commits']) if row['commits'] else 0)
            row['number_of_commits'] = len(row['commits']) if row['commits'] else 0

            row = {'type': row['type'], 'repo_id': row['repo_id'],
                   'year':row['year'], 'commits': row['number_of_commits']}
            rows.append(row)

    # write out as CSV (escape each)
    if rows:

        # create parent folder first
        pathlib.Path(dest_output_path).parent.mkdir(parents=True, exist_ok=True)

        with open(dest_output_path, 'w', newline='') as csvfile:
            fieldnames = sorted([str(key) for key in rows[0].keys()])
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    if os.path.exists(dest_output_path):
        output_result = human_readable_size(os.path.getsize(dest_output_path))
    else:
        output_result = "skipped"

    num_output_rows = len(rows)

    duration = time.time() - tstart
    logging.info(f"Done in {duration:.2f}s, wrote output to {dest_output_path} ({output_result}, {num_output_rows} rows)")

    return {'output_path': dest_output_path, 'duration': duration, 'num_output_rows': num_output_rows, 'num_input_rows': num_input_rows}


def run_with_python_baseline(args):
    startup_time = 0
    job_time = 0

    output_path = args.output_path
    input_pattern = args.input_pattern
    scratch_dir = args.scratch_dir

    if not output_path:
        raise ValueError('No output path specified')
    if not input_pattern:
        raise ValueError('No input_pattern specified')
    # if not scratch_dir:
    #     raise ValueError('No scratch directory specified')

    tstart = time.time()

    # Step 1: glob files (python only supports local mode (?) )
    input_paths = sorted(glob.glob(input_pattern))
    total_input_size = sum(map(lambda path: os.path.getsize(path), input_paths))
    logging.info(f"Found {len(input_paths)} input paths, total size: {human_readable_size(total_input_size)}")

    # Process each file now using hand-written pipeline
    total_output_rows = 0
    total_input_rows = 0
    for part_no, path in enumerate(input_paths):
        logging.info(f"Processing path {part_no+1}/{len(input_paths)}: {path} ({human_readable_size(os.path.getsize(path))})")
        ans = process_path_with_python(path, os.path.join(output_path, "part_{:04d}.csv".format(part_no)))
        total_output_rows += ans['num_output_rows']
        total_input_rows += ans['num_input_rows']

    job_time = time.time() - tstart
    logging.info(f'total output rows: {total_output_rows}')
    stats = {"startup_time_in_s": startup_time, "job_time_in_s": job_time, 'mode': 'tuplex',
             'output_path': output_path,
             'input_path': input_pattern, 'scratch_path': scratch_dir, 'total_input_paths_size_in_bytes': total_input_size,
             'total_output_rows': total_output_rows, 'total_input_rows': total_input_rows}
    return stats

def github_pipeline(ctx, input_pattern, s3_output_path, sm):

    ctx.json(input_pattern, True, True, sm) \
       .filter(lambda x: x['type'] == 'ForkEvent') \
       .withColumn('year', lambda x: int(x['created_at'].split('-')[0])) \
       .withColumn('repo_id', extract_repo_id) \
       .withColumn('commits', lambda row: row['payload'].get('commits')) \
       .withColumn('number_of_commits', lambda row: len(row['commits']) if row['commits'] else 0) \
       .selectColumns(['type', 'repo_id', 'year', 'number_of_commits']) \
       .tocsv(s3_output_path)


def run_with_tuplex(args):
    if not 'AWS_ACCESS_KEY_ID' in os.environ or 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        raise Exception('Did not find AWS credentials in environment, please set.')

    # if paths are None, use per default S3 ones
    lambda_size = "10000"
    lambda_threads = 3
    s3_scratch_dir = args.scratch_dir or S3_DEFAULT_SCRATCH_DIR
    use_hyper_specialization = not args.no_hyper
    use_filter_promotion = not args.no_promo
    use_constant_folding = False  # deactivate explicitly
    input_pattern = args.input_pattern or S3_DEFAULT_INPUT_PATTERN
    s3_output_path = args.output_pattern or S3_DEFAULT_OUTPUT_PATH
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
    print('    running in interpreter mode: {}'.format(args.python_mode))
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
            "aws.lambdaTimeout": 900,  # maximum allowed is 900s!
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
            "useInterpreterOnly": args.python_mode,
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
    stats = {"startup_time_in_s": startup_time, "job_time_in_s": job_time, 'mode': 'tuplex', 'output_path': s3_output_path,
             'input_path': input_pattern, 'scratch_path': s3_scratch_dir, 'options': ctx.options(), 'metrics': json.loads(m.as_json())}
    return stats

def setup_logging(log_path:Optional[str]) -> None:

    LOG_FORMAT="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
    LOG_DATE_FORMAT="%d/%b/%Y %H:%M:%S"

    handlers = []

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter(LOG_FORMAT)
    # tell the handler to use this format
    console.setFormatter(formatter)

    handlers.append(console)

    # add file handler to root logger
    if log_path:
        handler = logging.FileHandler(log_path)
        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)
        handlers.append*handler

        # set up logging to file - see previous section for more details
    logging.basicConfig(level=logging.INFO,
                        format=LOG_FORMAT,
                        datefmt=LOG_DATE_FORMAT,
                        handlers=handlers)


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
    parser.add_argument('--python-mode', dest='python_mode', action="store_true",
                        help="process in pure python mode.")
    parser.add_argument('--internal-fmt', dest='use_internal_fmt',
                        help='if active, use the internal tuplex storage format for exceptions, no CSV/JSON format optimization',
                        action='store_true')
    parser.add_argument('--samples-per-strata', dest='samples_per_strata', default=10,
                        help='how many samples to use per strata')
    parser.add_argument('--strata-size', dest='strata_size', default=1024,
                        help='how many samples to use per strata')
    parser.add_argument('--m', '--mode', dest='mode', choices=['tuplex', 'python'], default='tuplex', help='select whether to run benchmark using python baseline or tuplex')
    parser.add_argument('--input-pattern', default=None, dest='input_pattern', help='input files to read into github pipeline')
    parser.add_argument('--output-path', default=None, dest='output_path', help='where to store result of pipeline')
    parser.add_argument('--scratch-dir', default=None, dest='scratch_dir', help='where to store intermediate results')
    parser.add_argument('--log-path', default=None, dest='log_path', help='specify optional path where to store experiment log results.')
    parser.add_argument('--result_path', default='results.ndjson', help='new-line delimited JSON formatted result file')
    args = parser.parse_args()

    # set up logging, by default always render to console. If log path is present, store file as well
    setup_logging(args.log_path)
    logging.info("Running Github query benchmark for Tuplex/Viton")
    if args.log_path is not None:
        logging.info("Saving logs to {}".format(args.log_path))

    if args.mode == 'tuplex':
        ans = run_with_tuplex(args)
    elif args.mode == 'python':
        ans = run_with_python_baseline(args)

    logging.info(f"pipeline in mode {args.mode} took {ans['job_time_in_s']:.2f} seconds")
    logging.info(f"Storing results in {args.result_path} via append")
    with open(args.result_path, 'a') as f:
        json.dump(ans, f, sort_keys=True)
        f.write('\n')
    logging.info("Done.")





