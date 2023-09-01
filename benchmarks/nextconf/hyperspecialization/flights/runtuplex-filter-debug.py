#!/usr/bin/env python3

# Tuplex based cleaning script
import tuplex
import time
import sys
import json
import os
import glob
import argparse

# note: this doesn't work yet! Fix in tuplex
from udfs import *

def make_year_range(num_years):
    """
    helper function to create year range for year based experiment
    """
    if num_years <= 1:
        return [2003]
    start_year = max(1987, 2003 - (num_years - 1) // 2)
    end_year = start_year + num_years

    years = list(range(start_year, end_year))[:num_years]
    assert len(years) == num_years and 2003 in years
    return years

def run_pipeline(ctx, year_lower, year_upper, sm):
    ctx.csv(input_pattern, sampling_mode=sm) \
        .withColumn("features", extract_feature_vector) \
        .map(fill_in_delays) \
        .filter(lambda x: year_lower <= x['year'] <= year_upper) \
        .tocsv(s3_output_path)

# adjust settings for sampling experiment
def adjust_settings_for_sampling_exp(settings):
    settings['sample.maxDetectionMemory'] = '256KB'
    settings['sample.strataSize'] = 1
    settings['sample.samplesPerStrata'] = 1
    return settings

# adopted from sampling.runtuplex.py
def run_sampling_experiment(ctx, year_lower, year_upper):
    available_modes = [mode for mode in tuplex.dataset.SamplingMode]
    print('Following sampling modes are supported: {}'.format(available_modes))

    combos = [tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS,
              tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS,
              tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS,
              tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS,
              tuplex.dataset.SamplingMode.ALL_FILES | tuplex.dataset.SamplingMode.FIRST_ROWS,
              tuplex.dataset.SamplingMode.ALL_FILES | tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS]
    print('Testing sampling times for following modes: {}'.format(combos))

    rows = []
    for sm in combos:
        print('--> Running with Sampling mode {}'.format(sm))
        tstart = time.time()

        # run pipeline
        run_pipeline(ctx, year_lower, year_upper, sm)

        job_time = time.time() - tstart
        row = {'sampling_mode': str(sm), 'job_time': job_time, 'metrics': ctx.metrics.as_dict(), 'options': ctx.options()}
        print('--- done ---')
        print(row)
        rows.append(row)

    print('SAMPLING RESULTS::')
    print(rows)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Flights hyper specialization query')
    parser.add_argument('--no-hyper', dest='no_hyper', action="store_true",
                        help="deactivate hyperspecialization optimization explicitly.")
    parser.add_argument('--no-cf', dest='no_cf', action="store_true",
                        help="deactivate constant-folding optimization explicitly.")
    parser.add_argument('--no-nvo', dest='no_nvo', action="store_true",
                        help="deactivate null value optimization explicitly.")
    parser.add_argument('--python-mode', dest='python_mode', action="store_true",
                        help="process in pure python mode.")
    parser.add_argument('--internal-fmt', dest='use_internal_fmt',
                        help='if active, use the internal tuplec storage format for exceptions, no CSV format optimization',
                        action='store_true')
    parser.add_argument('--samples-per-strata', dest='samples_per_strata', default=10, help='how many samples to use per strata')
    parser.add_argument('--strata-size', dest='strata_size', default=1024,
                        help='how many samples to use per strata')
    parser.add_argument('--fast-client-sampling',  dest='fast_client_sampling', action="store_true",
                        help="whether to use fast client sampling, i.e. the original 256KB Tuplex uses.")
    parser.add_argument('--dataset', choices=["small", "full"], default='full', help='select whether to use full (all years) or small (2002-2005) dataset')
    parser.add_argument('--experiment', choices=["normal", "sampling"], default='normal', help='select whether to run query as is, or run cycling through sampling modes')
    parser.add_argument('--num-years', dest='num_years', action='store', choices=['auto'] + [str(year) for year in list(range(1, 2021-1987+2))], default='auto', help='if auto the range 2002-2005 will be used (equivalent to --num-years=4).')
    parser.add_argument('--sampling-mode', dest='sampling_mode', default=None, action='store', choices=['A', 'B', 'C', 'D', 'E'], help='select sampling mode')
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
    strata_size = args.strata_size
    samples_per_strata = args.samples_per_strata

    # full dataset here (oO)
    input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_*.csv'

    if args.dataset == 'small':
        input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_2002_*.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2003_*.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2004_*.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2005_*.csv'
    else:
        input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_*.csv'

    # manipulate inout pattern depending on files
    # here are the default values (auto)
    year_lower = 2002
    year_upper = 2005
    if args.num_years != 'auto':
        num_years = int(args.num_years)
        years = make_year_range(num_years)
        print('-- Running with filter over years: {}'.format(', '.join([str(year) for year in years])))
        year_lower = min(years)
        year_upper = max(years)
    print(f"-- filter: lambda x: {year_lower} <= x['year'] <= {year_upper}")
    
    # use following as debug pattern
    #input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_1987_10.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2021_10.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2021_11.csv'
    #input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_2002*.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2003*.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2004*.csv'
    sm_map = {'A' : tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS,
            'B': tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.FIRST_ROWS,
            'C':tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_FILE,
            'D':tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_FILE,
            'E':tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.ALL_FILES,
            'F':tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.ALL_FILES
            }

    sm = sm_map.get(args.sampling_mode, sm_map['D'])

    # use 'D' as default mode
    #if use_hyper_specialization:
    #    sm = sm_map['D']
    #else:
    #    sm = sm_map['D']

    #sm = sm | tuplex.dataset.SamplingMode.RANDOM_ROWS

    if use_hyper_specialization:
        s3_output_path += '/hyper'
    else:
        s3_output_path += '/general'

    # specialize sampling modes
    sampling_max_detection_rows = 10000
    sampling_max_detection_memory = '32MB'
    lam_sampling_max_detection_rows = sampling_max_detection_rows
    lam_sampling_max_detection_memory = sampling_max_detection_memory
    lam_strata_size = strata_size
    lam_samples_per_strata = samples_per_strata

    if args.fast_client_sampling:
        print('-- using fast client sampling (Tuplex mode)')
        sampling_max_detection_memory = '256KB'
        # deactivate stratified sampling like in original experiment, but keep for lambdas.
        samples_per_strata = 1
        strata_size = 1

    print('>>> running {} on {} -> {}'.format('tuplex', input_pattern, s3_output_path))
    print('    running in interpreter mode: {}'.format(args.python_mode))
    print('    hyperspecialization: {}'.format(use_hyper_specialization))
    print('    constant-folding: {}'.format(use_constant_folding))
    print('    null-value optimization: {}'.format(not args.no_nvo))
    print('    sampling mode: {} (={})'.format(args.sampling_mode, str(sm).replace('SamplingMode.', '')))
    print('    client:: strata: {} per {}'.format(samples_per_strata, strata_size))
    print('    lambda:: strata: {} per {}'.format(lam_samples_per_strata, lam_strata_size))
    # load data
    tstart = time.time()

    # configuration, make sure to give enough runtime memory to the executors!
    # run on Lambda
    conf = {"webui.enable" : False,
            "backend": "lambda",
            "aws.lambdaMemory": lambda_size,
            "aws.lambdaThreads": lambda_threads,
            "aws.lambdaTimeout": 900, # maximum allowed is 900s!
            "aws.httpThreadCount": 410,
            "aws.maxConcurrency": 410,
            'sample.maxDetectionRows': sampling_max_detection_rows,
            'sample.maxDetectionMemory': sampling_max_detection_memory,
            'sample.strataSize': strata_size,
            'sample.samplesPerStrata': samples_per_strata,
            'lambda.sample.maxDetectionRows': lam_sampling_max_detection_rows,
            'lambda.sample.maxDetectionMemory': lam_sampling_max_detection_memory,
            'lambda.sample.strataSize': lam_strata_size,
            'lambda.sample.samplesPerStrata': lam_samples_per_strata,
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
            "optimizer.filterPromotion": False,
            "optimizer.selectionPushdown": True,
            "useInterpreterOnly": args.python_mode,
            "experimental.forceBadParseExceptFormat": not args.use_internal_fmt}

    if os.path.exists('tuplex_config.json'):
        with open('tuplex_config.json') as fp:
            conf = json.load(fp)

    conf['inputSplitSize'] = '2GB' #'256MB' #'128MB'
    conf["tuplex.experimental.opportuneCompilation"] = True #False #True #False #True

    if args.no_nvo:
        conf["optimizer.nullValueOptimization"] = False
    else:
        conf["optimizer.nullValueOptimization"] = True

    if args.experiment == 'sampling':
        print('-- adjusting settings for sampling experiment (use Tuplex defaults)')
        conf = adjust_settings_for_sampling_exp(conf)

    tstart = time.time()
    import tuplex
    ctx = tuplex.Context(conf)

    startup_time = time.time() - tstart
    print('Tuplex startup time: {}'.format(startup_time))

    # check which experiment to use
    if args.experiment == 'sampling':
        run_sampling_experiment(ctx, year_lower, year_upper)
        sys.exit(0)
    else:
        assert args.experiment == 'normal', 'experiment should be normal'
        pass

    tstart = time.time()
    ### QUERY HERE ###

    run_pipeline(ctx, year_lower, year_upper, sm)

    ### END QUERY ###
    run_time = time.time() - tstart

    job_time = time.time() - tstart
    print('Tuplex job time: {} s'.format(job_time))
    m = ctx.metrics
    print(ctx.options())
    print(m.as_json())
    # print stats as last line
    print(json.dumps({"startupTime" : startup_time, "jobTime" : job_time}))
