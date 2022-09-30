#!/usr/bin/env python3

# Tuplex based cleaning script
import tuplex
import tuplex.dataset
import time
import sys
import json
import os
import glob
import argparse

# UDF to be hyper-specialized
def fill_in_delays(row):
    # want to fill in data for missing carrier_delay, weather delay etc.
    # only need to do that prior to 2003/06

    year = row['YEAR']
    month = row['MONTH']
    arr_delay = row['ARR_DELAY']

    if year == 2003 and month < 6 or year < 2003:
        # fill in delay breakdown using model and complex logic
        if arr_delay is None:
            # stays None, because flight arrived early
            # if diverted though, need to add everything to div_arr_delay
            return {'year' : year, 'month' : month,
                    'day' : row['DAY_OF_MONTH'],
                    'carrier': row['OP_UNIQUE_CARRIER'],
                    'flightno' : row['OP_CARRIER_FL_NUM'],
                    'origin': row['ORIGIN_AIRPORT_ID'],
                    'dest': row['DEST_AIRPORT_ID'],
                    'distance' : row['DISTANCE'],
                    'dep_delay' : row['DEP_DELAY'],
                    'arr_delay': None,
                    'carrier_delay' : None,
                    'weather_delay': None,
                    'nas_delay' : None,
                    'security_delay': None,
                    'late_aircraft_delay' : None}
        elif arr_delay < 0.:
            # stays None, because flight arrived early
            # if diverted though, need to add everything to div_arr_delay
            return {'year' : year, 'month' : month,
                    'day' : row['DAY_OF_MONTH'],
                    'carrier': row['OP_UNIQUE_CARRIER'],
                    'flightno' : row['OP_CARRIER_FL_NUM'],
                    'origin': row['ORIGIN_AIRPORT_ID'],
                    'dest': row['DEST_AIRPORT_ID'],
                    'distance' : row['DISTANCE'],
                    'dep_delay' : row['DEP_DELAY'],
                    'arr_delay': row['ARR_DELAY'],
                    'carrier_delay' : None,
                    'weather_delay': None,
                    'nas_delay' : None,
                    'security_delay': None,
                    'late_aircraft_delay' : None}
        elif arr_delay < 5.:
            # it's an ontime flight, just attribute any delay to the carrier
            carrier_delay = arr_delay
            # set the rest to 0
            # ....
            return {'year' : year, 'month' : month,
                    'day' : row['DAY_OF_MONTH'],
                    'carrier': row['OP_UNIQUE_CARRIER'],
                    'flightno' : row['OP_CARRIER_FL_NUM'],
                    'origin': row['ORIGIN_AIRPORT_ID'],
                    'dest': row['DEST_AIRPORT_ID'],
                    'distance' : row['DISTANCE'],
                    'dep_delay' : row['DEP_DELAY'],
                    'arr_delay': row['ARR_DELAY'],
                    'carrier_delay' : carrier_delay,
                    'weather_delay': None,
                    'nas_delay' : None,
                    'security_delay': None,
                    'late_aircraft_delay' : None}
        else:
            # use model to determine everything and set into (join with weather data?)
            # i.e., extract here a couple additional columns & use them for features etc.!
            crs_dep_time = float(row['CRS_DEP_TIME'])
            crs_arr_time = float(row['CRS_ARR_TIME'])
            crs_elapsed_time = float(row['CRS_ELAPSED_TIME'])
            carrier_delay = 1024 + 2.7 * crs_dep_time - 0.2 * crs_elapsed_time
            weather_delay = 2000 + 0.09 * carrier_delay * (carrier_delay - 10.0)
            nas_delay = 3600 * crs_dep_time / 10.0
            security_delay = 7200 / crs_dep_time
            late_aircraft_delay = (20 + crs_arr_time) / (1.0 + crs_dep_time)
            return {'year' : year, 'month' : month,
                    'day' : row['DAY_OF_MONTH'],
                    'carrier': row['OP_UNIQUE_CARRIER'],
                    'flightno' : row['OP_CARRIER_FL_NUM'],
                    'origin': row['ORIGIN_AIRPORT_ID'],
                    'dest': row['DEST_AIRPORT_ID'],
                    'distance' : row['DISTANCE'],
                    'dep_delay' : row['DEP_DELAY'],
                    'arr_delay': row['ARR_DELAY'],
                    'carrier_delay' : carrier_delay,
                    'weather_delay': weather_delay,
                    'nas_delay' : nas_delay,
                    'security_delay': security_delay,
                    'late_aircraft_delay' : late_aircraft_delay}
    else:
        # just return it as is
        return {'year' : year, 'month' : month,
                'day' : row['DAY_OF_MONTH'],
                'carrier': row['OP_UNIQUE_CARRIER'],
                'flightno' : row['OP_CARRIER_FL_NUM'],
                'origin': row['ORIGIN_AIRPORT_ID'],
                'dest': row['DEST_AIRPORT_ID'],
                'distance' : row['DISTANCE'],
                'dep_delay' : row['DEP_DELAY'],
                'arr_delay': row['ARR_DELAY'],
                'carrier_delay' : row['CARRIER_DELAY'],
                'weather_delay':row['WEATHER_DELAY'],
                'nas_delay' : row['NAS_DELAY'],
                'security_delay': row['SECURITY_DELAY'],
                'late_aircraft_delay' : row['LATE_AIRCRAFT_DELAY']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Flights hyper specialization sampling experiment')
    args = parser.parse_args()

    if not 'AWS_ACCESS_KEY_ID' in os.environ or 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        raise Exception('Did not find AWS credentials in environment, please set.')

    lambda_size = "10000"
    lambda_threads = 2
    s3_scratch_dir = "s3://tuplex-leonhard/scratch/flights-exp"
    input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_2003_*.csv'
    s3_output_path = 's3://tuplex-leonhard/experiments/flights_hyper'

    # full dataset here (oO)
    input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_2002*.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2003*.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2004*.csv'
    # minimal example where double write occassionally occurs. I.e., was something not properly reset?
    #input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_2002_01.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2003_11.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2004_12.csv'
    use_hyper_specialization = False
    s3_output_path += '/sampling'

    print('>>> running {} on {} -> {}'.format('tuplex', input_pattern, s3_output_path))

    print('    hyperspecialization: {}'.format(use_hyper_specialization))

    # load data
    tstart = time.time()

    # configuration, make sure to give enough runtime memory to the executors!
    # run on Lambda
    conf = {"webui.enable" : False,
            "backend": "lambda",
            "aws.lambdaMemory": lambda_size,
            "aws.httpThreadCount": 36,
            "aws.lambdaThreads": lambda_threads,
            "aws.scratchDir": s3_scratch_dir,
            "experimental.hyperspecialization": use_hyper_specialization,
            "executorCount": 0,
            "executorMemory": "2G",
            "driverMemory": "2G",
            "partitionSize": "32MB",
            "runTimeMemory": "128MB",
            "useLLVMOptimizer": True,
            "optimizer.nullValueOptimization": False,
            "tuplex.redirectToPythonLogging": True,
            "resolveWithInterpreterOnly": False,
            "csv.selectionPushdown" : True}

    if os.path.exists('tuplex_config.json'):
        with open('tuplex_config.json') as fp:
            conf = json.load(fp)

    conf["optimizer.nullValueOptimization"] = True

    tstart = time.time()
    import tuplex
    ctx = tuplex.Context(conf)

    startup_time = time.time() - tstart
    print('Tuplex startup time: {}'.format(startup_time))
    tstart = time.time()
    ### QUERY HERE ###
    sm = tuplex.dataset.SamplingMode.ALL_FILES | tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.RANDOM_ROWS
    #sm = tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS
    sm = tuplex.dataset.SamplingMode.LAST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS

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
        ctx.csv(input_pattern, sampling_mode=sm).map(fill_in_delays).tocsv(s3_output_path)
        job_time = time.time() - tstart
        row = {'sampling_mode': str(sm), 'job_time': job_time, 'metrics': ctx.metrics.as_dict()}
        print('--- done ---')
        print(row)
        rows.append(row)

    print('SAMPLING RESULTS::')
    print(rows)

    ### END QUERY ###
    run_time = time.time() - tstart

    job_time = time.time() - tstart
    print('Tuplex job time: {} s'.format(job_time))
    m = ctx.metrics
    print(ctx.options())
    print(m.as_json())
    # print stats as last line
    print(json.dumps({"startupTime" : startup_time, "jobTime" : job_time}))
