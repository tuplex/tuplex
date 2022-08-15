import tuplex
import os
import argparse
import time
import json
import glob

# Parser args
parser = argparse.ArgumentParser(description='Flight data query testing')
parser.add_argument('--data-path', type=str, dest='data_path', default='data',
                    help='path to flight data')
parser.add_argument('--output-path', type=str, dest='output_path', default='output',
                    help='path to save output data')

args = parser.parse_args()
data_path = args.data_path
output_path = args.output_path

# Validate and create paths
assert os.path.isdir(data_path), 'data path does not exist'

if not os.path.isdir(output_path):
    os.makedirs(output_path)
for f in glob.glob(os.path.join(output_path, "*.csv")):
    os.remove(f)

flights_path = os.path.join(data_path, 'flights*.csv')

# Tuplex config
conf = {
    "webui.enable": False,
    "executorCount": 7,
    "executorMemory": "1GB",
    "driverMemory": "1GB",
    "partitionSize": "64MB",
    "runTimeMemory": "128MB",
    "useLLVMOptimizer": False,
    "optimizer.nullValueOptimization": False,
    "csv.selectionPushdown": False,
    "optimizer.generateParser": True,
    "resolveWithInterpreterOnly": False,
}

if os.path.exists('tuplex_config.json'):
    with open('tuplex_config.json') as fp:
        conf = json.load(fp)

tstart = time.time()

ctx = tuplex.Context(conf)

startup_time = time.time() - tstart
print('Tuplex startup time: {}'.format(startup_time))

# Query
ds = ctx.csv(flights_path)
#
# # ARR_TIME = CRS_ARR_TIME + DELAY
# def validate_arrival_delay(row):
#     actual = row['ARR_TIME']
#     estimated = row['CRS_ARR_TIME']
#     delay = row['ARR_DELAY']
#
#     actual_mins = actual // 100 * 60 + actual % 100
#     estimated_mins = estimated // 100 * 60 + estimated % 100
#
#     expected_actual_mins = (estimated_mins + delay) % 1440
#     actual_mins = actual_mins % 1440
#
#     return actual_mins != expected_actual_mins
#
# # DEP_TIME == CRS_DEP_TIME + DELAY
# def validate_departure_delay(row):
#     actual = row['DEP_TIME']
#     estimated = row['CRS_DEP_TIME']
#     delay = row['DEP_DELAY']
#
#     actual_mins = actual // 100 * 60 + actual % 100
#     estimated_mins = estimated // 100 * 60 + estimated % 100
#
#     expected_actual_mins = (estimated_mins + delay) % 1440
#     actual_mins = actual_mins % 1440
#
#     return actual_mins != expected_actual_mins
# # SCHEMA CHANGE, RESOLVE NULL
# # WHEELS_OFF + AIR_TIME == WHEELS_ON
# def validate_air_time(row):
#     air_time = row['AIR_TIME']
#     wheels_off = row['WHEELS_OFF']
#     wheels_on = row['WHEELS_ON']
#
#     wheels_off_mins = wheels_off // 100 * 60 + wheels_off % 100
#     wheels_on_mins = wheels_on // 100 * 60 + wheels_on % 100
#
#     expected_wheels_on_mins = (wheels_off_mins + air_time) % 1440
#     wheels_on_mins = wheels_on_mins % 1440
#
#     return expected_wheels_on_mins != wheels_on_mins
#
# # ACTUAL_ELAPSED_TIME == TAXI_OUT + AIR_TIME + TAXI_IN
# def validate_actual_elapsed_time(row):
#     taxi_out = row['TAXI_OUT']
#     taxi_in = row['TAXI_IN']
#     air_time = row['AIR_TIME']
#     actual_elapsed_time = row['ACTUAL_ELAPSED_TIME']
#
#     return taxi_out + air_time + taxi_in != actual_elapsed_time
#
# # WHEELS_OFF == DEP_TIME + TAXI_OUT
# def validate_wheels_off(row):
#     dep_time = row['DEP_TIME']
#     taxi_out = row['TAXI_OUT']
#     wheels_off = row['WHEELS_OFF']
#
#     dep_time_mins = dep_time // 100 * 60 + dep_time % 100
#     wheels_off_mins = wheels_off // 100 * 60 + wheels_off % 100
#
#     expected_wheels_off_mins = (dep_time_mins + taxi_out) % 1440
#     wheels_off_mins = wheels_off_mins % 1440
#
#     return expected_wheels_off_mins != wheels_off_mins
#
# # ARR_TIME == WHEELS_ON_TIME + TAXI_IN
# def validate_wheels_on(row):
#     arr_time = row['ARR_TIME']
#     taxi_in = row['TAXI_IN']
#     wheels_on = row['WHEELS_ON']
#
#     arr_time_mins = arr_time // 100 * 60 + arr_time % 100
#     wheels_on_mins = wheels_on // 100 * 60 + wheels_on % 100
#
#     expected_arr_time_mins = (wheels_on_mins + taxi_in) % 1440
#     arr_time_mins = arr_time_mins % 1440
#
#     return expected_arr_time_mins != arr_time_mins
#
# def type_error(row):
#     arr_time = row['ARR_TIME']
#     taxi_in = row['TAXI_IN']
#     wheels_on = row['WHEELS_ON']
#
#     return arr_time is None or taxi_in is None or wheels_on is None
#
# # ds = ds.selectColumns(['YEAR', 'MONTH', 'DAY_OF_MONTH', 'OP_CARRIER', 'ORIGIN_AIRPORT_SEQ_ID', 'ORIGIN', 'DEST',
# #                        'CRS_DEP_TIME', 'DEP_TIME', 'DEP_DELAY',
# #                        'TAXI_OUT', 'WHEELS_OFF', 'WHEELS_ON', 'TAXI_IN',
# #                        'CRS_ARR_TIME', 'ARR_TIME', 'ARR_DELAY',
# #                        'CRS_ELAPSED_TIME', 'ACTUAL_ELAPSED_TIME', 'AIR_TIME',
# #                        'DISTANCE'])
#
# def calculate_elapsed_time(row):
#     dep_time = row['DEP_TIME']
#     arr_time = row['ARR_TIME']
#     actual_elapsed_time = row['ACTUAL_ELAPSED_TIME']
#
#     dep_time_mins = dep_time // 100 * 60 + dep_time % 100
#     arr_time_mins = arr_time // 100 * 60 + arr_time % 100
#
#     calculated_elapsed_time = arr_time_mins - dep_time_mins
#
#     assert calculated_elapsed_time == actual_elapsed_time
#
#     return calculated_elapsed_time
#
# def resolve_calculate_elapsed_time(row):
#     dep_time = row['DEP_TIME']
#     arr_time = row['ARR_TIME']
#     actual_elapsed_time = row['ACTUAL_ELAPSED_TIME']

# def extract_departure_delay(row):
#     crs_dep_time = row['CRS_DEP_TIME']
#     dep_time = row['DEP_TIME']
#
#
#
# def extract_elapsed_time(row):
#     dep_time = row['DEP_TIME']
#     arr_time = row['ARR_TIME']
#
#     assert dep_time < arr_time
#
#     dep_time_mins = dep_time // 100 * 60 + dep_time % 100
#     arr_time_mins = arr_time // 100 * 60 + arr_time % 100
#
#     return arr_time_mins - dep_time_mins
#
# def resolve_elapsed_time(row):
#     dep_time = row['DEP_TIME']
#     arr_time = row['ARR_TIME']
#
#     dep_time_mins = dep_time // 100 * 60 + dep_time % 100
#     arr_time_mins = arr_time // 100 * 60 + arr_time % 100
#
#     arr_time_mins += 1440
#
#     return arr_time_mins - dep_time_mins
#
# def extract_delay(row):
#     calculated_elapsed_time = row['CALCULATED_ELAPSED_TIME']
#     actual_elapsed_time = row['ACTUAL_ELAPSED_TIME']
#     crs_elapsed_time = row['CRS_ELAPSED_TIME']
#
#     assert calculated_elapsed_time == actual_elapsed_time
#
#     return calculated_elapsed_time - crs_elapsed_time
#
# def resolve_delay(row):
#     calculated_elapsed_time = row['CALCULATED_ELAPSED_TIME']
#     actual_elapsed_time = row['ACTUAL_ELAPSED_TIME']
#     crs_elapsed_time = row['CRS_ELAPSED_TIME']
#
#     assert (calculated_elapsed_time - actual_elapsed_time) % 60 == 0
#
#     return actual_elapsed_time - crs_elapsed_time
#
#
#
#
# def extract_delay_2(row):
#     arr_delay = row['ARR_DELAY']
#     arr_time = row['ARR_TIME']
#     crs_arr_time = row['CRS_ARR_TIME']
#
#     arr_time_mins = arr_time // 100 * 60 + arr_time % 100
#     crs_arr_time_mins = crs_arr_time // 100 * 60 + crs_arr_time % 100
#
#     assert arr_time_mins - crs_arr_time_mins == arr_delay
#
#     return arr_delay

# ds = ds.withColumn('CALCULATED_ELAPSED_TIME', extract_elapsed_time)
# # ds = ds.resolve(AssertionError, resolve_elapsed_time)
#
# ds = ds.withColumn('CALCULATED_DELAY', extract_delay)
# ds = ds.resolve(AssertionError, resolve_delay)

# ds = ds.withColumn('CALCULATED_DELAY_2', extract_delay_2)
#
# def view_exceptions(row):
#     calculated_elapsed_time = row['CALCULATED_ELAPSED_TIME']
#     actual_elapsed_time = row['ACTUAL_ELAPSED_TIME']
#
#     return (calculated_elapsed_time - actual_elapsed_time) % 60 != 0

# ds = ds.filter(view_exceptions)
#
# ds = ds.selectColumns(['CALCULATED_DELAY_2'])

# def validate_arr_delay(row):
#     arr_delay = row['ARR_DELAY']
#     arr_time = row['ARR_TIME']
#     crs_arr_time = row['CRS_ARR_TIME']
#
#     arr_time_mins = arr_time // 100 * 60 + arr_time % 100
#     crs_arr_time_mins = crs_arr_time // 100 * 60 + crs_arr_time % 100
#
#     assert crs_arr_time_mins + arr_delay == arr_time_mins
#
#     return arr_delay
#
# def resolve_validate_arr_delay_2(row):
#     arr_delay = row['ARR_DELAY']
#     arr_time = row['ARR_TIME']
#     crs_arr_time = row['CRS_ARR_TIME']
#
#     arr_time_mins = arr_time // 100 * 60 + arr_time % 100
#     crs_arr_time_mins = crs_arr_time // 100 * 60 + crs_arr_time % 100
#
#     assert (crs_arr_time_mins + arr_delay) % 1440 == arr_time_mins
#
#     return arr_delay
#
# def resolve_validate_arr_delay_2(row):
#     arr_delay = row['ARR_DELAY']
#     arr_time = row['ARR_TIME']
#     crs_arr_time = row['CRS_ARR_TIME']
#
#     arr_time_mins = arr_time // 100 * 60 + arr_time % 100
#     crs_arr_time_mins = crs_arr_time // 100 * 60 + crs_arr_time % 100
#
#     assert (crs_arr_time_mins + arr_delay) % 1440 == arr_time_mins % 1440
#
#     return arr_delay
#
# def is_on_time(row):
#     arr_delay = row['VALIDATED_ARR_DELAY']
#     return arr_delay < 15
#
# def resolve_is_on_time(row):
#     cancelled = row['CANCELLED']
#     diverted = row['DIVERTED']
#     return cancelled != 1 and diverted != 1

# ds = ds.withColumn('VALIDATED_ARR_DELAY', validate_arr_delay)
# ds = ds.resolve(AssertionError, resolve_validate_arr_delay)
# ds = ds.resolve(AssertionError, resolve_validate_arr_delay_2)
# ds = ds.withColumn('IS_ON_TIME', is_on_time)
# ds = ds.resolve(TypeError, resolve_is_on_time)

def extract_crs_dep_time_mins(row):
    crs_dep_time = row['CRS_DEP_TIME']
    return crs_dep_time // 100 * 60 + crs_dep_time % 100 if crs_dep_time else None

def extract_dep_time_mins(row):
    dep_time = row['DEP_TIME']
    return dep_time // 100 * 60 + dep_time % 100 if dep_time else None

def validate_dep_delay(row):
    dep_delay = row['DEP_DELAY']
    crs_dep_time_mins = row['CRS_DEP_TIME_MINS']
    dep_time_mins = row['DEP_TIME_MINS']

    # assert dep_delay is None or (crs_dep_time_mins + dep_delay == dep_time_mins)
    assert (crs_dep_time_mins + dep_delay == dep_time_mins)

    return dep_delay

def resolve_dep_delay_1(row):
    dep_delay = row['DEP_DELAY']
    crs_dep_time_mins = row['CRS_DEP_TIME_MINS']
    dep_time_mins = row['DEP_TIME_MINS']

    # assert dep_delay is None or ((crs_dep_time_mins + dep_delay) % 1440 == dep_time_mins)
    assert ((crs_dep_time_mins + dep_delay) % 1440 == dep_time_mins)

    return dep_delay

def resolve_dep_delay_2(row):
    dep_delay = row['DEP_DELAY']
    crs_dep_time_mins = row['CRS_DEP_TIME_MINS']
    dep_time_mins = row['DEP_TIME_MINS']

    # assert dep_delay is None or ((crs_dep_time_mins + dep_delay) % 1440 == dep_time_mins % 1440)
    assert ((crs_dep_time_mins + dep_delay) % 1440 == dep_time_mins % 1440)

    return dep_delay

def extract_crs_arr_time_mins(row):
    crs_arr_time = row['CRS_ARR_TIME']
    return crs_arr_time // 100 * 60 + crs_arr_time % 100 if crs_arr_time else None

def extract_arr_time_mins(row):
    arr_time = row['ARR_TIME']
    return arr_time // 100 * 60 + arr_time % 100 if arr_time else None

def validate_arr_delay(row):
    arr_delay = row['ARR_DELAY']
    crs_arr_time_mins = row['CRS_ARR_TIME_MINS']
    arr_time_mins = row['ARR_TIME_MINS']

    # assert arr_delay is None or (crs_arr_time_mins + arr_delay == arr_time_mins)
    assert (crs_arr_time_mins + arr_delay == arr_time_mins)

    return arr_delay

def resolve_arr_delay_1(row):
    arr_delay = row['ARR_DELAY']
    crs_arr_time_mins = row['CRS_ARR_TIME_MINS']
    arr_time_mins = row['ARR_TIME_MINS']

    # assert arr_delay is None or ((crs_arr_time_mins + arr_delay) % 1440 == arr_time_mins)
    assert ((crs_arr_time_mins + arr_delay) % 1440 == arr_time_mins)

    return arr_delay

def resolve_arr_delay_2(row):
    arr_delay = row['ARR_DELAY']
    crs_arr_time_mins = row['CRS_ARR_TIME_MINS']
    arr_time_mins = row['ARR_TIME_MINS']

    # assert arr_delay is None or ((crs_arr_time_mins + arr_delay) % 1440 == arr_time_mins % 1440)
    assert ((crs_arr_time_mins + arr_delay) % 1440 == arr_time_mins % 1440)

    return arr_delay

ds = ds.withColumn('CRS_DEP_TIME_MINS', extract_crs_dep_time_mins)
ds = ds.withColumn('DEP_TIME_MINS', extract_dep_time_mins)
ds = ds.withColumn('DEP_DELAY', validate_dep_delay)
ds = ds.resolve(AssertionError, resolve_dep_delay_1)
ds = ds.resolve(AssertionError, resolve_dep_delay_2)

ds = ds.withColumn('CRS_ARR_TIME_MINS', extract_crs_arr_time_mins)
ds = ds.withColumn('ARR_TIME_MINS', extract_arr_time_mins)
ds = ds.withColumn('ARR_DELAY', validate_arr_delay)
ds = ds.resolve(AssertionError, resolve_arr_delay_1)
ds = ds.resolve(AssertionError, resolve_arr_delay_2)

ds = ds.selectColumns(['ARR_DELAY'])
# ds = ds.selectColumns(['IS_ON_TIME'])
#
# def extract_delay(row):
#     crs_elapsed_time = row['CRS_ELAPSED_TIME']
#     actual_elapsed_time = row['ACTUAL_ELAPSED_TIME']
#
#     if crs_elapsed_time

ds.tocsv(output_path)
