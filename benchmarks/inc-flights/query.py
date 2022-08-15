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


def extract_elapsed_time(row):
    dep_time = row['DEP_TIME']
    arr_time = row['ARR_TIME']

    assert dep_time < arr_time

    dep_time_mins = dep_time // 100 * 60 + dep_time % 100
    arr_time_mins = arr_time // 100 * 60 + arr_time % 100

    return arr_time_mins - dep_time_mins

def resolve_elapsed_time(row):
    dep_time = row['DEP_TIME']
    arr_time = row['ARR_TIME']

    dep_time_mins = dep_time // 100 * 60 + dep_time % 100
    arr_time_mins = arr_time // 100 * 60 + arr_time % 100

    arr_time_mins += 1440

    return arr_time_mins - dep_time_mins

def extract_delay(row):
    calculated_elapsed_time = row['CALCULATED_ELAPSED_TIME']
    actual_elapsed_time = row['ACTUAL_ELAPSED_TIME']
    crs_elapsed_time = row['CRS_ELAPSED_TIME']

    assert calculated_elapsed_time == actual_elapsed_time

    return calculated_elapsed_time - crs_elapsed_time

def resolve_delay(row):
    calculated_elapsed_time = row['CALCULATED_ELAPSED_TIME']
    actual_elapsed_time = row['ACTUAL_ELAPSED_TIME']
    crs_elapsed_time = row['CRS_ELAPSED_TIME']

    assert (calculated_elapsed_time - actual_elapsed_time) % 60 == 0

    return actual_elapsed_time - crs_elapsed_time


ds = ds.withColumn('CALCULATED_ELAPSED_TIME', extract_elapsed_time)
ds = ds.resolve(AssertionError, resolve_elapsed_time)

ds = ds.withColumn('CALCULATED_DELAY', extract_delay)
ds = ds.resolve(AssertionError, resolve_delay)

def view_exceptions(row):
    calculated_elapsed_time = row['CALCULATED_ELAPSED_TIME']
    actual_elapsed_time = row['ACTUAL_ELAPSED_TIME']

    return (calculated_elapsed_time - actual_elapsed_time) % 60 != 0

# ds = ds.filter(view_exceptions)
#
# ds = ds.selectColumns(['YEAR', 'MONTH', 'ARR_TIME', 'DEP_TIME', 'ACTUAL_ELAPSED_TIME', 'CALCULATED_ELAPSED_TIME'])

ds.tocsv(output_path)
