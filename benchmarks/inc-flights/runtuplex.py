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
parser.add_argument('--inc-res', action='store_true',
                    help='whether to use incremental exception resolution')
parser.add_argument('--in-order', action='store_true',
                    help='whether to merge exceptions in order')

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
conf = {"webui.enable" : False,
        "executorCount" : 16,
        "executorMemory" : "12G",
        "driverMemory" : "12G",
        "partitionSize" : "32MB",
        "runTimeMemory" : "128MB",
        "useLLVMOptimizer" : True,
        "optimizer.nullValueOptimization" : False,
        "csv.selectionPushdown" : True,
        "optimizer.generateParser" : True,
        "optimizer.mergeExceptionsInOrder": args.in_order,
        "optimizer.incrementalResolution": args.inc_res}

if os.path.exists('tuplex_config.json'):
    with open('tuplex_config.json') as fp:
        conf = json.load(fp)

tstart = time.time()

ctx = tuplex.Context(conf)

startup_time = time.time() - tstart
print('Tuplex startup time: {}'.format(startup_time))

# Query
def validate_arr_delay(row):
    arr_delay = row['ARR_DELAY']
    crs_arr_time = row['CRS_ARR_TIME']
    arr_time = row['ARR_TIME']

    if arr_delay is None:
        return arr_delay

    crs_arr_time_mins = crs_arr_time // 100 * 60 + crs_arr_time % 100
    arr_time_mins = arr_time // 100 * 60 + arr_time % 100

    assert crs_arr_time_mins + arr_delay == arr_time_mins

    return arr_delay

def resolve_arr_delay_1(row):
    arr_delay = row['ARR_DELAY']
    crs_arr_time = row['CRS_ARR_TIME']
    arr_time = row['ARR_TIME']

    if arr_delay is None:
        return arr_delay

    crs_arr_time_mins = crs_arr_time // 100 * 60 + crs_arr_time % 100
    arr_time_mins = arr_time // 100 * 60 + arr_time % 100

    assert (crs_arr_time_mins + arr_delay) % 1440 == arr_time_mins

    return arr_delay

def resolve_arr_delay_2(row):
    arr_delay = row['ARR_DELAY']
    crs_arr_time = row['CRS_ARR_TIME']
    arr_time = row['ARR_TIME']

    if arr_delay is None:
        return arr_delay

    crs_arr_time_mins = crs_arr_time // 100 * 60 + crs_arr_time % 100
    arr_time_mins = arr_time // 100 * 60 + arr_time % 100

    assert (crs_arr_time_mins + arr_delay) % 1440 == arr_time_mins % 1440

    return arr_delay

def is_delayed(row):
    arr_delay = row['ARR_DELAY']
    return arr_delay >= 15

def resolve_is_delayed(row):
    return row['CANCELLED'] == 1 or row['DIVERTED'] == 1

metrics = []

for i in range(4):
    jobstart = time.time()
    ds = ctx.csv(flights_path)

    ds = ds.withColumn('ARR_DELAY', validate_arr_delay)
    if i > 0:
        ds = ds.resolve(AssertionError, resolve_arr_delay_1)
    if i > 1:
        ds = ds.resolve(AssertionError, resolve_arr_delay_2)

    ds = ds.withColumn('IS_DELAYED', is_delayed)
    if i > 2:
        ds = ds.resolve(TypeError, resolve_is_delayed)

    ds = ds.selectColumns(['ARR_DELAY', 'IS_DELAYED'])

    ds.tocsv(output_path)

    m = ctx.metrics
    m = m.as_dict()
    m["jobTime"] = time.time() - jobstart

    metrics.append(m)

for metric in metrics:
    print(json.dumps(metric))
