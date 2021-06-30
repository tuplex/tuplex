#!/usr/bin/env python
# coding: utf-8

# running tuplex experiment

# i.e. via python3 runtuplex.py --path /disk/data/flights/ --num-months 1
import time
import argparse
import json
import string
import os
import glob
import sys
import string

parser = argparse.ArgumentParser(description='Flight data cleaning example (3 joins)')
parser.add_argument('--path', type=str, dest='data_path', default=None,
                    help='path or pattern to flight data')
parser.add_argument('--num-months', type=int, dest='num_months', default=None,
                    help='specify on how many months to run the experiment')
parser.add_argument('--output-path', type=str, dest='output_path', default='tuplex_output/',
                    help='specify path where to save output data files')
parser.add_argument('--simulate-spark', dest='simulate_spark',
                    help='if active, add cache statements wherever pyspark needs some.'
                         ' I.e. this simulates a non-stage fusion setup', action='store_true')
args = parser.parse_args()

assert args.data_path, 'need to set data path!'

# config vars
perf_paths = [args.data_path]
carrier_hist_path = os.path.join(args.data_path, 'L_CARRIER_HISTORY.csv')
airport_data_path = os.path.join(args.data_path, 'GlobalAirportDatabase.txt')

# config vars
carrier_hist_path = 'data/L_CARRIER_HISTORY.csv'
airport_data_path = 'data/GlobalAirportDatabase.txt'
output_path = args.output_path

time_req_cols = ['ActualElapsedTime', 'Distance', 'CancellationCode', 'DivActualElapsedTime', 'OpUniqueCarrier',
            'LateAircraftDelay', 'NasDelay', 'ArrDelay', 'SecurityDelay', 'CarrierDelay',
            'CrsArrTime', 'TaxiOut', 'CrsElapsedTime', 'WeatherDelay', 'DayOfWeek', 'DayOfMonth', 'Month', 'Year',
            'CrsDepTime', 'Cancelled',
            'Diverted', 'OriginCityName', 'AirTime', 'Origin', 'Dest', 'DestCityName',
            'DivReachedDest', 'TaxiIn', 'DepDelay', 'OpCarrierFlNum']

# explicit globbing because dask can't handle patterns well...
if not os.path.isfile(args.data_path):
    # invert order here, so tuplex in no-opt case infers the right types
    # => it's because python solution is not yet implemented.
    file_paths = sorted(glob.glob(os.path.join(args.data_path, 'flights*.csv')))[::-1]
    if args.num_months:
        assert args.num_months > 0, 'at least one month must be given!'

        perf_paths = file_paths[:args.num_months]
    else:
        perf_paths = file_paths
else:
    perf_paths = [args.data_path]


if not perf_paths:
    print('found no flight data to process, abort.')
    sys.exit(1)

print('>>> running Tuplex on {}'.format(perf_paths))

startup_time = 0


# configuration, make sure to give enough runtime memory to the executors!
conf = {"webui.enable" : False,
        "executorCount" : 16,
        "executorMemory" : "2G",
        "driverMemory" : "2G",
        "partitionSize" : "32MB",
        "runTimeMemory" : "128MB",
        "useLLVMOptimizer" : False,
        "optimizer.nullValueOptimization" : False,
        "csv.selectionPushdown" : True,
        "tuplex.optimizer.generateParser": True}


if os.path.exists('tuplex_config.json'):
    with open('tuplex_config.json') as fp:
        conf = json.load(fp)

# always set merge exceptions in order to false
conf['optimizer.mergeExceptionsInOrder'] = False

tstart = time.time()
import tuplex
ctx = tuplex.Context(conf)

startup_time = time.time() - tstart
print('Tuplex startup time: {}'.format(startup_time))
tstart = time.time()

df = ctx.csv(','.join(perf_paths))

# rename weird column names from original file
renamed_cols = list(map(lambda c: ''.join(map(lambda w: w.capitalize(), c.split('_'))), df.columns))

for i, c in enumerate(df.columns):
    df = df.renameColumn(c, renamed_cols[i])

# if cache/nosf mode is active, prefilter
if args.simulate_spark and conf["csv.selectionPushdown"]:
    df = df.selectColumns(time_req_cols)
if args.simulate_spark:
    df = df.cache()

# DATAFRAME (3) airports
# Airport table
airport_cols = ['ICAOCode', 'IATACode', 'AirportName', 'AirportCity', 'Country',
                'LatitudeDegrees', 'LatitudeMinutes', 'LatitudeSeconds', 'LatitudeDirection',
                'LongitudeDegrees', 'LongitudeMinutes', 'LongitudeSeconds',
                'LongitudeDirection', 'Altitude', 'LatitudeDecimal', 'LongitudeDecimal']

df_airports = ctx.csv(airport_data_path, columns=airport_cols, delimiter=':', header=False, null_values=['', 'N/a', 'N/A'])
if args.simulate_spark:
    df_airports = df_airports.cache()

# DATAFRAME (2) Carrier table
# carrier table analysis
df_carrier = ctx.csv(carrier_hist_path)
if args.simulate_spark:
    df_carrier = df_carrier.cache()

io_load_time = time.time() - tstart
tstart = time.time()
if args.simulate_spark:
    print('CACHING took {}s'.format(io_load_time))

# split into city and state!
df = df.withColumn('OriginCity', lambda x: x['OriginCityName'][:x['OriginCityName'].rfind(',')].strip())
df = df.withColumn('OriginState', lambda x: x['OriginCityName'][x['OriginCityName'].rfind(',')+1:].strip())

df = df.withColumn('DestCity', lambda x: x['DestCityName'][:x['DestCityName'].rfind(',')].strip())
df = df.withColumn('DestState', lambda x: x['DestCityName'][x['DestCityName'].rfind(',')+1:].strip())

df = df.mapColumn('CrsArrTime', lambda x: '{:02}:{:02}'.format(int(x / 100), x % 100) if x else None)
df = df.mapColumn('CrsDepTime', lambda x: '{:02}:{:02}'.format(int(x / 100), x % 100) if x else None)

def cleanCode(t):
    if t["CancellationCode"] == 'A':
        return 'carrier'
    elif t["CancellationCode"] == 'B':
        return 'weather'
    elif t["CancellationCode"] == 'C':
        return 'national air system'
    elif t["CancellationCode"] == 'D':
        return 'security'
    else:
        return None

def divertedUDF(row):
    diverted = row['Diverted']
    ccode = row['CancellationCode']
    if diverted:
        return 'diverted'
    else:
        if ccode:
            return ccode
        else:
            return 'None'

def fillInTimesUDF(row):
    ACTUAL_ELAPSED_TIME = row['ActualElapsedTime']
    if row['DivReachedDest']:
        if float(row['DivReachedDest']) > 0:
            return float(row['DivActualElapsedTime'])
        else:
            return ACTUAL_ELAPSED_TIME
    else:
        return ACTUAL_ELAPSED_TIME


df = df.withColumn('CancellationCode', cleanCode) # works...
df = df.mapColumn('Diverted', lambda x: True if x > 0 else False)
df = df.mapColumn('Cancelled', lambda x: True if x > 0 else False)

df = df.withColumn('CancellationReason', divertedUDF)
df = df.withColumn('ActualElapsedTime', fillInTimesUDF).ignore(TypeError)

# other dataframes...
# DATAFRAME (2) process Carrier table
# carrier table analysis
def extractDefunctYear(t):
    x = t['Description']
    desc = x[x.rfind('-') + 1:x.rfind(')')].strip()
    return int(desc) if len(desc) > 0 else None

# Note: need to explicitly type here data, else it fails -.-
df_carrier = df_carrier.withColumn('AirlineName', lambda x: x['Description'][:x['Description'].rfind('(')].strip())
df_carrier = df_carrier.withColumn('AirlineYearFounded', lambda x: int(x['Description'][x['Description'].rfind('(') + 1:x['Description'].rfind('-')]))
df_carrier = df_carrier.withColumn('AirlineYearDefunct', extractDefunctYear)

# process airports
# clean airport names & city names by capitalizing words
df_airports = df_airports.mapColumn('AirportName', lambda x: string.capwords(x) if x else None)
df_airports = df_airports.mapColumn('AirportCity', lambda x: string.capwords(x) if x else None)

# nosf? -> materialize first!
if args.simulate_spark:
    df = df.cache()
    df_carrier = df_carrier.cache()

# JOINS
df_all = df.join(df_carrier, 'OpUniqueCarrier', 'Code')

# left Join with prefix/suffix...
if args.simulate_spark:
    # because aiports had UDFs called, materialize
    df_airports = df_airports.cache()

df_all = df_all.leftJoin(df_airports, 'Origin', 'IATACode', prefixes=(None, 'Origin'))
if args.simulate_spark:
    # because aiports had UDFs called, materialize
    # (no self-join opt)
    df_airports = df_airports.cache()

df_all = df_all.leftJoin(df_airports, 'Dest', 'IATACode', prefixes=(None, 'Dest'))

if args.simulate_spark:
    # data now getting passed again to UDFs -> need to materialize
    df_all = df_all.cache()

# operations after joins (could be reordered by optimizer!!!)
df_all = df_all.mapColumn('Distance', lambda x: x / 0.00062137119224) # not working...

# remove Inc., LLC or Co. from Airline name
df_all = df_all.mapColumn('AirlineName', lambda s: s.replace('Inc.', '') \
                          .replace('LLC', '') \
                          .replace('Co.', '').strip())

# rename some more columns
df_all = df_all.renameColumn('OriginLongitudeDecimal', 'OriginLongitude') \
    .renameColumn('OriginLatitudeDecimal', 'OriginLatitude') \
    .renameColumn('DestLongitudeDecimal', 'DestLongitude') \
    .renameColumn('DestLatitudeDecimal', 'DestLatitude')

df_all = df_all.renameColumn('OpUniqueCarrier', 'CarrierCode') \
    .renameColumn('OpCarrierFlNum', 'FlightNumber') \
    .renameColumn('DayOfMonth', 'Day') \
    .renameColumn('AirlineName', 'CarrierName') \
    .renameColumn('Origin', 'OriginAirportIATACode') \
    .renameColumn('Dest', 'DestAirportIATACode')


# remove rows that make no sense, i.e. all flights where the airline is defunct which may happen after the join
def filterDefunctFlights(row):
    year = row['Year']
    airlineYearDefunct = row['AirlineYearDefunct']

    if airlineYearDefunct:
        return int(year) < int(airlineYearDefunct)
    else:
        return True

df_all = df_all.filter(filterDefunctFlights)

# all delay numbers are given in the original file as doubles, cast to integer here!
numeric_cols = ['ActualElapsedTime', 'AirTime', 'ArrDelay',
                'CarrierDelay', 'CrsElapsedTime',
                'DepDelay', 'LateAircraftDelay', 'NasDelay',
                'SecurityDelay', 'TaxiIn', 'TaxiOut', 'WeatherDelay']

for c in numeric_cols:
    # TODO: allow simple functions, i.e. int/str directly!
    # TODO: NULL/None here?? ==> in presence of none conversion to 0s!

    # ==> use if else for this
    df_all = df_all.mapColumn(c, lambda x: int(x) if x else 0)

# select subset of columns to emit clean csv data file
df_all = df_all.selectColumns(['CarrierName', 'CarrierCode', 'FlightNumber',
                               'Day', 'Month', 'Year', 'DayOfWeek',
                               'OriginCity', 'OriginState', 'OriginAirportIATACode', 'OriginLongitude', 'OriginLatitude',
                               'OriginAltitude',
                               'DestCity', 'DestState', 'DestAirportIATACode', 'DestLongitude', 'DestLatitude', 'DestAltitude',
                               'Distance',
                               'CancellationReason', 'Cancelled', 'Diverted', 'CrsArrTime', 'CrsDepTime',
                               'ActualElapsedTime', 'AirTime', 'ArrDelay',
                               'CarrierDelay', 'CrsElapsedTime',
                               'DepDelay', 'LateAircraftDelay', 'NasDelay',
                               'SecurityDelay', 'TaxiIn', 'TaxiOut', 'WeatherDelay',
                               'AirlineYearFounded', 'AirlineYearDefunct'])

df_all.tocsv(output_path)
compute_time = time.time() - tstart
job_time = io_load_time + compute_time
print('Tuplex job time: {} s'.format(job_time))
m = ctx.metrics
print(ctx.options())
print(m.as_json())
print(json.dumps(conf))
# print stats as last line
print(json.dumps({"startupTime" : startup_time, "jobTime" : job_time, "io_load": io_load_time, "compute_and_write": compute_time}))
