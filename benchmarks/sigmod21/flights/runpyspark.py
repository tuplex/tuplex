#!/usr/bin/env python
# coding: utf-8

# run via spark-submit --master "local[*]" --driver-memory 100g runpyspark.py --path /disk/data/flights/ --num-months 5

# In[1]:

import time
import argparse
import json
import os
import glob
import sys


parser = argparse.ArgumentParser(description='Flight data cleaning example (3 joins)')
parser.add_argument('--path', type=str, dest='data_path', default=None,
                    help='path or pattern to flight data')
parser.add_argument('--output-path', type=str, dest='output_path', default='pyspark_output',
                    help='specify path where to save output data files')
parser.add_argument('--num-months', type=int, dest='num_months', default=None,
                    help='specify on how many months to run the experiment')
parser.add_argument('--pushdown',  dest='pushdown_cols', action='store_true', help='add artificial select columns to pyspark sql job')
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

print('>>> running Spark on {}'.format(perf_paths))


startup_time = 0


# In[4]:


tstart = time.time()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, column, coalesce, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DoubleType

spark = SparkSession.builder.appName("flights").getOrCreate()

startup_time = time.time() - tstart


# In[5]:


print('PySpark startup time: {}'.format(startup_time))


# In[9]:

tstart = time.time()

df = spark.read.csv(perf_paths, inferSchema=True, header=True,
                    mode='PERMISSIVE', multiLine=True, escape='"')

# rename weird column names from original file
renamed_cols = list(map(lambda c: ''.join(map(lambda w: w.capitalize(), c.split('_'))), df.columns))

for i, c in enumerate(df.columns):
    df = df.withColumnRenamed(c, renamed_cols[i])

# artificial pushdown added
if args.pushdown_cols:
    print('artificially adding select to pyspark, to simulate successful pushdown')
    df = df.select(['ActualElapsedTime', 'Distance', 'CancellationCode', 'DivActualElapsedTime', 'OpUniqueCarrier',
                    'LateAircraftDelay', 'NasDelay', 'ArrDelay', 'SecurityDelay', 'CarrierDelay',
                    'CrsArrTime', 'TaxiOut', 'CrsElapsedTime', 'WeatherDelay', 'DayOfWeek', 'DayOfMonth', 'Month', 'Year',
                    'CrsDepTime', 'Cancelled',
                    'Diverted', 'OriginCityName', 'AirTime', 'Origin', 'Dest', 'DestCityName',
                    'DivReachedDest', 'TaxiIn', 'DepDelay', 'OpCarrierFlNum'])


# split into city and state!
df = df.withColumn('OriginCity', udf(lambda x: x[:x.rfind(',')].strip(), StringType())('OriginCityName'))
df = df.withColumn('OriginState', udf(lambda x: x[x.rfind(',') + 1:].strip(), StringType())('OriginCityName'))

df = df.withColumn('DestCity', udf(lambda x: x[:x.rfind(',')].strip(), StringType())('DestCityName'))
df = df.withColumn('DestState', udf(lambda x: x[x.rfind(',') + 1:].strip(), StringType())('DestCityName'))

# transform times to nicely formatted string hh:mm
df = df.withColumn('CrsArrTime', udf(lambda x: '{:02}:{:02}'.format(int(x / 100), x % 100) if x else None, StringType())('CrsArrTime'))
df = df.withColumn('CrsDepTime', udf(lambda x: '{:02}:{:02}'.format(int(x / 100), x % 100) if x else None, StringType())('CrsDepTime'))

code_dict = {'A': 'carrier', 'B': 'weather', 'C': 'national air system', 'D': 'security'}


def clean_code(t):
    try:
        return code_dict[t]
    except:
        return 'None'


df = df.withColumn('CancellationCode', udf(clean_code, StringType())('CancellationCode'))

# translating diverted to bool column
df = df.withColumn('Diverted', udf(lambda x: True if x > 0 else False, BooleanType())('Diverted'))
df = df.withColumn('Cancelled', udf(lambda x: True if x > 0 else False, BooleanType())('Cancelled'))


# giving diverted flights a cancellationcode
def divertedUDF(diverted, ccode):
    if diverted:
        return 'diverted'
    else:
        if ccode:
            return ccode
        else:
            return 'None'


# left out b.c. of bug
df = df.withColumn('CancellationReason', udf(divertedUDF, StringType())('Diverted', 'CancellationCode'))


# fill in elapsed time from diverted column
def fillInTimesUDF(ACTUAL_ELAPSED_TIME, DIV_ACTUAL_ELAPSED_TIME, DIV_REACHED_DEST):
    if DIV_REACHED_DEST:
        if DIV_REACHED_DEST > 0:
            return DIV_ACTUAL_ELAPSED_TIME
        else:
            return ACTUAL_ELAPSED_TIME
    else:
        return ACTUAL_ELAPSED_TIME


df = df.withColumn('ActualElapsedTime', udf(fillInTimesUDF, DoubleType())('ActualElapsedTime',
                                                                          'DivActualElapsedTime',
                                                                          'DivReachedDest'))

# carrier table analysis
df_carrier = spark.read.csv(carrier_hist_path, inferSchema=True, header=True,
                            mode='PERMISSIVE',
                            escape='"')  # note: Spark fails for this file in multi-line mode -.- so buggy...

def extractDefunctYear(x):
    desc = x[x.rfind('-') + 1:x.rfind(')')].strip()
    return int(desc) if len(desc) > 0 else None

# Note: need to explicitly type here data, else it fails -.-
df_carrier = df_carrier.withColumn('AirlineName', udf(lambda x: x[:x.rfind('(')].strip(), StringType())('Description'))
df_carrier = df_carrier.withColumn('AirlineYearFounded',
                                   udf(lambda x: int(x[x.rfind('(') + 1:x.rfind('-')]), StringType())('Description'))
df_carrier = df_carrier.withColumn('AirlineYearDefunct', udf(extractDefunctYear, StringType())('Description'))

# Airport table
airport_cols = ['ICAOCode', 'IATACode', 'AirportName', 'City', 'Country',
                'LatitudeDegrees', 'LatitudeMinutes', 'LatitudeSeconds', 'LatitudeDirection',
                'LongitudeDegrees', 'LongitudeMinutes', 'LongitudeSeconds',
                'LongitudeDirection', 'Altitude', 'LatitudeDecimal', 'LongitudeDecimal']

# need to define airport schema directly, because Spark can't handle column names else

airport_schema = StructType([
    StructField("ICAOCode", StringType(), True),
    StructField("IATACode", StringType(), True),
    StructField("AirportName", StringType(), True),
    StructField("AirportCity", StringType(), True),
    StructField("AirportCountry", StringType(), True),
    StructField("LatitudeDegrees", IntegerType(), True),
    StructField("LatitudeMinutes", IntegerType(), True),
    StructField("LatitudeSeconds", IntegerType(), True),
    StructField("LatitudeDirection", StringType(), True),
    StructField("LongitudeDegrees", IntegerType(), True),
    StructField("LongitudeMinutes", IntegerType(), True),
    StructField("LongitudeSeconds", IntegerType(), True),
    StructField("LongitudeDirection", StringType(), True),
    StructField("Altitude", IntegerType(), True),
    StructField("LatitudeDecimal", DoubleType(), True),
    StructField("LongitudeDecimal", DoubleType(), True)])

df_airports = spark.read.csv(airport_data_path,
                             schema=airport_schema, header=False,
                             mode='PERMISSIVE', escape='"',
                             sep=':')  # note: Spark fails for this file in multi-line mode -.- so buggy...

# clean Airport Name
def cleanCase(x):
    if isinstance(x, str):
        # upper case words!
        words = x.split(' ')
        return ' '.join([w[0].upper() + w[1:].lower() for w in words]).strip()
    else:
        return None

df_airports = df_airports.withColumn('AirportName', udf(cleanCase, StringType())('AirportName'))
df_airports = df_airports.withColumn('AirportCity', udf(cleanCase, StringType())('AirportCity'))

# join with carrier table for lookup
df_all = df.join(df_carrier, df['OpUniqueCarrier'] == df_carrier['Code'])

df_all = df_all.withColumn('Distance', udf(lambda x: x / 0.00062137119224, DoubleType())('Distance'))

# remove Inc., LLC or Co. from Airline name
df_all = df_all.withColumn('AirlineName', udf(lambda s: s.replace('Inc.', '') \
                                              .replace('LLC', '') \
                                              .replace('Co.', '').strip(), StringType())('AirlineName'))

# join with airport table to get detailed info on origin/dest airport
df_all = df_all.join(df_airports, df_all['Origin'] == df_airports['IATACode'], 'left')

# rename df_all columns for dest join
for c in df_airports.columns:
    df_all = df_all.withColumnRenamed(c, 'Origin{}'.format(c))

df_all = df_all.join(df_airports, df_all['Dest'] == df_airports['IATACode'], 'left')

# rename joined in columns
for c in df_airports.columns:
    df_all = df_all.withColumnRenamed(c, 'Dest{}'.format(c))

# rename some more columns
df_all = df_all.withColumnRenamed('OriginLongitudeDecimal', 'OriginLongitude') \
    .withColumnRenamed('OriginLatitudeDecimal', 'OriginLatitude') \
    .withColumnRenamed('DestLongitudeDecimal', 'DestLongitude') \
    .withColumnRenamed('DestLatitudeDecimal', 'DestLatitude')

df_all = df_all.withColumnRenamed('OpUniqueCarrier', 'CarrierCode') \
    .withColumnRenamed('OpCarrierFlNum', 'FlightNumber') \
    .withColumnRenamed('DayOfMonth', 'Day') \
    .withColumnRenamed('AirlineName', 'CarrierName') \
    .withColumnRenamed('Origin', 'OriginAirportIATACode') \
    .withColumnRenamed('Dest', 'DestAirportIATACode')


# remove rows that make no sense, i.e. all flights where the airline is defunct which may happen after the join
def filterDefunctFlights(year, airlineYearDefunct):
    if airlineYearDefunct:
        return int(year) < int(airlineYearDefunct)
    else:
        return True


df_all = df_all.filter(udf(filterDefunctFlights, BooleanType())('Year', 'AirlineYearDefunct'))

# all delay numbers are given in the original file as doubles, cast to integer here!
numeric_cols = ['ActualElapsedTime', 'AirTime', 'ArrDelay',
                'CarrierDelay', 'CrsElapsedTime',
                'DepDelay', 'LateAircraftDelay', 'NasDelay',
                'SecurityDelay', 'TaxiIn', 'TaxiOut', 'WeatherDelay']
# use Spark's cast function for this
for c in numeric_cols:
    df_all = df_all.withColumn(c, coalesce(col(c).cast('int'), lit(0)))

# select subset of columns to emit clean csv data file
df_all.select(['CarrierName', 'CarrierCode', 'FlightNumber',
               'Day', 'Month', 'Year', 'DayOfWeek',
               'OriginCity', 'OriginState', 'OriginAirportIATACode', 'OriginLongitude', 'OriginLatitude',
               'OriginAltitude',
               'DestCity', 'DestState', 'DestAirportIATACode', 'DestLongitude', 'DestLatitude', 'DestAltitude',
               'Distance', 'CancellationReason',
               'Cancelled', 'Diverted', 'CrsArrTime', 'CrsDepTime',
               'ActualElapsedTime', 'AirTime', 'ArrDelay',
               'CarrierDelay', 'CrsElapsedTime',
               'DepDelay', 'LateAircraftDelay', 'NasDelay',
               'SecurityDelay', 'TaxiIn', 'TaxiOut', 'WeatherDelay',
               'AirlineYearFounded', 'AirlineYearDefunct']) \
    .write.csv(output_path, mode='overwrite', sep=',', header=True, escape='"', nullValue='',
               emptyValue='')

job_time = time.time() - tstart
print('Pyspark job time: {} s'.format(job_time))

# print spark plan
df_all.explain(extended=True)

# print stats as last line
print(json.dumps({"startupTime" : startup_time, "jobTime" : job_time}))
