#!/usr/bin/env python
# coding: utf-8

# In[1]:
# run via python3 rundask.py --path /disk/data/flights/ --num-months 5 e.g.

import time
import argparse
import json

tstart = time.time()

import dask
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from dask.distributed import Client
import dask.multiprocessing

# nan fixes for dask
import math

import os
import glob
import sys

import pandas as pd
import numpy as np

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Flight data cleaning example (3 joins)')
    parser.add_argument('--path', type=str, dest='data_path', default=None,
                        help='path or pattern to flight data')
    parser.add_argument('--output-path', type=str, dest='output_path', default='dask_output',
                        help='specify path where to save output data files')
    parser.add_argument('--num-months', type=int, dest='num_months', default=None, help='specify on how many months to run the experiment')
    args = parser.parse_args()

    assert args.data_path, 'need to set data path!'

    # config vars
    perf_paths = [args.data_path]
    carrier_hist_path = os.path.join('data', 'L_CARRIER_HISTORY.csv')
    airport_data_path = os.path.join('data', 'GlobalAirportDatabase.txt')
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

    print('>>> running Dask on {}'.format(perf_paths))


    #-----
    # Dask startup
    # configure dask here
    # (1) threads
    # https://docs.dask.org/en/latest/scheduling.html
    # dask.config.set(scheduler='threads') # 55s

    # (2) processes
    # dask.config.set(scheduler='processes') # 170.49


    # Note: this doesn't work, Dask just crashes...
    ## (3) distributed
    #from dask.distributed import Client, LocalCluster
    ##client = Client(n_workers=1, threads_per_worker=1, processes=False,
    ##                 memory_limit='25GB', scheduler_port=0,
    ##                 silence_logs=True, diagnostics_port=0)

    #cluster = LocalCluster(memory_limit='8GB')
    #client = Client(cluster)

    # distributed dask
    #dask.config.set(scheduler='processes')  # overwrite default with multiprocessing scheduler
    #client = Client(n_workers=8, threads_per_worker=1, processes=True) # default init
    # client = Client(n_workers=16, threads_per_worker=1, processes=True)

    client=Client(n_workers=16, threads_per_worker=1, processes=True, memory_limit='8GB')
    #client=Client()
    print(client)

    startup_time = time.time() - tstart

    print('Dask startup time: {}'.format(startup_time))


    # Todo: Use this here to get rid off stupid Dask warnings! https://docs.python.org/3/library/warnings.html

    # profile stuff via https://docs.dask.org/en/latest/diagnostics-local.html
    tstart = time.time()

    # Dask requires types to be explicitly specified, else it crashes!
    df = dd.read_csv(perf_paths, low_memory=False, dtype={'DIV1_TAIL_NUM' : str, 'DIV1_AIRPORT': str,
                                                          'DIV2_AIRPORT': str, 'DIV2_TAIL_NUM': str, 'DIV3_AIRPORT':str, 'DIV3_TAIL_NUM':str,
                                                          'ARR_DELAY_GROUP' : float,
                                                          'ARR_TIME': 'float64',
                                                          'CANCELLATION_CODE': 'str',
                                                          'DEP_DELAY_GROUP': 'float64',
                                                          'DEP_TIME': 'float64',
                                                          'WHEELS_OFF': 'float64',
                                                          'WHEELS_ON': 'float64',
                                                          'DIV_AIRPORT_LANDINGS': 'float64',
                                                          'CRS_ARR_TIME': 'float64',
                                                          'CRS_DEP_TIME': 'float64'})

    # rename weird column names from original file
    renamed_cols = list(map(lambda c: ''.join(map(lambda w: w.capitalize(), c.split('_'))), df.columns))

    df.columns = renamed_cols

    # split into city and state!
    df['OriginCity'] = df['OriginCityName'].apply(lambda x: x[:x.rfind(',')].strip(), meta=('OriginCityName', 'str'))
    df['OriginState'] = df['OriginCityName'].apply(lambda x: x[x.rfind(',')+1:].strip(), meta=('OriginCityName', 'str'))
    df['DestCity'] = df['DestCityName'].apply(lambda x: x[:x.rfind(',')].strip(), meta=('DestCityName', 'str'))
    df['DestState'] = df['DestCityName'].apply(lambda x: x[x.rfind(',')+1:].strip(), meta=('DestCityName', 'str'))

    #transform times to nicely formatted string hh:mm
    def fmtTime(x):
        try:
            return '{:02}:{:02}'.format(int(x / 100), int(x) % 100)
        except:
            return None
    df['CrsArrTime'] = df['CrsArrTime'].apply(fmtTime,
                                              meta=('CRSArrTime', 'str'))
    df['CrsDepTime'] = df['CrsDepTime'].apply(fmtTime,
                                              meta=('CRSDepTime', 'str'))

    code_dict = {'A' : 'carrier', 'B' : 'weather', 'C' : 'national air system', 'D':'security'}
    def clean_code(t):
        try:
            return code_dict[t]
        except:
            return 'None'

    df['CancellationCode'] = df['CancellationCode'].apply(clean_code, meta=('CancellationCode', 'str'))

    # translating diverted to bool column
    df['Diverted'] = df['Diverted'].apply(lambda x: True if x > 0 else False, meta=('Diverted', 'int'))
    df['Cancelled'] = df['Cancelled'].apply(lambda x: True if x > 0 else False, meta=('Cancelled', 'int'))

    # giving diverted flights a cancellationcode

    def divertedUDF(row):
        if row['Diverted']:
            return 'diverted'
        else:
            if row['CancellationCode']:
                return row['CancellationCode']
            else:
                return 'None'

    # left out because of bug
    df['CancellationReason'] = df[['CancellationCode', 'Diverted']].apply(divertedUDF, axis=1)

    # fill in elapsed time from diverted column

    def fillInTimesUDF(row):
        if row['DivReachedDest']:
            if row['DivReachedDest'] > 0:
                return row['DivActualElapsedTime']
            else:
                return row['ActualElapsedTime']
        else:
            return row['ActualElapsedTime']



    df['ActualElapsedTime'] = df[['ActualElapsedTime',
                                  'DivActualElapsedTime',
                                  'DivReachedDest']].apply(fillInTimesUDF, axis=1)
    # next dask dataframe
    df_carrier = dd.read_csv(carrier_hist_path)

    def extractDefunctYear(x):
        desc = x[x.rfind('-')+1:x.rfind(')')].strip()
        return int(desc) if len(desc) > 0 else None

    # Note: need to explicitly type here data, else it fails -.-

    df_carrier['AirlineName'] = df_carrier['Description'].apply(lambda x: x[:x.rfind('(')].strip())
    df_carrier['AirlineYearFounded'] = df_carrier['Description'].apply(lambda x: int(x[x.rfind('(')+1:x.rfind('-')]),
                                                                       meta=('x', str))
    df_carrier['AirlineYearDefunct'] = df_carrier['Description'].apply(extractDefunctYear,
                                                                       meta=('x', str))

    # merge with other dataframe
    # i.e. join into BTS flight data
    df_all = dd.merge(df, df_carrier, left_on='OpUniqueCarrier', right_on='Code', how='left')

    # convert distance in miles to meters
    df_all['Distance'] = df_all['Distance'].apply(lambda x: x / 0.00062137119224)

    airport_cols = ['ICAOCode', 'IATACode', 'AirportName','AirportCity','Country',
                    'LatitudeDegrees','LatitudeMinutes','LatitudeSeconds','LatitudeDirection',
                    'LongitudeDegrees','LongitudeMinutes','LongitudeSeconds',
                    'LongitudeDirection','Altitude','LatitudeDecimal','LongitudeDecimal']
    df_airports = dd.read_csv(airport_data_path, sep=':',
                              header=None)
    df_airports.columns = airport_cols

    # clean Airport Name
    def cleanCase(x):
        if isinstance(x, str):

            # upper case words!
            words = x.split(' ')
            return ' '.join([w[0].upper() + w[1:].lower() for w in words]).strip()
        else:
            return None

    df_airports['AirportName'] = df_airports['AirportName'].apply(cleanCase)
    df_airports['AirportCity'] = df_airports['AirportCity'].apply(cleanCase)
    df_airports['Altitude'] = df_airports['Altitude'].apply(lambda x: int(x))

    df_all = dd.merge(df_all, df_airports, left_on='Origin', right_on='IATACode', how="left")

    df_all = dd.merge(df_all, df_airports, left_on='Dest', right_on='IATACode', how="left", suffixes=('Origin', 'Dest'))

    # remove Inc., LLC or Co. from Airline name
    df_all['AirlineName'] = df_all['AirlineName'].apply(lambda s: s.replace('Inc.', '')                                                     .replace('LLC', '')                                                     .replace('Co.', '').strip())

    # select columns
    df_all = df_all.rename(columns={
        'AltitudeDest' : 'DestAltitude',
        'LongitudeDecimalDest' : 'DestLongitude',
        'LatitudeDecimalDest' : 'DestLatitude',
        'AirportNameDest' : 'DestAirportName',
        'AltitudeOrigin' : 'OriginAltitude',
        'LongitudeDecimalOrigin' : 'OriginLongitude',
        'LatitudeDecimalOrigin' : 'OriginLatitude',
        'AirportNameOrigin' : 'OriginAirportName',
        'OpUniqueCarrier' : 'CarrierCode',
        'OpCarrierFlNum' : 'FlightNumber',
        'DayOfMonth' : 'Day',
        'AirlineName' : 'CarrierName',
        'Origin' : 'OriginAirportIATACode',
        'Dest' : 'DestAirportIATACode'})


    # use a mask to run complex filter on dask
    # remove rows that make no sense, i.e. all flights where the airline is defunct which may happen after the join
    def filterDefunctFlights(row):
        year, airlineYearDefunct = row['Year'], row['AirlineYearDefunct']
        if airlineYearDefunct and not np.isnan(airlineYearDefunct):
            return int(year) < int(airlineYearDefunct)
        else:
            return True

    # masking is not yet implemented in dask -.-
    # mask = df_all[['Year', 'AirlineYearDefunct']].apply(filterDefunctFlights, axis=1,
    #                                                     meta=[('Year', str), ('AirlineYearDefunct', int)])
    # df_all = df_all[mask]

    # to get above function executed, add extra column & filter on it
    df_all['filter_cond'] = df_all[['Year', 'AirlineYearDefunct']].apply(filterDefunctFlights, axis=1,
                                                                         meta=('AirlineYearDefunct', str))

    df_all = df_all[df_all['filter_cond']]

    # all delay numbers are given in the original file as doubles, cast to integer here!
    numeric_cols = ['ActualElapsedTime', 'AirTime', 'ArrDelay',
                    'CarrierDelay', 'CrsElapsedTime',
                    'DepDelay', 'LateAircraftDelay', 'NasDelay',
                    'SecurityDelay', 'TaxiIn', 'TaxiOut', 'WeatherDelay']
    # use Spark's cast function for this
    for c in numeric_cols:
        #  df_all[c] = df_all[c].astype('int') # doesn;t work because of NAN values... -.-

        # map to int AND nulls to 0
        # df_all[c] = df_all[c].astype(pd.Int64Dtype()) # this would preserve Nulls...
        df_all[c] = df_all[c].fillna(0.0)
        df_all[c] = df_all[c].astype(int)

    # select interesting columns
    df_all = df_all[['CarrierName', 'CarrierCode', 'FlightNumber',
                     'Day', 'Month', 'Year', 'DayOfWeek',
                     'OriginCity', 'OriginState', 'OriginAirportIATACode', 'OriginLongitude', 'OriginLatitude', 'OriginAltitude',
                     'DestCity', 'DestState', 'DestAirportIATACode', 'DestLongitude', 'DestLatitude', 'DestAltitude', 'Distance',
                     'CancellationReason',
                     'Cancelled', 'Diverted', 'CrsArrTime', 'CrsDepTime',
                     'ActualElapsedTime', 'AirTime', 'ArrDelay',
                     'CarrierDelay', 'CrsElapsedTime',
                     'DepDelay', 'LateAircraftDelay', 'NasDelay',
                     'SecurityDelay', 'TaxiIn', 'TaxiOut', 'WeatherDelay',
                     'AirlineYearFounded', 'AirlineYearDefunct']]


    # Note: progress bar doesn't work for distributed...

    # write to disk
    df_all.to_csv(output_path, index=None) # Spark uses per default .2f!

    job_time = time.time() - tstart

    print('Dask job time: {} s'.format(job_time))

    # print stats as last line
    print(json.dumps({"startupTime" : startup_time, "jobTime" : job_time}))
