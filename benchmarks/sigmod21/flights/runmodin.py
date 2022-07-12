#!/usr/bin/env python
# coding: utf-8

# In[1]:
# run via python3 rundask.py --path /disk/data/flights/ --num-months 5 e.g.

import time
import argparse
import json

tstart = time.time()

import ray
ray.init(num_cpus=4)
import modin.pandas as dd
import os
import glob
import sys

import pandas as pd
import numpy as np

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Flight data cleaning example (3 joins)')
    parser.add_argument('--path', type=str, dest='data_path', default=None,
                        help='path or pattern to flight data')
    parser.add_argument('--num-months', type=int, dest='num_months', default=None, help='specify on how many months to run the experiment')
    args = parser.parse_args()

    assert args.data_path, 'need to set data path!'

    # config vars
    perf_paths = [args.data_path]
    carrier_hist_path = os.path.join(args.data_path, 'L_CARRIER_HISTORY.csv')
    airport_data_path = os.path.join(args.data_path, 'GlobalAirportDatabase.txt')
    output_path = 'dask_output'

    # explicit globbing because dask can't handle patterns well...
    if not os.path.isfile(args.data_path):
        file_paths = sorted(glob.glob(os.path.join(args.data_path, 'flights*.csv')))
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

    print('>>> running Ray/Modin on {}'.format(perf_paths))

    startup_time = time.time() - tstart

    print('Ray/Modin startup time: {}'.format(startup_time))

    # Todo: Use this here to get rid off stupid Dask warnings! https://docs.python.org/3/library/warnings.html

    # profile stuff via https://docs.dask.org/en/latest/diagnostics-local.html
    tstart = time.time()

    # Dask requires types to be explicitly specified, else it crashes!
    df = dd.read_csv(perf_paths[0], low_memory=False, dtype={'DIV1_TAIL_NUM' : str, 'DIV1_AIRPORT': str,
                                                          'DIV2_AIRPORT': str, 'DIV2_TAIL_NUM': str, 'DIV3_AIRPORT':str, 'DIV3_TAIL_NUM':str,
                                                          'ARR_DELAY_GROUP' : float,
                                                          'ARR_TIME': 'float64',
                                                          'CANCELLATION_CODE': 'str',
                                                          'DEP_DELAY_GROUP': 'float64',
                                                          'DEP_TIME': 'float64',
                                                          'WHEELS_OFF': 'float64',
                                                          'WHEELS_ON': 'float64',
                                                          'DIV_AIRPORT_LANDINGS': 'float64'})

    # rename weird column names from original file
    renamed_cols = list(map(lambda c: ''.join(map(lambda w: w.capitalize(), c.split('_'))), df.columns))

    df.columns = renamed_cols

    # split into city and state!
    df['OriginCity'] = df['OriginCityName'].apply(lambda x: x[:x.rfind(',')].strip())
    df['OriginState'] = df['OriginCityName'].apply(lambda x: x[x.rfind(',')+1:].strip())
    df['DestCity'] = df['DestCityName'].apply(lambda x: x[:x.rfind(',')].strip())
    df['DestState'] = df['DestCityName'].apply(lambda x: x[x.rfind(',')+1:].strip())

    #transform times to nicely formatted string hh:mm
    df['CrsArrTime'] = df['CrsArrTime'].apply(lambda x: '{:02}:{:02}'.format(int(x / 100), x % 100))
    df['CrsDepTime'] = df['CrsDepTime'].apply(lambda x: '{:02}:{:02}'.format(int(x / 100), x % 100))

    code_dict = {'A' : 'carrier', 'B' : 'weather', 'C' : 'national air system', 'D':'security'}
    def clean_code(t):
        try:
            return code_dict[t]
        except:
            return None

    df['CancellationReason'] = df['CancellationCode'].apply(clean_code)

    # translating diverted to bool column
    df['Diverted'] = df['Diverted'].apply(lambda x: True if x > 0 else False)
    df['Cancelled'] = df['Cancelled'].apply(lambda x: True if x > 0 else False)

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
    df_carrier['AirlineYearFounded'] = df_carrier['Description'].apply(lambda x: int(x[x.rfind('(')+1:x.rfind('-')]))
    df_carrier['AirlineYearDefunct'] = df_carrier['Description'].apply(extractDefunctYear)

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
    df_all['filter_cond'] = df_all[['Year', 'AirlineYearDefunct']].apply(filterDefunctFlights, axis=1)

    df_all = df_all[df_all['filter_cond']]

    # all delay numbers are given in the original file as doubles, cast to integer here!
    numeric_cols = ['ActualElapsedTime', 'AirTime', 'ArrDelay',
                  'CarrierDelay', 'CrsElapsedTime',
       'DepDelay', 'LateAircraftDelay', 'NasDelay',
       'SecurityDelay', 'TaxiIn', 'TaxiOut', 'WeatherDelay']
    # use Spark's cast function for this
    for c in numeric_cols:
        #  df_all[c] = df_all[c].astype('int') # doesn;t work because of NAN values... -.-
        df_all[c] = df_all[c].astype(pd.Int64Dtype())


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

    print('Ray/Modin job time: {} s'.format(job_time))

    # print stats as last line
    print(json.dumps({"startupTime" : startup_time, "jobTime" : job_time}))
