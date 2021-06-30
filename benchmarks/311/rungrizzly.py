#!/usr/bin/python
# Taken from https://github.com/weld-project/weld/blob/master/examples/python/grizzly/data_cleaning_grizzly.py
# The usual preamble
import pandas as pd
import grizzly.grizzly as gr
import time
import argparse

parser = argparse.ArgumentParser(description="311 cleaning query using Weld/Grizzly")
parser.add_argument(
    "--path",
    type=str,
    dest="data_path",
    default="data/311-service-requests.csv",
    help="path to 311 data",
)
parser.add_argument('--output-path', type=str, dest='output_path', default='weld_output',
                    help='specify path where to save output data files')

args = parser.parse_args()

load_time, query_time = 0,0
pandas_load_time = 0

# Get data (NYC 311 service request dataset) and start cleanup
tstart = time.time()
na_values = ['NO CLUE', 'N/A', '0']
raw_requests = pd.read_csv(args.data_path,
                           na_values=na_values, dtype={'Incident Zip': str})
pandas_load_time = time.time() - tstart
requests = gr.DataFrameWeld(raw_requests)
load_time = time.time() - tstart
print("Done reading input file...")

tstart = time.time()

# Fix requests with extra digits
requests['Incident Zip'] = requests['Incident Zip'].str.slice(0, 5)

# Fix requests with 00000 zipcodes
zero_zips = requests['Incident Zip'] == '00000'
requests['Incident Zip'][zero_zips] = "nan"

# Display unique incident zips again (this time cleaned)
print(requests['Incident Zip'].unique().evaluate())
query_time = time.time() - tstart

print("Total end-to-end time, including compilation: %.2f" % query_time)

print('framework,pandas_load,load,query\n{},{},{},{}'.format('weld-grizzly', pandas_load_time, load_time, query_time))