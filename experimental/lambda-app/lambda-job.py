#!/usr/bin/env python3

# streamlit app to quickly display experimental results

import streamlit as st
import pandas as pd
import json
import os
import glob
import numpy as np
import matplotlib.pyplot as plt

n_col = np.array([53, 66, 89]) / 255.0#sns.color_palette()[0]
g_col = np.array([140, 192, 222]) / 255.0 #sns.color_palette()[1]


DATA_PATH='/Users/leonhards/projects/tuplex-public/tuplex/cmake-build-debug/dist/bin/aws_job.json'
data = json.loads(open(DATA_PATH, 'r').read())

st.set_page_config(page_title="Lambda job explore app", layout='wide')
st.title('Lambda Job overview')


stage_runtime_in_s = (data['stageEndTimestamp'] - data['stageStartTimestamp']) / 1e9
col1, col2, col3 = st.columns(3)
col1.metric('Runtime', '{:.2f}s'.format(stage_runtime_in_s), help='how long the job took to run')
col2.metric('Hyperspecialization', 'On' if data['hyper_mode'] else 'Off')
col3.metric('Cost', '${:.2f}'.format(data['cost']), help='total cost to run (incl. S3)')

st.write('input paths rows took:')
# write out input paths rows took

# CSS to inject contained in a string
hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>
            """

# Inject CSS with Markdown
st.markdown(hide_table_row_index, unsafe_allow_html=True)

# Display a static table
in_normal = data['input_paths_taken']['normal']
in_general = data['input_paths_taken']['general']
in_fallback = data['input_paths_taken']['fallback']
in_unresolved = data['input_paths_taken']['unresolved']

out_normal = data['output_paths_taken']['normal']
out_unresolved = data['output_paths_taken']['unresolved']

in_df = pd.DataFrame({
    'total' : [in_normal + in_general + in_fallback + in_unresolved],
    'normal': [in_normal],
    'general': [in_general],
    'fallback': [in_fallback],
    'unresolved': [in_unresolved],
})
st.table(in_df)

st.write('output produced:')
out_df = pd.DataFrame({
    'normal': [out_normal],
    'unresolved': [out_unresolved],
})
st.table(out_df)
st.markdown("""---""")


def requests_to_pyplot(requests, ts_start):
    x, y1, y2 = [], [], []
    x = np.arange(len(requests))
    y1, y2 = np.zeros(x.shape), np.zeros(x.shape)

    for i, req in enumerate(requests):
        start, end = pd.to_datetime(req['tsRequestStart']), pd.to_datetime(req['tsRequestEnd'])
        y1[i] = (end - ts_start).total_seconds()
        y2[i] = (start - ts_start).total_seconds()
    return x, y1, y2

def plot_request_completion_chart(st, requests, ts_start):
    sort_key = 'tsRequestEnd'
    sorted_reqs = sorted(requests, key=lambda x: x[sort_key])

    fig, ax = plt.subplots(figsize=(12, 6))

    x, y1, y2 = requests_to_pyplot(sorted_reqs, ts_start)
    # pct
    x = x / (len(x) -1) * 100.0
    #label = 'hyper $\\!$ ($\\mu={:.1f}\\mathrm{{s}}$, \\, $\\sigma={:.1f}\\mathrm{{s}}$, \\, $\\${:.2f}$)'.format(hyper_mu, hyper_sigma, hyper_cost)
    plt.fill_between(x, y1, y2, alpha=1.0, color=n_col, linewidth=2)
    plt.grid(axis='x')
    #plt.legend(loc='upper left', fontsize=24)
    plt.ylabel('time in s')
    plt.xlabel('% of requests completed')
    plt.ylim(0, 75)
    plt.xlim(0, 100)

    st.pyplot(fig)


ts_start = pd.to_datetime(data['stageStartTimestamp'])
requests = data['requests']
col1, col2 = st.columns(2)
col1.metric('Requests issued', len(requests), help='number of requests')
plot_request_completion_chart(col2, requests, ts_start)

# create large table with all the requests in there!
sort_key = 'tsRequestEnd'
sorted_reqs = sorted(requests, key=lambda x: x[sort_key])
rows = []
for i, req in enumerate(sorted_reqs):
    start, end = pd.to_datetime(req['tsRequestStart']), pd.to_datetime(req['tsRequestEnd'])
    req_time = (end - start).total_seconds()

    row = {'time in s': req_time,
                 'status': 'ok' if 0 == req['returnCode'] else 'failed with code=' + str(req['returnCode'])}

    # breakdown of input/output rows
    row['normal (in)'] = req['input_paths_taken']['normal']
    row['general (in)'] = req['input_paths_taken']['general']
    row['fallback (in)'] = req['input_paths_taken']['fallback']
    row['unresolved (in)'] = req['input_paths_taken']['unresolved']

    row['normal (out)'] = req['output_paths_taken']['normal']
    row['unresolved (out)'] = req['output_paths_taken']['unresolved']

    rows.append(row)
df_reqs = pd.DataFrame(rows)
st.write('Individual requests overview:')
st.dataframe(df_reqs)

