#!/usr/bin/env python3

# streamlit app to quickly display experimental results

import streamlit as st
import pandas as pd
import json
import os
import glob


DATA_PATH='/Users/leonhards/projects/tuplex-public/tuplex/cmake-build-debug/dist/bin/aws_job.json'

data = json.loads(open(DATA_PATH, 'r').read())

st.title('Lambda Job overview')


stage_runtime_in_s = (data['stageEndTimestamp'] - data['stageStartTimestamp']) / 1e9
col1, col2, col3 = st.columns(3)
col1.metric('Runtime', '{:.2f}s'.format(stage_runtime_in_s), help='how long the job took to run')
col2.metric('Hyperspecialization', 'On' if data['hyper_mode'] else 'Off')
#col3.metric('Cost', '${:.2f}s'.format(data['cost']), help='total cost to run (incl. S3)')

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
