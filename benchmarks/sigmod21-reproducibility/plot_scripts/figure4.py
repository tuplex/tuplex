#!/usr/bin/env python
# coding: utf-8

# ## Flights core-exp plot

# In[1]:


import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import re
import json

import seaborn as sns

import datetime

from matplotlib.patches import Patch
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
from matplotlib.path import *

import scipy.constants
rho = scipy.constants.golden

import warnings
warnings.filterwarnings("ignore")
import logging

# make nice looking plot
from .paper import *

adjust_settings()


# In[2]:


def load_flights_to_df(data_root):
    files = os.listdir(data_root)
    rows = []

    for file in files:
        path = os.path.join(data_root, file)
        name = file[:file.find('-run')]
        if file.endswith('.txt'):
            # print(path)
            with open(path, 'r') as fp:
                lines = fp.readlines()

                # skip empty files (means timeout)
                if len(lines) == 0:
                    continue

                row = {}

                # get run number
                run_no = int(path[path.rfind('run-')+4:path.rfind('.txt')])
                    
                # tuplex decode
                if 'tuplex' in path:
                    try:
                        d = {}
                        if 'tuplex' in name:
                            d = json.loads(list(filter(lambda x: 'startupTime' in x, lines))[0])
                        else:
                            d = json.loads(lines[-1].replace("'", '"'))

                        load_time = 0.
                        if 'io_load_time' in d.keys():
                            load_time = d['io_load_time']
                        if 'io_load' in d.keys():
                            load_time = d['io_load']
                        row = {'startup_time' : d['startupTime'], 'job_time':d['jobTime'], "load":load_time}
                        row['framework'] = 'tuplex'

                        #breakdown time extract for tuplex
                        c = '\n'.join(lines)

                        pattern = re.compile('load&transform tasks in (\d+.\d+)s')
                        lt_times = np.array(list(map(float, pattern.findall(c))))

                        pattern = re.compile('compiled to x86 in (\d+.\d+)s')
                        cp_times = np.array(list(map(float, pattern.findall(c))))

                        m = re.search('writing output took (\d+.\d+)s', c)
                        if m:    
                            w_time = float(m[1])
                            row['write'] = w_time
                        lt_time = lt_times.sum()
                        cp_time = cp_times.sum()
                        row['compute'] = lt_time
                        row['compile'] = cp_time
                        
                        # override if compute is available
                        if 'compute_time' in d.keys():
                            row['compute'] = d['compute_time']
                    except Exception as e:
                        print(e)
                    row['mode'] = 'python3'
                else:
                    d = json.loads(lines[-1].replace("'", '"'))
                    
                    # clean framework
                    if 'pyspark' in file.lower():
                        # adjust spark types
                        row['framework'] = 'spark'
                    if 'dask' in file.lower():
                         # adjust spark types
                        row['framework'] = 'dask'
                        
                    # clean keys
                    if 'load_time' in d.keys():
                        row['load'] = d['load_time']
                    if 'run_time' in d.keys():
                        row['compute'] = d['run_time']
                    if 'write_time' in d.keys():
                        row['write'] = d['write_time']
                    if 'jobTime' in d.keys():
                        row['job_time'] = d['jobTime']
                    else:
                        row['job_time'] = d['job_time']
                    
                    if 'startup_time' in d.keys():
                        row['startup_time'] = d['startup_time']
                    row['mode'] = 'python3'

                # size
                if 'small' not in path:
                    row['size'] = '30G'
                if 'small' in path:
                    row['size'] = '5G'
                if len(row) > 0:
                    row['run'] = run_no
                    rows.append(row)
    return pd.DataFrame(rows)[['startup_time', 'job_time', 'framework', 'mode', 'size', 'run']]


# In[ ]:
def figure4(flights_path='r5d.8xlarge/flights', output_folder='plots'):

    df30 = load_flights_to_df(os.path.join(flights_path, 'flights'))
    df5 = load_flights_to_df(os.path.join(flights_path, 'flights_small/'))

    logging.info('Plotting flights graph')

    # exclude first run (warmup run)
    df30 = df30[df30['run'] != 1]
    df5 = df5[df5['run'] != 1]

    df30_mu = df30.groupby(['framework']).mean().reset_index()
    df5_mu = df5.groupby(['framework']).mean().reset_index()

    df30_std = df30.groupby(['framework']).std().reset_index()
    df5_std = df5.groupby(['framework']).std().reset_index()


    # links: https://stackoverflow.com/questions/14852821/aligning-rotated-xticklabels-with-their-respective-xticks
    sf = 1
    fig, axs = plt.subplots(figsize=(sf * column_width, sf *column_width / rho*.4), nrows=1, ncols=2, constrained_layout=True)
    rot = 0
    mks = 20
    w = .5
    w2 = w/2
    aoff = 20
    precision = 0
    cc_col = [0, 0, 0]
    tplx_col = sns.color_palette()[0]
    dask_col = np.array(sns.color_palette()[3])
    pyspark_col = 1.2 * np.array(sns.color_palette()[2])
    pysparksql_col = 0.6 * np.array(pyspark_col)

    py_col = pyspark_col
    cython_col = [161/255., 67/255., 133/255.]
    nuitka_col = [123/255, 88/255, 219/255.]

    axs = list(axs.flat)


    ##### small dataset ######
    ax = axs[0]

    pysparksql_err = np.array([df5_std[df5_std['framework'] == 'spark']['job_time']])
    tplx_err = np.array(df5_std[df5_std['framework'] == 'tuplex']['job_time'])

    plt_bar(ax, 0, df5_mu[df5_mu['framework'] == 'dask']['job_time'], w, dask_col,
            'Dask', 'center', precision=precision)
    plt_bar(ax, 1, df5_mu[df5_mu['framework'] == 'spark']['job_time'], w, pysparksql_col, 'PySparkSQL', 'above',
            precision=precision, above_offset=aoff, yerr=pysparksql_err)

    plt_bar(ax, 2, df5_mu[df5_mu['framework'] == 'tuplex']['job_time'], w, tplx_col, 'Tuplex', 'above',
            precision=precision, above_offset=aoff, yerr=tplx_err)


    legend_elements = [ Line2D([0], [0], marker='o', color='w', label='Dask',
                              markerfacecolor=dask_col, markersize=mks),
                        Line2D([0], [0], marker='o', color='w', label='PySparkSQL',
                              markerfacecolor=pysparksql_col, markersize=mks),
                       Line2D([0], [0],  marker='o', color='w', label='Tuplex',
                              markerfacecolor=tplx_col, markersize=mks)]
    L = ax.legend(handles=legend_elements, loc='upper right', fontsize=16,
                 bbox_to_anchor=(1, 1), borderaxespad=0., handletextpad=0.0, ncol=3, columnspacing=0)
    cols = [dask_col, pysparksql_col, tplx_col]
    for i, text in enumerate(L.get_texts()):
        text.set_color(cols[i])

    ax.set_yticks(np.arange(0, 200, 50))
    ax.set_xticks([0, 1, 2])
    ax.set_xticklabels(['Dask', 'PySparkSQL', 'Tuplex'], rotation=rot)
    ax.grid(axis='x')
    sns.despine()
    ax.set_ylim(0, 250)
    ax.set_xlim(-.5, 2.5)
    ax.set_xlabel('(a) 5.9 GB input', fontsize=27, labelpad=10)
    ax.set_ylabel('runtime in s', labelpad=10)

    ##### Large dataset ######
    ax = axs[1]


    pysparksql_err = np.array([df30_std[df30_std['framework'] == 'spark']['job_time']])
    tplx_err = np.array(df30_std[df30_std['framework'] == 'tuplex']['job_time'])

    plt_bar(ax, 0, df30_mu[df30_mu['framework'] == 'dask']['job_time'], w, dask_col,
            'Dask', 'center', precision=precision, cutoff=240, th=10)
    plt_bar(ax, 1, df30_mu[df30_mu['framework'] == 'spark']['job_time'], w, pysparksql_col,
            'PySparkSQL', 'center', precision=precision, above_offset= 4 * aoff,
           yerr=pysparksql_err)
    plt_bar(ax, 2, df30_mu[df30_mu['framework'] == 'tuplex']['job_time'], w, tplx_col, 'Tuplex',
            'above', precision=precision, above_offset=aoff,
           yerr=tplx_err)

    ax.set_yticks(np.arange(0, 250, 50))
    ax.set_xticks([0, 1, 2])
    ax.set_xticklabels(['Dask', 'PySparkSQL', 'Tuplex'], rotation=rot)
    ax.grid(axis='x')
    sns.despine()
    ax.set_ylim(0, 250)
    ax.set_xlim(-.5, 2.5)
    ax.set_xlabel('(b) 30.4 GB input', fontsize=27, labelpad=10)

    plt.savefig(os.path.join(output_folder, 'figure4_flights.pdf'), transparent=True, bbox_inches = 'tight', pad_inches = 0)