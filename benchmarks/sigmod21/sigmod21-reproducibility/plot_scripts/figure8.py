#!/usr/bin/env python
# coding: utf-8

# ## 311
# This notebook contains code to plot figure 8

# In[1]:
import warnings
warnings.filterwarnings("ignore")
import logging

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

# make nice looking plot
from .paper import *

adjust_settings()


# In[91]:


def load_311_to_df(paths):
    rows = []

    for path in sorted(paths):
        file = os.path.basename(path)
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
                run_no = int(path[path.rfind('-')+1:path.rfind('.txt')])

                # tuplex decode
                if 'tuplex' in file:
                    d = json.loads(list(filter(lambda x: 'startupTime' in x, lines))[0])

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
                elif 'weld-run' in file:
                    keys = lines[-2].strip().split(',')
                    vals = lines[-1].strip().split(',')
                    d = dict(zip(keys, vals))
                else:
                    d = json.loads(lines[-1].replace("'", '"'))

                readTime = d['readTime'] if 'readTime' in d else 0.
                writeTime = d['writeTime'] if 'writeTime' in d else 0.
                startupTime = d['startupTime'] if 'startupTime' in d else 0.
                row = {'startup_time' : startupTime, "load":readTime, "write": writeTime}
                if 'weld' in file:
                    row['compute'] = d['query'] if 'query' in d else d['jobTime']
                    row['job_time'] = 0
                    if 'weld-run' in file:
                        row['job_time'] = d['query']
                        row['load'] = d['pandas_load'] + d['load']
                    else:
                        row['job_time'] = d['jobTime']
                    row['mode'] = 'weld'
                else:
                    assert 'e2e' in file
                    row['compute'] = 0
                    row['job_time'] = d['jobTime']
                    row['mode'] = 'e2e'
                row['framework'] = name
                if name == 'sttuplex':
                    row['framework'] = 'tuplex'
                    row['mode'] += '-st'
                if len(row) > 0:
                    row['run'] = run_no
                    rows.append(row)
                else:
                    print(file)
    return pd.DataFrame(rows)


# In[121]:


def load_311_to_df(paths):
    rows = []

    for path in sorted(paths):
        file = os.path.basename(path)
        name = file[:file.find('-run')]
        if file.endswith('.txt'):
            # print(path)
            with open(path, 'r') as fp:
                lines = fp.readlines()

                # skip empty files (means timeout)
                if len(lines) == 0:
                    continue

                row = {}

                # tuplex decode
                if 'tuplex' in file:
#                     d = {}
#                     if 'tuplex' in name:
                    d = json.loads(list(filter(lambda x: 'startupTime' in x, lines))[0])
#                     else:
#                         d = json.loads(lines[-1].replace("'", '"'))

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
                else:
                    d = json.loads(lines[-1].replace("'", '"'))

#                     # clean keys
#                     row = {'startup_time': d['startupTime'], 'job_time': d['jobTime'], }
#                     if 'load_time' in d.keys():
#                         row['load'] = d['load_time']
#                     if 'run_time' in d.keys():
#                         row['compute'] = d['run_time']
#                     if 'write_time' in d.keys():
#                         row['write'] = d['write_time']
#                     if 'jobTime' in d.keys():
#                         row['job_time'] = d['jobTime']
#                     else:
#                         row['job_time'] = d['job_time']
                    
#                     if 'startup_time' in d.keys():
#                         row['startup_time'] = d['startup_time']

                readTime = d['readTime'] if 'readTime' in d else 0.
                writeTime = d['writeTime'] if 'writeTime' in d else 0.
                row = {'startup_time' : d['startupTime'], "load":readTime, "write": writeTime}
                if 'weld' in file:
                    row['compute'] = d['jobTime']
                    row['job_time'] = 0
                    row['mode'] = 'weld'
                else:
                    assert 'e2e' in file
                    row['compute'] = 0
                    row['job_time'] = d['jobTime']
                    row['mode'] = 'e2e'
                row['framework'] = name
                if name == 'sttuplex':
                    row['framework'] = 'tuplex'
                    row['mode'] += '-st'
                if len(row) > 0:
                    rows.append(row)
                else:
                    print(file)
    return pd.DataFrame(rows)[['startup_time', 'job_time', 'load', 'compute', 'write', 'framework', 'mode']]


# In[132]:


# from benchmark/311/notebooks/DataConversion

def load_paths(paths):
    rows = []
    for path in paths:
        text = open(path, 'r').readlines()
        D = dict(zip(text[-2].strip().split(','), text[-1].strip().split(',')))
        name = os.path.basename(path)
        fw = name[:name.find('-run')]
        D['framework'] = fw
        rows.append(D)
    return rows


def figure8(path_311='r5d.8xlarge/311/', output_folder='plots'):

    logging.info('Loading 311 benchmark data...')
    if not path_311.endswith('/'):
        path_311 += '/'
    weld_paths = glob.glob(path_311 + 'weld-run-*.txt')
    original_weld_paths = weld_paths

    # exclude first run
    weld_paths = list(filter(lambda p: 'run-1.txt' not in p, weld_paths))


    df_weld = pd.DataFrame(load_paths(weld_paths))
    df_weld['load'] = df_weld['load'].astype(float)
    df_weld['query'] = df_weld['query'].astype(float)
    df_weld['pandas_load'] = df_weld['pandas_load'].astype(float)

    # note: changed from previous paper plot, so it's a true end-to-end time for weld!
    df_weld['load'] += df_weld['pandas_load']

    df_weld['total'] = df_weld['load'] + df_weld['query']
    df_weld = df_weld[['framework', 'load', 'query', 'total']]
    df_weld['e2e'] = False
    df_weld['threads'] = 1

    nonweld_paths = set(glob.glob(path_311 + '/*.txt')) - set(original_weld_paths)

    # exclude first runs
    nonweld_paths = list(filter(lambda p: 'run-1.txt' not in p, nonweld_paths))

    df = load_311_to_df(nonweld_paths)


    # In[135]:


    df['e2e'] = df['mode'].apply(lambda x: 'e2e' in x)
    df['query'] = df['compute'] + df['write'] + df['job_time']
    df['total'] = df['query'] + df['load']
    df['threads'] = df['mode'].apply(lambda x: 1 if '-st' in x else 16).astype(int)
    df = df[['framework', 'load', 'query', 'total', 'e2e', 'threads']]

    df = pd.concat((df_weld, df)).sort_values(by=['threads', 'total'])
    # df.to_csv('311.csv', index=None)

    df_mu = df.groupby(['framework', 'threads', 'e2e']).mean()[['load', 'query', 'total']].reset_index()
    df_std = df.groupby(['framework', 'threads', 'e2e']).std()[['load', 'query', 'total']].reset_index()

    logging.info('Plotting 311 graph...')

    # global plot settings
    rot = 45
    mks = 20
    w = .8
    w2 = w/2
    aoff = 0.5
    precision = 1
    cc_col = [0, 0, 0]
    tplx_col = sns.color_palette()[0]
    tplxst_col = 0.6 * np.array(tplx_col)
    dask_col = np.array(sns.color_palette()[3])
    pyspark_col = 1.2 * np.array(sns.color_palette()[2])
    pysparksql_col = 0.6 * np.array(pyspark_col)

    py_col = pyspark_col
    cython_col = [161/255., 67/255., 133/255.]
    nuitka_col = [123/255, 88/255, 219/255.]

    weld_col = [0.5, 0.5, 0.5]
    hyper_col = [0.7, 0.7, 0.7]

    weld_row_mu = df_mu[df_mu['framework'] == 'weld']
    weld_row_std = df_std[df_std['framework'] == 'weld']

    dask_row_mu = df_mu[df_mu['framework'] == 'dask']
    dask_row_std = df_std[df_std['framework'] == 'dask']

    # helper function for one subplot
    def plot_bars(ax, subdf_mu, subdf_std, key='total', aoff=aoff, precision=precision,number_positions=None,cutoff=None):
        if number_positions is None:
            number_positions = ['above'] + ['center', 'center', 'center', 'center'] * 2


        plt_bar(ax, 1, subdf_mu[(subdf_mu['framework'] == 'tuplex') & (subdf_mu['threads'] == 1)][key], w, tplxst_col, 'Tuplex', number_positions[0],
            precision=precision, above_offset=aoff,
           yerr=subdf_std[(subdf_std['framework'] == 'tuplex') & (subdf_std['threads'] == 1)][key])

        plt_bar(ax, 0, weld_row_mu[key], w, weld_col, 'Weld', number_positions[1],
                precision=precision, above_offset=aoff,
               yerr=weld_row_std[key])

        ax.axvline(1.5, linestyle='--', lw=2,color=[.6,.6,.6])

    #     plt_bar(ax, 2, subdf_mu[subdf_mu['framework'] == 'hyper'][key], w, hyper_col, 'Hyper', number_positions[2],
    #             precision=precision, above_offset=aoff,
    #            yerr=subdf_std[subdf_std['framework'] == 'hyper'][key])

        plt_bar(ax, 5, subdf_mu[(subdf_mu['framework'] == 'tuplex') & (subdf_mu['threads'] == 16)][key], w, tplx_col, 'Tuplex', number_positions[2],
            precision=precision, above_offset=aoff,
           yerr=subdf_std[(subdf_std['framework'] == 'tuplex') & (subdf_std['threads'] == 16)][key])

        plt_bar(ax, 4, dask_row_mu[key], w, dask_col, 'Dask', number_positions[3],
                precision=precision, above_offset=aoff,
               yerr=dask_row_std[key])

        mu = subdf_mu[subdf_mu['framework'] == 'pysparksql'][key]
        sig = subdf_std[subdf_std['framework'] == 'pysparksql'][key]
    #     if cutoff and mu.mean() <= cutoff:
    #         cutoff = None

        th = 0.75

        # hack:
        if cutoff > 100:
            th *= 10

        plt_bar(ax, 3, mu, w, pysparksql_col,
                'PySparkSQL', number_positions[4],
                precision=precision, above_offset=aoff, cutoff=cutoff, th=th,
               yerr=sig)

        mu = subdf_mu[subdf_mu['framework'] == 'pyspark'][key]
        sig = subdf_std[subdf_std['framework'] == 'pyspark'][key]
    #     if cutoff and mu.mean() <= cutoff:
    #         cutoff = None

        plt_bar(ax, 2, mu, w, pyspark_col,
                'PySpark', number_positions[5],
                precision=precision, above_offset=aoff, cutoff=cutoff, th=th,
               yerr=sig)

        # plt_bar(ax, 5, subdf_mu[subdf_mu['framework'] == 'pandas'][key], w, pyspark_col, 'Pandas', number_positions[5],
        #        precision=precision, above_offset=aoff,
        #       yerr=subdf_std[subdf_std['framework'] == 'pandas'][key])


        # ax.set_xticks([0, 1, 2, 3, 4, 5])
        # ax.set_xticklabels(['Tuplex', 'Weld', 'Hyper', 'PySparkSQL', 'PySpark', 'Pandas'], rotation=rot)
        ax.set_xticks([0, 1, 2, 3, 4, 5])
        ax.set_xticklabels(['Weld', 'Tuplex', 'PySpark', 'PySparkSQL', 'Dask', 'Tuplex'], ha='right', rotation=rot)
        ax.grid(axis='x')
        sns.despine()


    # In[157]:


    ee = ['pyspark', 'pysparksql', 'weld', 'tuplex', 'dask']


    sf = 1.2
    fig, axs = plt.subplots(figsize=(sf * column_width, sf *column_width / rho*.6),
                            nrows=1, ncols=2, constrained_layout=True)
    axs = list(axs.flat)

    lim_high = 16
    cutoff = 14

    ao = 0.75

    ##### SF-1 ######
    ax = axs[1]
    subdf_mu = df_mu[df_mu['e2e'] == True]
    subdf_std = df_std[df_std['e2e'] == True]
    plot_bars(ax, subdf_mu, subdf_std, 'total', aoff=ao*10, cutoff=cutoff * 10,
             number_positions=['above', 'above', 'above', 'above', 'center', 'center'])
    ax.set_xlabel('(b) end-to-end', fontsize=27, labelpad=10)
    ax.set_ylabel('time in s', labelpad=6)
    ax.text(0.375, lim_high*10, 'single-threaded', va='center', ha='center', fontsize=16)
    ax.text(5.5, lim_high*10, 'multi-threaded (16x)', va='center', ha='right', fontsize=16)
    ax.set_ylim(0, lim_high * 10)
    ax.set_yticks(np.arange(0, 15, 5) * 10)
    ax = axs[0]
    subdf_mu = df_mu[df_mu['e2e'] == False]
    subdf_std = df_std[df_std['e2e'] == False]
    plot_bars(ax, subdf_mu, subdf_std, 'query', aoff=ao, cutoff=cutoff,
             number_positions=['center', 'center', 'above', 'center', 'center', 'center'])
    ax.set_xlabel('(a) query-time only', fontsize=27, labelpad=10)
    ax.set_ylabel('time in s', labelpad=10)
    ax.set_ylim(0, lim_high)
    ax.text(0.375, lim_high, 'single-threaded', va='center', ha='center', fontsize=16)
    ax.text(5.5, lim_high, 'multi-threaded (16x)', va='center', ha='right', fontsize=16)
    ax.set_yticks(np.arange(0, 15, 5))
    plt.savefig(os.path.join(output_folder, 'figure8_311.pdf'), dpi=120, transparent=True)
