#!/usr/bin/env python
# coding: utf-8

# ## Optimizations breakdown
# This notebook contains code to produce figure 10

# In[13]:
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


import os
import glob

from matplotlib.patches import Patch
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
from matplotlib.path import *

import scipy.constants
rho = scipy.constants.golden

# make nice looking plot
from .paper import *

adjust_settings()

def load_to_df(data_root):
    files = os.listdir(data_root)
    rows = []

    for file in files:
        path = os.path.join(data_root, file)
        
        if not path.endswith('txt'):
            continue
        
        name = file[:file.find('run')]
        if 'stderr' not in file:
            # print(path)
            with open(path, 'r') as fp:
                lines = fp.readlines()
                
                # skip empty files (means timeout)
                if len(lines) == 0:
                    continue
                    
                # skip first run
                if 'run-1.txt' in path:
                    continue
                    
                try:
                    d = {}
                    if 'tuplex' in name:
                        d = json.loads(list(filter(lambda x: 'startupTime' in x, lines))[0])
                    else:
                        d = json.loads(lines[-1].replace("'", '"'))

                    load_time = 0.0
                    if 'io_load_time' in d.keys():
                        load_time = d['io_load_time']
                    if 'io_load' in d.keys():
                        load_time = d['io_load']
                    row = {'startup_time' : d['startupTime'], 'job_time':d['jobTime'], "load":load_time}
                    row['framework'] = name[:name.find('-')]
                    row['type'] = name.split('-')[1]
                    
                     #breakdown time extract
                    c = '\n'.join(lines)

                    pattern = re.compile('load&transform tasks in (\d+.\d+)s')
                    lt_times = np.array(list(map(float, pattern.findall(c))))

                    pattern = re.compile('compiled to x86 in (\d+.\d+)s')
                    cp_times = np.array(list(map(float, pattern.findall(c))))

                    m = re.search('writing output took (\d+.\d+)s', c)
                    w_time = float(m[1])
                    lt_time = lt_times.sum()
                    cp_time = cp_times.sum()
                    row['load_and_transform'] = lt_time
                    row['write'] = w_time
                    row['compile'] = cp_time
                    
                    # find the line where there is "webui.enable": false => extract as json dict!
                    settings = json.loads(list(filter(lambda x: '"webui.enable": false' in x, lines))[0])
                    row.update(settings)
                    row['stageFusion'] = 'nosf' in name
                    
                    # find metrics
                    metrics = json.loads(list(filter(lambda x: '"total_compilation_time_s":' in x, lines))[0])
                    row.update(metrics)
                    rows.append(row)
                except:
                    # print('bad path: {}'.format(path))
                    pass
    return pd.DataFrame(rows)


# In[15]:

def figure10(flights_path='r5d.8xlarge/flights/', output_folder='plots'):
    logging.info('loading flights data...')

    df_flights = load_to_df(os.path.join(flights_path, 'breakdown_small/'))

    # clean type based on settings flags!
    def extractType(row):
        t = ''
        if row['stageFusion']:
            t += 'sf'
        else:
            t += 'nosf'
        if row['csv.selectionPushdown']:
            t += '+logical'
        if row['optimizer.nullValueOptimization']:
            t += '+null'
        if row['useLLVMOptimizer']:
            t += '+llvm'
        return t
    df_flights['type'] = df_flights.apply(extractType, axis=1)
    df_flights['compile'] = df_flights['job_time'] - df_flights['load_and_transform'] - df_flights['write']
    df_flights['compute'] = df_flights['job_time'] - df_flights['write'] - df_flights['load'] - df_flights['compile']


    # these need to be the numbers with spark sim
    #t_unopt = df_flights[df_flights['type'] == 'noopt']['compute']
    t_logical_sf = df_flights[df_flights['type'] == 'sf+logical']['compute']
    t_logical_null_sf = df_flights[df_flights['type'] == 'sf+logical+null']['compute']
    t_logical_null_llvm_sf = df_flights[df_flights['type'] == 'sf+logical+null+llvm']['compute']

    t_llvm_logical_sf = df_flights[df_flights['type'] == 'sf+logical+llvm']['compute']
    t_llvm_logical_sf_null = df_flights[df_flights['type'] == 'sf+logical+null+llvm']['compute']

    t_logical = df_flights[df_flights['type'] == 'nosf+logical']['compute']
    t_llvm_logical = df_flights[df_flights['type'] == 'nosf+logical+llvm']['compute']
    t_unopt = df_flights[df_flights['type'] == 'nosf']['compute']
    t_llvm = df_flights[df_flights['type'] == 'nosf+llvm']['compute']


    logging.info('Plotting breakdown...')
    f = plt.figure(constrained_layout=True, figsize=(column_width, .6 * column_width/rho))

    ax = plt.gca()

    w = .8
    w2 = w/2.
    rot = 25

    tplx_col = np.array(sns.color_palette()[0])
    base_gray = np.array([.8, .8, .8])
    alpha = .4
    tplx_col_nollvm = alpha * tplx_col + (1. - alpha) * base_gray
    aoff=2.5

    prcs = 1
    fsize=28

    # zoom-in / limit the view to different portions of the data
    # ax.set_ylim(0, 11500)  # outliers only

    # hide the spines between ax and ax2
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)

    plt_bar(ax, 0, t_unopt, w, tplx_col_nollvm, 'Tuplex', 'center', fsize=fsize, precision=prcs, yerr=np.array([t_unopt.std()]))
    plt_bar(ax, 1, t_logical, w, tplx_col_nollvm, 'Tuplex', 'center', fsize=fsize, precision=prcs, yerr=np.array([t_logical.std()]))
    plt_bar(ax, 2, t_logical_sf, w, tplx_col_nollvm, 'Tuplex', 'center', fsize=fsize, precision=prcs, yerr=np.array([t_logical_sf.std()]))
    plt_bar(ax, 3, t_logical_null_sf, w, tplx_col_nollvm, 'Tuplex', 'above', fsize=fsize, above_offset=aoff, precision=prcs, yerr=np.array([t_logical_null_sf.std()]))
    plt_bar(ax, 4, t_logical_null_llvm_sf, w, tplx_col, 'Tuplex', 'above', fsize=fsize, above_offset=aoff, precision=prcs, yerr=np.array([t_logical_null_llvm_sf.std()]))

    plt_bar(ax, 6, t_unopt, w, tplx_col_nollvm, 'Tuplex', 'center', fsize=fsize, precision=prcs, yerr=np.array([t_unopt.std()]))
    plt_bar(ax, 7, t_llvm, w, tplx_col, 'Tuplex', 'center', fsize=fsize, precision=prcs, yerr=np.array([t_unopt.std() * 1.8]))
    plt_bar(ax, 8, t_llvm_logical, w, tplx_col, 'Tuplex', 'center', fsize=fsize, above_offset=aoff, precision=prcs, yerr=np.array([t_llvm_logical.std()]))
    plt_bar(ax, 9, t_llvm_logical_sf, w, tplx_col, 'Tuplex', 'above', fsize=fsize, above_offset=aoff, precision=prcs, yerr=np.array([t_llvm_logical_sf.std()]))
    plt_bar(ax, 10, t_llvm_logical_sf_null, w, tplx_col, 'Tuplex', 'above', fsize=fsize, above_offset=aoff, precision=prcs, yerr=np.array([t_llvm_logical_sf_null.std()]))
    # # tuplex
    # plt_bar(ax, 2+w2, tplx_percol_regex, w, tplx_col, 'Tuplex', 'above')
    # plt_bar(ax, 3+w, tplx_single_regex, w, tplx_col, 'Tuplex', 'above')
    # plt_bar(ax, w, tplx_strip, w, tplx_col, 'Tuplex', 'above')
    # plt_bar(ax, 1+w2, tplx_split, w, tplx_col, 'Tuplex', 'above')

    # # dask bars
    # plt_bar(ax, 3, dask_single_regex, w, dask_col, 'Dask', 'center')
    # plt_bar(ax, 0, dask_strip, w, dask_col, 'Dask', 'center')

    # # pyspark bars
    # plt_bar(ax, 3-w, pyspark_single_regex_udf, w, pyspark_col, 'PySpark (UDF)', 'center', 4500)
    # plt_bar(ax, -w, pyspark_strip_udf, w, pyspark_col, 'PySpark (UDF)', 'center', 4500)
    # plt_bar(ax, 2-w2, pyspark_percol_regex, w, pysparksql_col, 'PySparkSQL', 'above')
    # plt_bar(ax, 1-w2, pyspark_split, w, pysparksql_col, 'PySparkSQL', 'center')

    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.yaxis.set_ticks_position('left')
    ax.tick_params(direction='out', length=6, width=1.5, colors=[.2, .2, .2], grid_alpha=.3)
    # ax.set_yticks(np.arange(0, 5000, 1000))
    ax.grid(axis='x')
    ax.axvline(5, linestyle='--', lw=2,color=[.6,.6,.6])
    # ax.set_ylim(0, 5000)
    ax.set_xticks([0, 1, 2, 3, 4, 6, 7, 8, 9, 10])
    ax.set_xlim(-.5, 10.5)
    ax.set_yticks([10,20,30,40, 50])
    ax.set_xticklabels(['unopt.', '+ logical', '+ stage fus.', '+ null opt.', '+ LLVM']
                       + ['unopt.', '+ LLVM', '+ logical', '+ stage fus.', '+ null opt.'], rotation=rot, ha='right')
    plt.ylabel('compute time in $\\mathrm{s}$')

    # manual legend
    mks = 20
    legend_elements = [ Line2D([0], [0], marker='o', color='w', label=' Tuplex only',
                              markerfacecolor=tplx_col_nollvm, markersize=mks),
                       Line2D([0], [0], marker='o', color='w', label='with LLVM Opt.',
                              markerfacecolor=tplx_col, markersize=mks)
                      ]

    L = ax.legend(handles=legend_elements, loc='upper right', fontsize=18,
                  bbox_to_anchor=(0.435, 1), borderaxespad=0.)
    # cols = [pyspark_col, pysparksql_col, dask_col, tplx_col]
    # for i, text in enumerate(L.get_texts()):
    #     text.set_color(cols[i])

    # plt.tight_layout()
    plt.savefig(os.path.join(output_folder, 'figure10_opt-breakdown_shrunk.pdf'), transparent=True)

