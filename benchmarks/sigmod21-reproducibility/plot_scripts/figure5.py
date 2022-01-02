#!/usr/bin/env python
# coding: utf-8

# In[1]:


import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import re
import json

from matplotlib.patches import Patch
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
from matplotlib.path import *

import scipy.constants

import seaborn as sns


import os
import glob

import warnings
warnings.filterwarnings("ignore")
import logging

# make nice looking plot
from .paper import *

adjust_settings()


# In[2]:


matplotlib.__version__


# In[3]:


rho = scipy.constants.golden


# In[4]:
def figure5(logs_path='r5d.8xlarge/logs/', output_folder='plots'):
    logging.info('loading logs data')

    df = parse_logs_exp_to_df(logs_path)

    tplx_percol_regex = df[(df['framework'] == 'tuplex') & (df['type'] == 'split-regex')]['job_time']
    tplx_single_regex = df[(df['framework'] == 'tuplex') & (df['type'] == 'regex')]['job_time']
    tplx_strip = df[(df['framework'] == 'tuplex') & (df['type'] == 'strip')]['job_time']
    tplx_split = df[(df['framework'] == 'tuplex') & (df['type'] == 'split')]['job_time']



    dask_single_regex = df[(df['framework'] == 'dask') & (df['type'] == 'regex')]['job_time']
    dask_strip = df[(df['framework'] == 'dask') & (df['type'] == 'strip')]['job_time']


    pyspark_single_regex_udf = df[(df['framework'] == 'pyspark') & (df['type'] == 'python-regex')]['job_time']
    pyspark_strip_udf = df[(df['framework'] == 'pyspark') & (df['type'] == 'python-strip')]['job_time']

    pysparksql_percol_regex = df[(df['framework'] == 'pysparksql') & (df['type'] == 'regex')]['job_time']
    pysparksql_split = df[(df['framework'] == 'pysparksql') & (df['type'] == 'split')]['job_time']

    logging.info('Plotting logs')
    tplx_col = sns.color_palette()[0]
    dask_col = np.array(sns.color_palette()[3])
    pyspark_col = 1.2 * np.array(sns.color_palette()[2])
    pysparksql_col = 0.6 * np.array(pyspark_col)


    # for rect in b:
    #    height = rect.get_height()
    # plt.text(rect.get_x() + rect.get_width()/2.0, height, '%d' % int(height), ha='center', va='bottom')
    # In[8]:


    df[['framework', 'type']].drop_duplicates()


    # In[9]:


    f = plt.figure(constrained_layout=True, figsize=(column_width, column_width/rho*.5))

    ax = plt.gca()

    w = .25
    w2 = w/2.

    ao = 40

    # zoom-in / limit the view to different portions of the data
    ax.set_ylim(0, 11500)  # outliers only

    # hide the spines between ax and ax2
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)

    # tuplex
    plt_bar(ax, 2+w2, tplx_percol_regex, w, tplx_col, 'Tuplex', 'above', above_offset=ao, yerr=np.array([tplx_percol_regex.std()]))
    plt_bar(ax, 3+w, tplx_single_regex, w, tplx_col, 'Tuplex', 'above', above_offset=ao, yerr=np.array([tplx_single_regex.std()]))
    plt_bar(ax, w, tplx_strip, w, tplx_col, 'Tuplex', 'above', above_offset=ao, yerr=np.array([tplx_strip.std()]))
    plt_bar(ax, 1+w2, tplx_split, w, tplx_col, 'Tuplex', 'above', above_offset=ao, yerr=np.array([tplx_split.std()]))

    # dask bars
    plt_bar(ax, 3, dask_single_regex, w, dask_col, 'Dask', 'center', yerr=np.array([dask_single_regex.std()]))
    plt_bar(ax, 0, dask_strip, w, dask_col, 'Dask', 'center', yerr=np.array([dask_strip.std()]))

    # pyspark bars
    plt_bar(ax, 3-w, pyspark_single_regex_udf, w, pyspark_col, 'PySpark (UDF)', 'center', 1120, th=40)
    plt_bar(ax, -w, pyspark_strip_udf, w, pyspark_col, 'PySpark (UDF)', 'center', 1120, th=40)
    plt_bar(ax, 2-w2, pysparksql_percol_regex, w, pysparksql_col, 'PySparkSQL', 'center', yerr=np.array([pysparksql_percol_regex.std()]))
    plt_bar(ax, 1-w2, pysparksql_split, w, pysparksql_col, 'PySparkSQL', 'center', yerr=np.array([pysparksql_split.std()]))

    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.yaxis.set_ticks_position('left')
    ax.tick_params(direction='out', length=6, width=1.5, colors=[.2, .2, .2], grid_alpha=.3)
    ax.set_yticks(np.arange(0, 5000, 1000))
    ax.grid(axis='x')
    ax.set_xticks([0, 1, 2, 3])
    ax.set_xlim(-.5, 3.5)
    ax.set_ylim(0, 1200)
    ax.set_yticks(np.arange(0, 1100, 250))
    ax.set_xticklabels(['strip', 'split',
                        'per-column\nregex', 'single\nregex'])
    plt.ylabel('runtime in $\\mathrm{s}$')

    # manual legend
    mks = 20
    legend_elements = [ Line2D([0], [0], marker='o', color='w', label='PySpark',
                              markerfacecolor=pyspark_col, markersize=mks),
                      Line2D([0], [0], marker='o', color='w', label='PySparkSQL',
                              markerfacecolor=pysparksql_col, markersize=mks),
                        Line2D([0], [0], marker='o', color='w', label='Dask',
                              markerfacecolor=dask_col, markersize=mks),
                       Line2D([0], [0], pickradius=2, marker='o', color='w', label='Tuplex',
                              markerfacecolor=tplx_col, markersize=mks)]

    L = ax.legend(handles=legend_elements, loc='upper center', fontsize=16,
                  bbox_to_anchor=(.466, 1), borderaxespad=0., handletextpad=0., ncol=4, columnspacing=0)
    cols = [pyspark_col, pysparksql_col, dask_col, tplx_col]
    for i, text in enumerate(L.get_texts()):
        text.set_color(cols[i])

    plt.tight_layout()
    plt.savefig(os.path.join(output_folder, 'figure5_logsquery.pdf'), transparent=True, bbox_inches = 'tight', pad_inches = 0)