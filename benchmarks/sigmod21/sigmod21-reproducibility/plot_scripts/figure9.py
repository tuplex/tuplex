#!/usr/bin/env python
# coding: utf-8

# ## Combined TPC-H Q6 and Q19 Plot

# In[96]:
import warnings
warnings.filterwarnings("ignore")
import logging

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None # disable annoying warnings
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


def figure9(tpch_path='r5d.8xlarge/tpch', output_folder='plots'):
    logging.info('Loading TPCH benchmark data')
    # ### Q19: Data preprocessing & extraction

    # ### 1. Hyper
    if not tpch_path.endswith('/'):
        tpch_path += '/'

    hyper_files = glob.glob(tpch_path + 'q19/data/hyper*.txt')

    rows = []
    for path in hyper_files:
        with open(path, 'r') as fp:
            content = fp.readlines()
            base_name = os.path.basename(path)
            row = dict(zip(content[-2].strip().split(','), content[-1].strip().split(',')))
            row['mode'] = '1x'if 'single' in base_name else '16x'
            row['load'] = float(row['load'])
            row['query'] = float(row['query'])
            row['result'] = float(row['result'])
            row['sf'] = 10
            run = int(base_name[base_name.find('run-') + 4:base_name.rfind('.txt')])
            row['run'] = run
            rows.append(row)
    df_hyper = pd.DataFrame(rows)
    df_hyper['total'] = df_hyper['load'] + df_hyper['query']
    df_hyper = df_hyper[df_hyper['run'] != 1]


    # ### Clean Q19/Q6 files

    def load_paths(paths, sf=1):
        rows = []
        for path in paths:
            if 'tuplex' in path:
                text = open(path, 'r').read()
                name = os.path.basename(path)
                fw = name[:name.find('-run')]
                subtext = text[text.find('framework,version,load,query'):]
                lines = subtext.split()
                D = dict(zip(lines[0].split(','), lines[1].split(',')))
                D['sf'] = sf
                D['framework'] = fw
                rows.append(D)
            else:
                text = open(path, 'r').readlines()
                D = dict(zip(text[-2].strip().split(','), text[-1].strip().split(',')))
                name = os.path.basename(path)
                fw = name[:name.find('-run')]
                D['framework'] = fw.replace('-preprocessed', '') # fix for hyper??
                D['sf'] = sf
                rows.append(D)
        df = pd.DataFrame(rows)
        df['load'] = df['load'].astype(float)
        df['query'] = df['query'].astype(float)
        df['compile'] = df['compile'].astype(float).fillna(0)
        # there's only compile for weld, add it to query time!
        df['query'] = df['query'] + df['compile']
        df['total'] = df['total'].astype(float)
        df['load'] = df['load'].fillna(0)
        df['total'] = df['query'] + df['load']

        df = df.sort_values(by='total').reset_index(drop=True)
        return df

    df = load_paths(glob.glob(tpch_path + 'q6/data/*.txt'), sf=10)
    df.to_csv('q6.csv', index=None)
    df = load_paths(glob.glob(tpch_path + 'q19/data/*.txt'), sf=10)
    df.to_csv('q19.csv', index=None)


    # ### 2. Tuplex

    df_q6 = pd.read_csv('q6.csv')
    df_q6['tpch'] = 'Q06'
    df_q19 = pd.read_csv('q19.csv')
    df_q19['tpch'] = 'Q19'

    # map q6 to q19 format
    df_q6['parallelism'] = df_q6['framework'].apply(lambda x: '1x' if '-st' in x or 'weld' == x else '16x')
    df_q6['mode'] = df_q6['framework'].apply(lambda x: 'split' if 'cached' in x else 'e2e')
    df_q6['framework'] = df_q6['framework'].apply(lambda x: x.replace('-cached', '').replace('-st', ''))

    df_q19['parallelism'] = df_q19['framework'].apply(lambda x: '1x' if '-st' in x or 'weld' == x or '-weldmode' in x else '16x')
    df_q19['mode'] = df_q19['framework'].apply(lambda x: 'split' if 'cached' in x else 'e2e')
    df_q19['framework'] = df_q19['framework'].apply(lambda x: x.replace('-cached', '').replace('-st', ''))
    df = pd.concat((df_q6, df_q19))

    # clean out weldmode
    df = df[df['framework'] != 'tuplex-weldmode']
    df['framework'] = df['framework'].apply(lambda x: x.replace('-weldmode-pushdown', ''))

    g_cols = ['framework', 'sf', 'mode', 'tpch', 'parallelism']
    df_mu = df.groupby(g_cols).mean()[['load', 'query', 'total']].reset_index()
    df_std = df.groupby(g_cols).std()[['load', 'query', 'total']].reset_index()

    logging.info('Plotting TPCH queries')

    # ## End-to-end-times
    # Full plot with everything

    # In[143]:


    # global plot settings
    rot = 30
    mks = 20
    w = .8
    w2 = w/2
    w4 = w2/2
    aoff = 1
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

    mpl.rcParams['hatch.linewidth'] = 0.1  # previous pdf hatch linewidth
    mpl.rcParams['hatch.linewidth'] = 1.0  # previous svg hatch linewidth

    # helper function for one subplot
    def plot_bars(ax, subdf_mu, subdf_std, key='total', aoff=aoff,
                  precision=precision,number_positions=None,cutoff=None,hatch=None):
    #     if number_positions is None:
    #         number_positions = ['above'] + ['center', 'center', 'center', 'center'] * 2

        m = subdf_mu
        s = subdf_std

        fsize=25

        q6_hatch = '//'


        # Weld
    #     aoff=0.8
        hatch = q6_hatch
        plt_bar(ax, -w4, m[(m['framework'] == 'weld') & (m['parallelism'] == '1x') & (m['tpch'] == 'Q06')][key],
                w2, weld_col, 'Weld', None,
                precision=precision, above_offset=aoff,
               yerr=s[(s['framework'] == 'weld') & (s['parallelism'] == '1x')  & (s['tpch'] == 'Q06')][key], hatch=hatch)
        hatch = None
        plt_bar(ax, +w4, m[(m['framework'] == 'weld') & (m['parallelism'] == '1x') & (m['tpch'] == 'Q19')][key],
                w2, weld_col, 'Weld', None,
                precision=precision, above_offset=aoff,
               yerr=s[(s['framework'] == 'weld') & (s['parallelism'] == '1x')  & (s['tpch'] == 'Q19')][key],
                hatch=hatch)

         # Tuplex
        hatch = q6_hatch
        plt_bar(ax, 1-w4, m[(m['framework'] == 'tuplex') & (m['parallelism'] == '1x') & (m['tpch'] == 'Q06')][key],
                w2, tplxst_col, 'Tuplex', None,
                precision=precision, above_offset=aoff,
               yerr=s[(s['framework'] == 'weld') & (s['parallelism'] == '1x')  & (s['tpch'] == 'Q06')][key], hatch=hatch)
        hatch = None
        plt_bar(ax, 1+w4, m[(m['framework'] == 'tuplex') & (m['parallelism'] == '1x') & (m['tpch'] == 'Q19')][key]
                , w2, tplxst_col, 'Tuplex', None,
                precision=precision, above_offset=aoff,
               yerr=s[(s['framework'] == 'weld') & (s['parallelism'] == '1x')  & (s['tpch'] == 'Q19')][key], hatch=hatch)


        ax.axvline(1.5, linestyle='--', lw=2,color=[.6,.6,.6])


        # pyspark
        fw = 'pyspark'
        p = '16x'
        off = 2
    #     aoff = 0.8 if key == 'query' else 0.8 * 5.
        th = 0.2 if key == 'query' else 1.0
        hatch = q6_hatch
        plt_bar(ax, off-w4, m[(m['framework'] == fw) & (m['parallelism'] == p) & (m['tpch'] == 'Q06')][key],
                w2, pyspark_col, fw, None,
                precision=precision, above_offset=aoff,
               yerr=s[(s['framework'] == fw) & (s['parallelism'] == p)  & (s['tpch'] == 'Q06')][key], hatch=hatch)
        hatch = None
        plt_bar(ax, off+w4, m[(m['framework'] == fw) & (m['parallelism'] == p) & (m['tpch'] == 'Q19')][key]
                , w2, pyspark_col, fw, 'above', cutoff=cutoff, th=th,
                precision=precision, above_offset=aoff, fsize=fsize,
               yerr=s[(s['framework'] == fw) & (s['parallelism'] == p)  & (s['tpch'] == 'Q19')][key], hatch=hatch)

         # pysparksql
        fw = 'pyspark-sql'
        p = '16x'
        off = 3
    #     aoff=0.8
        hatch = q6_hatch
        plt_bar(ax, off-w4, m[(m['framework'] == fw) & (m['parallelism'] == p) & (m['tpch'] == 'Q06')][key],
                w2, pysparksql_col, fw, None,
                precision=precision, above_offset=aoff,
               yerr=s[(s['framework'] == fw) & (s['parallelism'] == p)  & (s['tpch'] == 'Q06')][key], hatch=hatch)
        hatch = None
        plt_bar(ax, off+w4, m[(m['framework'] == fw) & (m['parallelism'] == p) & (m['tpch'] == 'Q19')][key]
                , w2, pysparksql_col, fw, None,
                precision=precision, above_offset=aoff,
               yerr=s[(s['framework'] == fw) & (s['parallelism'] == p)  & (s['tpch'] == 'Q19')][key], hatch=hatch)

        # Hyper
        fw = 'hyper'
        p = '16x'
        off = 4
    #     aoff=1
        num_label = 'above' if key == 'query' else None
        hatch = q6_hatch
        plt_bar(ax, off-w4, m[(m['framework'] == fw) & (m['parallelism'] == p) & (m['tpch'] == 'Q06')][key],
                w2, hyper_col, fw, num_label,
                precision=precision+1, above_offset=aoff,  fsize=fsize,
               yerr=s[(s['framework'] == fw) & (s['parallelism'] == p)  & (s['tpch'] == 'Q06')][key], hatch=hatch)
        hatch = None
        plt_bar(ax, off+w4, m[(m['framework'] == fw) & (m['parallelism'] == p) & (m['tpch'] == 'Q19')][key]
                , w2, hyper_col, fw, num_label,  fsize=fsize,
                precision=precision+1, above_offset=aoff,
               yerr=s[(s['framework'] == fw) & (s['parallelism'] == p)  & (s['tpch'] == 'Q19')][key], hatch=hatch)


        # Tuplex
        fw = 'tuplex'
        p = '16x'
        off = 5
    #     aoff = 0.8 if key == 'query' else 0.8 * 5.
        hatch = q6_hatch
        plt_bar(ax, off-w4, m[(m['framework'] == fw) & (m['parallelism'] == p) & (m['tpch'] == 'Q06')][key],
                w2, tplx_col, fw, 'above',
                precision=precision+1, above_offset=aoff,  fsize=fsize,
               yerr=s[(s['framework'] == fw) & (s['parallelism'] == p)  & (s['tpch'] == 'Q06')][key], hatch=hatch)
        hatch = None
        plt_bar(ax, off+w4, m[(m['framework'] == fw) & (m['parallelism'] == p) & (m['tpch'] == 'Q19')][key]
                , w2, tplx_col, fw, 'above',
                precision=precision+1, above_offset=aoff,  fsize=fsize,
               yerr=s[(s['framework'] == fw) & (s['parallelism'] == p)  & (s['tpch'] == 'Q19')][key], hatch=hatch)


        # ax.set_xticks([0, 1, 2, 3, 4, 5])
        # ax.set_xticklabels(['Tuplex', 'Weld', 'Hyper', 'PySparkSQL', 'PySpark', 'Pandas'], rotation=rot)
        ax.set_xticks([0, 1, 2, 3, 4, 5])
        ax.set_xticklabels(['Weld', 'Tuplex', 'PySpark', 'SparkSQL', 'Hyper', 'Tuplex'], ha='right', rotation=rot)
        ax.grid(axis='x')
        sns.despine()


    # In[144]:


    sf = 1.1
    fig, axs = plt.subplots(figsize=(sf * column_width, sf *column_width / rho*.75),
                            nrows=1, ncols=2, constrained_layout=True)
    axs = list(axs.flat)

    cutoff = 7
    lim_y = 8
    ao = 0.5

    ax = axs[0]
    subdf_mu = df_mu[(df_mu['mode'] == 'split') | (df_mu['framework'].isin(['hyper', 'weld']))]
    subdf_std = df_std[(df_std['mode'] == 'split') | (df_std['framework'].isin(['hyper', 'weld']))]
    plot_bars(ax, subdf_mu, subdf_std, 'query', aoff=ao, cutoff=cutoff)
    ax.set_xlabel('(a) query-time only', fontsize=27, labelpad=15)
    ax.set_ylabel('compute time in s', labelpad=6)
    ax.set_ylim(0, 4.4)
    # ax.text(0.375, 14.5, 'single-threaded', va='center', ha='center', fontsize=16)
    # ax.text(5.5, 14.5, 'multi-threaded (16x)', va='center', ha='right', fontsize=16)
    ax.text(-0.625, lim_y, 'single-threaded\nintegers', va='center', ha='left', fontsize=16)
    ax.text(5.625, lim_y, 'multi-threaded (16x)\nstrings', va='center', ha='right', fontsize=16)
    # ax.set_yscale('log')
    ax.set_ylim(0.01, lim_y)
    ax.set_yticks(np.arange(0, lim_y-1, 2))
    # ax.set_yticks(list(range(1, 10, 2)) + list(range(10, 100, 20)))

    # add legend
    # https://matplotlib.org/3.1.1/tutorials/intermediate/legend_guide.html
    handles = []
    handles.append(mpatches.Patch(edgecolor='w', facecolor=tplx_col, hatch='//', label='Q06'))
    handles.append(mpatches.Patch(edgecolor='none', facecolor=tplx_col, label='Q19'))
    ax.legend(handles=handles, fontsize=20, handletextpad=0.5, bbox_to_anchor=(1.03,0.8), loc='upper right')


    ax = axs[1]
    subdf_mu = df_mu[(df_mu['mode'] == 'e2e') | (df_mu['framework'].isin(['hyper', 'weld']))]
    subdf_std = df_std[(df_std['mode'] == 'e2e') | (df_std['framework'].isin(['hyper', 'weld']))]
    plot_bars(ax, subdf_mu, subdf_std, 'total', aoff=ao*5, cutoff=5 * cutoff)
    ax.set_xlabel('(b) end-to-end', fontsize=27, labelpad=15)
    ax.set_ylabel('end-to-end time in s', labelpad=10)
    # ax.set_yscale('log')
    ax.set_ylim(0.01, 5 * lim_y)
    ax.set_yticks(np.arange(0, (lim_y-1) * 5, 2 * 5))
    # ax.set_yticks(list(range(1, 10, 2)) + list(range(10, 100, 20)))
    #ax.set_ylim(0, 44)
    ax.text(-0.625, lim_y*5, 'single-threaded\nintegers', va='center', ha='left', fontsize=16)
    ax.text(5.625, lim_y*5, 'multi-threaded (16x)\nstrings', va='center', ha='right', fontsize=16)


    plt.savefig(os.path.join(output_folder, 'figure9_Q06Q19_combined.pdf'), transparent=True, dpi=120, bbox_inches = 'tight', pad_inches = 0)

    os.remove('q6.csv')
    os.remove('q19.csv')