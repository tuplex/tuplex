#!/usr/bin/env python
# coding: utf-8

# ## 15. Dirty Zillow experiment
# Plotting bar chart as overview for dirty zillow experiment + providing individual numbers, which is figure 6 in the paper

# In[1]:


import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import re
import json
from brokenaxes import brokenaxes

import sys, traceback
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

import math

adjust_settings()


# In[2]:


# global plot settings
rot = 45
mks = 20
w = .8
w2 = w/2
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

py_col = pyspark_col
cython_col = [161/255., 67/255., 133/255.]
nuitka_col = [123/255, 88/255, 219/255.]

base_gray = np.array([0.8, .8, .8])
alpha = 0.1
py_col_g = alpha * np.array(py_col) + (1. - alpha) * base_gray
tplx_col_g = alpha * np.array(tplx_col) + (1. - alpha) * base_gray
dask_col_g = alpha * dask_col + (1. - alpha) * base_gray
pyspark_col_g = alpha * np.array(pyspark_col) + (1. - alpha) * base_gray
pysparksql_col_g = alpha * np.array(pysparksql_col) + (1. - alpha) * base_gray



def figure6(zillow_path='r5d.8xlarge/zillow', output_folder='plots'):
        
    # ### Data preparation
    # Goal is to create stacked plots with breakdown, therefore let's load and clean the data
    if not zillow_path.endswith('/'):
        zillow_path += '/'

    data_root = zillow_path + '/Zdirty/'
    
    def load_dirty_zillow_to_df(data_root):
        files = os.listdir(data_root)
        rows = []
    
        for file in files:
            path = os.path.join(data_root, file)
            name = file[file.find('-run')+5:]
    
            if file.endswith('.txt'):
                
                dataset = 'synth' if 'synth' in name else 'original'
                single_threaded = '-st-' in name
                run = int(name[name.rfind('-')+1:name.rfind('.')])
                in_order = '-in-order' in name
                interpreter_only = '-interpreter' in name
                mode = name[:name.find('-')]
                
                with open(path, 'r') as fp:
                    lines = fp.readlines()
    
                    try:
                        row = {}
                        
                        row['dataset'] = dataset
                        row['single_threaded'] = single_threaded
                        row['run'] = run
                        row['in_order'] = in_order
                        row['mode'] = mode
                        row['interpreter_only'] = interpreter_only
                        
                        try:
                            d = json.loads(list(filter(lambda x: 'startupTime' in x, lines))[0])
                            row['startup_time'] = d['startupTime']
                            row['job_time'] = d['jobTime']
                        except:
                            pass
                        
                        #breakdown time extract for tuplex
                        c = '\n'.join(lines)
    
                        pattern = re.compile('load&transform tasks in (\d+.\d+)s')
                        lt_times = np.array(list(map(float, pattern.findall(c))))
    
                        pattern = re.compile('compiled to x86 in (\d+.\d+)s')
                        cp_times = np.array(list(map(float, pattern.findall(c))))
    
                        try:
                            m = re.search('writing output took (\d+.\d+)s', c)
                            w_time = float(m[1])
                            row['write'] = w_time
                        except:
                            pass
                        lt_time = lt_times.sum()
                        cp_time = cp_times.sum()
                        row['compute'] = lt_time
                        row['compile'] = cp_time
                     
                        row['framework'] = 'tuplex'
    
                        
                        # needle: [Transform Stage] Stage 0 completed 1 load&transform tasks in 0.0540315s
                        m = re.search('Stage \d+ completed \d+ load&transform tasks in (\d+.\d+)s', c)
                        row['normal_case_time'] = float(m[1]) if m else 0.0
                        
                        # slowPath resolved 4712/4712 exceptions in 0.00902577s
                        # needle: Stage 0 completed 1 resolve task in 0.00905142s
                        m = re.search('Stage \d+ completed \d+ resolve .* in (\d+.\d+)s', c)
                        row['general_case_time'] = float(m[1]) if m else 0.0
    
                        
                        # load metrics
                        try:
                            d = json.loads(list(filter(lambda x: 'logical_optimization_time_s' in x, lines))[0])
                            row['fast_path_per_row_time_ns'] = d['stages'][0]['fast_path_per_row_time_ns']
                            row['slow_path_per_row_time_ns'] = d['stages'][0]['slow_path_per_row_time_ns']
                            row['sampling_time_s'] = d['sampling_time_s']
                        except:
                            pass
                        
                        
                        rows.append(row)
                    except Exception as e:
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        print("*** print_tb:")
                        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
                        print('{} has issues {} ...'.format(path, e))
        return pd.DataFrame(rows)
    
    logging.info('Loading Zillow exception experiment data...')
    
    df = load_dirty_zillow_to_df(data_root)

    # drop first run, because that's the IO warmup run
    df = df[df['run'] != 1]

    # for breakdown, compute the following times
    df['other_time'] = df['job_time'] - (df['normal_case_time'] + df['general_case_time']  + df['compile'] + df['write'] + df['sampling_time_s'])
    
    # sanity check, other time should be > 0
    
    assert df['other_time'].min() > 0

    g_df = df[set(df.columns) - {'run'}].groupby(['dataset', 'mode', 'single_threaded', 'in_order', 'interpreter_only'])
    df_mu = g_df.mean().reset_index()
    df_std = g_df.std().reset_index()
    df_cnt = g_df.count().reset_index()

    # ## Plotting bar chart with the runtimes for slow/fast path and the different settings
    
    df_mu_st = df_mu[df_mu['single_threaded']]
    
    df_mu_mt = df_mu[(~df_mu['single_threaded']) & (~df_mu['in_order'])]
    df_std_mt = df_std[(~df_std['single_threaded']) & (~df_std['in_order'])]
    df_mu_mt.sort_values(by='fast_path_per_row_time_ns')

    logging.info('PLotting exception plot...')
    # ### Paper plot
    # 
    # ```
    # ## Notes: 
    # # divide interpreter general case  by 16, add as lower bound to 
    # # ~~> estimated 
    # # resolve (interpreter) 16x* <-- explain in text + caption.
    # # change 'write' to 'write output'
    # # => change to horizontal bar chart for space reasons
    # 
    # 
    # # labels: -> put over bars
    # # ignore all exceptions
    # # altered UDFs with manual resolution
    # # Tuplex resolvers (compiled)
    # # Tuplex resolvers (interpreted*)
    # ```
    # 
    
    def plot_breakdown(df):
        
        def create_label(t):
            label = ''
            idx = 0
            if t[1] == 'plain':
                label = 'ignore all'
                idx = 0
            elif t[1] == 'resolve':
                if t[2]:
                    label = 'resolve (interpreter)'
                    idx = 2.5
                else:
                    label = 'resolve (compiled)'
                    idx = 2
            elif t[1] == 'custom':
                label = 'handwritten'
                idx = 1
            if t[0]:
                label += ' 1x'
            else:
                label += ' 16x'
            return label, idx
        df['label'] = df[['single_threaded', 'mode', 'interpreter_only']].apply(lambda t: create_label(t)[0], axis=1)
        df['sort_idx'] = df[['single_threaded', 'mode', 'interpreter_only']].apply(lambda t: create_label(t)[1], axis=1)
        
        df_mu = df.groupby(['dataset', 'single_threaded', 'in_order',
                            'mode', 'interpreter_only', 'label', 'sort_idx']).mean().reset_index()
        df_mu.sort_values(inplace=True, by='sort_idx')
        
        df_int = df_mu[df_mu['label'].str.contains('interpreter')]
        # df_int.loc[:, 'normal_case_time'] = df_int['normal_case_time'] / 16 # only when using single-threaded numbers!
        df_int.loc[:, 'general_case_time'] = df_int['general_case_time'] / 16
    
        
        # because we do not use slow path at all, exclude sampling
        #df_int.loc[:, 'sampling_time_s'] = 2 * df_int.loc[:, 'sampling_time_s']
        
        df_mu = pd.concat((df_mu[df_mu['label'].str.contains('16x') & ~df_mu['interpreter_only']], df_int))[::-1].reset_index()
        
        sf = 1.2
    #     fig, ax = plt.subplots(figsize=(sf * column_width, sf *column_width / rho*.75),
    #                             nrows=1, ncols=1, constrained_layout=True)
        fig = plt.figure(figsize=(sf * column_width, sf *column_width / rho*.6))
        
        # tweak left, bottom, right, top for tight layout!
        ax = brokenaxes(xlims=((0, 4.5), (50.5, 53)), hspace=0.05,
                       left = 0.008, bottom = 0.25)
    
        # plot breakdown
        #     df['normal_case_time'] + df['general_case_time'] \
        #                                      + df['compile'] + df['write'] + df['sampling_time_s']
        h = .45
        bottom = 0
        xq = list(range(len(df_mu)))
        bp = ax.barh(xq, df_mu['sampling_time_s'], height=h, color='k', label='sampling')
        write_col = [.6,.6,.6]
        other_col = [.3,.3,.3]
        bottom = df_mu['sampling_time_s']
        ax.barh(xq, df_mu['compile'], height=h, color=tplx_col, label='compilation', left=bottom)
        bottom += df_mu['compile']
        ax.barh(xq, df_mu['write'], height=h, color=write_col, label='write', left=bottom)
        bottom += df_mu['write']
        ax.barh(xq, df_mu['other_time'], height=h, color=other_col, label='other', left=bottom)
        bottom += df_mu['other_time']
        ax.barh(xq, df_mu['normal_case_time'], height=h, color=py_col, label='normal case', left=bottom)
        bottom += df_mu['normal_case_time']
        ax.barh(xq, df_mu['general_case_time'], height=h, color=py_col_g, label='exception resolution', left=bottom)
        bottom += df_mu['general_case_time']
        
        print('numbers top-bottom')
        print(bottom[::-1])
       
        ax.set_ylim(-.5, 3.5)
        ax.tick_params(left=False, labelleft=False)
        ax.set_xlabel('time in s', labelpad=0)
        ax.grid(axis='y')
        ax.axs[0].set_yticks([])
        
        # manually fix xticks
        ax.axs[0].set_xticks(np.arange(0, 4.5))
    #     ax.axs[1].set_xticks(np.arange(146, 149))
        
        # print labels over bars
        labels = ['ignore all exceptions', 'altered UDFs with manual resolution',
                  'Tuplex resolvers (compiled)',
                  'Tuplex resolvers (interpreted*)'][::-1]
        lax = ax.axs[0]
        for i, y in enumerate(xq):
            lax.text(0.25, y + h/2 + 0.08, labels[i],
                     fontsize=20, ha='left', va='bottom', bbox=dict(facecolor='white', alpha=0.9))
        #ax.text(0.05, 0.05, 'test', ha='center', va='bottom')
        # ax.grid(False)
        
        rax = ax.axs[1]
        # https://matplotlib.org/3.1.1/tutorials/intermediate/legend_guide.html
        hlabels=[('sampling', 'k'), ('compilation', tplx_col),
                 ('write output', write_col), ('other', other_col), ('normal case', py_col), 
                ('exception resolution', py_col_g)]
        handles = []
        for h in hlabels:
            handles.append(Line2D([0], [0], marker='o', color='w', label=h[0],
                                markerfacecolor=h[1], markersize=20))
        rax.legend(handles=handles, fontsize=16, handletextpad=0.05)
        #rax.legend(fontsize=22) # default legend
    
    plot_breakdown(df[(df['dataset'] == 'original') & ~df['in_order'] & ~df['single_threaded']].copy())
    plt.savefig(os.path.join(output_folder, 'figure6_dirty_zillow_breakdown.pdf'), transparent=True, dpi=120, bbox_inches = 'tight', pad_inches = 0)
    
    # ## What is the overhead of compiled resolve vs. manual resolution?

    subdf = df_mu[(df_mu['dataset'] == 'original') & ~df_mu['single_threaded'] & ~df_mu['in_order']]

    # overhead in %
    resolve_time = subdf[subdf['mode'] == 'resolve']['job_time'].iloc[0]
    overhead_resolve_vs_custom = (resolve_time / subdf[subdf['mode'] == 'custom']['job_time'].iloc[0] - 1) * 100.0
    
    logging.info('overhead when using Tuplex resolve(compiled) vs. custom: {:.2f}%'.format(overhead_resolve_vs_custom))
    

