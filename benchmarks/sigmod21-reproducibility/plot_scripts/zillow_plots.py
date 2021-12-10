#!/usr/bin/env python
# coding: utf-8

# ## Zillow experiment plots
# this notebook produces all plots necessary for Figure 3 in the final number + numbers for the accompanying text.
# Figure 3, 7 and table3
# In[24]:


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

import warnings
warnings.filterwarnings("ignore")

import scipy.constants
rho = scipy.constants.golden

# make nice looking plot
from .paper import *

import logging

adjust_settings()


# In[75]:


def load_zillow_to_df(data_root):
    files = os.listdir(data_root)
    rows = []

    for file in files:
        path = os.path.join(data_root, file)
        name = file[:file.find('-run')]
        
        # skip compile runs, they should be loaded separately...
        if 'compile-run' in file:
            continue
        
        try:
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
                    
                    # CC baseline decode
                    if 'cc-' in path or 'cpp_' in path:
                        d = json.loads(lines[-1])
                        row = {}
                        row['write'] = d['output'] / 10**9
                        row['job_time'] = d['total'] / 10**9
                        if 'load' in d.keys():
                            row['load'] = d['load'] / 10**9
                            row['compute'] = d['compute'] / 10 ** 9
                        else:
                            row['compute'] = d['transform'] / 10 ** 9

                        if 'no-preload' in file or 'no_preload' in file:
                            row['type'] = 'no-preload'
                        else:
                            row['type'] = 'preload'
                        row['framework'] = 'c++'

                        # hand-measured compile time
                        # TODO: update!
                        row['compile'] = 7.529
                        row['mode'] = 'c++'

                        # override if compute is available
                        if 'compute_time' in d.keys():
                            row['compute'] = d['compute_time']
                    elif 'scala' in path: # Scala
                        
                        row['framework'] = 'scala'
                        row['type'] = 'single-threaded'
                        row['mode'] = 'scala'
                        if 'sparksql' in path:
                            row['framework'] = 'spark'
                            row['type'] = 'scala-sql'
                            row['mode'] = 'scala'
                        
                        # compile times & Co (hand measured)
                        row['job_time'] = float(re.sub('[^0-9.]*', '', lines[1]))
                        row['startup_time'] = float(re.sub('[^0-9.]*', '', lines[0]))
                        
                    # tuplex decode
                    elif 'tuplex' in path:
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
                            
                            if '-st' in path or '_st' in path:
                                row['type'] = 'single-threaded'
                            elif '-io' in path:
                                row['type'] = 'cached'
                            else:
                                row['type'] = 'multi-threaded'
                                
                            if 'preload' in path:
                                row['type'] = row['type'] + '-preload'
                            if 'nonvo' in path:
                                row['type'] = row['type'] + '-no-nvo'

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
                        row['framework'] = d['framework']
                        if 'Spark' in row['framework']:
                            row['framework'] = 'spark'
                        if 'dask' in row['framework']:
                            row['framework'] = 'dask'
                        if 'pandas' in row['framework']:
                            row['framework'] = 'pandas'

                        # override framework for nuitka + cython
                        if 'nuitka' in path:
                            row['framework'] = 'nuitka'
                        if 'cython' in path:
                            row['framework'] = 'cython'

                        # clean keys
                        if 'load_time' in d.keys():
                            row['load'] = d['load_time']
                        if 'run_time' in d.keys():
                            row['compute'] = d['run_time']
                        if 'write_time' in d.keys():
                            row['write'] = d['write_time']
                        if 'nuitka' or 'cython' in path:

                            # regex extract
                            m = re.search("compilation via \w+ took: (\d\.\d+)s", '\n'.join(lines))
                            if m: 
                                row['compile'] = float(m[1])
                        row['type']  = d['type']

                        # adjust spark types
                        if 'RDD dict' in row['type']:
                            row['type'] = 'dict'
                        if 'RDD tuple' in row['type']:
                            row['type'] = 'tuple'
                        if 'DataFrame' in row['type']:
                            row['type'] = 'sql'

                        row['job_time'] = d['job_time']

                        if 'startup_time' in d.keys():
                            row['startup_time'] = d['startup_time']

                        if 'pypy' in path:
                            row['mode'] = 'pypy3'
                        else:
                            row['mode'] = 'python3'

                    if len(row) > 0:
                        row['run'] = run_no
                        rows.append(row)
        except Exception as e:
            # print('file: {}'.format(file))
            # print(type(e))
            # print(e)
            pass
    return pd.DataFrame(rows)


# In[76]:

def load_data(zillow_folder='r5d.8xlarge/zillow'):
    df_Z1 = load_zillow_to_df(os.path.join(zillow_folder, 'Z1/'))
    df_Z1 = df_Z1[sorted(df_Z1.columns)]

    # exclude first, warmup run
    df_Z1 = df_Z1[df_Z1['run'] != 1]

    df_Z2 = load_zillow_to_df(os.path.join(zillow_folder, 'Z2/'))
    df_Z2 = df_Z2[sorted(df_Z2.columns)]

    # exclude first, warmup run
    df_Z2 = df_Z2[df_Z2['run'] != 1]

    df = df_Z1.copy()
    subdf = df[df['type'].isin(['single-threaded-preload', 'preload'])]
    subdf = subdf.groupby(['framework', 'mode', 'type']).mean().sort_values(by='compute').reset_index()
    recs = subdf.to_dict('records')
    tuplex_time = recs[1]['job_time'] #note the switch due to how times are measured
    cc_time = recs[0]['compute']

    logging.info('Zillow Z1:: Tuplex takes {:.2f}s vs. C++ {:.2f}s, i.e. comes within {:.2f}%'.format(tuplex_time, cc_time,
                                                                             (tuplex_time - cc_time) / tuplex_time * 100))

    df = df_Z2.copy()
    subdf = df[df['type'].isin(['single-threaded-preload', 'preload'])]
    subdf = subdf.groupby(['framework', 'mode', 'type']).mean().sort_values(by='compute').reset_index()
    recs = subdf.to_dict('records')
    tuplex_time = recs[1]['job_time'] #note the switch due to how times are measured
    cc_time = recs[0]['compute']

    logging.info('Zillow Z2:: Tuplex takes {:.2f}s vs. C++ {:.2f}s, i.e. comes within {:.2f}%'.format(tuplex_time, cc_time,
                                                                             (tuplex_time - cc_time) / tuplex_time * 100))
    tuplex_time, cc_time

    return df_Z1, df_Z2

def table3(df): # use here df_Z1!
    # ## Table 3: Cython vs. Nuitka vs. Tuplex
    subdf = df[(df['mode'].isin(['python3', 'c++'])) & (df['framework'].isin(['cython', 'tuplex', 'nuitka', 'c++']))]
    subdf = subdf[subdf['type'].isin(['single-threaded', 'tuple', 'no-preload'])]

    df_mean = subdf.groupby(['framework', 'mode', 'type']).mean().reset_index()

    df_mean = df_mean.sort_values(by='compute').reset_index(drop=True)
    table3 = df_mean[['framework', 'mode', 'type', 'compile', 'compute']]
    logging.info('Table3:\n{}'.format(table3))

    # => For the paper, use compile and compute columns

    tplx_dict = df_mean[df_mean['framework'] == 'tuplex'].to_dict('records')[0]
    tplx_compile, tplx_compute = tplx_dict['compile'], tplx_dict['compute']

    min_compile_sp = df_mean[df_mean['framework'] != 'tuplex']['compile'].min() / tplx_compile
    max_compile_sp = df_mean[df_mean['framework'] != 'tuplex']['compile'].max() / tplx_compile

    min_compute_sp = df_mean[df_mean['framework'] != 'tuplex']['compute'].min() / tplx_compute
    max_compute_sp = df_mean[df_mean['framework'] != 'tuplex']['compute'].max() / tplx_compute

    logging.info('Tuplex compile speedup: {:.2f}x - {:.2f}x'.format(min_compile_sp, max_compile_sp))
    logging.info('Tuplex compute speedup: {:.2f}x - {:.2f}x'.format(min_compute_sp, max_compute_sp))


    # Compare this to best CPython result
    best_cpython_result = df[df['framework'] == 'python3'].groupby(['framework', 'mode', 'type'])  .mean().reset_index().sort_values('compute').head(1)[['framework', 'mode', 'type', 'compute']]
    logging.info('Best CPYthon result: {}s'.format(best_cpython_result['compute'].values[0]))


def figure3(df_Z1, df_Z2, output_folder):

    # ## Zillow Z1 plots
    logging.info('Plotting Z1')
    st_fws = ['tuplex', 'cython', 'nuitka', 'python3', 'c++', 'pandas', 'scala']
    mt_fws = ['tuplex', 'spark', 'dask']

    # which df to use?
    df = df_Z1.copy()
    # drop tuplex preload
    df = df[df['type'] != 'single-threaded-preload']
    query_name = 'Z1'

    df_st = df[(df['mode'].isin(['python3', 'c++', 'scala'])) & (df['framework'].isin(st_fws))]
    df_st = df_st[~df_st['type'].isin(['multi-threaded', 'cached', 'preload'])]
    df_st_mu = df_st.groupby(['framework', 'type']).mean().reset_index()
    df_st_std = df_st.groupby(['framework', 'type']).std().reset_index()

    df_st_tuple = df_st_mu[df_st_mu['type'] == 'tuple'].sort_values(by='job_time').reset_index(drop=True)
    df_st_dict = df_st_mu[df_st_mu['type'] == 'dict'].sort_values(by='job_time').reset_index(drop=True)
    df_st_rest = df_st_mu[~df_st_mu['type'].isin(['tuple', 'dict'])].sort_values(by='job_time').reset_index(drop=True)

    df_st_tuple_std = df_st_std[df_st_std['type'] == 'tuple']
    df_st_dict_std = df_st_std[df_st_std['type'] == 'dict']
    df_st_rest_std = df_st_std[~df_st_std['type'].isin(['tuple', 'dict'])]

    df_st = df[(df['mode'].isin(['python3', 'c++', 'scala'])) & (df['framework'].isin(st_fws))]
    df_st = df_st[~df_st['type'].isin(['multi-threaded', 'cached', 'preload'])]
    df_st_mu = df_st.groupby(['framework', 'type']).mean().reset_index()
    df_st_std = df_st.groupby(['framework', 'type']).std().reset_index()

    df_st_tuple = df_st_mu[df_st_mu['type'] == 'tuple'].sort_values(by='job_time').reset_index(drop=True)
    df_st_dict = df_st_mu[df_st_mu['type'] == 'dict'].sort_values(by='job_time').reset_index(drop=True)
    df_st_rest = df_st_mu[~df_st_mu['type'].isin(['tuple', 'dict'])].sort_values(by='job_time').reset_index(drop=True)

    df_st_tuple_std = df_st_std[df_st_std['type'] == 'tuple']
    df_st_dict_std = df_st_std[df_st_std['type'] == 'dict']
    df_st_rest_std = df_st_std[~df_st_std['type'].isin(['tuple', 'dict'])]

    df_mt = df[(df['mode'].isin(['python3', 'c++', 'scala'])) & (df['framework'].isin(mt_fws))]
    df_mt = df_mt[~df_mt['type'].isin(['single-threaded', 'cached', 'preload'])]
    df_mt_mu = df_mt.groupby(['framework', 'type']).mean().reset_index()
    df_mt_std = df_mt.groupby(['framework', 'type']).std().reset_index()

    # links: https://stackoverflow.com/questions/14852821/aligning-rotated-xticklabels-with-their-respective-xticks
    sf = 1.1
    fig, axs = plt.subplots(figsize=(sf * column_width,
                                     sf *column_width / rho*0.65), nrows=1, ncols=2, constrained_layout=True)
    rot = 25
    mks = 14
    w = .8
    w2 = w/2
    precision = 1
    cc_col = [0, 0, 0]
    tplx_col = sns.color_palette()[0]
    dask_col = np.array(sns.color_palette()[3])
    pyspark_col = 1.2 * np.array(sns.color_palette()[2])
    pysparksql_col = 0.6 * np.array(pyspark_col)
    fsize=30

    py_col = pyspark_col
    cython_col = [161/255., 67/255., 133/255.]
    nuitka_col = [123/255, 88/255, 219/255.]
    scala_col = [.6, .6, .6]
    axs = list(axs.flat)


    ao = 3.5
    lim_high = 62

    ##### SINGLE-THREADED ######
    ax = axs[0]

    python3_dict_err = np.array([df_st_dict_std[df_st_dict_std['framework'] == 'python3']['job_time']])
    python3_tuple_err = np.array([df_st_tuple_std[df_st_tuple_std['framework'] == 'python3']['job_time']])
    pandas_err = np.array([df_st_rest_std[df_st_rest_std['framework'] == 'pandas']['job_time']])
    tplx_err = np.array([df_st_rest_std[df_st_rest_std['framework'] == 'tuplex']['job_time']])
    cc_err = np.array([df_st_rest_std[df_st_rest_std['framework'] == 'c++']['job_time']])
    scala_err = np.array([df_st_rest_std[df_st_rest_std['framework'] == 'scala']['job_time']])

    plt_bar(ax, 1, df_st_dict[df_st_dict['framework'] == 'python3']['job_time'], w, py_col,
            'Python', 'center', precision=precision, yerr=python3_dict_err,fsize=fsize)
    plt_bar(ax, 2, df_st_tuple[df_st_tuple['framework'] == 'python3']['job_time'],
            w, py_col, 'Python', 'center', above_offset=ao*10, precision=precision, yerr=python3_tuple_err,fsize=fsize)
    plt_bar(ax, 0, df_st_rest[df_st_rest['framework'] == 'pandas']['job_time'],
            w, dask_col, 'Pandas', 'center', above_offset=ao*10, precision=precision, yerr=pandas_err,fsize=fsize)
    plt_bar(ax, 3, df_st_rest[df_st_rest['framework'] == 'tuplex']['job_time'],
            w, tplx_col, 'Tuplex', 'above', precision=precision, above_offset=ao*10, yerr=tplx_err,fsize=fsize)
    plt_bar(ax, 4, df_st_rest[df_st_rest['framework'] == 'scala']['job_time'],
            w, scala_col, 'Scala', 'above', precision=precision, above_offset=ao*10, yerr=scala_err,fsize=fsize)
    plt_bar(ax, 5, df_st_rest[df_st_rest['framework'] == 'c++']['job_time'],
            w, cc_col, 'C++', 'above', precision=precision, above_offset=ao*10, yerr=cc_err,fsize=fsize)
    ax.axvline(3.5, linestyle='--', lw=2,color=[.6,.6,.6])


    legend_elements = [ Line2D([0], [0], marker='o', color='w', label='Python   ',
                              markerfacecolor=py_col, markersize=mks),
                        Line2D([0], [0], marker='o', color='w', label='Pandas',
                              markerfacecolor=dask_col, markersize=mks),
                       Line2D([0], [0], pickradius=2, marker='o', color='w', label='Tuplex',
                              markerfacecolor=tplx_col, markersize=mks),
                       Line2D([0], [0], marker='o', color='w', label='Scala (man-opt.)',
                              markerfacecolor=scala_col, markersize=mks),
                        Line2D([0], [0], pickradius=2, marker='o', color='w', label='C++ (man-opt.)',
                              markerfacecolor=cc_col, markersize=mks)]
    # legend, also valid for Z2
    L = ax.legend(handles=legend_elements, loc='upper right', fontsize=15, bbox_to_anchor=(1, 1),
                  borderaxespad=-.8, handletextpad=0., ncol=2, columnspacing=0)
    cols = [py_col, dask_col, tplx_col, scala_col, cc_col]
    for i, text in enumerate(L.get_texts()):
        text.set_color(cols[i])

    ax.set_xticks([0, 1, 2, 3, 4, 5])
    ax.set_xticklabels(['Pandas', 'dict', 'tuple', 'Tuplex', 'Scala', 'C++'], rotation=rot)
    ax.grid(axis='x')
    sns.despine()
    ax.set_ylim(0, lim_high * 10)
    ax.set_xlim(-.5, 5.5)
    ax.set_xlabel('({}a) single-threaded'.format(query_name), fontsize=27, labelpad=15)
    ax.set_ylabel('runtime in s', labelpad=10)

    ##### MULTI-THREADED ######
    ax = axs[1]

    spark_sql_time = df_mt_mu[(df_mt_mu['framework'] == 'spark') & (df_mt_mu['type'] == 'sql')]['job_time']
    spark_tuple_time = df_mt_mu[(df_mt_mu['framework'] == 'spark') & (df_mt_mu['type'] == 'tuple')]['job_time']
    spark_dict_time = df_mt_mu[(df_mt_mu['framework'] == 'spark') & (df_mt_mu['type'] == 'dict')]['job_time']
    spark_scala_time = df_mt_mu[(df_mt_mu['framework'] == 'spark') & (df_mt_mu['type'] == 'scala-sql')]['job_time']

    spark_sql_time_err = np.array([df_mt_std[(df_mt_std['framework'] == 'spark') & (df_mt_std['type'] == 'sql')]['job_time']])
    spark_tuple_time_err = np.array([df_mt_std[(df_mt_std['framework'] == 'spark') & (df_mt_std['type'] == 'tuple')]['job_time']])
    spark_dict_time_err = np.array([df_mt_std[(df_mt_std['framework'] == 'spark') & (df_mt_std['type'] == 'dict')]['job_time']])
    spark_scala_time_err = np.array([df_mt_std[(df_mt_std['framework'] == 'spark') & (df_mt_std['type'] == 'scala-sql')]['job_time']])

    plt_bar(ax, 2, spark_dict_time, w, pyspark_col, 'PySpark (dict)', 'center',
            precision=precision,yerr=spark_dict_time_err, fsize=fsize)
    plt_bar(ax, 3, spark_tuple_time, w, pyspark_col, 'PySpark (tuple)', 'center', precision=precision,
            yerr=spark_tuple_time_err, fsize=fsize)
    plt_bar(ax, 1, spark_sql_time, w, pysparksql_col, 'PySparkSQL', 'center', precision=precision,
            yerr=spark_sql_time_err, fsize=fsize)


    dask_time_err = np.array([df_mt_std[df_mt_std['framework'] == 'dask']['job_time']])
    tplx_time_err = np.array([df_mt_std[df_mt_std['framework'] == 'tuplex']['job_time']])

    plt_bar(ax, 0, df_mt_mu[df_mt_mu['framework'] == 'dask']['job_time'], w, dask_col,
            'Dask', 'center', above_offset=ao, precision=precision, yerr=dask_time_err, fsize=fsize)
    plt_bar(ax, 4, df_mt_mu[df_mt_mu['framework'] == 'tuplex']['job_time'], w, tplx_col,
            'Tuplex', 'above', above_offset=ao, precision=precision, yerr=tplx_time_err, fsize=fsize)

    ax.axvline(4.5, linestyle='--', lw=2,color=[.6,.6,.6])

    plt_bar(ax, 5, spark_scala_time, w, scala_col,
            'Scala', 'center', above_offset=ao, precision=precision, yerr=spark_scala_time_err, fsize=fsize)

    legend_elements = [
                       Line2D([0], [0], marker='o', color='w', label='PySpark',
                              markerfacecolor=pyspark_col, markersize=mks),
                        Line2D([0], [0], marker='o', color='w', label='PySpark',
                              markerfacecolor=pysparksql_col, markersize=mks),
                         Line2D([0], [0], marker='o', color='w', label='Dask',
                              markerfacecolor=dask_col, markersize=mks),
                       Line2D([0], [0],  marker='o', color='w', label='Tuplex',
                              markerfacecolor=tplx_col, markersize=mks),
                       Line2D([0], [0], marker='o', color='w', label='SparkSQL(Scala)',
                              markerfacecolor=scala_col, markersize=mks),]

    # legend (also valid for Z2)
    L = ax.legend(handles=legend_elements, loc='upper right', fontsize=15,
                 bbox_to_anchor=(1, 1), borderaxespad=-.4, handletextpad=0., ncol=3, columnspacing=0)
    cols = [ pyspark_col, pysparksql_col, dask_col, tplx_col, scala_col]
    for i, text in enumerate(L.get_texts()):
        text.set_color(cols[i])


    ax.set_xticks([0, 1, 2, 3, 4, 5])
    ax.set_xticklabels(['Dask', 'SQL', 'dict', 'tuple', 'Tuplex', 'Scala'], rotation=rot)
    ax.grid(axis='x')
    sns.despine()
    # ax.set_ylim(0, 245)
    ax.set_ylim(0, lim_high)
    ax.set_xlim(-.5, 5.5)
    ax.set_xlabel('({}b) 16x parallelism'.format(query_name), fontsize=27, labelpad=15)
    # ax.set_ylabel('runtime in s', labelpad=-5)

    plt.savefig(os.path.join(output_folder, 'figure3_zillow_10G_{}.pdf'.format(query_name)), transparent=True, bbox_inches = 'tight', pad_inches = 0)

    # ------------------------------------------------------------------------------------------------------------------------------
    # Z2 part
    #------------------------------------------------------------------------------------------------------------------------------
    # ### Z2:
    logging.info('Plotting Z2')
    # In[94]:

    df = df_Z2.copy()
    query_name = 'Z2'
    # drop tuplex preload
    df = df[df['type'] != 'single-threaded-preload']

    # In[95]:

    df_st = df[(df['mode'].isin(['python3', 'c++', 'scala'])) & (df['framework'].isin(st_fws))]
    df_st = df_st[~df_st['type'].isin(['multi-threaded', 'cached', 'preload'])]
    df_st_mu = df_st.groupby(['framework', 'type']).mean().reset_index()
    df_st_std = df_st.groupby(['framework', 'type']).std().reset_index()

    df_st_tuple = df_st_mu[df_st_mu['type'] == 'tuple'].sort_values(by='job_time').reset_index(drop=True)
    df_st_dict = df_st_mu[df_st_mu['type'] == 'dict'].sort_values(by='job_time').reset_index(drop=True)
    df_st_rest = df_st_mu[~df_st_mu['type'].isin(['tuple', 'dict'])].sort_values(by='job_time').reset_index(drop=True)

    df_st_tuple_std = df_st_std[df_st_std['type'] == 'tuple']
    df_st_dict_std = df_st_std[df_st_std['type'] == 'dict']
    df_st_rest_std = df_st_std[~df_st_std['type'].isin(['tuple', 'dict'])]

    df_st = df[(df['mode'].isin(['python3', 'c++', 'scala'])) & (df['framework'].isin(st_fws))]
    df_st = df_st[~df_st['type'].isin(['multi-threaded', 'cached', 'preload'])]
    df_st_mu = df_st.groupby(['framework', 'type']).mean().reset_index()
    df_st_std = df_st.groupby(['framework', 'type']).std().reset_index()

    df_st_tuple = df_st_mu[df_st_mu['type'] == 'tuple'].sort_values(by='job_time').reset_index(drop=True)
    df_st_dict = df_st_mu[df_st_mu['type'] == 'dict'].sort_values(by='job_time').reset_index(drop=True)
    df_st_rest = df_st_mu[~df_st_mu['type'].isin(['tuple', 'dict'])].sort_values(by='job_time').reset_index(drop=True)

    df_st_tuple_std = df_st_std[df_st_std['type'] == 'tuple']
    df_st_dict_std = df_st_std[df_st_std['type'] == 'dict']
    df_st_rest_std = df_st_std[~df_st_std['type'].isin(['tuple', 'dict'])]

    df_mt = df[(df['mode'].isin(['python3', 'c++', 'scala'])) & (df['framework'].isin(mt_fws))]
    df_mt = df_mt[~df_mt['type'].isin(['single-threaded', 'cached', 'preload'])]
    df_mt_mu = df_mt.groupby(['framework', 'type']).mean().reset_index()
    df_mt_std = df_mt.groupby(['framework', 'type']).std().reset_index()

    # links: https://stackoverflow.com/questions/14852821/aligning-rotated-xticklabels-with-their-respective-xticks
    sf = 1.1
    fig, axs = plt.subplots(figsize=(sf * column_width,
                                     sf * column_width / rho * 0.54), nrows=1, ncols=2, constrained_layout=True)
    rot = 25
    mks = 20
    w = .8
    w2 = w / 2
    precision = 1
    cc_col = [0, 0, 0]
    tplx_col = sns.color_palette()[0]
    dask_col = np.array(sns.color_palette()[3])
    pyspark_col = 1.2 * np.array(sns.color_palette()[2])
    pysparksql_col = 0.6 * np.array(pyspark_col)

    py_col = pyspark_col
    cython_col = [161 / 255., 67 / 255., 133 / 255.]
    nuitka_col = [123 / 255, 88 / 255, 219 / 255.]
    scala_col = [.6, .6, .6]
    axs = list(axs.flat)

    ao = 2.5 * 650 / 540

    ##### SINGLE-THREADED ######
    ax = axs[0]

    python3_dict_err = np.array([df_st_dict_std[df_st_dict_std['framework'] == 'python3']['job_time']])
    python3_tuple_err = np.array([df_st_tuple_std[df_st_tuple_std['framework'] == 'python3']['job_time']])
    pandas_err = np.array([df_st_rest_std[df_st_rest_std['framework'] == 'pandas']['job_time']])
    tplx_err = np.array([df_st_rest_std[df_st_rest_std['framework'] == 'tuplex']['job_time']])
    cc_err = np.array([df_st_rest_std[df_st_rest_std['framework'] == 'c++']['job_time']])
    scala_err = np.array([df_st_rest_std[df_st_rest_std['framework'] == 'scala']['job_time']])

    plt_bar(ax, 1, df_st_dict[df_st_dict['framework'] == 'python3']['job_time'], w, py_col,
            'Python', 'center', precision=precision, yerr=python3_dict_err)
    plt_bar(ax, 2, df_st_tuple[df_st_tuple['framework'] == 'python3']['job_time'],
            w, py_col, 'Python', 'center', above_offset=ao * 10, precision=precision, yerr=python3_tuple_err)
    plt_bar(ax, 0, df_st_rest[df_st_rest['framework'] == 'pandas']['job_time'],
            w, dask_col, 'Pandas', 'center', above_offset=ao * 10, precision=precision, yerr=pandas_err)
    plt_bar(ax, 3, df_st_rest[df_st_rest['framework'] == 'tuplex']['job_time'],
            w, tplx_col, 'Tuplex', 'above', precision=precision, above_offset=ao * 10, yerr=tplx_err)
    plt_bar(ax, 4, df_st_rest[df_st_rest['framework'] == 'scala']['job_time'],
            w, scala_col, 'Scala', 'above', precision=precision, above_offset=ao * 10, yerr=scala_err)
    plt_bar(ax, 5, df_st_rest[df_st_rest['framework'] == 'c++']['job_time'],
            w, cc_col, 'C++', 'above', precision=precision, above_offset=ao * 10, yerr=cc_err)
    ax.axvline(3.5, linestyle='--', lw=2, color=[.6, .6, .6])

    legend_elements = [Line2D([0], [0], marker='o', color='w', label='Python   ',
                              markerfacecolor=py_col, markersize=mks),
                       Line2D([0], [0], marker='o', color='w', label='Pandas',
                              markerfacecolor=dask_col, markersize=mks),
                       Line2D([0], [0], pickradius=2, marker='o', color='w', label='Tuplex',
                              markerfacecolor=tplx_col, markersize=mks),
                       Line2D([0], [0], marker='o', color='w', label='Scala (hand-opt.)',
                              markerfacecolor=scala_col, markersize=mks),
                       Line2D([0], [0], pickradius=2, marker='o', color='w', label='C++ (hand-opt.)',
                              markerfacecolor=cc_col, markersize=mks)]

    ax.set_xticks([0, 1, 2, 3, 4, 5])
    ax.set_xticklabels(['Pandas', 'dict', 'tuple', 'Tuplex', 'Scala', 'C++'], rotation=rot)
    ax.grid(axis='x')
    sns.despine()
    ax.set_ylim(0, 650)
    ax.set_xlim(-.5, 5.5)
    ax.set_xlabel('({}a) single-threaded'.format(query_name), fontsize=27, labelpad=15)
    ax.set_ylabel('runtime in s', labelpad=10)

    ##### MULTI-THREADED ######
    ax = axs[1]

    spark_sql_time = df_mt_mu[(df_mt_mu['framework'] == 'spark') & (df_mt_mu['type'] == 'sql')]['job_time']
    spark_tuple_time = df_mt_mu[(df_mt_mu['framework'] == 'spark') & (df_mt_mu['type'] == 'tuple')]['job_time']
    spark_dict_time = df_mt_mu[(df_mt_mu['framework'] == 'spark') & (df_mt_mu['type'] == 'dict')]['job_time']
    spark_scala_time = df_mt_mu[(df_mt_mu['framework'] == 'spark') & (df_mt_mu['type'] == 'scala-sql')]['job_time']

    spark_sql_time_err = np.array(
        [df_mt_std[(df_mt_std['framework'] == 'spark') & (df_mt_std['type'] == 'sql')]['job_time']])
    spark_tuple_time_err = np.array(
        [df_mt_std[(df_mt_std['framework'] == 'spark') & (df_mt_std['type'] == 'tuple')]['job_time']])
    spark_dict_time_err = np.array(
        [df_mt_std[(df_mt_std['framework'] == 'spark') & (df_mt_std['type'] == 'dict')]['job_time']])
    spark_scala_time_err = np.array(
        [df_mt_std[(df_mt_std['framework'] == 'spark') & (df_mt_std['type'] == 'scala-sql')]['job_time']])

    plt_bar(ax, 2, spark_dict_time, w, pyspark_col, 'PySpark (dict)', 'center', precision=precision,
            yerr=spark_dict_time_err)
    plt_bar(ax, 3, spark_tuple_time, w, pyspark_col, 'PySpark (tuple)', 'center', precision=precision,
            yerr=spark_tuple_time_err)
    plt_bar(ax, 1, spark_sql_time, w, pysparksql_col, 'PySparkSQL', 'center', precision=precision,
            yerr=spark_sql_time_err)

    dask_time_err = np.array([df_mt_std[df_mt_std['framework'] == 'dask']['job_time']])
    tplx_time_err = np.array([df_mt_std[df_mt_std['framework'] == 'tuplex']['job_time']])

    plt_bar(ax, 0, df_mt_mu[df_mt_mu['framework'] == 'dask']['job_time'], w, dask_col,
            'Dask', 'center', precision=precision, yerr=dask_time_err)
    plt_bar(ax, 4, df_mt_mu[df_mt_mu['framework'] == 'tuplex']['job_time'], w, tplx_col,
            'Tuplex', 'above', above_offset=ao, precision=precision, yerr=tplx_time_err)

    ax.axvline(4.5, linestyle='--', lw=2, color=[.6, .6, .6])

    plt_bar(ax, 5, spark_scala_time, w, scala_col,
            'Scala', 'above', above_offset=ao, precision=precision, yerr=spark_scala_time_err)

    legend_elements = [
        Line2D([0], [0], marker='o', color='w', label='PySpark',
               markerfacecolor=pyspark_col, markersize=mks),
        Line2D([0], [0], marker='o', color='w', label='PySpark',
               markerfacecolor=pysparksql_col, markersize=mks),
        Line2D([0], [0], marker='o', color='w', label='Dask',
               markerfacecolor=dask_col, markersize=mks),
        Line2D([0], [0], marker='o', color='w', label='Tuplex',
               markerfacecolor=tplx_col, markersize=mks),
        Line2D([0], [0], marker='o', color='w', label='SparkSQL(Scala)',
               markerfacecolor=scala_col, markersize=mks), ]

    # disable legend, use from Z1
    # L = ax.legend(handles=legend_elements, loc='upper right', fontsize=15,
    #              bbox_to_anchor=(1, 1), borderaxespad=-.4, handletextpad=0., ncol=2, columnspacing=0)
    # cols = [ pyspark_col, pysparksql_col, dask_col, tplx_col, scala_col]
    # for i, text in enumerate(L.get_texts()):
    #     text.set_color(cols[i])

    ax.set_xticks([0, 1, 2, 3, 4, 5])
    ax.set_xticklabels(['Dask', 'SQL', 'dict', 'tuple', 'Tuplex', 'Scala'], rotation=rot)
    ax.grid(axis='x')
    sns.despine()
    ax.set_ylim(0, 65)
    ax.set_xlim(-.5, 5.5)
    ax.set_xlabel('({}b) 16x parallelism'.format(query_name), fontsize=27, labelpad=15)
    # ax.set_ylabel('runtime in s', labelpad=-5)

    plt.savefig(os.path.join(output_folder, 'figure3_zillow_10G_{}.pdf'.format(query_name)), transparent=True, bbox_inches='tight', pad_inches=0)


def figure7(df_Z1, output_folder):
    # ## Zillow Z1 CPython vs. Pypy3

    st_fws = ['tuplex', 'cython', 'nuitka', 'python3', 'c++', 'pandas', 'scala']
    mt_fws = ['tuplex', 'spark', 'dask']

    # which df to use?
    df = df_Z1.copy()
    # drop tuplex preload
    df = df[df['type'] != 'single-threaded-preload']
    query_name = 'Z1'

    df_st = df[(df['mode'].isin(['python3', 'c++', 'scala'])) & (df['framework'].isin(st_fws))]
    df_st = df_st[~df_st['type'].isin(['multi-threaded', 'cached', 'preload'])]
    df_st_mu = df_st.groupby(['framework', 'type']).mean().reset_index()
    df_st_std = df_st.groupby(['framework', 'type']).std().reset_index()

    df_st_tuple = df_st_mu[df_st_mu['type'] == 'tuple'].sort_values(by='job_time').reset_index(drop=True)
    df_st_dict = df_st_mu[df_st_mu['type'] == 'dict'].sort_values(by='job_time').reset_index(drop=True)
    df_st_rest = df_st_mu[~df_st_mu['type'].isin(['tuple', 'dict'])].sort_values(by='job_time').reset_index(drop=True)

    df_st_tuple_std = df_st_std[df_st_std['type'] == 'tuple']
    df_st_dict_std = df_st_std[df_st_std['type'] == 'dict']
    df_st_rest_std = df_st_std[~df_st_std['type'].isin(['tuple', 'dict'])]

    df_st = df[(df['mode'].isin(['python3', 'c++', 'scala'])) & (df['framework'].isin(st_fws))]
    df_st = df_st[~df_st['type'].isin(['multi-threaded', 'cached', 'preload'])]
    df_st_mu = df_st.groupby(['framework', 'type']).mean().reset_index()
    df_st_std = df_st.groupby(['framework', 'type']).std().reset_index()

    df_st_tuple = df_st_mu[df_st_mu['type'] == 'tuple'].sort_values(by='job_time').reset_index(drop=True)
    df_st_dict = df_st_mu[df_st_mu['type'] == 'dict'].sort_values(by='job_time').reset_index(drop=True)
    df_st_rest = df_st_mu[~df_st_mu['type'].isin(['tuple', 'dict'])].sort_values(by='job_time').reset_index(drop=True)

    df_st_tuple_std = df_st_std[df_st_std['type'] == 'tuple']
    df_st_dict_std = df_st_std[df_st_std['type'] == 'dict']
    df_st_rest_std = df_st_std[~df_st_std['type'].isin(['tuple', 'dict'])]

    df_mt = df[(df['mode'].isin(['python3', 'c++', 'scala'])) & (df['framework'].isin(mt_fws))]
    df_mt = df_mt[~df_mt['type'].isin(['single-threaded', 'cached', 'preload'])]
    df_mt_mu = df_mt.groupby(['framework', 'type']).mean().reset_index()
    df_mt_std = df_mt.groupby(['framework', 'type']).std().reset_index()

    pp_st = df[(df['mode'].isin(['pypy3'])) & (df['framework'].isin(st_fws))]
    pp_st = pp_st[~pp_st['type'].isin(['multi-threaded', 'cached', 'preload'])]
    pp_st_mu = pp_st.groupby(['framework', 'type', 'mode']).mean().reset_index()

    pp_st_std = pp_st.groupby(['framework', 'type', 'mode']).std().reset_index()

    pp_st_tuple = pp_st_mu[pp_st_mu['type'] == 'tuple'].sort_values(by='job_time').reset_index(drop=True)
    pp_st_dict = pp_st_mu[pp_st_mu['type'] == 'dict'].sort_values(by='job_time').reset_index(drop=True)
    pp_st_rest = pp_st_mu[~pp_st_mu['type'].isin(['tuple', 'dict'])].sort_values(by='job_time').reset_index(drop=True)

    pp_st_tuple_std = pp_st_std[pp_st_std['type'] == 'tuple']
    pp_st_dict_std = pp_st_std[pp_st_std['type'] == 'dict']
    pp_st_rest_std = pp_st_std[~pp_st_std['type'].isin(['tuple', 'dict'])]


    # In[90]:


    pp_mt = df[(df['mode'].isin(['pypy3', 'c++'])) & (df['framework'].isin(mt_fws))]
    pp_mt = pp_mt[~pp_mt['type'].isin(['single-threaded', 'cached', 'preload'])]
    pp_mt_mu = pp_mt.groupby(['framework', 'type', 'mode']).mean().reset_index()
    pp_mt_std = pp_mt.groupby(['framework', 'type', 'mode']).std().reset_index()

    # links: https://stackoverflow.com/questions/14852821/aligning-rotated-xticklabels-with-their-respective-xticks
    sf = 1.1
    fig, axs = plt.subplots(figsize=(sf * column_width, sf *column_width / rho * .6),
                            nrows=1, ncols=2, constrained_layout=True)
    rot = 25
    mks = 20
    w = .8
    w2 = w/2
    w4 = w/4
    precision = 1
    data_fsize=26
    cc_col = [0, 0, 0]
    tplx_col = sns.color_palette()[0]
    dask_col = np.array(sns.color_palette()[3])
    pyspark_col = 1.2 * np.array(sns.color_palette()[2])
    pysparksql_col = 0.6 * np.array(pyspark_col)

    py_col = pyspark_col
    cython_col = [161/255., 67/255., 133/255.]
    nuitka_col = [123/255, 88/255, 219/255.]

    base_gray = np.array([0.8, .8, .8])
    alpha = 0.1
    py_col_g = alpha * np.array(py_col) + (1. - alpha) * base_gray
    dask_col_g = alpha * dask_col + (1. - alpha) * base_gray
    pyspark_col_g = alpha * np.array(pyspark_col) + (1. - alpha) * base_gray
    pysparksql_col_g = alpha * np.array(pysparksql_col) + (1. - alpha) * base_gray
    axs = list(axs.flat)

    lim_high=90
    ao = 5

    ##### SINGLE-THREADED ######
    ax = axs[0]

    python3_dict_err = np.array([df_st_dict_std[df_st_dict_std['framework'] == 'python3']['job_time']])
    python3_tuple_err = np.array([df_st_tuple_std[df_st_tuple_std['framework'] == 'python3']['job_time']])
    pandas_err = np.array([df_st_rest_std[df_st_rest_std['framework'] == 'pandas']['job_time']])
    tplx_err = np.array([df_st_rest_std[df_st_rest_std['framework'] == 'tuplex']['job_time']])
    cc_err = np.array([df_st_rest_std[df_st_rest_std['framework'] == 'c++']['job_time']])
    scala_err = np.array([df_st_rest_std[df_st_rest_std['framework'] == 'scala']['job_time']])

    py_err = np.array([df_st_dict_std[df_st_dict_std['framework'] == 'python3']['job_time']])
    pypy_err = np.array([pp_st_dict_std[pp_st_dict_std['framework'] == 'python3']['job_time']])

    plt_bar(ax, -w4, df_st_dict[df_st_dict['framework'] == 'python3']['job_time'], w2, py_col_g, 'Python', yerr=py_err)
    plt_bar(ax, w4, pp_st_dict[pp_st_dict['framework'] == 'python3']['job_time'], w2, py_col, 'Pypy', yerr=pypy_err)

    py_err = np.array([df_st_tuple_std[df_st_tuple_std['framework'] == 'python3']['job_time']])
    pypy_err = np.array([pp_st_tuple_std[pp_st_tuple_std['framework'] == 'python3']['job_time']])

    plt_bar(ax, 1-w4, df_st_tuple[df_st_tuple['framework'] == 'python3']['job_time'], w2, py_col_g, 'Python', yerr=py_err)
    plt_bar(ax, 1+w4, pp_st_tuple[pp_st_tuple['framework'] == 'python3']['job_time'], w2, py_col, 'Python', yerr=pypy_err)


    py_err = np.array([df_st_rest_std[df_st_rest_std['framework'] == 'pandas']['job_time']])
    pypy_err = np.array([pp_st_rest_std[pp_st_rest_std['framework'] == 'pandas']['job_time']])

    plt_bar(ax, 2-w4, df_st_rest[df_st_rest['framework'] == 'pandas']['job_time'], w2, dask_col_g,
            'Pandas', yerr=pandas_err)
    plt_bar(ax, 2+w4, pp_st_rest[pp_st_rest['framework'] == 'pandas']['job_time'], w2, dask_col,
            'Pandas', yerr=pypy_err)


    tplx_err = np.array([df_st_rest_std[df_st_rest_std['framework'] == 'tuplex']['job_time']])
    cc_err = np.array([df_st_rest_std[df_st_rest_std['framework'] == 'c++']['job_time']])

    plt_bar(ax, 3, df_st_rest[df_st_rest['framework'] == 'tuplex']['job_time'], w2, tplx_col, 'Tuplex',
            'above', precision=precision, above_offset=ao*10, yerr=tplx_err, fsize=data_fsize)
    plt_bar(ax, 4, df_st_rest[df_st_rest['framework'] == 'c++']['job_time'], w2, cc_col, 'C++',
            'above', precision=precision, above_offset=ao*10, yerr=cc_err, fsize=data_fsize)
    ax.axvline(3.5, linestyle='--', lw=2,color=[.6,.6,.6])


    # legend_elements = [ Line2D([0], [0], marker='o', color='w', label='Python   ',
    #                           markerfacecolor=py_col, markersize=mks),
    #                     Line2D([0], [0], marker='o', color='w', label='Pandas',
    #                           markerfacecolor=dask_col, markersize=mks),
    #                    Line2D([0], [0], pickradius=2, marker='o', color='w', label='Tuplex',
    #                           markerfacecolor=tplx_col, markersize=mks),
    #                     Line2D([0], [0], pickradius=2, marker='o', color='w', label='C++ (hand-opt.)',
    #                           markerfacecolor=cc_col, markersize=mks)]
    # L = ax.legend(handles=legend_elements, loc='upper right', fontsize=18, bbox_to_anchor=(1, 1), borderaxespad=0.)
    # cols = [py_col, dask_col, tplx_col, cc_col]
    # for i, text in enumerate(L.get_texts()):
    #     text.set_color(cols[i])


    ax.set_xticks([0, 1, 2, 3, 4])
    ax.set_xticklabels(['dict', 'tuple', 'Pandas', 'Tuplex', 'C++'], rotation=rot)
    ax.grid(axis='x')
    sns.despine()
    ax.set_ylim(0, 10*lim_high)
    ax.set_xlim(-.5, 4.5)
    ax.set_yticks([200, 400, 600])
    ax.set_xlabel('(a) single-threaded', fontsize=27, labelpad=10)
    ax.set_ylabel('runtime in s', labelpad=5)

    ##### MULTI-THREADED ######
    ax = axs[1]

    spark_sql_time = df_mt_mu[(df_mt_mu['framework'] == 'spark') & (df_mt_mu['type'] == 'sql')]['job_time']
    spark_tuple_time = df_mt_mu[(df_mt_mu['framework'] == 'spark') & (df_mt_mu['type'] == 'tuple')]['job_time']
    spark_dict_time = df_mt_mu[(df_mt_mu['framework'] == 'spark') & (df_mt_mu['type'] == 'dict')]['job_time']

    spark_sql_time_err = np.array([df_mt_std[(df_mt_std['framework'] == 'spark') & (df_mt_std['type'] == 'sql')]['job_time']])
    spark_tuple_time_err = np.array([df_mt_std[(df_mt_std['framework'] == 'spark') & (df_mt_std['type'] == 'tuple')]['job_time']])
    spark_dict_time_err = np.array([df_mt_std[(df_mt_std['framework'] == 'spark') & (df_mt_std['type'] == 'dict')]['job_time']])

    pp_spark_sql_time = pp_mt_mu[(pp_mt_mu['framework'] == 'spark') & (pp_mt_mu['type'] == 'sql')]['job_time']
    pp_spark_tuple_time = pp_mt_mu[(pp_mt_mu['framework'] == 'spark') & (pp_mt_mu['type'] == 'tuple')]['job_time']
    pp_spark_dict_time = pp_mt_mu[(pp_mt_mu['framework'] == 'spark') & (pp_mt_mu['type'] == 'dict')]['job_time']

    pp_spark_sql_time_err = np.array([pp_mt_std[(pp_mt_std['framework'] == 'spark') & (pp_mt_std['type'] == 'sql')]['job_time']])
    pp_spark_tuple_time_err = np.array([pp_mt_std[(pp_mt_std['framework'] == 'spark') & (pp_mt_std['type'] == 'tuple')]['job_time']])
    pp_spark_dict_time_err = np.array([pp_mt_std[(pp_mt_std['framework'] == 'spark') & (pp_mt_std['type'] == 'dict')]['job_time']])

    plt_bar(ax, -w4, spark_dict_time, w2, pyspark_col_g, 'PySpark (dict)', yerr=spark_dict_time_err)
    plt_bar(ax, w4, pp_spark_dict_time, w2, pyspark_col, 'PySpark (dict)', yerr=pp_spark_dict_time_err)

    plt_bar(ax, 1-w4, spark_tuple_time, w2, pyspark_col_g, 'PySpark (tuple)', yerr=spark_tuple_time_err)
    plt_bar(ax, 1+w4, pp_spark_tuple_time, w2, pyspark_col, 'PySpark (tuple)', yerr=pp_spark_tuple_time_err)

    plt_bar(ax, 2-w4, spark_sql_time, w2, pysparksql_col_g, 'PySparkSQL', yerr=spark_sql_time_err)
    plt_bar(ax, 2+w4, pp_spark_sql_time, w2, pysparksql_col, 'PySparkSQL', yerr=pp_spark_sql_time_err)

    dask_py_err = np.array([df_mt_std[df_mt_std['framework'] == 'dask']['job_time']])
    dask_pypy_err = np.array([pp_mt_std[df_mt_std['framework'] == 'dask']['job_time']])

    plt_bar(ax, 3-w4, df_mt_mu[df_mt_mu['framework'] == 'dask']['job_time'], w2, dask_col_g,'Dask', yerr=dask_py_err)
    plt_bar(ax, 3+w4, pp_mt_mu[df_mt_mu['framework'] == 'dask']['job_time'], w2, dask_col,'Dask', yerr=dask_pypy_err)


    tplx_err = np.array([df_mt_std[df_mt_std['framework'] == 'tuplex']['job_time']])
    plt_bar(ax, 4, df_mt_mu[df_mt_mu['framework'] == 'tuplex']['job_time'], w2, tplx_col,
            'Tuplex', 'above', above_offset=ao, precision=precision, yerr=tplx_err, fsize=data_fsize)

    legend_elements = [ Line2D([0], [0], marker='o', color='w', label='Python / PySpark',
                              markerfacecolor=pyspark_col, markersize=mks),
                        Line2D([0], [0], marker='o', color='w', label='PySparkSQL',
                              markerfacecolor=pysparksql_col, markersize=mks),
                       Line2D([0], [0], marker='o', color='w', label='Pandas / Dask',
                              markerfacecolor=dask_col, markersize=mks),
                       Line2D([0], [0],  marker='o', color='w', label='Tuplex',
                              markerfacecolor=tplx_col, markersize=mks),
                      Line2D([0], [0], pickradius=2, marker='o', color='w', label='C++ (hand-opt.)',
                              markerfacecolor=cc_col, markersize=mks)]
    L = fig.legend(handles=legend_elements, loc='upper right', bbox_transform = plt.gcf().transFigure,
                   bbox_to_anchor = (0.01,0,1,1), fontsize=17,
                 borderaxespad=0.4, handletextpad=0., ncol=5, columnspacing=0)
    cols = [pyspark_col, pysparksql_col, dask_col, tplx_col, cc_col]
    for i, text in enumerate(L.get_texts()):
        text.set_color(cols[i])


    ax.set_xticks([0, 1, 2, 3, 4])
    ax.set_xticklabels(['dict', 'tuple', 'SQL', 'Dask', 'Tuplex'], rotation=rot)
    ax.grid(axis='x')
    sns.despine()
    ax.set_ylim(0, lim_high)
    ax.set_xlim(-.5, 4.5)
    ax.set_yticks([20, 40, 60])
    ax.set_xlabel('(b) 16x parallelism', fontsize=27, labelpad=10)
    # ax.set_ylabel('runtime in s', labelpad=-5)

    plt.savefig(os.path.join(output_folder, 'figure7_zillow_Z1_10G_pypy.pdf'), transparent=True, bbox_inches = 'tight', pad_inches = 0)