#!/usr/bin/env python3
# contains all code to plot figure3 of the paper
import os

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

adjust_settings()

# Z1 query
def load_zillow_Z1_to_df(data_root):
    files = os.listdir(data_root)
    rows = []

    for file in files:
        path = os.path.join(data_root, file)
        name = file[:file.find('-run')]

        try:
            if file.endswith('.txt'):
                # print(path)
                with open(path, 'r') as fp:
                    lines = fp.readlines()

                    # skip empty files (means timeout)
                    if len(lines) == 0:
                        continue

                    row = {}

                    # CC baseline decode
                    if 'cc-' in path or 'cpp' in path:
                        d = json.loads(lines[-1])
                        row = {}
                        row['write'] = d['output'] / 10 ** 9
                        row['job_time'] = d['total'] / 10 ** 9
                        if 'load' in d.keys():
                            row['load'] = d['load'] / 10 ** 9
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
                    elif 'scala' in path:  # Scala

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
                            row = {'startup_time': d['startupTime'], 'job_time': d['jobTime'], "load": load_time}
                            row['framework'] = 'tuplex'

                            if '-st' in path or '_st' in path:
                                row['type'] = 'single-threaded'
                            elif '-io' in path or '_io' in path:
                                row['type'] = 'cached'
                            else:
                                row['type'] = 'multi-threaded'

                            # breakdown time extract for tuplex
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
                        row['type'] = d['type']

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
                        rows.append(row)
        except Exception as e:
            print(path + str(type(e)))
    return pd.DataFrame(rows)

def plot_Z1(Z1_path='benchmark_results/zillow/Z1/', output_folder='plots'):

    os.makedirs(output_folder, exist_ok=True)
    df = load_zillow_Z1_to_df(os.path.join(Z1_path, 'pyperf/'))
    df_base = load_zillow_Z1_to_df(os.path.join(Z1_path, 'baselines/'))

    df = pd.concat((df, df_base))

    st_fws = ['tuplex', 'cython', 'nuitka', 'python3', 'c++', 'pandas', 'scala']
    mt_fws = ['tuplex', 'spark', 'dask']

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
                                     sf * column_width / rho * 0.68), nrows=1, ncols=2, constrained_layout=True)
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
            w, py_col, 'Python', 'center', precision=precision, yerr=python3_tuple_err)
    plt_bar(ax, 0, df_st_rest[df_st_rest['framework'] == 'pandas']['job_time'],
            w, dask_col, 'Pandas', 'center', precision=precision, yerr=pandas_err)
    plt_bar(ax, 3, df_st_rest[df_st_rest['framework'] == 'tuplex']['job_time'],
            w, tplx_col, 'Tuplex', 'above', precision=precision, above_offset=40, yerr=tplx_err)
    plt_bar(ax, 4, df_st_rest[df_st_rest['framework'] == 'scala']['job_time'],
            w, scala_col, 'Scala', 'above', precision=precision, above_offset=40, yerr=scala_err)
    plt_bar(ax, 5, df_st_rest[df_st_rest['framework'] == 'c++']['job_time'],
            w, cc_col, 'C++', 'above', precision=precision, above_offset=40, yerr=cc_err)
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
    L = ax.legend(handles=legend_elements, loc='upper right', fontsize=15, bbox_to_anchor=(1, 1),
                  borderaxespad=-.4, handletextpad=0., ncol=2, columnspacing=0)
    cols = [py_col, dask_col, tplx_col, scala_col, cc_col]
    for i, text in enumerate(L.get_texts()):
        text.set_color(cols[i])

    ax.set_xticks([0, 1, 2, 3, 4, 5])
    ax.set_xticklabels(['Pandas', 'dict', 'tuple', 'Tuplex', 'Scala', 'C++'], rotation=rot)
    ax.grid(axis='x')
    sns.despine()
    ax.set_ylim(0, 1390)
    ax.set_xlim(-.5, 5.5)
    ax.set_xlabel('(Z1a) single-threaded', fontsize=27, labelpad=10)
    ax.set_ylabel('runtime in s', labelpad=0)

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
            'Tuplex', 'above', above_offset=5, precision=precision, yerr=tplx_time_err)

    ax.axvline(4.5, linestyle='--', lw=2, color=[.6, .6, .6])

    plt_bar(ax, 5, spark_scala_time, w, scala_col,
            'Scala', 'center', above_offset=5, precision=precision, yerr=spark_scala_time_err)

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
    L = ax.legend(handles=legend_elements, loc='upper right', fontsize=15,
                  bbox_to_anchor=(1, 1), borderaxespad=-.4, handletextpad=0., ncol=3, columnspacing=0)
    cols = [pyspark_col, pysparksql_col, dask_col, tplx_col, scala_col]
    for i, text in enumerate(L.get_texts()):
        text.set_color(cols[i])

    ax.set_xticks([0, 1, 2, 3, 4, 5])
    ax.set_xticklabels(['Dask', 'SQL', 'dict', 'tuple', 'Tuplex', 'Scala'], rotation=rot)
    ax.grid(axis='x')
    sns.despine()
    ax.set_ylim(0, 139)
    ax.set_xlim(-.5, 5.5)
    ax.set_xlabel('(Z1b) 16x parallelism', fontsize=27, labelpad=10)
    # ax.set_ylabel('runtime in s', labelpad=-5)

    plt.savefig(os.path.join(output_folder, 'figure3_zillow_10G_Z1.pdf'), transparent=True, bbox_inches='tight', pad_inches=0)

# Z2 query

def load_zillow_Z2_to_df(data_root):
    files = os.listdir(data_root)
    rows = []

    for file in files:
        path = os.path.join(data_root, file)
        name = file[:file.find('-run')]

        try:
            if file.endswith('.txt'):
                # print(path)
                with open(path, 'r') as fp:
                    lines = fp.readlines()

                    # skip empty files (means timeout)
                    if len(lines) == 0:
                        continue

                    row = {}

                    # CC baseline decode
                    if 'cc-' in path:
                        d = json.loads(lines[-1])
                        row = {}
                        row['write'] = d['output'] / 10 ** 9
                        row['job_time'] = d['total'] / 10 ** 9
                        if 'load' in d.keys():
                            row['load'] = d['load'] / 10 ** 9
                            row['compute'] = d['compute'] / 10 ** 9
                        else:
                            row['compute'] = d['transform'] / 10 ** 9

                        if 'no-preload' in file:
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
                    elif 'scala' in path:  # Scala

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
                            row = {'startup_time': d['startupTime'], 'job_time': d['jobTime'], "load": load_time}
                            row['framework'] = 'tuplex'

                            if '-st' in path:
                                row['type'] = 'single-threaded'
                            elif '-io' in path:
                                row['type'] = 'cached'
                            else:
                                row['type'] = 'multi-threaded'

                            # breakdown time extract for tuplex
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
                        row['type'] = d['type']

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
                        rows.append(row)
        except Exception as e:
            print(e)
    return pd.DataFrame(rows)

def plot_Z2(Z2_path='benchmark_results/zillow/Z2/', output_folder='plots'):

    os.makedirs(output_folder, exist_ok=True)

    df = load_zillow_Z2_to_df(os.path.join(Z2_path, 'pyperf/'))

    # load baselines
    df_base = load_zillow_Z2_to_df(os.path.join(Z2_path, 'baselines/'))
    df = pd.concat((df, df_base))

    st_fws = ['tuplex', 'cython', 'nuitka', 'python3', 'c++', 'pandas', 'scala']
    mt_fws = ['tuplex', 'spark', 'dask']

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
            w, py_col, 'Python', 'above', above_offset=60, precision=precision, yerr=python3_tuple_err)
    plt_bar(ax, 0, df_st_rest[df_st_rest['framework'] == 'pandas']['job_time'],
            w, dask_col, 'Pandas', 'center', precision=precision, yerr=pandas_err)
    plt_bar(ax, 3, df_st_rest[df_st_rest['framework'] == 'tuplex']['job_time'],
            w, tplx_col, 'Tuplex', 'above', precision=precision, above_offset=60, yerr=tplx_err)
    plt_bar(ax, 4, df_st_rest[df_st_rest['framework'] == 'scala']['job_time'],
            w, scala_col, 'Scala', 'above', precision=precision, above_offset=60, yerr=scala_err)
    plt_bar(ax, 5, df_st_rest[df_st_rest['framework'] == 'c++']['job_time'],
            w, cc_col, 'C++', 'above', precision=precision, above_offset=60, yerr=cc_err)
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

    # disable, use from Z1
    # L = ax.legend(handles=legend_elements, loc='upper right', fontsize=15, bbox_to_anchor=(1, 1),
    #               borderaxespad=-.4, handletextpad=0., ncol=2, columnspacing=0)
    # cols = [py_col, dask_col, tplx_col, scala_col, cc_col]
    # for i, text in enumerate(L.get_texts()):
    #     text.set_color(cols[i])

    ax.set_xticks([0, 1, 2, 3, 4, 5])
    ax.set_xticklabels(['Pandas', 'dict', 'tuple', 'Tuplex', 'Scala', 'C++'], rotation=rot)
    ax.grid(axis='x')
    sns.despine()
    ax.set_ylim(0, 2100)
    ax.set_xlim(-.5, 5.5)
    ax.set_xlabel('(Z2a) single-threaded', fontsize=27, labelpad=15)
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
            'Tuplex', 'above', above_offset=10, precision=precision, yerr=tplx_time_err)

    ax.axvline(4.5, linestyle='--', lw=2, color=[.6, .6, .6])

    plt_bar(ax, 5, spark_scala_time, w, scala_col,
            'Scala', 'center', above_offset=5, precision=precision, yerr=spark_scala_time_err)

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
    # ax.set_ylim(0, 245)
    ax.set_ylim(0, 210)
    ax.set_xlim(-.5, 5.5)
    ax.set_xlabel('(Z2b) 16x parallelism', fontsize=27, labelpad=15)
    # ax.set_ylabel('runtime in s', labelpad=-5)

    plt.savefig(os.path.join(output_folder, 'figure3_zillow_10G_Z2.pdf'), transparent=True, bbox_inches='tight', pad_inches=0)