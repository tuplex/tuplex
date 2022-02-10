import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import re
import json
import seaborn as sns
from matplotlib.patches import Patch
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
from matplotlib.path import *

import os
import glob

column_width = 3.33 * 3


# adjust settings for final paper (colors & Co)
def adjust_settings():
    # make nice looking plot

    sns.set_style('whitegrid')
    sns.set_context('poster')  # Everything is larger

    # matplotlib.rcParams['font.family'] = "serif"

    # red 900c3f

    # 48A70B
    sns.set_palette(
        [[.22, .52, .71], [.53, .80, .81], [144 / 255., 12 / 255, 63 / 255.], [67 / 255., 135 / 255., 107 / 255.],
         [248 / 255., 194 / 255., 145 / 255.]])

    matplotlib.rc('font', family='serif', size=9)
    matplotlib.rc('text.latex', preamble=['\\usepackage{times,mathptmx}'])
    matplotlib.rc('text', usetex=True)
    matplotlib.rc('legend', fontsize=8)
    matplotlib.rc('figure', figsize=(2.5, 1.4))
    matplotlib.rc('axes', linewidth=0.5)
    matplotlib.rc('lines', linewidth=0.5)


def parse_logs_exp_to_df(data_root):
    files = os.listdir(data_root)
    rows = []

    for file in files:
        path = os.path.join(data_root, file)

        if not path.endswith('txt'):
            continue

        # skip first run
        if 'run-1.txt' in file:
            continue

        name = file[:file.find('run')]
        if 'stderr' not in file:
            # print(path)
            with open(path, 'r') as fp:
                lines = fp.readlines()

                # skip empty files (means timeout)
                if len(lines) == 0:
                    continue

                try:
                    d = {}
                    if 'tuplex' in name or 'dask' in name:
                        d = json.loads(list(filter(lambda x: 'startupTime' in x, lines))[0])
                    else:
                        d = json.loads(lines[-1].replace("'", '"'))

                    load_time = 0.
                    if 'io_load_time' in d.keys():
                        load_time = d['io_load_time']
                    if 'io_load' in d.keys():
                        load_time = d['io_load']
                    row = {'startup_time': d['startupTime'], 'job_time': d['jobTime'], "load": load_time}
                    row['framework'] = name[:name.find('-')]
                    row['type'] = '-'.join(name.split('-')[1:-1])

                    # breakdown time extract for tuplex
                    if 'tuplex' in name:
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

                    rows.append(row)
                except Exception as e:
                    print('bad path: {}'.format(path))
                    print(e)
                    pass
    return pd.DataFrame(rows)


def plt_bar(ax, x, data, w, color, name,
            value=None, cutoff=None, fsize=30, edge_width=2.5, th=100,
            above_offset=200, precision=0, yerr=None, hatch=None):
    mu = data.mean()
    std = data.std()
    h = mu
    ec = 'white'
    if cutoff:
        h = cutoff + th / 2
        ec = 'none' if not hatch else color

    if hatch:
        plt.rcParams.update({'hatch.color': 'w'})
        plt.rcParams.update({'hatch.linewidth': '4'})
        b = ax.bar(x, h, w, color=color, edgecolor=ec, hatch='//', linewidth=0)
    else:
        b = ax.bar(x, h, w, color=color, edgecolor=ec)

    for rect in b:

        # if hatch:
        #     plt.rcParams.update({'hatch.color': 'w'})
        #     plt.rcParams.update({'hatch.linewidth': '4'})
        #
        #     rect.set_hatch(hatch)
        #     # col = rect.get_facecolor()
        #     # rect.set_color('none')
        #     # rect.set_ec(col)
        #     # rect.set_lw(2)
        #     col = rect.get_facecolor()
        #     rect.set_color(col)
        #
        #     # rect.set_ec('w')
        #     # rect.set_lw(5)
        dx = 0
        if cutoff:
            ax.plot([rect.get_x() + dx, rect.get_x() + dx], [rect.get_y(), rect.get_y() + rect.get_height() + th],
                    lw=edge_width, color='w')
            ax.plot([rect.get_x() + rect.get_width() - dx, rect.get_x() + rect.get_width() - dx],
                    [rect.get_y(), rect.get_y() + rect.get_height() + th],
                    lw=edge_width, color='w')

        label = '${}$'.format(int(mu))
        if precision > 0:
            label = ('${:.' + str(precision) + 'f}$').format(mu)

        neutral_col = 'k' if hatch else 'white'

        if value == 'center':
            ax.text(rect.get_x() + rect.get_width() / 2.0,
                    rect.get_height() / 2.0,
                    label,
                    ha='center',
                    va='center', rotation=90, fontSize=fsize, fontweight='bold', color=neutral_col)
        if value == 'above':
            ax.text(rect.get_x() + rect.get_width() / 2.0,
                    rect.get_height() + above_offset,
                    label,
                    ha='center',
                    va='bottom', rotation=90, fontSize=fsize, fontweight='bold', color=color)

    # add tile to cutoff bar
    if cutoff:

        tw = w / 2
        ty = h
        tx = x - w / 2
        vertsA = [
            (tx, ty),
            (tx + tw / 2, ty + th),
            (tx + tw, ty),
            (tx + tw, ty - 20),
            (tx + 0, ty - 20),
            (tx + 0, ty)
        ]
        codesA = [
            Path.MOVETO,
            Path.CURVE3,
            Path.CURVE3,
            Path.LINETO,
            Path.LINETO,
            Path.CLOSEPOLY,
        ]
        tx = x
        vertsB = [
            (tx, ty),
            (tx + tw / 2, ty - th),
            (tx + tw, ty),
            (tx, ty)
        ]

        codesB = [
            Path.MOVETO,
            Path.CURVE3,
            Path.CURVE3,
            Path.LINETO,
        ]

        if not hatch:
            pathA = Path(vertsA, codesA)
            pathB = Path(vertsB, codesB)
            patchA = mpatches.PathPatch(pathA, facecolor=color, edgecolor='none')
            ax.add_patch(patchA)
            patchB = mpatches.PathPatch(pathB, facecolor='white', edgecolor='none')
            ax.add_patch(patchB)

    else:
        if yerr is not None:
            if len(yerr) == 1:
                yerr = np.array(yerr)[0]
            ax.errorbar(x, h,
                        yerr=yerr,
                        capsize=10,
                        capthick=edge_width,
                        color='k', fmt='none',
                        linewidth=edge_width)
