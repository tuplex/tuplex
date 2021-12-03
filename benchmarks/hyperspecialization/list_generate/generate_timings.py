import os
import argparse
import statistics
import matplotlib.pyplot as plt

NUM_TRIALS = 10


# generator parameters
NUM_LISTS = "1000000"
TYPE_STR = "int"
DISTR_STR = "uniform" # space separated

def gen_data():
    os.system(f"python3 generate_data.py --num_lists {NUM_LISTS} --types {TYPE_STR} --distributions {DISTR_STR}")
    print(f'generated data')
    return

def run_cc(ident, filename_str):
    result_distr_str = DISTR_STR.replace(' ', '')
    os.system(f'rm {filename_str}.{ident}.out')
    for _ in range(NUM_TRIALS):
        os.system(f"./{ident} {filename_str}.csv >> {filename_str}.{ident}.out")

def run_baseline_py(filename_str):
    result_distr_str = DISTR_STR.replace(' ', '')
    os.system(f'rm {filename_str}.py.out')
    for _ in range(NUM_TRIALS):
        os.system(f"python3 count_unique_baseline_freq.py --filename {filename_str}.csv >> {filename_str}.py.out")

def cc_timings(ident, filename_str):
    result_distr_str = DISTR_STR.replace(' ', '')
    curr_timings = []
    with open(f'{filename_str}.{ident}.out', 'r') as f:
        for line in f:
            curr_timings.append(float(line.strip('\n')))
    return (statistics.mean(curr_timings), statistics.stdev(curr_timings))


def plot_timings(ys, legends, title):

    # need to add standard deviation bars here.

    print(ys, legends)

    import numpy as np
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    color = ('red', '#00b050', '#00b0f0', 'yellow', 'grey', 'black', 'purple')
    objects = legends
    y_pos = np.arange(len(objects))
    performance = [y[0] for y in ys]
    stdevs = [y[1] for y in ys]
    width = 0.35  # the width of the bars
    plt.bar(y_pos, performance, yerr=stdevs, align='center', color=color)
    # plt.xticks(y_pos, objects)
    # plt.ylim(0, 20)  # this adds a little space at the top of the plot, to compensate for the annotation
    plt.ylabel('Runtime in ms', fontsize=16)

    # map names to colors
    cmap = dict(zip(performance, color))

    # create the rectangles for the legend
    patches = [Patch(color=v, label=k) for k, v in cmap.items()]

    # add the legend
    plt.legend(labels=objects, handles=patches, bbox_to_anchor=(1.04, 0.5), loc='center left', borderaxespad=0, fontsize=15, frameon=False)

    plt.suptitle(title)
    plt.savefig('1000_0_to_1000.png', bbox_inches='tight')
# add the annotations

    # for y_list, legend in zip(ys, legends):
    #     plt.plot(LIST_LENGTHS, y_list, marker='o', label=legend)
    # plt.legend(loc='upper left')
    # plt.show()


def baseline_py_timings(filename_str):
    result_distr_str = DISTR_STR.replace(' ', '')
    all_timings = []
    curr_timings = []
    with open(f'{filename_str}.py.out', 'r') as f:
        for line in f:
            curr_timings.append(float(line.strip('\n')))
    all_timings.append(statistics.mean(curr_timings))
    return (statistics.mean(curr_timings), statistics.stdev(curr_timings))

def get_cc_timings(filename_str):
    to_run_cc = ['stdmap_string_int', 'stdumap_string_int', 'stdmap_int_int', 'stdumap_int_int', 'fixed_range_nopydict']
    # run_baseline_py()
    to_run_timings = []

    for ident in to_run_cc:
        run_cc(ident, filename_str) ################ <-------------------------
        to_run_timings.append(cc_timings(ident, filename_str))
    return to_run_timings

def main():
    to_run_cc_legend = ['C++ std::map (key: string, val: int)',
   'C++ std::unordered_map (key: string, val: int)', 'C++ std::map (key: int, val: int)',  'C++ std::unordered_map (key: int, val: int)', 'C++ fixed range specialized']

    filename_strs = ['1000_of_10000_int_uniform_0_to_1000']

    run_baseline_py(filename_strs[0])
    baseline_py = baseline_py_timings(filename_strs[0])
    cc = get_cc_timings(filename_strs[0])
    # for filename in filename_strs:
    # run_baseline_py(filename)

    # # run_cc happens in get_cc_timings
    # baseline_py.append(baseline_py_timings(filename))
    # cc.append(get_cc_timings(filename))
    
    plot_timings([baseline_py] + cc, ['Python3 dict baseline'] + to_run_cc_legend, 
        '1000 lists of 10000 integers in uniform(0, 1000) (converted to PyDict)')


if __name__ == "__main__":
    main()