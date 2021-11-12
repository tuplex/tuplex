import os
import statistics
import matplotlib.pyplot as plt

NUM_TRIALS = 10

# generator parameters
NUM_LISTS = "10_100000"
TYPE_STR = "int"
DISTR_STR = "binomial" # space separated

def gen_data():
    os.system(f"python3 generate_data.py --num_lists {NUM_LISTS} --types {TYPE_STR} --distributions {DISTR_STR}")
    print(f'generated data')
    return

def run_cc(ident):
    result_distr_str = DISTR_STR.replace(' ', '')
    filename_str = NUM_LISTS + '_' + TYPE_STR + '_' + result_distr_str
    os.system(f'rm {filename_str}.{ident}.out')
    for _ in range(NUM_TRIALS):
        os.system(f"./{ident} {filename_str}.csv >> {filename_str}.{ident}.out")

def run_baseline_py():
    result_distr_str = DISTR_STR.replace(' ', '')
    filename_str = NUM_LISTS + '_' + TYPE_STR + '_' + result_distr_str
    os.system(f'rm {filename_str}.py.out')
    for _ in range(NUM_TRIALS):
        os.system(f"python3 count_unique_baseline_freq.py --filename {filename_str}.csv >> {filename_str}.py.out")

def cc_timings(ident):
    result_distr_str = DISTR_STR.replace(' ', '')
    all_timings = []
    filename_str = NUM_LISTS + '_' + TYPE_STR + '_' + result_distr_str
    curr_timings = []
    with open(f'{filename_str}.{ident}.out', 'r') as f:
        for line in f:
            curr_timings.append(float(line.strip('\n')))
    all_timings.append(statistics.mean(curr_timings))
    return statistics.mean(curr_timings)

def baseline_py_timings():
    result_distr_str = DISTR_STR.replace(' ', '')
    all_timings = []
    filename_str = NUM_LISTS + '_' + TYPE_STR + '_' + result_distr_str
    curr_timings = []
    with open(f'{filename_str}.py.out', 'r') as f:
        for line in f:
            curr_timings.append(float(line.strip('\n')))
    all_timings.append(statistics.mean(curr_timings))
    return statistics.mean(curr_timings)

def plot_timings(ys, legends):

    print(ys, legends)

    import numpy as np
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    color = ('red', '#00b050', '#00b0f0', 'yellow', 'grey')
    objects = legends
    y_pos = np.arange(len(objects))
    performance = ys
    width = 0.35  # the width of the bars
    plt.bar(y_pos, performance, align='center', color=color)
    # plt.xticks(y_pos, objects)
    # plt.ylim(0, 20)  # this adds a little space at the top of the plot, to compensate for the annotation
    plt.ylabel('Runtime in ms', fontsize=16)

    # map names to colors
    cmap = dict(zip(performance, color))

    # create the rectangles for the legend
    patches = [Patch(color=v, label=k) for k, v in cmap.items()]

    # add the legend
    plt.legend(labels=objects, handles=patches, bbox_to_anchor=(1.04, 0.5), loc='center left', borderaxespad=0, fontsize=15, frameon=False)

    plt.savefig('test.png', bbox_inches='tight')
# add the annotations

    # for y_list, legend in zip(ys, legends):
    #     plt.plot(LIST_LENGTHS, y_list, marker='o', label=legend)
    # plt.legend(loc='upper left')
    # plt.show()


def main():
    to_run_cc = ['count_unique_bench_freq_int_stdmap', 'count_unique_bench_freq_string_stdmap', 'count_unique_bench_freq_int_stdumap',
    'count_unique_bench_freq_string_stdumap']
    to_run_cc_legend = ['C++ std::map (key: int, val: int)', 'C++ std::map (key: string, val: int)', 'C++ std::unordered_map (key: int, val: int)',
   'C++ std::unordered_map (key: string, val: int)']
    # gen_data()
    # run_baseline_py()
    to_run_timings = []

    for ident in to_run_cc:
        # run_cc(ident)
        to_run_timings.append(cc_timings(ident))

    # run_baseline_py()
    baseline_py = baseline_py_timings()

    plot_timings(to_run_timings + [baseline_py], to_run_cc_legend + ['Python3 dict baseline'])


if __name__ == "__main__":
    main()