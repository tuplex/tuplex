import os
import statistics
import matplotlib.pyplot as plt

NUM_TRIALS = 10

# generator parameters
NUM_LISTS = "1000000"
TYPE_STR = "int"
DISTR_STR = "uniform" # space separated

# should only try one list
LIST_LENGTHS = [100]

def gen_data():
    for curr_len in LIST_LENGTHS:
        os.system(f"python3 generate_data.py --num_lists {NUM_LISTS} --length {curr_len} --types {TYPE_STR} --distributions {DISTR_STR}")
        print(f'generated {curr_len} csv')
    return

def run_cc(ident):
    result_distr_str = DISTR_STR.replace(' ', '')
    for curr_len in LIST_LENGTHS:
        filename_str = NUM_LISTS + '_' + str(curr_len) + '_' + TYPE_STR + '_' + result_distr_str
        os.system(f'rm {filename_str}.{ident}.out')
        for _ in range(NUM_TRIALS):
            os.system(f"./{ident} {filename_str}.csv >> {filename_str}.{ident}.out")

def run_baseline_py():
    result_distr_str = DISTR_STR.replace(' ', '')
    for curr_len in LIST_LENGTHS:
        filename_str = NUM_LISTS + '_' + str(curr_len) + '_' + TYPE_STR + '_' + result_distr_str
        os.system(f'rm {filename_str}.py.out')
        for _ in range(NUM_TRIALS):
            os.system(f"python3 count_unique_baseline_freq.py --filename {filename_str}.csv >> {filename_str}.py.out")

def cc_timings(ident):
    result_distr_str = DISTR_STR.replace(' ', '')
    all_timings = []
    for curr_len in LIST_LENGTHS:
        filename_str = NUM_LISTS + '_' + str(curr_len) + '_' + TYPE_STR + '_' + result_distr_str
        curr_timings = []
        with open(f'{filename_str}.{ident}.out', 'r') as f:
            for line in f:
                curr_timings.append(float(line.strip('\n')))
        all_timings.append(statistics.median(curr_timings))
    return all_timings

def baseline_py_timings():
    result_distr_str = DISTR_STR.replace(' ', '')
    all_timings = []
    for curr_len in LIST_LENGTHS:
        filename_str = NUM_LISTS + '_' + str(curr_len) + '_' + TYPE_STR + '_' + result_distr_str
        curr_timings = []
        with open(f'{filename_str}.py.out', 'r') as f:
            for line in f:
                curr_timings.append(float(line.strip('\n')))
        all_timings.append(statistics.median(curr_timings))
    return all_timings

def plot_timings(ys, legends):
    for y_list, legend in zip(ys, legends):
        plt.plot(LIST_LENGTHS, y_list, marker='o', label=legend)
    plt.legend(loc='upper left')
    plt.show()


def main():
    to_run_cc = ['count_unique_bench_freq_int_stdmap', 'count_unique_bench_freq_string_stdmap', 'count_unique_bench_freq_int_stdumap',
    'count_unique_bench_freq_string_stdumap']
    # gen_data()
    # run_baseline_py()
    to_run_timings = []

    for ident in to_run_cc:
        # run_cc(ident)
        to_run_timings.append(cc_timings(ident))

    run_baseline_py()
    baseline_by = baseline_py_timings()
    plot_timings(to_run_timings + [baseline_by], to_run_cc + ['baseline_python_list_comprehension'])


if __name__ == "__main__":
    main()