import matplotlib.pyplot as plt
import numpy as np
import matplotlib.patches as mpatches
from enum import Enum
import argparse
import os
import json

PLAIN_COLOR = "#4285F4"
INCREMENTAL_COLOR = '#DB4437'
COMMIT_COLOR = "#F4B400"

class Mode(Enum):
    OUT_OF_ORDER = 1
    IN_ORDER = 2
    COMMIT = 3

def in_order_total(save_path, plain_times, incremental_times, commit_times):
    width = 0.7
    separator = 0.02

    labels = ['No Resolvers', 'Bedroom Resolve', 'Bedroom Ignore', 'Bathroom Resolve', 'Bathroom Ignore', 'Sqft Ignore', 'Price Resolve', 'Price Ignore']
    x = np.arange(len(labels))

    fig = plt.figure()

    plt.bar(x - width/3 - separator, plain_times, width/3, color=PLAIN_COLOR)
    plt.bar(x, incremental_times, width/3, color=INCREMENTAL_COLOR)
    plt.bar(x + width/3 + separator, commit_times, width/3, color=COMMIT_COLOR)

    plt.title('In Order | 100GB')
    plt.xticks(x, labels)
    plt.ylabel('Time (s)')
    plt.xlabel('Number of Resolvers in Pipeline')
    plt.legend(handles=[
        mpatches.Patch(color=PLAIN_COLOR, label='Plain'),
        mpatches.Patch(color=INCREMENTAL_COLOR, label='Incremental'),
        mpatches.Patch(color=COMMIT_COLOR, label='Commit')
    ], loc='upper right')

    fig.savefig(os.path.join(save_path, 'in-order-total.png'), dpi=400)

def time_breakdown(save_path, save_name, fast_path, slow_path, write):
    width = 0.6
    separator = 0.02

    labels = ['No Resolvers', 'Bedroom Resolve', 'Bedroom Ignore', 'Bathroom Resolve', 'Bathroom Ignore', 'Sqft Ignore', 'Price Resolve', 'Price Ignore']
    x = np.arange(len(labels))

    fig = plt.figure()

    plt.bar(x, fast_path, width, color=PLAIN_COLOR)
    plt.bar(x, slow_path, width, bottom=fast_path, color=INCREMENTAL_COLOR)
    plt.bar(x, write, width, bottom=fast_path + slow_path, color=COMMIT_COLOR)

    plt.title('Out of Order | 100GB')
    plt.xticks(x, labels)
    plt.ylabel('Time (s)')
    plt.xlabel('Number of Resolvers in Pipeline')

    plt.legend(handles=[
        mpatches.Patch(color=PLAIN_COLOR, label='Fast Path'),
        mpatches.Patch(color=INCREMENTAL_COLOR, label='Slow Path'),
        mpatches.Patch(color=COMMIT_COLOR, label='Write')
    ], loc='upper right')

    fig.savefig(os.path.join(save_path, save_name), dpi=400)


def out_of_order_total(save_path, plain_times, incremental_times):
    width = 0.35
    separator = 0.02

    labels = ['No Resolvers', 'Bedroom Resolve', 'Bedroom Ignore', 'Bathroom Resolve', 'Bathroom Ignore', 'Sqft Ignore', 'Price Resolve', 'Price Ignore']
    x = np.arange(len(labels))

    fig = plt.figure()

    plt.bar(x - width/2 - separator, plain_times, width + separator, color=PLAIN_COLOR)
    plt.bar(x + width/2 + separator, incremental_times, width + separator, color=INCREMENTAL_COLOR)

    plt.title('Out of Order | 100GB')
    plt.xticks(x, labels)
    plt.ylabel('Time (s)')
    plt.xlabel('Number of Resolvers in Pipeline')
    plt.legend(handles=[
        mpatches.Patch(color=PLAIN_COLOR, label='Plain'),
        mpatches.Patch(color=INCREMENTAL_COLOR, label='Incremental')
    ], loc='upper right')

    fig.savefig(os.path.join(save_path, 'out-of-order-total.png'), dpi=400)

def validate_experiment(compare_path):
    with open(compare_path) as f:
        lines = f.read().splitlines()
        return ">>> contents of folders match." in lines

def get_metric(path, metric, step):
    with open(path) as f:
        lines = f.read().splitlines()
        ind = lines.index("EXPERIMENTAL RESULTS") + 2
        line = lines[ind + step]
        metrics = json.loads(line)
        if metric == 'jobTime':
            return metrics[metric]
        else:
            return metrics["stages"][0][metric]

def compare_path(trial, mode):
    return "tuplex-compare-{}{}-ssd-{}.txt".format('out-of-order' if mode == Mode.OUT_OF_ORDER else 'in-order',
                                                   '-commit' if mode == Mode.COMMIT else '',
                                                   trial)


def experiment_path(trial, incremental, mode):
    return "tuplex-{}-{}{}-ssd-{}.txt".format('incremental' if incremental else 'plain',
                                              'out-of-order' if mode == Mode.OUT_OF_ORDER else 'in-order',
                                              '-commit' if mode == Mode.COMMIT else '',
                                              trial)

def get_average_times(results_path, metric, num_trials, num_steps, incremental, mode):
    times = []
    for i in range(num_steps):
        total = 0
        for j in range(num_trials):
            total += get_metric(os.path.join(results_path, experiment_path(j + 1, incremental, mode)), metric, i)
        total /= num_trials
        times.append(total)
    return np.array(times)

def main():
    parser = argparse.ArgumentParser(description='Parse results of experiment')
    parser.add_argument('--results-path', type=str, dest='results_path', default='results_dirty_zillow@10G')
    parser.add_argument('--num-trials', type=int, dest='num_trials', default=1)
    parser.add_argument('--num-steps', type=int, dest='num_steps', default=7)
    parser.add_argument('--save-path', type=str, dest='save_path', default='graphs')
    args = parser.parse_args()

    results_path = args.results_path
    num_trials = args.num_trials
    num_steps = args.num_steps
    save_path = args.save_path

    if not os.path.isdir(save_path):
        os.makedirs(save_path)
    assert os.path.isdir(results_path)

    # for i in range(num_trials):
    #     for mode in Mode:
    #         validate_path = os.path.join(results_path, compare_path(i + 1, mode))
    #         assert validate_experiment(validate_path)


    # Total Times
    plain_times = get_average_times(results_path, 'jobTime', num_trials, num_steps, False, Mode.OUT_OF_ORDER)
    inc_times = get_average_times(results_path, 'jobTime', num_trials, num_steps, True, Mode.OUT_OF_ORDER)
    out_of_order_total(save_path, plain_times, inc_times)

    plain_times = get_average_times(results_path, 'jobTime', num_trials, num_steps, False, Mode.IN_ORDER)
    inc_times = get_average_times(results_path, 'jobTime', num_trials, num_steps, True, Mode.IN_ORDER)
    commit_times = get_average_times(results_path, 'jobTime', num_trials, num_steps, True, Mode.COMMIT)
    in_order_total(save_path, plain_times, inc_times, commit_times)

    # Time Break Down
    plain_fast = get_average_times(results_path, 'fast_path_time_s', num_trials, num_steps, False, Mode.OUT_OF_ORDER)
    plain_slow = get_average_times(results_path, 'slow_path_time_s', num_trials, num_steps, False, Mode.OUT_OF_ORDER)
    plain_write = get_average_times(results_path, 'write_output_wall_time_s', num_trials, num_steps, False, Mode.OUT_OF_ORDER)
    time_breakdown(save_path, 'out-of-order-plain-breakdown.png', plain_fast, plain_slow, plain_write)

    inc_fast = get_average_times(results_path, 'fast_path_time_s', num_trials, num_steps, True, Mode.OUT_OF_ORDER)
    inc_slow = get_average_times(results_path, 'slow_path_time_s', num_trials, num_steps, True, Mode.OUT_OF_ORDER)
    inc_write = get_average_times(results_path, 'write_output_wall_time_s', num_trials, num_steps, True, Mode.OUT_OF_ORDER)
    time_breakdown(save_path, 'out-of-order-incremental-breakdown.png', inc_fast, inc_slow, inc_write)

    plain_fast = get_average_times(results_path, 'fast_path_time_s', num_trials, num_steps, False, Mode.IN_ORDER)
    plain_slow = get_average_times(results_path, 'slow_path_time_s', num_trials, num_steps, False, Mode.IN_ORDER)
    plain_write = get_average_times(results_path, 'write_output_wall_time_s', num_trials, num_steps, False, Mode.IN_ORDER)
    time_breakdown(save_path, 'in-order-plain-breakdown.png', plain_fast, plain_slow, plain_write)

    inc_fast = get_average_times(results_path, 'fast_path_time_s', num_trials, num_steps, True, Mode.IN_ORDER)
    inc_slow = get_average_times(results_path, 'slow_path_time_s', num_trials, num_steps, True, Mode.IN_ORDER)
    inc_write = get_average_times(results_path, 'write_output_wall_time_s', num_trials, num_steps, True, Mode.IN_ORDER)
    time_breakdown(save_path, 'in-order-incremental-breakdown.png', inc_fast, inc_slow, inc_write)

    commit_fast = get_average_times(results_path, 'fast_path_time_s', num_trials, num_steps, True, Mode.COMMIT)
    commit_slow = get_average_times(results_path, 'slow_path_time_s', num_trials, num_steps, True, Mode.COMMIT)
    commit_write = get_average_times(results_path, 'write_output_wall_time_s', num_trials, num_steps, True, Mode.COMMIT)
    time_breakdown(save_path, 'in-order-commit-breakdown.png', commit_fast, commit_slow, commit_write)



if __name__ == '__main__':
    main()