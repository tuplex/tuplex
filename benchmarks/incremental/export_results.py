import argparse
import os
import json
from enum import Enum

class Mode(Enum):
    OUT_OF_ORDER = 1
    IN_ORDER = 2
    COMMIT = 3

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

def write_metric_to_file(f, results_path, num_trials, num_steps, mode, metric):
    header = "{},".format("Out of Order" if mode == Mode.OUT_OF_ORDER else "In Order") + \
             "," * num_trials + "Plain," + \
             "," * num_trials + "Incremental" + \
             ("," * (num_trials + 1) + "Commit\n" if mode == Mode.IN_ORDER else "\n")
    f.write(header)

    header = "Resolvers," + \
             ','.join(["Trial {}".format(i + 1) for i in range(num_trials)]) + \
             ",Average," + \
             ','.join(["Trial {}".format(i + 1) for i in range(num_trials)]) + \
             ",Average" + \
             ("," + ','.join(["Trial {}".format(i + 1) for i in range(num_trials)]) + ",Average\n" if mode == Mode.IN_ORDER else "\n")
    f.write(header)

    for step in range(num_steps):
        line = f"{step},"

        plain_total = 0
        for trial in range(num_trials):
            plain_path = os.path.join(results_path, experiment_path(trial + 1, False, Mode.OUT_OF_ORDER))
            plain_time = get_metric(plain_path, metric, step)

            plain_total += plain_time
            line += f"{plain_time},"
        line += f"{plain_total / num_trials},"

        incremental_total = 0
        for trial in range(num_trials):
            incremental_path = os.path.join(results_path, experiment_path(trial + 1, True, Mode.IN_ORDER))
            incremental_time = get_metric(incremental_path, metric, step)

            incremental_total += incremental_time
            line += f"{incremental_time},"
        line += f"{incremental_total / num_trials}"

        if mode == Mode.IN_ORDER:
            line += ","
            commit_total = 0
            for trial in range(num_trials):
                commit_path = os.path.join(results_path, experiment_path(trial + 1, True, Mode.COMMIT))
                commit_time = get_metric(commit_path, metric, step)

                commit_total += commit_time
                line += f"{commit_time},"
            line += f"{commit_total / num_trials}\n"
        else:
            line += "\n"

        f.write(line)

def export_experiments(results_path, num_trials, num_steps):
    # Validate all experiments
    # for i in range(num_trials):
    #     for mode in Mode:
    #         validate_path = os.path.join(results_path, compare_path(i + 1, mode))
    #         assert validate_experiment(validate_path)

    metrics = ['jobTime', 'fast_path_time_s', 'slow_path_time_s', 'write_output_wall_time_s']

    file_path = "experiments.csv"
    with open(file_path, 'w') as f:
        for metric in metrics:
            write_metric_to_file(f, results_path, num_trials, num_steps, Mode.OUT_OF_ORDER, metric)
            write_metric_to_file(f, results_path, num_trials, num_steps, Mode.IN_ORDER, metric)

def main():
    parser = argparse.ArgumentParser(description='Parse results of experiment')
    parser.add_argument('--results-path', type=str, dest='results_path', default='results_dirty_zillow@10G')
    parser.add_argument('--num-trials', type=int, dest='num_trials', default=1,)
    parser.add_argument('--num-steps', type=int, dest='num_steps', default=7)
    args = parser.parse_args()

    results_path = args.results_path
    num_trials = args.num_trials
    num_steps = args.num_steps

    assert os.path.isdir(results_path)

    export_experiments(results_path, num_trials, num_steps)

if __name__ == '__main__':
    main()
