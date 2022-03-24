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


def job_times(path):
    times = []
    with open(path) as f:
        lines = f.read().splitlines()
        ind = lines.index("EXPERIMENTAL RESULTS") + 2
        lines = lines[ind:ind + 7]
        for line in lines:
            times.append(json.loads(line)['jobTime'])
    return times


def compare_path(trial, mode):
    return "tuplex-compare-{}{}-ssd-{}.txt".format('out-of-order' if mode == Mode.OUT_OF_ORDER else 'in-order',
                                                   '-commit' if mode == Mode.COMMIT else '',
                                                   trial)


def experiment_path(trial, incremental, mode):
    return "tuplex-{}-{}{}-ssd-{}.txt".format('incremental' if incremental else 'plain',
                                              'out-of-order' if mode == Mode.OUT_OF_ORDER else 'in-order',
                                              '-commit' if mode == Mode.COMMIT else '',
                                              trial)

def write_total_time_to_file(f, results_path, num_trials, num_steps, mode):
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
            plain_times = job_times(plain_path)

            plain_total += plain_times[step]
            line += f"{plain_times[step]},"
        line += f"{plain_total / num_trials},"

        incremental_total = 0
        for trial in range(num_trials):
            incremental_path = os.path.join(results_path, experiment_path(trial + 1, True, Mode.IN_ORDER))
            incremental_times = job_times(incremental_path)

            incremental_total += incremental_times[step]
            line += f"{incremental_times[step]},"
        line += f"{incremental_total / num_trials}"

        if mode == Mode.IN_ORDER:
            line += ","
            commit_total = 0
            for trial in range(num_trials):
                commit_path = os.path.join(results_path, experiment_path(trial + 1, True, Mode.COMMIT))
                commit_times = job_times(commit_path)

                commit_total += commit_times[step]
                line += f"{commit_times[step]},"
            line += f"{commit_total / num_trials}\n"
        else:
            line += "\n"

        f.write(line)

def export_total_time_experiment(results_path, num_trials, num_steps):
    # Validate all experiments
    for i in range(num_trials):
        for mode in Mode:
            validate_path = os.path.join(results_path, compare_path(i + 1, mode))
            assert validate_experiment(validate_path)

    file_path = "total_time_experiment.csv"
    with open(file_path, 'w') as f:
        write_total_time_to_file(f, results_path, num_trials, num_steps, Mode.OUT_OF_ORDER)
        write_total_time_to_file(f, results_path, num_trials, num_steps, Mode.IN_ORDER)

# def export_results(results_path, output_path, num_trials, in_order, ssd):
#     for i in range(num_trials):
#         validate_path = os.path.join(results_path, compare_path(i + 1, in_order, False, ssd))
#         assert validate_experiment(validate_path)
#         if in_order:
#             validate_path = os.path.join(results_path, compare_path(i + 1, in_order, True, ssd))
#             assert validate_experiment(validate_path)
#
#     path = "experiments.csv"
#     f = open(path, 'a')
#
#     header = "{} | {},".format('In Order' if in_order else 'Out of Order',
#                                'SSD' if ssd else 'HD') + "," * num_trials + "Plain," + "," * num_trials + "Incremental" + (
#                  "," * (num_trials + 1) + "Commit\n" if in_order else "\n")
#     f.write(header)
#
#     header = "Resolvers,"
#     for i in range(num_trials):
#         header += f"Trial {i + 1},"
#     header += "Average,"
#     for i in range(num_trials):
#         header += f"Trial {i + 1},"
#     header += "Average"
#     if in_order:
#         header += ","
#         for i in range(num_trials):
#             header += f"Trial {i + 1},"
#         header += "Average\n"
#     else:
#         header += "\n"
#
#     f.write(header)
#
#     num_resolvers = 7
#     for step in range(num_resolvers):
#         line = f"{step},"
#
#         plain_total = 0
#         for trial in range(num_trials):
#             plain_path = os.path.join(results_path, experiment_path(trial + 1, False, in_order, False, ssd))
#             plain_times = job_times(plain_path)
#
#             plain_total += plain_times[step]
#             line += f"{plain_times[step]},"
#         line += f"{plain_total / num_trials},"
#
#         incremental_total = 0
#         for trial in range(num_trials):
#             incremental_path = os.path.join(results_path, experiment_path(trial + 1, True, in_order, False, ssd))
#             incremental_times = job_times(incremental_path)
#
#             incremental_total += incremental_times[step]
#             line += f"{incremental_times[step]},"
#         line += f"{incremental_total / num_trials}"
#
#         if in_order:
#             line += ","
#             commit_total = 0
#             for trial in range(num_trials):
#                 commit_path = os.path.join(results_path, experiment_path(trial + 1, True, in_order, True, ssd))
#                 commit_times = job_times(commit_path)
#
#                 commit_total += commit_times[step]
#                 line += f"{commit_times[step]},"
#             line += f"{commit_total / num_trials}\n"
#         else:
#             line += "\n"
#
#         f.write(line)
#     f.write("\n")


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

    export_total_time_experiment(results_path, num_trials, num_steps)

    # export_total_time(results_path, num_trials)
    # # Out-of-Order SSD
    # export_results(results_path, output_path, num_trials, False, True)
    #
    # # In-Order SSD
    # export_results(results_path, output_path, num_trials, True, True)

    # Out-of-Order HD
    # export_results(results_path, output_path, num_trials, False, False)

    # # In-Order HD
    # export_results(results_path, output_path, num_trials, True, False)


if __name__ == '__main__':
    main()
