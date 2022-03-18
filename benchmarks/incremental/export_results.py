import argparse
import os
import json

def validate_experiment(compare_path):
    with open(compare_path) as f:
        lines = f.read().splitlines()
        return ">>> contents of folders match." in lines

def job_times(path):
    times = []
    with open(path) as f:
        lines = f.read().splitlines()
        ind = lines.index("EXPERIMENTAL RESULTS") + 2
        lines = lines[ind:ind+7]
        for line in lines:
            times.append(json.loads(line)['jobTime'])
    return times

def compare_path(trial, in_order, commit, ssd):
    return "tuplex-compare-{}{}-{}-{}.txt".format('in-order' if in_order else 'out-of-order', '-commit' if commit else '', 'ssd' if ssd else 'hd', trial)

def experiment_path(trial, incremental, in_order, commit, ssd):
    return "tuplex-{}-{}{}-{}-{}.txt".format('incremental' if incremental else 'plain', 'in-order' if in_order else 'out-of-order', '-commit' if commit else '', 'ssd' if ssd else 'hd', trial)

def export_results(results_path, output_path, num_trials, in_order, commit, ssd):
    for i in range(num_trials):
        validate_path = os.path.join(results_path, compare_path(i + 1, in_order, commit, ssd))
        assert validate_experiment(validate_path)

    path = os.path.join(output_path, "experiment-{}{}-{}.csv".format('in-order' if in_order else 'out-of-order', '-commit' if commit else '', 'ssd' if ssd else 'hd'))
    f = open(path, 'w')

    header = "Resolvers,"
    for i in range(num_trials):
        header += f"Trial {i+1},"
    header += "Average,"
    for i in range(num_trials):
        header += f"Trial {i+1},"
    header += "Average\n"

    f.write(header)

    num_resolvers = 7
    for step in range(num_resolvers):
        line = f"{step},"

        plain_total = 0
        for trial in range(num_trials):
            plain_path = os.path.join(results_path, experiment_path(trial + 1, False, in_order, commit, ssd))
            plain_times = job_times(plain_path)

            plain_total += plain_times[step]
            line += f"{plain_times[step]},"
        line += f"{plain_total / num_trials},"

        incremental_total = 0
        for trial in range(num_trials):
            incremental_path = os.path.join(results_path, experiment_path(trial + 1, True, in_order, commit, ssd))
            incremental_times = job_times(incremental_path)

            incremental_total += incremental_times[step]
            line += f"{incremental_times[step]},"
        line += f"{incremental_total / num_trials}\n"
        f.write(line)

def main():
    parser = argparse.ArgumentParser(description='Parse results of experiment')
    parser.add_argument('--results-path', type=str, dest='results_path', default='results_dirty_zillow@10G')
    parser.add_argument('--output-path', type=str, dest='output_path', default='experiment_output')
    parser.add_argument('--num-trials', type=int, dest='num_trials', default=1)
    args = parser.parse_args()

    results_path = args.results_path
    output_path = args.output_path
    num_trials = args.num_trials

    assert os.path.isdir(results_path)
    if not os.path.isdir(output_path):
        os.makedirs(output_path)

    # In-Order SSD
    export_results(results_path, output_path, num_trials, True, False, True)
    #
    # # In-Order Commit SSD
    # export_results(results_path, output_path, num_trials, True, True, True)

    # Out-of-Order SSD
    export_results(results_path, output_path, num_trials, False, False, True)

    # In-Order HD
    export_results(results_path, output_path, num_trials, True, False, False)

    # Out-of-Order HD
    export_results(results_path, output_path, num_trials, False, False, False)

if __name__ == '__main__':
    main()