import argparse
import os.path
import json

class Experiment:
    def __init__(self, results_path, num_trials, num_steps, save_path):
        self.results_path = results_path
        self.num_trials = num_trials
        self.num_steps = num_steps
        self.save_path = save_path

    def get_path(self, out_of_order, mode, step, trial):
        filename = f"{mode}-{'out-of-order' if out_of_order else 'in-order'}-e{step}-t{trial}.txt"
        return os.path.join(self.results_path, filename)

    def get_results(self, out_of_order, mode):
        results = []
        for step in range(self.num_steps):
            step_results = []
            for trial in range(self.num_trials):
                path = self.get_path(out_of_order, mode, step, trial + 1)
                step_results.append(self.get_metric(path))
            results.append(sum(step_results) / len(step_results))
        return results

    def get_metric(self, path):
        with open(path, 'r') as fp:
            lines = fp.read().splitlines()
            ind = lines.index("EXPERIMENTAL RESULTS") + 1
            line = lines[ind]
            metrics = json.loads(line)
            return metrics['totalRunTime']

def main():
    parser = argparse.ArgumentParser(description='Graph results of synthetic experiment')
    parser.add_argument('--results-path', type=str, dest='results_path', default='results_synthetic')
    parser.add_argument('--num-trials', type=str, dest='num_trials', default=1)
    parser.add_argument('--num-steps', type=str, dest='num_steps', default=11)
    parser.add_argument('--save-path', type=str, dest='save_path', default='graphs-synthetic')
    args = parser.parse_args()

    results_path = args.results_path
    num_trials = args.num_trials
    num_steps = args.num_steps
    save_path = args.save_path

    if not os.path.isdir(save_path):
        os.makedirs(save_path)
    assert os.path.isdir(results_path)

    e = Experiment(results_path, num_trials, num_steps, save_path)
    print(e.get_results(True, 'plain'))
    print(e.get_results(True, 'incremental'))


if __name__ == '__main__':
    main()