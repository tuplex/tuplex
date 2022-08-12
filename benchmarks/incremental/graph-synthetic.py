import argparse
import os.path
import json
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

PLAIN_COLOR = "#4285F4"
INCREMENTAL_COLOR = '#DB4437'
COMMIT_COLOR = "#F4B400"

class Experiment:
    def __init__(self, results_path, num_trials, num_steps, save_path):
        self.results_path = results_path
        self.num_trials = num_trials
        self.num_steps = num_steps
        self.save_path = save_path

    def graph_in_order(self):
        plain_results = self.get_results(False, 'plain')
        inc_results = self.get_results(False, 'incremental')
        commit_results = self.get_results(False, 'commit')

        fig = plt.figure(figsize=(6, 4))

        plt.plot(plain_results, marker='o', color=PLAIN_COLOR)
        plt.plot(inc_results, marker='o', color=INCREMENTAL_COLOR)
        plt.plot(commit_results, marker='o', color=COMMIT_COLOR)
        # plt.ylim(0, 110)

        plt.ylabel('Total Execution Time (s)')
        plt.xlabel('Amount of Exceptions')
        labels = ['0', '0.1', '0.2', '0.3', '0.4', '0.5', '0.6', '0.7', '0.8', '0.9', '1.0']
        x = np.arange(len(labels))
        plt.xticks(x, labels)

        plt.title('In Order | Synthetic')
        plt.legend(handles=[
            mpatches.Patch(color=PLAIN_COLOR, label='Plain'),
            mpatches.Patch(color=INCREMENTAL_COLOR, label='Incremental'),
            mpatches.Patch(color=COMMIT_COLOR, label='Commit')
        ], loc='lower right')

        fig.savefig(os.path.join(self.save_path, 'in-order-synth.png'), dpi=400, bbox_inches='tight')

    def graph_out_of_order(self):
        plain_results = self.get_results(True, 'plain')
        inc_results = self.get_results(True, 'incremental')

        fig = plt.figure(figsize=(6, 4))

        plt.plot(plain_results, marker='o', color=PLAIN_COLOR)
        plt.plot(inc_results, marker='o', color=INCREMENTAL_COLOR)
        # plt.ylim(0, 110)

        plt.ylabel('Total Execution Time (s)')
        plt.xlabel('Amount of Exceptions')
        labels = ['0', '0.1', '0.2', '0.3', '0.4', '0.5', '0.6', '0.7', '0.8', '0.9', '1.0']
        x = np.arange(len(labels))
        plt.xticks(x, labels)

        plt.title('Out of Order | Synthetic')
        plt.legend(handles=[
            mpatches.Patch(color=PLAIN_COLOR, label='Plain'),
            mpatches.Patch(color=INCREMENTAL_COLOR, label='Incremental'),
        ], loc='upper right')

        fig.savefig(os.path.join(self.save_path, 'out-of-order-synth.png'), dpi=400, bbox_inches='tight')

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
        return np.array(results)

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
    parser.add_argument('--num-trials', type=int, dest='num_trials', default=1)
    parser.add_argument('--num-steps', type=int, dest='num_steps', default=11)
    parser.add_argument('--save-path', type=str, dest='save_path', default='graphs-synthetic')
    args = parser.parse_args()

    results_path = args.results_path
    num_trials = args.num_trials
    num_steps = args.num_steps
    save_path = args.save_path

    if not os.path.isdir(save_path):
        os.makedirs(save_path)
    assert os.path.isdir(results_path)

    params = {'font.family': 'Times',
              'legend.fontsize': 'medium',
              'axes.labelsize': 'medium',
              'axes.titlesize': 'medium'}
    plt.rcParams.update(params)

    e = Experiment(results_path, num_trials, num_steps, save_path)
    e.graph_out_of_order()
    e.graph_in_order()

if __name__ == '__main__':
    main()