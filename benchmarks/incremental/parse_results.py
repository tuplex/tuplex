import argparse
import os


def validate_experiment(compare_path):
    with open(compare_path) as f:
        lines = f.readlines()
        return ">>> contents of folders match.\n" in lines

def main():
    parser = argparse.ArgumentParser(description='Parse results of experiment')
    parser.add_argument('--results-path', type=str, dest='results_path', default='results_dirty_zillow@10G')
    args = parser.parse_args()

    print(validate_experiment(os.path.join(args.results_path, "tuplex-compare-in-order-ssd-1.txt")))
    print(validate_experiment(os.path.join(args.results_path, "tuplex-compare-ssd-1.txt")))

if __name__ == '__main__':
    main()