import argparse
import random
from tqdm import tqdm

def main():
    parser = argparse.ArgumentParser(description='Synthesize data')
    parser.add_argument('--row-size', type=int, dest='row_size', default=200, help='number of bytes per row')
    parser.add_argument('--exceptions', type=float, dest='exceptions', default=0.25, help='amount of exception rows in dataset')
    parser.add_argument('--dataset-size', type=int, dest='dataset_size', default=10, help='number of megabytes in dataset')
    parser.add_argument('--output-path', type=str, dest='output_path', default='synthetic.csv', help='path to output the file')
    args = parser.parse_args()

    row_size = args.row_size
    exceptions = args.exceptions
    dataset_size = args.dataset_size * 1000000
    output_path = args.output_path

    num_rows = dataset_size // row_size
    num_exceptions = int(exceptions * num_rows)

    exps = set(random.sample(range(num_rows), num_exceptions))

    with open(output_path, 'w') as fp:
        header = "a,b\n"
        fp.write(header)

        padding = 'a' * (row_size - 3)
        norm_row = "1," + padding + "\n"
        exp_row = "0," + padding + "\n"
        for i in tqdm(range(num_rows)):
            if i in exps:
                fp.write(exp_row)
            else:
                fp.write(norm_row)

if __name__ == '__main__':
    main()