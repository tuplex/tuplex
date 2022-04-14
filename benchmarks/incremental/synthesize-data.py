import argparse
import math
import random
from tqdm import tqdm

def generate_data(num_rows, row_size, exceptions):
    num_exceptions = int(exceptions * num_rows)
    exps = set(random.sample(range(num_rows), num_exceptions))

    padding = 'a' * (row_size - 3)
    norm_row = "1," + padding + "\n"
    exp_row = "0," + padding + "\n"

    data = []
    for i in range(num_rows):
        if i in exps:
            data.append(exp_row)
        else:
            data.append(norm_row)
    return data

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
    num_sample_rows = min(num_rows, 100000)

    print(num_rows)
    print(num_sample_rows)
    print(num_rows // num_sample_rows)

    data = generate_data(num_sample_rows, row_size, exceptions)

    with open(output_path, 'w') as fp:
        header = "a,b\n"
        fp.write(header)

        for _ in tqdm(range(math.ceil(num_rows // num_sample_rows))):
            fp.writelines(data)

if __name__ == '__main__':
    main()