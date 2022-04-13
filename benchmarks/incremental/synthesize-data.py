import argparse
import random

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

    with open(output_path, 'w') as fp:
        bytes_written = 0
        while bytes_written < dataset_size:
            row = f"{0 if random.choices([True, False], weights=(exceptions, 1 - exceptions))[0] else 1},{'a' * (row_size // 8 - 1)}\n"
            bytes_written += fp.write(row)

if __name__ == '__main__':
    main()