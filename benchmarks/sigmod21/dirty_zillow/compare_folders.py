#!/usr/bin/env python3
# (c) L.Spiegelberg 2021
# compare the csv output contents of two folders (ignoring order)

import os
import sys
import argparse
import glob


def wc_files(path):
    files = sorted(glob.glob(os.path.join(path, '*.csv')))

    all_lines = []
    num_rows = 0
    header = None
    matching_headers = 0
    for f in files:
        with open(f, 'r') as fp:
            lines = fp.readlines()
            if header is None and len(lines) > 0:
                header = lines[0]
            num_rows += len(lines)
            if len(lines) > 0:
                if header == lines[0]:
                    matching_headers += 1
                    all_lines += lines[1:]
                else:
                    all_lines += lines

    if matching_headers == len(files):
        num_rows -= matching_headers

    print('-- counted {} rows in {} files in folder {}'.format(num_rows, len(files), path))
    return num_rows, len(files), all_lines


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("folderA")
    parser.add_argument("folderB")
    args = parser.parse_args()

    print('== Dirty Zillow experiment validation ==')

    # count lines in each folder
    print('-- loading folder contents...')
    rowCountA, filesA, rowsA  = wc_files(args.folderA)
    rowCountB, filesB, rowsB = wc_files(args.folderB)

    if rowCountA != rowCountB:
        print('>>> number of rows does not match')
        sys.exit(1)

    # sort lines and compare them
    print('-- sorting rows from {}'.format(args.folderA))
    rowsA = sorted(rowsA)
    print('-- sorting rows from {}'.format(args.folderB))
    rowsB = sorted(rowsB)
    print('-- computing comparison of rows...')
    non_matching_indices = [i for i, j in zip(rowsA, rowsB) if i != j]

    if len(non_matching_indices) > 0:
        print('>>> rows do not match up, details:')

        for idx in non_matching_indices:
            print('{:5d}: {} != {}'.format(idx, rowsA[idx], rowsB[idx]))
        sys.exit(1)

    print('>>> contents of folders match.')

    sys.exit(0)


if __name__ == '__main__':
    main()
