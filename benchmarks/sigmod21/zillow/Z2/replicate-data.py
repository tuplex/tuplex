#!/usr/bin/env python3

import argparse
import os
from tqdm import tqdm

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Zillow cleaning')
    parser.add_argument('-i', '--in', type=str, dest='input_path', default='data/zillow_clean.csv',
                        help='path or pattern to zillow data')
    parser.add_argument('-o', '--output-path', type=str, dest='output_path', default='data/zillow_clean@10G.csv',
                        help='specify path where to save output data files')
    parser.add_argument('-s', '--scale-factor', type=int, dest='scale_factor', default=1869, help='how many times to replicate file')
    parser.add_argument('--include-header', action='store_true', dest='include_header',
                        help='whether to explicitly include the first line or not when replicating')
    args = parser.parse_args()

    assert args.input_path, 'need to set input data path!'
    assert args.output_path, 'need to set output data path!'

    args.scale_factor = int(max(1, args.scale_factor)) # no fractional support yet

    print('>>> reading input file')
    with open(args.input_path, 'r') as fp:
        lines = fp.readlines()

    print('>>> replicating data {}x'.format(args.scale_factor))
    with open(args.output_path, 'w') as fp:
        # write lines as is
        fp.writelines(lines)

        if not args.include_header:
            lines = lines[1:]
        for n in tqdm(range(args.scale_factor)):
            fp.writelines(lines)

    print('done.')