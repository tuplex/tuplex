#!/usr/bin/env python3

import argparse
import os
import itertools
from tqdm import tqdm

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Zillow cleaning, create synthetic dataset or clean data with rows which get written to output eventually.')
    parser.add_argument('-i', '--in', type=str, dest='input_path', default='data/zillow_dirty.csv',
                        help='path or pattern to zillow data')
    parser.add_argument('-j', '--indices', type=str, dest='idx_path', default='data/exception_rows_indices.csv', help='file to indices where to replace rows')
    parser.add_argument('-r', '--resolved-indices', type=str, dest='resolved_idx_path', default='data/exception_rows_resolved_indices.csv', help='file to indices which to use when replacing rows')
    parser.add_argument('-o', '--output-path', type=str, dest='output_path', default='data/zillow_dirty_synthetic@10G.csv',
                        help='specify path where to save output data files')
    parser.add_argument('-s', '--scale-factor', type=int, dest='scale_factor', default=1460, help='how many times to replicate file')
    parser.add_argument('--include-header', action='store_true', dest='include_header', help='whether to explicitly include the first line or not when replicating')
    parser.add_argument('-m', '--mode',  type=str,
                        dest="mode",
                        choices=["synth", "clean"],
                        default="synth",
                        help="whether to create a synthetic dataset or the clean one")
    args = parser.parse_args()

    assert args.input_path, 'need to set input data path!'
    assert args.output_path, 'need to set output data path!'

    args.scale_factor = int(max(1, args.scale_factor)) # no fractional support yet

    print('>>> reading input files')
    with open(args.input_path, 'r') as fp:
        lines = fp.readlines()

    with open(args.idx_path, 'r') as fp:
        except_indices = fp.readlines()
        except_indices = except_indices[1:]
        except_indices = list(map(int, except_indices))
    with open(args.resolved_idx_path, 'r') as fp:
        except_resolved_indices = fp.readlines()
        except_resolved_indices = except_resolved_indices[1:]
        except_resolved_indices = list(map(int, except_resolved_indices))

    header = lines[0]
    lines = lines[1:]

    # substitute lines with rows that get written to output completely
    # indices are extracted from flowchart notebook, hard coded here
    if args.mode == 'synth':
        print('>>> substituting lines')
        c = itertools.cycle(except_resolved_indices)

        for idx in except_indices:
            lines[idx] = lines[next(c)]
    if args.mode == 'clean':
        print('>>> remove rows causing exceptions')
        exclude_indices = set(except_indices) | set(except_resolved_indices)
        before_len = len(lines)
        lines = list(map(lambda t: t[1], filter(lambda t: t[0] not in exclude_indices, enumerate(lines))))
        assert len(lines) + len(exclude_indices) == before_len, 'lengths not matching'

    print('>>> replicating data {}x'.format(args.scale_factor))
    with open(args.output_path, 'w') as fp:
        # write header
        fp.write(header)

        for n in tqdm(range(args.scale_factor)):
            fp.writelines(lines)

    print('done.')