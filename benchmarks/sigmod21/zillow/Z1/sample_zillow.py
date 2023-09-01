#!/usr/bin/env python3
# (c) L.Spiegelberg 2019

# generates test files for Zillow use case
# by upsampling zillow data
import sys
import random
import numpy as np
from tqdm import tqdm
import pickle
import itertools

def print_usage():
    if len(sys.argv) != 3:
        print('usage: python3 {} <out_path> <out_size_in_bytes>'.format(sys.argv[0]))
        exit(1)


def sample(outfile, outsize):
    random.seed(42)

    # open original file to sample from & pickle file to exclude lines from...
    with open('data/zillow_dirty_noexc.csv', 'r') as f:
        lines = f.readlines()
    with open('data/badidxs.pkl', 'rb') as fp:
        bad_indices = pickle.load(fp)
    header = lines[0]
    lines = lines[1:]

    good_indices = set(range(len(lines))) - set(bad_indices)
    lines = list(np.array(lines)[list(good_indices)])

    bytes_written = 0

    c_lines = itertools.cycle(lines)

    with tqdm(total=outsize, unit=' bytes') as pbar:
        with open(outfile, 'w') as f:
            f.write(header)
            bytes_written += len(header)

            while bytes_written < outsize:
                rand_line = next(c_lines)#random.choice(lines)
                f.write(rand_line)
                bytes_written += len(rand_line)
                pbar.update(len(rand_line))
    print('wrote {} bytes to {}'.format(bytes_written, outfile))



if __name__ == '__main__':
    print_usage()


    # sample file
    sample(sys.argv[1], int(sys.argv[2]))
