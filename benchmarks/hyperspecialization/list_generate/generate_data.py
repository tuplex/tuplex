# condition on a) type, b) length, c) uniqueness of elements
import argparse
import pickle
import random
import pdb
import string
import os
import csv
import numpy as np
import scipy.stats as ss
from tqdm import tqdm


def get_list_length():
    return int(1e3)

MIN_INT = 0
MAX_INT = 1000

def randint(dist):
    if dist == 'uniform':
        return random.randint(MIN_INT, MAX_INT)
    if dist == 'binomial':
        PVAL = 0.5
        return int(np.random.binomial(MAX_INT - MIN_INT + 1, PVAL) - (MAX_INT - MIN_INT + 1) * PVAL)        

    print("invalid distribution supplied for randint")
    print("valid distributions: uniform")
    exit(0)


def randfloat(dist):
    if dist == 'uniform':
        return random.random()

    print("invalid distribution supplied for randfloat")
    print("valid distributions: uniform")
    exit(0)

def randstring(dist, seed=0):
    # use dollar to prevent string interning
    if dist == 'uniform':
        MIN_LENGTH = 10
        MAX_LENGTH = 10
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=random.randint(MIN_LENGTH, MAX_LENGTH))) + '$'
    if dist == 'binomialchar':
        LENGTH = 10
        PVAL = 0.5
        allchars = list(string.ascii_uppercase + string.digits)
        return ''.join([allchars[np.random.binomial(len(allchars) - 1, PVAL)] for _ in range(LENGTH)]) + '$'
    if dist == 'binomialbag':
        PVAL = 0.1
        return bag[np.random.binomial(BAGLEN - 1, PVAL)]
    
    print("invalid distribution supplied for randstring")
    print("valid distributions: uniform, binomialchar, binomialbag")
    exit(0)

def randlist(types, distribution_dict):
    result = []
    length = get_list_length() # list length
    for _ in range(length):
        currtype = random.randint(0, len(types) - 1)
        newval = globals()[f"rand{types[currtype]}"](distribution_dict[types[currtype]])
        result.append(newval)
    return result

valid_types = ['string', 'float', 'int']

# normal 'bag' based string generation preprocessing.
BAGLEN = 1000 # decrease to add duplicates        
BAGSTDDEV = 5
bag = [randstring('uniform') for _ in range(BAGLEN)]

def main():
    parser = argparse.ArgumentParser(description='Generate lists for count unique.')
    parser.add_argument('--num_lists', type=int)
    parser.add_argument('--types', type=str, nargs='+')
    parser.add_argument('--distributions', nargs='+')
    parser.add_argument('--max_int', type=int)
    
    args = parser.parse_args()

    if len(args.distributions) != len(args.types):
        print('please specify a distribution for each type')
        exit(0)

    if args.max_int:
        print(f'set max_int to {args.max_int}')
        MAX_INT = args.max_int

    distribution_dict = {}
    for idx, arg in enumerate(args.distributions):
        distribution_dict[args.types[idx]] = arg

    for x in args.types:
        if x not in valid_types:
            print(f'invalid type: {x}, expected one of: string float int')
            exit(0)
    
    dist_str = ''.join([name for name in args.distributions])
    filename = f'{args.num_lists}_of_{get_list_length()}_{"".join(args.types)}_{dist_str}_({MIN_INT}_to_{MAX_INT}).csv'
    print(f'args.num_lists = {args.num_lists}')

    with open(filename, 'w') as f:
        wr = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        for i in tqdm(range(args.num_lists)):
            csv_row = randlist(args.types, distribution_dict)
            wr.writerow(csv_row)
    
    print(filename)

    print('successfully written!')

if __name__ == "__main__":
    main()