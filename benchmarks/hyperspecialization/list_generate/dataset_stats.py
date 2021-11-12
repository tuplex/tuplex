import csv
import numpy as np
import scipy.stats
import matplotlib.pyplot as plt
import pandas as pd
import argparse
import gc

def readlist_csv(filename):
    list_of_lists = []
    with open(filename, newline='') as f:
        list_of_lists = list(csv.reader(f))
    return list_of_lists

def count_unique(simple_list):
    my_dict = {}
    for element in simple_list:
        if element in my_dict:
            my_dict[element] += 1
        else:
            my_dict[element] = 0
    return my_dict


def main():
    parser = argparse.ArgumentParser(description='Run baseline implementation of count unique.')
    parser.add_argument('--filename', type=str)
    args = parser.parse_args()
    randlist = readlist_csv(args.filename)

    flat_list = [item for sublist in randlist for item in sublist]
    arr = np.array(flat_list)

    df = pd.DataFrame(arr)
    print(df.describe())

    # unique_count = count_unique(flat_list)
    # print(scipy.stats.describe(arr))

    # print('plotting')

    # plt.hist(arr, bins='auto', histtype='step')
    # plt.show()
    # plt.savefig(f'{args.filename}.png')

if __name__ == "__main__":
    main()
    gc.collect()
    