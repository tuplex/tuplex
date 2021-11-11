import count_unique
import time
import csv
import argparse

def readlist_csv(filename):
    list_of_lists = []
    with open(filename, newline='') as f:
        list_of_lists = list(csv.reader(f))
    # print(len([len(li) for li in list_of_lists]), [len(li) for li in list_of_lists])
    return list_of_lists

parser = argparse.ArgumentParser(description='Run baseline implementation of count unique.')
parser.add_argument('--filename', type=str)
args = parser.parse_args()

randlist = readlist_csv(args.filename)

count_unique.count_unique(randlist)