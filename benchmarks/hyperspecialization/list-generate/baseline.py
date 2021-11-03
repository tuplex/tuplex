import argparse
import time
import pickle
import csv
import pdb

def simple_func(simple_list):
    my_dict = {}
    for element in simple_list:
        if element in my_dict:
            my_dict[element] += 1
        else:
            my_dict[element] = 0
    return my_dict

def simple_func_for_loop(list_of_lists):
    tstart = time.time_ns()
    result = []
    for my_list in list_of_lists:
        result.append(simple_func(my_list))
    duration = time.time_ns() - tstart
    print(duration / 1_000_000)
    return result

def simple_func_list_comprehension(list_of_lists):
    tstart = time.time_ns()
    result = [simple_func(my_list) for my_list in list_of_lists]
    duration = time.time_ns() - tstart
    print(duration / 1_000_000)
    return result

def simple_func_map(list_of_lists):
    tstart = time.time_ns()
    result = list(map(simple_func, list_of_lists))
    duration = time.time_ns() - tstart
    print(duration / 1_000_000)
    return result

def readlist_pickle(filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)

def readlist_csv(filename):
    list_of_lists = []
    with open(filename, newline='') as f:
        list_of_lists = list(csv.reader(f))
    print(len([len(li) for li in list_of_lists]), [len(li) for li in list_of_lists])
    return list_of_lists

def main():
    parser = argparse.ArgumentParser(description='Run baseline implementation of count unique.')
    parser.add_argument('--filename', type=str)
    args = parser.parse_args()

    randlist = readlist_csv(args.filename)
    # pdb.set_trace()
    # print(randlist)

    simple_func_for_loop(randlist)
    # simple_func_for_loop(randlist)

    # simple_func_list_comprehension(randlist)


if __name__ == "__main__":
    main()