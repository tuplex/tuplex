import argparse
import time
import pickle


def simple_func(simple_list):
    my_dict = {}
    for element in simple_list:
        if element in my_dict:
            my_dict[element] += 1
        else:
            my_dict[element] = 0

    return my_dict

def run_simple_func(list_of_lists):

    tstart = time.time()
    result = []
    for my_list in list_of_lists:
        result.append(simple_func(my_list))
    
    duration = time.time() - tstart
    print(duration)
    return result

def readlist(filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)

def main():
    parser = argparse.ArgumentParser(description='Run baseline implementation of count unique.')
    parser.add_argument('--filename', type=str)
    args = parser.parse_args()

    randlist = readlist(args.filename)
    print(simple_func(randlist))

main()