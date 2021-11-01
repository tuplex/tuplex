import argparse
import time
import generate_data


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

def main():
    randlist = generate_data.randlist(10, ['int', 'string', 'float'], True)
    print(simple_func(randlist))
    # read file and then call run run_simple_func
    # pass

main()