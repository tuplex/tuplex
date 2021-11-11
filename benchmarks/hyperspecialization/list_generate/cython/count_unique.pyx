import time

def count_unique(list simple_list):
    cdef int element
    my_dict = {}
    for element in simple_list:
        if element in my_dict:
            my_dict[element] += 1
        else:
            my_dict[element] = 0
    return my_dict

def count_unique_lists(list list_of_lists):
    tstart = time.time_ns()
    cdef list result = [count_unique(my_list) for my_list in list_of_lists]
    duration = time.time_ns() - tstart
    print(duration / 1_000_000)
    return result
