# condition on a) type, b) length, c) uniqueness of elements
import argparse
import pickle
import random
import pdb
import string
import os

def randint():
    return random.randint(0, 1e9)

def randfloat():
    return random.random()

def randstring():
    # use dollar to prevent string interning
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=random.randint(10, 50))) + '$'

def randlist(length, types, unique):
    result = []
    mydict = {}

    for i in range(length):
        currtype = random.randint(0, len(types) - 1)
        newval = globals()[f"rand{types[currtype]}"]()
        if unique:
            while newval in mydict:
                newval = globals()[f"rand{types[currtype]}"]()
            mydict[newval] = True
        result.append(newval)
    return result

valid_types = ['string', 'float', 'int']

def main():
    parser = argparse.ArgumentParser(description='Generate lists for count unique.')
    parser.add_argument('--length', type=int)
    parser.add_argument('--types', type=str, nargs='+')
    parser.add_argument('--unique', dest='unique', action='store_true')
    args = parser.parse_args()

    if args.length <= 0:
        print('expected length >= 0')
        exit(0)
    
    for x in args.types:
        if x not in valid_types:
            print(f'invalid type: {x}, expected one of: string float int')
            exit(0)
    
    result = randlist(args.length, args.types, args.unique)
    filename = f'{args.length}_{"".join(args.types)}_{args.unique}'

    with open(filename, 'wb') as f:
        pickle.dump(result, f)

    print(filename)

main()