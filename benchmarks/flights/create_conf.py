#!/usr/bin/env python3
# (c) 2020 L.Spiegelberg
# this script creates Tuplex json configuration files for benchmarks

import json
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--executor-memory', default='6G', help='how much memory each thread gets')
    parser.add_argument('--executor-count', default=15, help='how many worker threads')
    parser.add_argument('--partition-size', default='32MB', help='task size')
    parser.add_argument('--runtime-memory', default='64MB', help='how much maximum runtime memory to use')
    parser.add_argument('--input-split-size', default='64MB', help='chunk size of input files')
    parser.add_argument('--opt-null', help='enable null value optimization', action='store_true')
    parser.add_argument('--opt-pushdown', help='enable projection pushdown', action='store_true')
    parser.add_argument('--opt-filter', help='enable filter pushdown', action='store_true')
    parser.add_argument('--opt-parser', help='generate CSV parser', action='store_true')
    parser.add_argument('--opt-llvm', help='run llvm optimizers', action='store_true')

    args = parser.parse_args()

    conf = {'webui.enable' : False,
            'executorMemory' : args.executor_memory,
            'executorCount' : args.executor_count,
            'driverMemory' : args.executor_memory,
            'partitionSize' : args.partition_size,
            'runTimeMemory' : args.runtime_memory,
            'inputSplitSize' : args.input_split_size,
            'useLLVMOptimizer' : args.opt_llvm,
            'optimizer.nullValueOptimization' : args.opt_null,
            'csv.selectionPushdown' : args.opt_pushdown,
            'optimizer.generateParser' : args.opt_parser,
            'optimizer.mergeExceptionsInOrder' : False,
            'optimizer.filterPushdown' : args.opt_filter,
            'scratchDir': '/results/scratch'}

    print(json.dumps(conf))