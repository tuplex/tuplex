import tuplex
import time
import sys
import json
import os
import glob
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Write to csv file format')
    parser.add_argument('--path', type=str, dest='data_path', default='data/large100MB.csv',
                        help='path or pattern to zillow data')
    parser.add_argument('--output-path', type=str, dest='output_path', default='tuplex_output/',
                        help='specify path where to save output data files')
    args = parser.parse_args()

    assert args.data_path, 'need to set data path!'

    # config vars
    paths = [args.data_path]
    output_path = args.output_path

    # explicit globbing because dask can't handle patterns well...
    if not os.path.isfile(args.data_path):
        paths = sorted(glob.glob(os.path.join(args.data_path, '*.csv')))
    else:
        paths = [args.data_path]

    if not paths:
        print('found no zillow data to process, abort.')
        sys.exit(1)

    print('>>> running {} on {}'.format('tuplex', paths))

    # configuration, make sure to give enough runtime memory to the executors!
    conf = {"webui.enable" : False,
            "executorCount" : 16,
            "executorMemory" : "2G",
            "driverMemory" : "2G",
            "partitionSize" : "32MB",
            "runTimeMemory" : "128MB",
            "useLLVMOptimizer" : True,
            "optimizer.nullValueOptimization" : False,
            "csv.selectionPushdown" : True}

    if os.path.exists('tuplex_config.json'):
        with open('tuplex_config.json') as fp:
            conf = json.load(fp)

    tstart = time.time()
    import tuplex
    ctx = tuplex.Context(conf)

    startup_time = time.time() - tstart
    print('Tuplex startup time: {}'.format(startup_time))
    tstart = time.time()

    # Tuplex pipeline
    data = ctx.csv(','.join(paths)) \
        .tocsv(output_path + "/out.csv")

    job_time = time.time() - tstart
    print('Tuplex job time: {} s'.format(job_time))

    # print stats as last line
    print(json.dumps({"startupTime" : startup_time, "jobTime" : job_time}))
