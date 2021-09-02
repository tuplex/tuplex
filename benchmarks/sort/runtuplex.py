import time
import argparse
import json
import os
import glob
import sys
import typing

parser = argparse.ArgumentParser(description="sort data cleaning")
parser.add_argument(
    "--weld-mode",
    dest="weld_mode",
    help="if active, add cache statements like weld does; else, do end-to-end",
    action="store_true",
)
parser.add_argument(
    "--path",
    type=str,
    dest="data_path",
    default="../../tuplex/test/resources/pipelines/flights/flights_on_time_performance_2009_01.sample.csv",
    help="path or pattern to flights data",
)
parser.add_argument('--output-path', type=str, dest='output_path', default='tuplex_output/',
                    help='specify path where to save output data files')

args = parser.parse_args()



# save the run configuration
output_path = args.output_path

# get the input files
perf_paths = [args.data_path]
if not os.path.isfile(args.data_path):
    file_paths = sorted(glob.glob(os.path.join(args.data_path, "*.*.*.txt")))
    perf_paths = file_paths

if not perf_paths:
    print("found no sort data to process, abort.")
    sys.exit(1)


# import tuplex
startup_time = 0

if os.path.exists("tuplex_config.json"):
    with open("tuplex_config.json") as fp:
        conf = json.load(fp)
else:
    # configuration, make sure to give enough runtime memory to the executors!
    conf = {
        "webui.enable": False,
        "executorMemory": "2G",
        "driverMemory": "2G",
        "partitionSize": "64MB",
        "runTimeMemory": "128MB",
        "useLLVMOptimizer": True,
        "optimizer.nullValueOptimization": True,
        "csv.selectionPushdown": True,
        "optimizer.generateParser": True,
        "tuplex.optimizer.mergeExceptionsInOrder": False,
        "csv.filterPushdown": True,
    }

tstart = time.time()
import tuplex

ctx = tuplex.Context(conf)

startup_time = time.time() - tstart
print("Tuplex startup time: {}".format(startup_time))

# Read the data in
tstart = time.time()
df = ctx.csv(
    ",".join(perf_paths),
    null_values=["Unspecified", "NO CLUE", "NA", "N/A", "0", ""],
    # type_hints={0: typing.Optional[str]},
)
# Do the pipeline
byy = {3: tuplex.SortBy.ASCENDING}
df = df.sort(by=byy).tocsv(output_path)
job_time = time.time() - tstart
print(json.dumps({"startupTime": startup_time, "jobTime": job_time}))
