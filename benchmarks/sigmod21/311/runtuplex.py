import time
import argparse
import json
import os
import glob
import sys
import typing

parser = argparse.ArgumentParser(description="311 data cleaning")
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
    default="../../test/resources/pipelines/311/311-service-requests-sf=1.csv",
    help="path or pattern to 311 data",
)
parser.add_argument('--output-path', type=str, dest='output_path', default='tuplex_output/',
                    help='specify path where to save output data files')

args = parser.parse_args()


def fix_zip_codes(zips):
    if not zips:
        return None
    # Truncate everything to length 5
    s = zips[:5]

    # Set 00000 zip codes to nan
    if s == "00000":
        return None
    else:
        return s


# save the run configuration
output_path = args.output_path

# get the input files
perf_paths = [args.data_path]
if not os.path.isfile(args.data_path):
    file_paths = sorted(glob.glob(os.path.join(args.data_path, "*.*.*.txt")))
    perf_paths = file_paths

if not perf_paths:
    print("found no 311 data to process, abort.")
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

if args.weld_mode:
    # Read the data in
    tstart = time.time()
    df = ctx.csv(
        ",".join(perf_paths),
        null_values=["Unspecified", "NO CLUE", "NA", "N/A", "0", ""],
        type_hints={0: typing.Optional[str]},
    ).cache()
    read_time = time.time() - tstart

    # Do the pipeline
    tstart = time.time()
    df = df.mapColumn("Incident Zip", fix_zip_codes).unique().cache()
    job_time = time.time() - tstart

    # Output to csv
    tstart = time.time()
    df.tocsv(output_path)
    write_time = time.time() - tstart
    print(json.dumps({"startupTime": startup_time, "readTime": read_time, "jobTime": job_time, "writeTime": write_time}))
else:
    # Read the data in
    tstart = time.time()
    df = ctx.csv(
        ",".join(perf_paths),
        null_values=["Unspecified", "NO CLUE", "NA", "N/A", "0", ""],
        type_hints={0: typing.Optional[str]},
    )
    # Do the pipeline
    df = df.mapColumn("Incident Zip", fix_zip_codes).unique()
    # Output to csv
    df.tocsv(output_path)
    job_time = time.time() - tstart
    print(json.dumps({"startupTime": startup_time, "jobTime": job_time}))
