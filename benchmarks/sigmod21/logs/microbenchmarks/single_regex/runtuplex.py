import time
import argparse
import json
import os
import glob
import sys

parser = argparse.ArgumentParser(description="Apache data cleaning + join")
parser.add_argument(
    "--path",
    type=str,
    dest="data_path",
    default="../../test/resources/2000.01.01.txt",
    help="path or pattern to log data",
)
parser.add_argument(
    "--ip_blacklist_path",
    type=str,
    dest="ip_blacklist_path",
    default="../../test/resources/bad_ips_all.txt",
)
parser.add_argument(
    "--pipeline_type",
    type=str,
    dest="pipeline_type",
    choices=["regex"],
    default="regex",
    help="whether to use the regex clean function or the string strip based one",
)

args = parser.parse_args()

def ParseWithRegex(logline):
    match = re_search('^(\S+) \S+ \S+ \[[\w:/]+\s[+\-]\d{4}\] "\S+ \S+\s*\S*\s*" \d{3} \S+', logline)
    if(match):
        return {"ip": match[1],
                "client_id": ''}
    else:
        return {"ip": '',
                "client_id": ''}

# save the run configuration
output_path = f"tuplex_output_{args.pipeline_type}"
clean_function = ParseWithRegex

# get the input files
perf_paths = [args.data_path]
if not os.path.isfile(args.data_path):
    file_paths = sorted(glob.glob(os.path.join(args.data_path, '*.*.*.txt')))
    perf_paths = file_paths

if not perf_paths:
    print('found no log data to process, abort.')
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
        "useLLVMOptimizer": False,
        "nullValueOptimization": False,
        "csv.selectionPushdown": False,
        "optimizer.generateParser": False
    }

tstart = time.time()
import tuplex

ctx = tuplex.Context(conf)

startup_time = time.time() - tstart
print("Tuplex startup time: {}".format(startup_time))

# open file, regex, and write to csv
tstart = time.time()
df = ctx.text(','.join(perf_paths)).map(clean_function)
df.selectColumns(["ip"]).tocsv(output_path)

job_time = time.time() - tstart
print("Tuplex job time: {} s".format(job_time))

print(json.dumps({"startupTime": startup_time, "jobTime": job_time}))
