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
    choices=["regex", "strip"],
    default="regex",
    help="whether to use the regex clean function or the string strip based one",
)

args = parser.parse_args()

def ParseWithRegex(logline):
    match = re_search('^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)', logline)
    if(match):
        return {"ip": match[1],
                "client_id": match[2],
                "user_id": match[3],
                "date": match[4],
                "method": match[5],
                "endpoint": match[6],
                "protocol": match[7],
                "response_code": int(match[8]),
                "content_size": 0 if match[9] == '-' else int(match[9])}
    else:
        return {"ip": '',
                "client_id": '',
                "user_id": '',
                "date": '',
                "method": '',
                "endpoint": '',
                "protocol": '',
                "response_code": -1,
                "content_size": -1}


def ParseWithStrip(x):
    y = x

    i = y.find(" ")
    ip = y[:i]
    y = y[i + 1 :]

    i = y.find(" ")
    client_id = y[:i]
    y = y[i + 1 :]

    i = y.find(" ")
    user_id = y[:i]
    y = y[i + 1 :]

    i = y.find("]")
    date = y[:i][1:]
    y = y[i + 2 :]

    y = y[y.find('"') + 1 :]

    method = ""
    endpoint = ""
    protocol = ""
    failed = False
    if y.find(" ") < y.rfind('"'):
        i = y.find(" ")
        method = y[:i]
        y = y[i + 1 :]

        i = y.find(" ")  # needs to be any whitespace
        endpoint = y[:i]
        y = y[i + 1 :]

        i = y.rfind('"')
        protocol = y[:i]
        protocol = protocol[protocol.rfind(" ") + 1 :]
        y = y[i + 2 :]
    else:
        failed = True
        i = y.rfind('"')
        y = y[i + 2 :]

    i = y.find(" ")
    response_code = y[:i]
    content_size = y[i + 1 :]

    if not failed:
        return {"ip": ip,
                "client_id": client_id,
                "user_id": user_id,
                "date": date,
                "method": method,
                "endpoint": endpoint,
                "protocol": protocol,
                "response_code": int(response_code),
                "content_size": 0 if content_size == '-' else int(content_size)}
    else:
        return {"ip": "",
                "client_id": "",
                "user_id": "",
                "date": "",
                "method": "",
                "endpoint": "",
                "protocol": "",
                "response_code": -1,
                "content_size": -1}

def randomize_udf(x):
    return re_sub('^/~[^/]+', '/~' + ''.join([random_choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ') for t in range(10)]), x)

# save the run configuration
output_path = f"tuplex_output_{args.pipeline_type}"
clean_function = ParseWithRegex if args.pipeline_type == "regex" else ParseWithStrip

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

# open file
tstart = time.time()
df = ctx.text(','.join(perf_paths)).cache()
bad_ip_df = ctx.csv(args.ip_blacklist_path).cache()
io_load_time = time.time() - tstart
print('CACHED in {}s'.format(io_load_time))
tstart = time.time()

df = df.map(clean_function).mapColumn("endpoint", randomize_udf)

# join on bad ips

df_malicious_requests = df.join(bad_ip_df, "ip", "BadIPs")

df_malicious_requests.selectColumns(["ip", "date", "method", "endpoint", "protocol", "response_code", "content_size"]).tocsv(output_path)

job_time = time.time() - tstart + io_load_time
print("Tuplex job time: {} s".format(job_time))

print(json.dumps({"startupTime": startup_time, "jobTime": job_time, "io_load": io_load_time}))
