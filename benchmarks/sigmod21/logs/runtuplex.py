import time
import argparse
import json
import os
import glob
import sys
import re
import random

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
    choices=["regex", "strip", "split_regex", "split"],
    default="regex",
    help="whether to use the regex clean function, the split regex function, or the string strip based one",
)
parser.add_argument(
    "--output-path", 
    type=str,
    dest="output_path",
    default="tuplex_output/",
    help='specify path where to save output data files',
)
args = parser.parse_args()

def ParseWithRegex(logline):
    match = re.search(r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)', logline)
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


def extract_ip(x):
    match = re.search(r"(^\S+) ", x['logline'])
    if match:
        return match[1]
    else:
        return ''
def extract_client_id(x):
    match = re.search(r"^\S+ (\S+) ", x['logline'])
    if match:
        return match[1]
    else:
        return ''
def extract_user_id(x):
    match = re.search(r"^\S+ \S+ (\S+) ", x['logline'])
    if match:
        return match[1]
    else:
        return ''
def extract_date(x):
    match = re.search(r"^.*\[([\w:/]+\s[+\-]\d{4})\]", x['logline'])
    if match:
        return match[1]
    else:
        return ''
def extract_method(x):
    match = re.search(r'^.*"(\S+) \S+\s*\S*\s*"', x['logline'])
    if match:
        return match[1]
    else:
        return ''
def extract_endpoint(x):
    match = re.search(r'^.*"\S+ (\S+)\s*\S*\s*"', x['logline'])
    if match:
        return match[1]
    else:
        return ''
def extract_protocol(x):
    match = re.search(r'^.*"\S+ \S+\s*(\S*)\s*"', x['logline'])
    if match:
        return match[1]
    else:
        return ''
def extract_response_code(x):
    match = re.search(r'^.*" (\d{3}) ', x['logline'])
    if match:
        return int(match[1])
    else:
        return -1
def extract_content_size(x):
    match = re.search(r'^.*" \d{3} (\S+)', x['logline'])
    if match:
        return 0 if match[1] == '-' else int(match[1])
    else:
        return -1


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
    return re.sub('^/~[^/]+', '/~' + ''.join([random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ') for t in range(10)]), x)

# save the run configuration
output_path = args.output_path
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
if args.pipeline_type == 'split_regex':
    tstart = time.time()
    df = (
        ctx.text(','.join(perf_paths))
        .map(lambda x: {'logline':x})
        .withColumn("ip", extract_ip)
        .withColumn("client_id", extract_client_id)
        .withColumn("user_id", extract_user_id)
        .withColumn("date", extract_date)
        .withColumn("method", extract_method)
        .withColumn("endpoint", extract_endpoint)
        .withColumn("protocol", extract_protocol)
        .withColumn("response_code", extract_response_code)
        .withColumn("content_size", extract_content_size)
        .filter(lambda x: len(x['endpoint']) > 0)
        .mapColumn("endpoint", randomize_udf)
    )
elif args.pipeline_type == 'split':
    tstart = time.time()
    df = (
        ctx.text(','.join(perf_paths))
        .map(lambda x: {'logline': x})
        .withColumn("cols", lambda x: x['logline'].split(' '))
        .withColumn("ip", lambda x: x['cols'][0].strip())
        .withColumn("client_id", lambda x: x['cols'][1].strip())
        .withColumn("user_id", lambda x: x['cols'][2].strip())
        .withColumn("date", lambda x: x['cols'][3] + " " + x['cols'][4])
        .mapColumn("date", lambda x: x.strip())
        .mapColumn("date", lambda x: x[1:-1]) # TODO: check this
        .withColumn("method", lambda x: x['cols'][5].strip())
        .mapColumn("method", lambda x: x[1:])
        .withColumn("endpoint", lambda x: x['cols'][6].strip())
        .withColumn("protocol", lambda x: x['cols'][7].strip())
        .mapColumn("protocol", lambda x: x[:-1])
        .withColumn("response_code", lambda x: int(x['cols'][8].strip()))
        .withColumn("content_size", lambda x: x['cols'][9].strip())
        .mapColumn("content_size", lambda x: 0 if x == '-' else int(x))
        .filter(lambda x: len(x['endpoint']) > 0)
        .mapColumn("endpoint", randomize_udf)
    )
else:
    tstart = time.time()
    df = ctx.text(','.join(perf_paths)).map(clean_function).mapColumn("endpoint", randomize_udf)

# join on bad ips
bad_ip_df = ctx.csv(args.ip_blacklist_path)

df_malicious_requests = df.join(bad_ip_df, "ip", "BadIPs")

df_malicious_requests.selectColumns(["ip", "date", "method", "endpoint", "protocol", "response_code", "content_size"]).tocsv(output_path)

job_time = time.time() - tstart
print("Tuplex job time: {} s".format(job_time))

print(json.dumps({"startupTime": startup_time, "jobTime": job_time}))
