import time
import argparse
import json
import os
import glob
import sys
import re
import random
import string

# parse the arguments
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
    help="path or pattern to the ip blacklist",
)
parser.add_argument(
    "--pipeline_type",
    type=str,
    dest="pipeline_type",
    choices=["regex", "strip", "split_regex", "split"],
    default="regex",
    help="whether to use the regex clean function or the string strip based one",
)
parser.add_argument(
    "--output-path", 
    type=str,
    dest="output_path",
    default="dask_output/",
    help='specify path where to save output data files',
)
args = parser.parse_args()

# define the parsing functions
def ParseWithRegex(logline):
    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0
    """
    try:
        match = re.search(
            '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)',
            logline,
        )
        if match is None:
            return ("", "", "", "", "", "", "", -1, -1)
        size_field = match.group(9)
        if size_field == "-":
            size = 0
        else:
            size = int(match.group(9))
        return (
            match.group(1),
            match.group(2),
            match.group(3),
            match.group(4),
            match.group(5),
            match.group(6),
            match.group(7),
            int(match.group(8)),
            size,
        )
    except:
        return ("", "", "", "", "", "", "", -1, -1)

def extract_ip(x):
    match = re.search("(^\S+) ", x)
    if match:
        return match[1]
    else:
        return ''
def extract_client_id(x):
    match = re.search("^\S+ (\S+) ", x)
    if match:
        return match[1]
    else:
        return ''
def extract_user_id(x):
    match = re.search("^\S+ \S+ (\S+) ", x)
    if match:
        return match[1]
    else:
        return ''
def extract_date(x):
    match = re.search("^.*\[([\w:/]+\s[+\-]\d{4})\]", x)
    if match:
        return match[1]
    else:
        return ''
def extract_method(x):
    match = re.search('^.*"(\S+) \S+\s*\S*\s*"', x)
    if match:
        return match[1]
    else:
        return ''
def extract_endpoint(x):
    match = re.search('^.*"\S+ (\S+)\s*\S*\s*"', x)
    if match:
        return match[1]
    else:
        return ''
def extract_protocol(x):
    match = re.search('^.*"\S+ \S+\s*(\S*)\s*"', x)
    if match:
        return match[1]
    else:
        return ''
def extract_response_code(x):
    match = re.search('^.*" (\d{3}) ', x)
    if match:
        return int(match[1])
    else:
        return -1
def extract_content_size(x):
    match = re.search('^.*" \d{3} (\S+)', x)
    if match:
        return 0 if match[1] == '-' else int(match[1])
    else:
        return -1


def ParseWithStrip(x):
    try:
        y = x.strip()

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
            return (
                ip,
                client_id,
                user_id,
                date,
                method,
                endpoint,
                protocol,
                int(response_code),
                0 if content_size == "-" else int(content_size),
            )
        else:
            return ("", "", "", "", "", "", "", -1, -1)
    except:
        return ("", "", "", "", "", "", "", -2, -2)


def RandomizeEndpointUDF(x):
    return re.sub(
        "^/~[^/]+",
        "/~" + "".join([random.choice(string.ascii_uppercase) for _ in range(10)]),
        x,
        )

def try_int(x):
    try:
        return int(x)
    except:
        return -1

# save the run configuration
output_path = args.output_path
clean_function = ParseWithRegex if args.pipeline_type == "regex" else ParseWithStrip

# get the input files
perf_paths = [args.data_path]
if not os.path.isfile(args.data_path):
    # inner join in dask is broken, run therefore without 4G of data
    file_paths = sorted(list(filter(lambda p: not '2011.12.08' in p and not '2011.12.09' in p, glob.glob(os.path.join(args.data_path, "*.*.*.txt")))))
    perf_paths = file_paths

if not perf_paths:
    print("found no log data to process, abort.")
    sys.exit(1)

if __name__ == "__main__":
    # import dask
    startup_time = 0

    tstart = time.time()
    import dask
    import dask.dataframe as dd
    import dask.bag as db
    from dask.diagnostics import ProgressBar
    from dask.distributed import Client
    import dask.multiprocessing
    import os
    import glob
    import sys

    import pandas as pd
    import numpy as np

    client = Client(n_workers=16, threads_per_worker=1, processes=True, memory_limit='8GB')
    print(client)
    startup_time = time.time() - tstart
    print("Dask startup time: {}".format(startup_time))

    # define regex function

    # open file
    print("*Dask Start Time: {}".format(time.time()))

    # parse the rows
    if args.pipeline_type == 'split_regex':
        tstart = time.time()
        b = db.read_text(perf_paths, linedelimiter="\n")
        df = b.to_dataframe()
        df["ip"] = df[0].apply(extract_ip, meta=str)
        df["client_id"] = df[0].apply(extract_client_id, meta=str)
        df["user_id"] = df[0].apply(extract_user_id, meta=str)
        df["date"] = df[0].apply(extract_date, meta=str)
        df["method"] = df[0].apply(extract_method, meta=str)
        df["endpoint"] = df[0].apply(extract_endpoint, meta=str)
        df["protocol"] = df[0].apply(extract_protocol, meta=str)
        df["response_code"] = df[0].apply(extract_response_code, meta=int)
        df["content_size"] = df[0].apply(extract_content_size, meta=int)
        df = df[df.endpoint.str.len() > 0]
        df["endpoint"] = df['endpoint'].apply(RandomizeEndpointUDF, meta=str)
    elif args.pipeline_type == 'split':
        tstart = time.time()
        b = db.read_text(perf_paths, linedelimiter="\n")
        df = b.to_dataframe()
        df["cols"] = df[0].apply(lambda x: x.split(' '), meta=object)
        df["ip"] = df['cols'].apply(lambda x: x[0].strip() if len(x) > 0 else '', meta=str)
        df["client_id"] = df['cols'].apply(lambda x: x[1].strip() if len(x) > 1 else '', meta=str)
        df["user_id"] = df['cols'].apply(lambda x: x[2].strip() if len(x) > 2 else '', meta=str)
        df["date"] = df['cols'].apply(lambda x: x[3] + " " + x[4] if len(x) > 4 else '', meta=str)
        df["date"] = df['date'].apply(lambda x: x.strip(), meta=str)
        df["date"] = df['date'].apply(lambda x: x[1:-1], meta=str)
        df["method"] = df['cols'].apply(lambda x: x[5].strip() if len(x) > 5 else '', meta=str)
        df["method"] = df['method'].apply(lambda x: x[1:], meta=str)
        df["endpoint"] = df['cols'].apply(lambda x: x[6].strip() if len(x) > 6 else '', meta=str)
        df["protocol"] = df['cols'].apply(lambda x: x[7].strip() if len(x) > 7 else '', meta=str)
        df["protocol"] = df['protocol'].apply(lambda x: x[:-1], meta=str)
        df["response_code"] = df['cols'].apply(lambda x: try_int(x[8].strip()) if len(x) > 8 else -1, meta=int)
        df["content_size"] = df['cols'].apply(lambda x: x[9].strip() if len(x) > 9 else '', meta=str)
        df["content_size"] = df['content_size'].apply(lambda x: 0 if x == '-' else try_int(x), meta=int)
        df = df[df.endpoint.str.len() > 0]
        df["endpoint"] = df['endpoint'].apply(RandomizeEndpointUDF, meta=str)
    else:
        tstart = time.time()
        b = db.read_text(perf_paths, linedelimiter="\n")
        df = b.to_dataframe()
        df["new"] = df[0].apply(clean_function, meta=(0, "object"))
        df["ip"] = df["new"].apply(lambda x: x[0], meta=str)
        df["client_id"] = df["new"].apply(lambda x: x[1], meta=str)
        df["user_id"] = df["new"].apply(lambda x: x[2], meta=str)
        df["date"] = df["new"].apply(lambda x: x[3], meta=str)
        df["method"] = df["new"].apply(lambda x: x[4], meta=str)
        df["endpoint"] = (
            df["new"].apply(lambda x: x[5], meta=str).apply(RandomizeEndpointUDF, meta=str)
        )
        df["protocol"] = df["new"].apply(lambda x: x[6], meta=str)
        df["response_code"] = df["new"].apply(lambda x: x[7], meta=int)
        df["content_size"] = df["new"].apply(lambda x: x[8], meta=int)

    # join on bad ips
    bad_ip_df = dd.read_csv([args.ip_blacklist_path], low_memory=False).repartition(npartitions=1)

    df_malicious_requests = dd.merge(
        df, bad_ip_df, left_on="ip", right_on="BadIPs", how="inner"
    )

    df_malicious_requests = df_malicious_requests[
        [
            "ip",
            "date",
            "method",
            "endpoint",
            "protocol",
            "response_code",
            "content_size",
        ]
    ]

    df_malicious_requests.to_csv(output_path, index=None)

    job_time = time.time() - tstart
    print("Dask job time: {} s".format(job_time))

    print(json.dumps({"startupTime": startup_time, "jobTime": job_time}))
