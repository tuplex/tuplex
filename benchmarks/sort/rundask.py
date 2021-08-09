import time
import argparse
import json
import os
import glob
import sys
import typing
import math

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
parser.add_argument('--output-path', type=str, dest='output_path', default='dask_output',
                    help='specify path where to save output data files')

args = parser.parse_args()


def fix_zip_codes(zips):
    if not zips or (isinstance(zips, float) and math.isnan(zips)):
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


if __name__ == "__main__":
    # import dask
    startup_time = 0

    tstart = time.time()
    # startup
    import dask
    import dask.dataframe as dd
    from dask.diagnostics import ProgressBar
    from dask.distributed import Client
    import dask.multiprocessing
    import os
    import glob
    import sys

    import pandas as pd
    import numpy as np

    client = Client(
        n_workers=16, threads_per_worker=1, processes=True, memory_limit="8GB"
    )
    # end startup
    startup_time = time.time() - tstart
    print("Dask startup time: {}".format(startup_time))

    # TODO: seems like compute doesn't work
    if args.weld_mode:
        # Input data
        tstart = time.time()
        df = dd.read_csv(perf_paths, low_memory=False, dtype=str).compute()
        read_time = time.time() - tstart

        # computation
        tstart = time.time()
        df["Incident Zip"] = df["Incident Zip"].apply(fix_zip_codes)
        df = df.drop_duplicates()
        job_time = time.time() - tstart

        # end pipeline
        tstart = time.time()
        df.to_csv(output_path, index=None)
        write_time = time.time() - tstart

        print(json.dumps({"startupTime": startup_time, "readTime": read_time, "jobTime": job_time, "writeTime": write_time}))
    else:
        # pipeline
        tstart = time.time()
        df = dd.read_csv(perf_paths, low_memory=False, dtype=str)
        df = df[df["Incident Zip"] != "NO CLUE"]
        df["Incident Zip"] = df["Incident Zip"].apply(
            fix_zip_codes, meta=("Incident Zip", object)
        )
        df = df.drop_duplicates()
        df.to_csv(output_path, index=None)

        # end pipeline
        job_time = time.time() - tstart
        print("Dask job time: {} s".format(job_time))

        print(json.dumps({"startupTime": startup_time, "jobTime": job_time}))
