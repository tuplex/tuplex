#!/usr/bin/env python3
# S3/Lambda version of Zillow pipeline.
# Tuplex based cleaning script


import tuplex
import time
import sys
import json
import os
import glob
import argparse


# Functions to extract relevant fields from input data
def extractBd(x):
    val = x["facts and features"]
    max_idx = val.find(" bd")
    if max_idx < 0:
        max_idx = len(val)
    s = val[:max_idx]

    # find comma before
    split_idx = s.rfind(",")
    if split_idx < 0:
        split_idx = 0
    else:
        split_idx += 2
    r = s[split_idx:]
    return int(r)


def extractBa(x):
    val = x["facts and features"]
    max_idx = val.find(" ba")
    if max_idx < 0:
        max_idx = len(val)
    s = val[:max_idx]

    # find comma before
    split_idx = s.rfind(",")
    if split_idx < 0:
        split_idx = 0
    else:
        split_idx += 2
    r = s[split_idx:]
    return int(r)


def extractSqft(x):
    val = x["facts and features"]
    max_idx = val.find(" sqft")
    if max_idx < 0:
        max_idx = len(val)
    s = val[:max_idx]

    split_idx = s.rfind("ba ,")
    if split_idx < 0:
        split_idx = 0
    else:
        split_idx += 5
    r = s[split_idx:]
    r = r.replace(",", "")
    return int(r)


def extractOffer(x):
    offer = x["title"].lower()
    if "sale" in offer:
        return "sale"
    if "rent" in offer:
        return "rent"
    if "sold" in offer:
        return "sold"
    if "foreclose" in offer.lower():
        return "foreclosed"
    return offer


def extractType(x):
    t = x["title"].lower()
    type = "unknown"
    if "condo" in t or "apartment" in t:
        type = "condo"
    if "house" in t:
        type = "house"
    return type


def extractPrice(x):
    price = x["price"]
    p = 0
    if x["offer"] == "sold":
        # price is to be calculated using price/sqft * sqft
        val = x["facts and features"]
        s = val[val.find("Price/sqft:") + len("Price/sqft:") + 1 :]
        r = s[s.find("$") + 1 : s.find(", ") - 1]
        price_per_sqft = int(r)
        p = price_per_sqft * x["sqft"]
    elif x["offer"] == "rent":
        max_idx = price.rfind("/")
        p = int(price[1:max_idx].replace(",", ""))
    else:
        # take price from price column
        p = int(price[1:].replace(",", ""))

    return p


# Functions to filter based on various fields
def filterPrice(x):
    return 100000 < x["price"] <= 2e7


def filterType(x):
    return x["type"] == "house"


def filterBd(x):
    return x["bedrooms"] < 10

print('TODO: update to use PERSONAL bucket... -> default user bucket')

if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(description="Zillow cleaning")
    parser.add_argument(
        "--path",
        type=str,
        dest="data_path",
        default="s3://tuplex-public/data/1000GB/*.csv",
        help="path or pattern to zillow data",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        dest="output_path",
        default="s3://tuplex-leonhard/scratch/zillow.csv",
        help="specify path where to save output data files",
    )
    parser.add_argument(
        "--scratch-dir",
        type=str,
        dest="scratch_dir",
        default="s3://tuplex-leonhard/scratch",
        help="specify scratch directory for Tuplex to use",
    )
    parser.add_argument('--lambda-memory',
    type=int,
    default=3000,
    help='how many MB to assign to Lambda runner')
    args = parser.parse_args()

    assert args.data_path, "need to set data path!"

    if not args.data_path.startswith("s3://"):
        print("found no s3 zillow data to process, abort.")
        sys.exit(1)

    print(">>> running {} on {}".format("tuplex", args.data_path))

    # load data
    tstart = time.time()

    # configuration: make sure to give enough runtime memory to the executors!
    conf = {
        "backend": "lambda",
        "scratchDir": args.scratch_dir,
        "aws.httpThreadCount": 100,
        "aws.requestTimeout": 600,
        "aws.connectTimeout": 30,
        "aws.maxConcurrency": 400, # adjust this to allow for more concurrency with the function...
        "aws.requesterPay": True,
        "aws.lambdaMemory": args.lambda_memory,
        "webui.enable": False,
        "executorCount": 16,
        "executorMemory": "2G",
        "driverMemory": "2G",
        "partitionSize": "32MB",
        "runTimeMemory": "128MB",
        "useLLVMOptimizer": True,
        "optimizer.nullValueOptimization": False,
        "csv.selectionPushdown": True,
    }

    # Begin pipeline
    tstart = time.time()
    import tuplex

    ctx = tuplex.Context(conf)

    startup_time = time.time() - tstart
    print("Tuplex startup time: {}".format(startup_time))
    tstart = time.time()

    # Tuplex pipeline
    ctx.csv(args.data_path) \
       .withColumn("bedrooms", extractBd) \
       .filter(lambda x: x["bedrooms"] < 10) \
       .withColumn("type", extractType) \
       .filter(lambda x: x["type"] == "house") \
       .withColumn("zipcode", lambda x: "%05d" % int(x["postal_code"])) \
       .mapColumn("city", lambda x: x[0].upper() + x[1:].lower()) \
       .withColumn("bathrooms", extractBa) \
       .withColumn("sqft", extractSqft) \
       .withColumn("offer", extractOffer) \
       .withColumn("price", extractPrice) \
       .filter(lambda x: 100000 < x["price"] < 2e7) \
       .selectColumns(["url","zipcode","address","city","state","bedrooms","bathrooms","sqft","offer","type","price"]) \
       .tocsv(args.output_path)

    run_time = time.time() - tstart

    job_time = time.time() - tstart
    print("Tuplex job time: {} s".format(job_time))

    # print stats as last line
    print(json.dumps({"startupTime": startup_time, "jobTime": job_time}))
