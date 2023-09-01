#!/usr/bin/env python3
#----------------------------------------------------------------------------------------------------------------------#
#                                                                                                                      #
#                                       Tuplex: Blazing Fast Python Data Science                                       #
#                                                                                                                      #
#                                                                                                                      #
#  (c) 2017 - 2021, Tuplex team                                                                                        #
#  Created by Leonhard Spiegelberg first on 1/1/2021                                                                   #
#  License: Apache 2.0                                                                                                 #
#----------------------------------------------------------------------------------------------------------------------#

import boto3
import argparse

client = boto3.client("logs")


def delete_stream(log_group_name, stream):
    delete_response = client.delete_log_stream(
        logGroupName=log_group_name, logStreamName=stream["logStreamName"]
    )
    return delete_response


if __name__ == "__main__":
    # Get log group name
    parser = argparse.ArgumentParser(description="Clear CloudWatch Log Group")
    parser.add_argument(
        "--log-group",
        type=str,
        dest="log_group",
        default="/aws/lambda/tuplex-lambda-runner",
        help="name of log group to clear",
    )
    args = parser.parse_args()

    log_group_name = args.log_group

    # get log streams
    log_streams_response = client.describe_log_streams(logGroupName=log_group_name)

    # delete log streams
    results = map(
        lambda x: delete_stream(log_group_name, x), log_streams_response["logStreams"],
    )

    # show results
    print(list(results))
