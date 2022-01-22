import boto3
import argparse

import time

client = boto3.client("logs")

log_group_name = '/aws/lambda/tuplex-lambda-runner'

limit = 50

# get log streams
log_streams_response = client.describe_log_streams(logGroupName=log_group_name, limit=limit)

while 'nextToken' in log_streams_response:
    tstart = time.time()
    print('Starting to delete logs...')
    nextToken = log_streams_response['nextToken']
    n_logs = len(log_streams_response['logStreams'])
    
    responses = [client.delete_log_stream(
        logGroupName=log_group_name, logStreamName=stream["logStreamName"]
    ) for stream in log_streams_response['logStreams']]
    
    print('Deleted {} logs in {:.2f}s'.format(len(responses), time.time() - tstart))
    
    log_streams_response = client.describe_log_streams(logGroupName=log_group_name, nextToken=nextToken, limit=limit)
