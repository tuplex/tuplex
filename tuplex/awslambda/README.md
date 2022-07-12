## AWS Lambda function

## Changing runner behavior
For simplicity, certain behaviors can be changed of the AWS Lambda runner by submitting approriate Environment variables.
This is because it's cheaper to update the environment than to upload everytime a new package.

TUPLEX_ENABLE_FULL_AWS_LOGGING=TRUE


## Old content:
This folder contains code for the AWS C++ Lambda executor which can be deployed when using the AWS Lambda backend.

Is only built on Linux (for MacOS use the hosted package).











# Setup Guide
## Initial AWS Setup
- Install AWS CLI (`sudo apt install awscli`)
- Run `aws configure` and set up your aws credentials
  - For simplicity, can put `us-east-1` (or whatever is closest to you) as default region, `json` as output format.
- Create an s3 bucket (e.g. `tuplex-test-<name>`)

## AWS Lambda Setup
- Pull the tuplex lambda function with `aws s3 cp s3://tuplex-public/tplxlam.zip .`
- Create new Lambda with [`resources/scripts/make_lambda.sh`](resources/scripts/make_lambda.sh)` tplxlam.zip`

## Running Experiments
- Go to `benchmark/distributed/tuplex/`
- Run `python runtuplex.py --output-path s3://<your-bucket>/zillow.csv --scratch-dir s3://<your-bucket>/scratch` 
- You can add `--path s3://tuplex-public/data/1000GB/*.csv` to the command above to run over 1000GB instead of the default (100GB).

### AWS Parameters
- Some relevant parameters that can be set up in the tuplex config are:
  - `scratchDir`: an s3 location that tuplex can use for scratch files (MUST BE SET TO RUN ANY PIPELINE!)
  - `aws.httpThreadCount`: the number of http threads to be used,
  - `aws.requestTimeout`: the request timeout (s),
  - `aws.connectTimeout`: the connection timeout (s),
  - `aws.maxConcurrency`: the maximum amount of executors to be used concurrently,
  - `aws.requesterPay`: whether to make the requester pay to access s3 files (required for `s3://tuplex-public`),
    
## Debugging
 - We can view logs on Cloudwatch to debug lambda execution
   - Go to CloudWatch > CloudWatch Logs > Log Groups > /aws/lambda/tplxlam
   - Click and view individual logs
 - Run [`resources/scripts/clear_logs.py`](resources/scripts/clear_logs.py) to clean up the periodically clean up these logs

# Local Development
## Basic Setup
- Change `S3_TEST_BUCKET` in `/core/include/ee/aws/AWSLambdaBackend.h` to your own S3 bucket (`tuplex-test-<name>`)
- Rebuild the project.
- Run `make aws-lambda-package-tplxlam` to build + update the lambda function

## Running Tests
- Run `test/core/AwsLambdaTest.cc` to make sure the setup works.

# Notes
 - In case `tplxlam.zip` gets larger than 50MB, need to upload it first to S3 and then provide it with `--code` option in AWS CLI (instead of `--zip-file`).

## Debugging:
`sudo yum install elfutils-devel` or `sudo yum install binutils-devel` on Amazon Linux.
This will enable further debug info in cloud logs.
