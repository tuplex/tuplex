#!/usr/bin/env python3
#----------------------------------------------------------------------------------------------------------------------#
#                                                                                                                      #
#                                       Tuplex: Blazing Fast Python Data Science                                       #
#                                                                                                                      #
#                                                                                                                      #
#  (c) 2017 - 2021, Tuplex team                                                                                        #
#  Created by Leonhard Spiegelberg first on 11/4/2021                                                                   #
#  License: Apache 2.0                                                                                                 #
#----------------------------------------------------------------------------------------------------------------------#

try:
    import boto3
    import botocore.exceptions
except Exception as e:
    # ignore here, because boto3 is optional
    pass
    #raise Exception('To use distributed version, please install boto3')

import logging
import tempfile
import logging
import os
import base64
import datetime
import socket
import json
import sys
import threading
import time

# Tuplex specific imports
from tuplex.utils.common import in_jupyter_notebook, in_google_colab, is_in_interactive_mode, current_user, host_name


def current_iam_user():
    iam = boto3.resource('iam')
    user = iam.CurrentUser()
    return user.user_name.lower()


def default_lambda_name():
    return 'tuplex-lambda-runner'


def default_lambda_role():
    return 'tuplex-lambda-role'


def default_bucket_name():
    return 'tuplex-' + current_iam_user()

def default_scratch_dir():
    return default_bucket_name() + '/scratch'

def current_region():
    session = boto3.session.Session()
    region = session.region_name

    if region is None:
        # could do fancier auto-detect here...
        return 'us-east-1'

    return region

def check_credentials(aws_access_key_id=None, aws_secret_access_key=None):
    kwargs = {}
    if isinstance(aws_access_key_id, str):
        kwargs['aws_access_key_id'] = aws_access_key_id
    if isinstance(aws_secret_access_key, str):
        kwargs['aws_secret_access_key'] = aws_secret_access_key
    client = boto3.client('s3', **kwargs)
    try:
        client.list_buckets()
    except botocore.exceptions.NoCredentialsError as e:
        logging.error('Could not connect to AWS, Details: {}. To configure AWS credentials please confer the guide under https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials'.format(e))
        return False
    return True

def ensure_s3_bucket(s3_client, bucket_name, region):
    bucket_names = list(map(lambda b: b['Name'], s3_client.list_buckets()['Buckets']))

    if bucket_name not in bucket_names:
        logging.info('Bucket {} not found, creating (private bucket) in {} ...'.format(bucket_name, region))

        # bug in boto3:
        if region == current_region():
            s3_client.create_bucket(Bucket=bucket_name)
            logging.info('Bucket {} created in {}'.format(bucket_name, region))
        else:
            location = {'LocationConstraint': region.strip()}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
            logging.info('Bucket {} created in {}'.format(bucket_name, region))
    else:
        logging.info('Found bucket {}'.format(bucket_name))


def create_lambda_role(iam_client, lambda_role):
    # Roles required for AWS Lambdas
    trust_policy = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
    lambda_access_to_s3 = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*MultipartUpload*","s3:Get*","s3:ListBucket","s3:Put*"],"Resource":"*"}]}'
    lambda_invoke_others = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["lambda:InvokeFunction","lambda:InvokeAsync"],"Resource":"*"}]}'

    iam_client.create_role(RoleName=lambda_role,
                           AssumeRolePolicyDocument=trust_policy,
                           Description='Auto-created Role for Tuplex AWS Lambda runner')
    iam_client.attach_role_policy(RoleName=lambda_role,
                                  PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole')
    iam_client.put_role_policy(RoleName=lambda_role, PolicyName='InvokeOtherlambdas',
                               PolicyDocument=lambda_invoke_others)
    iam_client.put_role_policy(RoleName=lambda_role, PolicyName='LambdaAccessForS3', PolicyDocument=lambda_access_to_s3)
    logging.info('Created Tuplex AWS Lambda runner role ({})'.format(lambda_role))

    # check it exists
    try:
        response = iam_client.get_role(RoleName=lambda_role)
    except:
        raise Exception('Failed to create AWS Lambda Role')


def remove_lambda_role(iam_client, lambda_role):
    # detach policies...
    try:
        iam_client.detach_role_policy(RoleName=lambda_role,
                                      PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole')
    except Exception as e:
        logging.error(
            'Error while detaching policy AWSLambdaBasicExecutionRole, Tuplex setup corrupted? Details: {}'.format(e))

    policy_names = iam_client.list_role_policies(RoleName=lambda_role)['PolicyNames']

    for name in policy_names:
        try:
            iam_client.delete_role_policy(RoleName=lambda_role, PolicyName=name)
        except Exception as e:
            logging.error('Error while detaching policy {}, Tuplex setup corrupted? Details: {}'.format(name, e))

    # delete role...
    iam_client.delete_role(RoleName=lambda_role)


def setup_lambda_role(iam_client, lambda_role, region, overwrite):
    try:
        response = iam_client.get_role(RoleName=lambda_role)
        logging.info('Found Lambda role from {}'.format(response['Role']['CreateDate']))

        # throw dummy exception to force overwrite
        if overwrite:
            remove_lambda_role(iam_client, lambda_role)
            logging.info('Overwriting existing role {}'.format(lambda_role))
            create_lambda_role(iam_client, lambda_role)

    except iam_client.exceptions.NoSuchEntityException as e:
        logging.info('Role {} was not found in {}, creating ...'.format(lambda_role, region))
        create_lambda_role(iam_client, lambda_role)


def sizeof_fmt(num, suffix="B"):
    # from https://stackoverflow.com/questions/1094841/get-human-readable-version-of-file-size
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, sizeof_fmt(self._seen_so_far), sizeof_fmt(self._size),
                    percentage))
            sys.stdout.flush()


def s3_split_uri(uri):
    assert '/' in uri, 'at least one / is required!'
    uri = uri.replace('s3://', '')

    bucket = uri[:uri.find('/')]
    key = uri[uri.find('/') + 1:]
    return bucket, key


def upload_lambda(iam_client, lambda_client, lambda_function_name, lambda_role,
                  lambda_zip_file, overwrite=False, s3_client=None, s3_scratch_space=None, quiet=False):
    # AWS only allows 50MB to be uploaded directly via request. Else, requires S3 upload.

    ZIP_UPLOAD_LIMIT_SIZE = 50000000

    # Lambda defaults, be careful what to set here!
    # for runtime, choose https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtimes.html
    RUNTIME = "provided.al2"
    HANDLER = "tplxlam"  # this is how the executable is called...
    ARCHITECTURES = ['x86_64']
    DEFAULT_MEMORY_SIZE = 1536
    DEFAULT_TIMEOUT = 30  # 30s timeout

    if not os.path.isfile(lambda_zip_file):
        raise Exception('Could not find local lambda zip file {}'.format(lambda_zip_file))
    file_size = os.stat(lambda_zip_file).st_size

    # if file size is smaller than limit, check how large the base64 encoded version is...
    CODE = None
    if file_size < ZIP_UPLOAD_LIMIT_SIZE:
        logging.info('Encoding Lambda as base64 ({})'.format(sizeof_fmt(file_size)))
        with open(lambda_zip_file, 'rb') as fp:
            CODE = fp.read()
            CODE = base64.b64encode(CODE)
            b64_file_size = len(CODE) + 1
            logging.info('File size as base64 is {}'.format(sizeof_fmt(b64_file_size)))
    else:
        b64_file_size = ZIP_UPLOAD_LIMIT_SIZE + 42  # to not trigger below if

    # get ARN of lambda role
    response = iam_client.get_role(RoleName=lambda_role)
    lambda_role_arn = response['Role']['Arn']

    # check if Lambda function already exists, if overwrite delete!
    l_response = lambda_client.list_functions(FunctionVersion='ALL')
    functions = list(filter(lambda f: f['FunctionName'] == lambda_function_name, l_response['Functions']))
    if len(functions) > 0:
        if len(functions) != 1:
            logging.warning('Found multiple functions with name {}, deleting them all.'.format(lambda_function_name))

        if not overwrite:
            raise Exception(
                'Found existing Lambda function {}, specify overwrite=True to replace'.format(lambda_function_name))

        for f in functions:
            lambda_client.delete_function(FunctionName=f['FunctionName'])
            logging.info('Removed existing function {} (Runtime={}, MemorySize={}) from {}'.format(f['FunctionName'],
                                                                                                   f['Runtime'],
                                                                                                   f['MemorySize'],
                                                                                                   f['LastModified']))

    logging.info('Assigning role {} to runner'.format(lambda_role_arn))

    user = current_user()
    host = host_name()

    DEPLOY_MESSAGE = "Auto-deployed Tuplex Lambda Runner function." \
                     " Uploaded by {} from {} on {}".format(user, host, datetime.datetime.now())

    if b64_file_size < ZIP_UPLOAD_LIMIT_SIZE:
        logging.info('Found packaged lambda ({})'.format(sizeof_fmt(file_size)))

        logging.info('Loading local zipped lambda...')

        logging.info('Uploading Lambda to AWS ({})'.format(sizeof_fmt(file_size)))
        try:
            # upload directly, we use Custom
            response = lambda_client.create_function(FunctionName=lambda_function_name,
                                                     Runtime=RUNTIME,
                                                     Handler=HANDLER,
                                                     Role=lambda_role_arn,
                                                     Code={'ZipFile': CODE},
                                                     Description=DEPLOY_MESSAGE,
                                                     PackageType='Zip',
                                                     MemorySize=DEFAULT_MEMORY_SIZE,
                                                     Timeout=DEFAULT_TIMEOUT)
        except Exception as e:
            logging.error('Failed with: {}'.format(type(e)))
            logging.error('Details: {}'.format(str(e)[:2048]))
            raise e
    else:
        if s3_client is None or s3_scratch_space is None:
            raise Exception("Local packaged lambda to large to upload directly, " \
                            "need S3. Please specify S3 client + scratch space")
        logging.info("Lambda function is larger than current limit ({}) AWS allows, " \
                     " deploying via S3...".format(sizeof_fmt(ZIP_UPLOAD_LIMIT_SIZE)))

        # upload to s3 temporarily
        s3_bucket, s3_key = s3_split_uri(s3_scratch_space)

        # scratch space, so naming doesn't matter
        TEMP_NAME = 'lambda-deploy.zip'
        s3_key_obj = s3_key + '/' + TEMP_NAME
        s3_target_uri = 's3://' + s3_bucket + '/' + s3_key + '/' + TEMP_NAME
        callback = ProgressPercentage(lambda_zip_file) if not quiet else None
        s3_client.upload_file(lambda_zip_file, s3_bucket, s3_key_obj, Callback=callback)
        logging.info('Deploying Lambda from S3 ({})'.format(s3_target_uri))

        try:
            # upload directly, we use Custom
            response = lambda_client.create_function(FunctionName=lambda_function_name,
                                                     Runtime=RUNTIME,
                                                     Handler=HANDLER,
                                                     Role=lambda_role_arn,
                                                     Code={'S3Bucket': s3_bucket, 'S3Key': s3_key_obj},
                                                     Description=DEPLOY_MESSAGE,
                                                     PackageType='Zip',
                                                     MemorySize=DEFAULT_MEMORY_SIZE,
                                                     Timeout=DEFAULT_TIMEOUT)
        except Exception as e:
            logging.error('Failed with: {}'.format(type(e)))
            logging.error('Details: {}'.format(str(e)[:2048]))

            # delete S3 file from scratch
            s3_client.delete_object(Bucket=s3_bucket, Key=s3_key_obj)
            logging.info('Removed {} from S3'.format(s3_target_uri))

            raise e

        # delete S3 file from scratch
        s3_client.delete_object(Bucket=s3_bucket, Key=s3_key_obj)
        logging.info('Removed {} from S3'.format(s3_target_uri))

    # print out deployment details
    logging.info('Lambda function {} deployed (MemorySize={}MB, Timeout={}).'.format(response['FunctionName'],
                                                                                     response['MemorySize'],
                                                                                     response['Timeout']))

    # return lambda response
    return response


def find_lambda_package():
    """
    Check whether a compatible zip file in tuplex/other could be found for auto-upload
    Returns: None or path to lambda zip to upload

    """

    this_directory = os.path.abspath(os.path.dirname(__file__))

    # check if folder other exists & file tplxlam.zip in it!
    candidate_path = os.path.join(this_directory, 'other', 'tplxlam.zip')
    if os.path.isfile(candidate_path):
        logging.info('Found Lambda runner package in {}'.format(candidate_path))
        return candidate_path

    return None

def setup_aws(aws_access_key=None, aws_secret_key= None,
              overwrite=True,
              iam_user=None,
              lambda_name=None,
              lambda_role=None,
              lambda_file=None,
              region=None,
              s3_scratch_uri=None,
              quiet=False
              ):

    start_time = time.time()

    # detect defaults. Important to do this here, because don't want to always invoke boto3/botocore
    if iam_user is None:
        iam_user = current_iam_user()
    if lambda_name is None:
        lambda_name = default_lambda_name()
    if lambda_role is None:
        lambda_role = default_lambda_role()
    if lambda_file is None:
        lambda_file = find_lambda_package()
    if region is None:
        region = current_region()
    if s3_scratch_uri is None:
        s3_scratch_uri = default_scratch_dir()

    if lambda_file is None:
        raise Exception('Must specify a lambda runner to upload, i.e. set ' \
        'parameter lambda_file=<path to tplxlam.zip>. Please check the REAMDE.md to ' \
        ' read about instructions on how to build the lambda runner or visit ' \
        'the project website to download prebuilt runners.')

    assert lambda_file is not None, 'must specify file to upload'

    # check credentials are existing on machine --> raises exception in case
    logging.info('Validating AWS credentials')
    check_credentials(aws_access_key, aws_access_key)

    logging.info('Setting up AWS Lambda backend for IAM user {}'.format(iam_user))
    logging.info('Configuring backend in zone: {}'.format(region))

    # check if iam user is found?
    # --> skip for now, later properly authenticate using assume_role as described in
    # https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-api.html

    # create all required client objects for setup
    # key credentials for clients
    client_kwargs = {'aws_access_key_id': aws_access_key,
                     'aws_secret_access_key': aws_secret_key,
                     'region_name': region}

    iam_client = boto3.client('iam', **client_kwargs)
    s3_client = boto3.client('s3', **client_kwargs)
    lambda_client = boto3.client('lambda', **client_kwargs)

    # Step 1: ensure S3 scratch space exists
    s3_bucket, s3_key = s3_split_uri(s3_scratch_uri)
    ensure_s3_bucket(s3_client, s3_bucket, region)

    # Step 2: create Lambda role
    setup_lambda_role(iam_client, lambda_role, region, overwrite)

    # Step 3: upload/create Lambda
    upload_lambda(iam_client, lambda_client, lambda_name, lambda_role, lambda_file, overwrite, s3_client, s3_scratch_uri, quiet)

    # done, print if quiet was not set to False
    if not quiet:
        print('\nCompleted lambda setup in {:.2f}s'.format(time.time() - start_time))
