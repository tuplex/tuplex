import boto3
import tempfile
import logging
import os
import base64

import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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

def current_region():
    session = boto3.session.Session()
    region = session.region_name
    return region


lambda_role=default_lambda_role()

region = current_region()
overwrite = True


def create_lambda_role(iam_client, lambda_role):

    # Roles required for AWS Lambdas
    trust_policy = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
    lambda_access_to_s3 = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*MultipartUpload*","s3:Get*","s3:ListBucket","s3:Put*"],"Resource":"*"}]}'
    lambda_invoke_others = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["lambda:InvokeFunction","lambda:InvokeAsync"],"Resource":"*"}]}'

    iam_client.create_role(RoleName=lambda_role,
                           AssumeRolePolicyDocument=trust_policy,
                           Description='Auto-created Role for Tuplex AWS Lambda runner')
    iam_client.attach_role_policy(RoleName=lambda_role, PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole')
    iam_client.put_role_policy(RoleName=lambda_role, PolicyName='InvokeOtherlambdas', PolicyDocument=lambda_invoke_others)
    iam_client.put_role_policy(RoleName=lambda_role, PolicyName='LambdaAccessForS3', PolicyDocument=lambda_access_to_s3)
    logging.info('Created Tuplex AWS Lambda runner role ({})'.format(lambda_role))

    # check it exists
    try:
        response = iam_client.get_role(RoleName=lambda_role)
        print(response)
    except:
        raise Exception('Failed to create AWS Lambda Role')

def remove_lambda_role(iam_client, lambda_role):

    # detach policies...
    try:
        iam_client.detach_role_policy(RoleName=lambda_role, PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole')
    except Exception as e:
        logging.error('Error while detaching policy AWSLambdaBasicExecutionRole, Tuplex setup corrupted? Details: {}'.format(e))

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

lambda_client = boto3.client('lambda')

lambda_function_name=default_lambda_name()
lambda_zip_file = './tplxlam.zip'

try:
    response = lambda_client.get_function(FunctionName=lambda_function_name)
    print(response)
except lambda_client.exceptions.ResourceNotFoundException as e:
    logging.info('Function {} was not found in {}, uploading ...'.format(lambda_function_name, region))

# from utils.common
try:
    import pwd
except ImportError:
    import getpass
    pwd = None

import datetime
import socket

def current_user():
    """
    retrieve current user name
    Returns: username as string

    """
    if pwd:
        return pwd.getpwuid(os.geteuid()).pw_name
    else:
        return getpass.getuser()

def host_name():
    """
    retrieve host name to identify machine
    Returns: some hostname as string

    """
    if socket.gethostname().find('.') >= 0:
        return socket.gethostname()
    else:
        return socket.gethostbyaddr(socket.gethostname())[0]


def sizeof_fmt(num, suffix="B"):
    # from https://stackoverflow.com/questions/1094841/get-human-readable-version-of-file-size
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def upload_lambda(lambda_client, lambda_function_name, lambda_role,
                  lambda_zip_file, overwrite=False, s3_client=None, s3_scratch_space=None):
    # AWS only allows 50MB to be uploaded directly via request. Else, requires S3 upload.

    ZIP_UPLOAD_LIMIT_SIZE=50000000

    # Lambda defaults, be careful what to set here!
    # for runtime, choose https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtimes.html
    RUNTIME="provided.al2"
    ARCHITECTURES=['x86_64']
    DEFAULT_MEMORY_SIZE=1536

    if not os.path.isfile(lambda_zip_file):
        raise Exception('Could not find local lambda zip file {}'.format(lambda_zip_file))
    file_size = os.stat(lambda_zip_file).st_size
    if file_size < ZIP_UPLOAD_LIMIT_SIZE:
        logging.info('Found packaged lambda ({})'.format(sizeof_fmt(file_size)))

        user = current_user()
        host = host_name()

        DEPLOY_MESSAGE="Auto-deployed Tuplex Lambda Runner function." \
        " Uploaded by {} from {} on {}".format(user, host, datetime.datetime.now())

        logging.info('Loading local zipped lambda...')
        with open(lambda_zip_file, 'rb') as fp:
            CODE = fp.read()

            CODE = base64.b64encode(CODE)
            logging.info('Lambda encoded as base64 ({})'.format(sizeof_fmt(len(CODE))))

            logging.info('Uploading Lambda to AWS ({})'.format(sizeof_fmt(file_size)))
            try:
                # upload directly, we use Custom
                lambda_client.create_function(FunctionName=lambda_function_name,
                                             Runtime=RUNTIME,
                                             Role=lambda_role,
                                             Code={'ZipFile': CODE},
                                             Description=DEPLOY_MESSAGE,
                                             PackageType='Zip')
            except Exception as e:
                logging.error('Failed with: {}'.format(type(e)))

                logging.error('Details: {}'.format(str(e)[:2048]))
            logging.info('Lambda function deployed.')
    else:
        if s3_client is None or s3_scratch_space is None:
            raise Exception("Local packaged lambda to large to upload directly, " \
                            "need S3. Please specify S3 client + scratch space")
        # upload to s3 temporarily

        # delete temp s3 file after delete.

upload_lambda(lambda_client, lambda_function_name, lambda_role, lambda_zip_file)
