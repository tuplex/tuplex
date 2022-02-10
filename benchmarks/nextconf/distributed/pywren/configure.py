#!/usr/bin/env python3
# run this file to obtain AWS settings automatically or adjust them manually

import boto3
import os
import logging
import argparse

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s', level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def current_iam_user():
    iam = boto3.resource('iam')
    user = iam.CurrentUser()
    return user.user_name.lower()

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


def main():
    parser = argparse.ArgumentParser(description='PyWren AWS settings helper')
    parser.add_argument('--region', type=str, dest='region', default=None,
                        help='AWS region to use')
    parser.add_argument('--bucket', type=str, dest='bucket', default=None,
                        help='AWS bucket to use')
    parser.add_argument('--account', type=id, dest='account', default=None,
                        help='AWS account id to use')
    args = parser.parse_args()

    # fill in values if None automatically from aws configure
    account_id = args.account
    if account_id is None:
        sts_client = boto3.client('sts')
        account_id = sts_client.get_caller_identity()["Account"]

    region = args.region
    if region is None:
        session = boto3.session.Session()
        region = session.region_name

    s3_client = boto3.client('s3')
    s3_bucket = args.bucket
    if s3_bucket is None:
        user = current_iam_user()
        logging.info('AWS IAM user for whom to configure: {}'.format(user))
        # create bucket pywren-<user> if not exists
        s3_bucket = 'pywren-{}'.format(user)

    logging.info('AWS account id: {}'.format(account_id))
    logging.info('AWS region to use: {}'.format(region))
    ensure_s3_bucket(s3_client, s3_bucket, region)

    template = open('pywren_config_template.yaml').read()
    with open('.pywren_config','w') as fp:
        fp.write(template.format(aws_account_id = account_id, aws_region=region,pywren_bucket=s3_bucket))
    logging.info('Wrote .pywren_config file to include in docker container')
    logging.info('Done.')

if __name__ == '__main__':
    main()