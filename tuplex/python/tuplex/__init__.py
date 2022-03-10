#!/usr/bin/env python3
# ----------------------------------------------------------------------------------------------------------------------#
#                                                                                                                      #
#                                       Tuplex: Blazing Fast Python Data Science                                       #
#                                                                                                                      #
#                                                                                                                      #
#  (c) 2017 - 2021, Tuplex team                                                                                        #
#  Created by Leonhard Spiegelberg first on 1/1/2021                                                                   #
#  License: Apache 2.0                                                                                                 #
# ----------------------------------------------------------------------------------------------------------------------#

from tuplex.repl import *
from .context import Context
from .dataset import DataSet

# expose aws setup for better convenience
import tuplex.distributed
import logging
from tuplex.distributed import setup_aws

from tuplex.utils.version import __version__ as __version__

# for convenience create a dummy function to return a default-configured Lambda context
def LambdaContext(conf=None, name=None, s3_scratch_dir=None, **kwargs):
    import uuid

    if s3_scratch_dir is None:
        s3_scratch_dir = tuplex.distributed.default_scratch_dir()
        logging.debug('Detected default S3 scratch dir for this user as {}'.format(s3_scratch_dir))

    lambda_conf = {'backend': 'lambda',
                   'partitionSize': '1MB',
                   'aws.scratchDir': s3_scratch_dir,
                   'aws.requesterPay': True}

    if conf:
        lambda_conf.update(conf)

    # go through kwargs and update conf with them!
    for k, v in kwargs.items():
        if k in conf.keys():
            lambda_conf[k] = v
        elif 'tuplex.' + k in conf.keys():
            lambda_conf['tuplex.' + k] = v
        else:
            lambda_conf[k] = v

    if name is None:
        name = 'AWSLambdaContext-' + str(uuid.uuid4())[:8]

    # There's currently a bug in the Lambda backend when transferring local data to S3: The full partition
    # gets transferred, not just what is needed.

    # c'tor of context is defined as  def __init__(self, conf=None, name="", **kwargs):
    return Context(name=name, conf=lambda_conf)
