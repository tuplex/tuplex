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

from tuplex.repl import *
from .context import Context
from .dataset import DataSet


# expose aws setup for better convenience
import tuplex.distributed
from tuplex.distributed import setup_aws

# for convenience create a dummy function to return a default-configured Lambda context
def LambdaContext(s3_scratch_dir=None, **kwargs):

    if s3_scratch_dir is None:
        s3_scratch_dir = tuplex.distributed.default_scratch_dir()

    # There's currently a bug in the Lambda backend when transferring local data to S3: The full partition gets transferred,
    # not just what is needed.
    return Context(conf={'backend': 'lambda',
                         'partitionSize': '1MB',
                         'aws.scratchDir': s3_scratch_dir,
                         'aws.requesterPay': True}, **kwargs)