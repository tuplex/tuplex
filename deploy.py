import tuplex
from tuplex.distributed import setup_aws

setup_aws(lambda_file='build-lambda/tplxlam.zip')
exit()
