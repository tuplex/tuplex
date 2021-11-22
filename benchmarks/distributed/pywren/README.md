## PyWren benchmark directory

This directory contains benchmark implementations of (PyWren)[https://github.com/pywren/pywren] to be compared against Tuplex.

To facilitate benchmarking, it's wrapped up as docker container.

Start container via

docker run -it -e AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION} -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} pywren bash


## Steps required to fix PyWren:

1. update runtime to newer python (3.9 ?)
2. code base only works with python 3.7 currently
3. setup script doesn't work yet properly...


##setup config

This is the PyWren interactive setup script
Your AWS configuration appears to be set up, and your account ID is 587583095482
This interactive script will set up your initial PyWren configuration.
If this is the first time you are using PyWren then accepting the defaults should be fine.
What is your default aws region? [us-west-2]: us-east-1
Location for config file:  [/root/.pywren_config]:
PyWren requires an s3 bucket to store intermediate data. What s3 bucket would you like to use? [root-pywren-368]: pywren-leonhard
PyWren prefixes every object it puts in S3 with a particular prefix.
PyWren s3 prefix:  [pywren.jobs]:
Would you like to configure advanced PyWren properties? [y/N]: y
Each lambda function runs as a particularIAM role. What is the name of the role youwould like created for your lambda [pywren_exec_role_1]: pywren_lambda_role
Each lambda function has a particular function name.What is your function name? [pywren_1]: pywren_runner
PyWren standalone mode uses dedicated AWS instances to run PyWren tasks. This is more flexible, but more expensive with fewer simultaneous workers.
Would you like to enable PyWren standalone mode? [y/N]:
Creating config /root/.pywren_config
new default file created in /root/.pywren_config
lambda role is pywren_lambda_role
Creating role.
Deploying lambda.
