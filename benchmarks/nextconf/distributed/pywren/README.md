## PyWren benchmark directory

This directory contains benchmark implementations of (PyWren)[https://github.com/pywren/pywren] to be compared against Tuplex. In order to make PyWren run on AWS, several things had to be fixed first:
	1. update runtime to newer python (3.7), this is the only version PyWren is compatible with. It fails for newer Python, i.e. Python3.9
	2. Fix setup.py script to install more recent package versions
All these changes can be found in the forked repo https://github.com/LeonhardFS/pywren

Before running your benchmark, make sure you have some S3 bucket acting as both intermediate and output storage. Edit the pywren_config.yaml file in this directory, then build the docker container to include it.

To facilitate benchmarking, it's wrapped up as docker container.

Start container after editing pywren_config to your settings via

docker run -it -e AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION} -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} pywren bash

You can also invoke the benchmark directly via...



## Steps required to fix PyWren:

1. update runtime to newer python (3.9 ?)
2. code base only works with python 3.7 currently
3. setup script doesn't work yet properly...


TODO: force cold start using https://stackoverflow.com/questions/47445815/force-discard-aws-lambda-container/47447475#47447475
i.e. update some comment in code using date time or so.


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



## config file:
```
root@58c8c5765f67:/work/pywren# cat ~/.pywren_config
account:
    aws_account_id: 587583095482
    aws_lambda_role: pywren_lambda_role
    aws_region: us-east-1


lambda:
    memory : 1536
    timeout : 300
    function_name : pywren_runner

s3:
    bucket: pywren-leonhard
    pywren_prefix: pywren.jobs

runtime:
    s3_bucket: pywren-runtimes-public-us-east-1
    s3_key: pywren.runtimes/default_3.7.meta.json

scheduler:
    map_item_limit: 10000

standalone:
    ec2_instance_type: m4.large
    sqs_queue_name: pywren-jobs-1
    visibility: 10
    ec2_ssh_key : PYWREN_DEFAULT_KEY
    target_ami : ami-0ff8a91507f77f867
    instance_name: pywren-standalone
    instance_profile_name: pywren-standalone
    max_idle_time: 60
    idle_terminate_granularity: 3600
```



Note that in order to run zillow query, large functions are necessary. I.e., the 10GB version works.

Complete waste of money but finished in ~90s.
