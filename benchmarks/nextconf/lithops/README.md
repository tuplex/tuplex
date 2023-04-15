Setting up lithops cloud environment

https://github.com/lithops-cloud/lithops


## Setup
```
python3.9 -m venv venv
source ./venv/bin/activate

# install lithops package (latest)
pip3 install "lithops[aws]"
```

Follow lithops guide from https://github.com/lithops-cloud/lithops/blob/master/docs/source/compute_config/aws_lambda.md to setup role etc.

Then, store following credentials in `$HOME/.lithops/config`
```yaml
lithops:
    backend: aws_lambda
    storage: aws_s3

aws:
    region: us-east-1
    access_key_id: <YOUR SECRET>
    secret_access_key: <YOUR SECRET>

aws_lambda:
    region_name: us-east-1
    execution_role: <ARN OF ROLE CREATED>
    runtime_memory: 10000
    runtime_timeout: 900
    invoke_pool_threads: 410

aws_s3:
    region_name: us-east-1
    storage_bucket: tuplex-lithops
```

Check that it works via `lithops test -b aws_lambda -s aws_s3`