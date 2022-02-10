## AWS EMR benchmark

This folder contains and experimental distributed benchmark using AWS Serverless EMR (). As of now, you need to request preview activation for your account to run this.


## Setup
(as described in <https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/getting-started.html>)

I.e., run the following commands to add aws-emr support to aws cli and create the appropriate execution role:
```
aws configure add-model --service-model file://service.json

aws iam create-role \
    --role-name emrExecutionRole \
    --assume-role-policy-document file://emr-serverless-trust-policy.json
```

Fetch the arn for the newly created role, e.g. via:

```
EMR_ARN=$(aws iam get-role --role-name emrExecutionRole | jq -r '."Role"."Arn"')
```
Create IAM policy using the provided file:

```
aws iam create-policy \
    --policy-name emrS3AccessPolicy \
    --policy-document file://emr-access-policy.json
```
Again, fetch the ARN of that policy in order to attach it to the role created before:
arn:aws:iam::587583095482:policy/emrS3AccessPolicy

```
aws iam attach-role-policy \
    --role-name emrExecutionRole \
    --policy-arn "arn:aws:iam::587583095482:policy/emrS3AccessPolicy"
```

Time to create the serverless application now (PySpark).
```
aws emr-serverless create-application \
    --release-label emr-6.5.0-preview \
    --type 'SPARK' \
    --name emr-zillow
```

Fetch the application ID, i.e. via:
aws emr-serverless list-applications

To describe the application, use:
aws emr-serverless get-application --application-id 00eublscuhvnu609

00eublscuhvnu609


Check and confer: https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/spark-jobs.html

--> I.e. test zillow query vs. EMR Spark / PyWren / Tuplex.
--> Fix parallelism etc.!
--> This will be added to fair comparison on frameworks.

--> tasks should be launched from EC2 test-instance.
