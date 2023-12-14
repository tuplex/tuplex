//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#!/usr/bin/env bash
# (c) 2017 - 2023 L. Spiegelberg, R. Yesantharao
# Create AWS Lambda function and upload the provided function zip file.

LAMBDA_NAME=tplxlam
LAMBDA_ROLE="lambda-${LAMBDA_NAME}-role"

# create role with required permissions
ROLE_ARN=$(aws iam get-role --role-name ${LAMBDA_ROLE} --query "Role.Arn" --output text)
if [ $? -eq 0 ]; then
  echo "*Role ${LAMBDA_ROLE} Already Exists!"
else
  aws iam create-role --role-name ${LAMBDA_ROLE} --assume-role-policy-document file://../config/trust-policy.json
  aws iam attach-role-policy --role-name ${LAMBDA_ROLE} --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
  aws iam put-role-policy --role-name ${LAMBDA_ROLE} --policy-name InvokeOtherLambdas --policy-document file://../config/invoke-other-lambdas.json
  aws iam put-role-policy --role-name ${LAMBDA_ROLE} --policy-name LambdaAccessForS3 --policy-document file://../config/lambda-access-for-s3.json
  echo "*Role ${LAMBDA_ROLE} Created!"
  ROLE_ARN=$(aws iam get-role --role-name ${LAMBDA_ROLE} --query "Role.Arn" --output text)
fi
echo "*Role ARN: $ROLE_ARN"

# create the lambda
aws lambda get-function --function-name ${LAMBDA_NAME}
if [ $? -eq 0 ]; then # just to speed up accidental reruns
  echo "*Function ${LAMBDA_NAME} Already Exists!"
else
  aws lambda create-function --function-name ${LAMBDA_NAME} --role ${ROLE_ARN} --runtime provided --timeout 15 --memory-size 512 --handler ${LAMBDA_NAME} --zip-file "fileb://$1"
  echo "*Function ${LAMBDA_NAME} Created!"
fi