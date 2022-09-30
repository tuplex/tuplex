#!/usr/bin/env bash
# this file creates the docker container & deploys pywren



pywren create_role
pywren deploy_lambda

aws lambda update-function-configuration --function-name pywren_runner --memory-size 1536
