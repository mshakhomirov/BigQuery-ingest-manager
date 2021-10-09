#!/usr/bin/env bash

aws cloudformation package \
    --region eu-west-1 \
    --template-file cf-config.yaml \
    --output-template-file cf-deploy.yaml \
    --s3-bucket lambdas.bq-shakhomirov.aws


aws cloudformation deploy \
    --region eu-west-1 \
    --template-file cf-deploy.yaml \
    --stack-name bq-ingest-manager \
    --capabilities CAPABILITY_IAM \
    --parameter-overrides \
        Testing="false"
