#!/usr/bin/env bash
# chmod +x deploy.sh
# Run ./deploy.sh
# This is a microservice to load data into BigQuery from S3 in streaming mode.

base=${PWD##*/}
zp=$base".zip"
echo $zp

rm -f $zp

rm -rf node_modules
npm i --only=production
# change Lambda role arn to your values
zip -r $zp * -x deploy.sh

lambdaRole= # for example: 111111:role/service-role/bq-payment-lambda-role-something

if ! aws lambda create-function  \
    --function-name $base-staging \
    --description "Loads data into BigQuery from S3 in streaming mode." \
    --handler app.handler \
    --runtime nodejs12.x \
    --memory-size 128 \
    --timeout 90 \
    --role arn:aws:iam::$lambdaRole \
    --environment Variables="{DEBUG=true,TESTING=true}" \
    --zip-file fileb://$zp;

    then

    echo ""
    echo "Function already exists, updating instead..."

    aws lambda update-function-code  \
    --function-name $base-staging \
    --zip-file fileb://$zp;
fi
