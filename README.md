# BigQuery Ingest Manager
Simple and reliable ingest manager for BigQuery written in Node.JS

- Serverless design and *AWS Lambda functions*.
- Cost effectiveness. Optimised for **batch load jobs** which means you don't need to pay for data loading. Basically it's free but check BigQuery load job limits.
- Can use *streaming* inserts (BigQuery streaming loading).
- Tailored for **AWS** but can be easily migrated to GCP, Azure.
- Architecture as code built with AWS Cloudformation. Deploy in one click in any other AWS account.
- Effective load job monitoring and file duplicates handling with AWS Dynamo.
- Custom BigQuery job ids. Another way to prevent duplication attempts if you don't want to use Dynamo.
- Support for unit and integration tests.

# Solution overview
[img](img/ingestManager.drawio.png)

# Prerequisites
- AWS account
- BigQuery project. Replace service account key file with yous: `./stack/bq-ingest-manager/bq-shakhomirov-b86071c11c27Example.json`

# Usage
##  Testing locally
[1] Create some files in your datalake S3 bucket, i.e.
~~~bash
aws s3 cp ./data/simple_transaction0.csv s3://bq-shakhomirov.bigquery.aws
aws s3 cp ./data/simple_transaction s3://bq-shakhomirov.bigquery.aws
~~~
[2] Supply this object to `./test/data.json`, i.e.
~~~json
{
  "Records": [
      {
          "bucket": {
              "name": "bq-shakhomirov.bigquery.aws"
          },
          "object": {
              "key": "reconcile/paypal_transaction/2021/10/03/05/paypal_transaction181"
          }
      }
  ]
}
~~~
This will emulate **s3ObjCreate** event which would trigger the Lambda when deployed and file lands in your datalake ready for data ingestion into BigQuery.

[3] In command line Run `npm run test`

If this file name contains any of table names you mentioned in `./config/staging.yaml` it will be uploaded into **BigQuery** into a relevant table:
~~~yaml
Tables:
  -
    pipeName: paypal_transaction              # pick all files which have this in file key.
    bigqueryName: paypal_transaction_src      # BigQuery table name to insert data.
    datasetId: source
    schema:
      - name: "src"
        type: "STRING"
        mode: "NULLABLE"
    # partitionField: created_at              # if empty use date(ingestion time) as partition. Default BigQuery setting.
    fileFormat:
      load: CSV                               # load as.
      delimiter: 'þ'                          # hacky way of loading into one column. An individual JSON object per one row.
      transform:                              # Transform from this into load format accepted by BigQuery.
        from: OUTER_ARRAY_JSON                # Array of individual JSON objects to SRC, i.e. [{...},{...},{...}] >>> '{...}'\n'{...}'\n'{...}'\n
      compression: none
    dryRun: false                             # If true then don't insert into a table.
    notes: For example, daily extract from PayPal API by bq-data-connectors/paypal-revenue AWS Lambda
~~~

Loads data from CSV file format into BigQuery in batch mode.
It's memory effective as it converts batch to stream (not *BigQuery streaming*).
BigQuery streaming !== Node streaming so even though table.insert is not a streaming API in Node terms, it is a streaming API in **BigQuery** terms.

## Loading multiple files located in s3 bucket
You can upload multiple files in one go.

[1] Create a payload with all the files which match S3 bucket name, prefix and contain a pipe name as defined in  `./stack/bq-ingest-manager/test/integration/loadTestPipelines.json`
```shell
npm run test-service
```
This will update the file with sample paypload in `./stack/bq-ingest-manager/test/integration/loadTestPayload.json`

[2] Run the service locally with this payload you've just created:
```shell
npm run test-load
```


# Deploy
## Deploy with Cloudformation
- Run `npm run predeploy` first. This would make the package lighter removing dev dependencies.
- in command line run:
~~~bash
aws cloudformation package \
    --region eu-west-1 \
    --template-file cf-config.yaml \
    --output-template-file cf-deploy.yaml \
    --s3-bucket lambdas.bq-shakhomirov.aws
~~~
**Note**: replace `lambdas.bq-shakhomirov.aws` with your bucket for dev artifacts.

~~~bash
aws cloudformation deploy \
    --region eu-west-1 \
    --template-file cf-deploy.yaml \
    --stack-name bq-ingest-manager \
    --capabilities CAPABILITY_IAM \
    --parameter-overrides \
        Testing="false"
~~~

# Contributing
Contributions are welcome. Please read the code of conduct and the contributing guidelines.

# License Summary
This project is licensed under the Apache-2.0 License.

# Features requests
We are tracking features requests from users here to prioritise what is important for our users. Please file an issue and we'll answer you as soon as possible.

# More
Contact me at

