# Deploy with Cloudformation
- Run `npm run predeploy` first. this would make the package lighter removing dev dependencies.
- Run:
~~~bash
$ ./cf-install.sh
~~~


# Stack
* bq-ingest-manager : data loader into BigQuery which is triggered by events from another microservice (npm run test pulls event from `functions/bq-ingest-manager/stack/bq-ingest-manager/test/data.json`)
* 

# Testing locally

[1]
~~~bash
aws s3 cp ./data/simple_transaction0.csv s3://bq-shakhomirov.bigquery.aws
aws s3 cp ./data/simple_transaction s3://bq-shakhomirov.bigquery.aws
~~~
[2] Supply this object to `./test/data.json`

[3] Run `npm run test`

If this file name contains any of table names you mentioned in ./config.json it will be uploaded into BigQuery into relevant table.

Loads data from CSV file format into BigQuery in batch mode which means we don't pay for this.
It's memory performant as it converts batch to stream (not BigQuery streaming).
BigQuery streaming !== Node streaming so even though table.insert is not a streaming API in Node terms, it is a streaming API in BigQuery terms.

# Possible errors
[1]  code: 404,
  errors: [
    {
      message: 'Not found: Dataset your-project-name-data:source',
      domain: 'global',
      reason: 'notFound'
    }
  ],
[2] NoSuchKey: The specified key does not exist.
 statusCode: 404,

[3] job completed
ResourceNotFoundException: Requested resource not found
- Dynamo table does not exist 
[4] Error: Invalid JSON (Unexpected "a" at position 2 in state STOP)
- when try to upload CSV as NLDJ