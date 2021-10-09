/* eslint-disable camelcase */
/* eslint-disable arrow-body-style */
/* eslint-disable no-use-before-define */

const DEBUG = process.env.DEBUG || 'false';
const DYNAMO_SUCCESS_TABLE = process.env.DYNAMO_SUCCESS_TABLE || 'ingest-manager-success';
const CHECK_FILE_DUPLICATES = process.env.CHECK_FILE_DUPLICATES || 'false'; // When 'true' jobId === fileKey ; Disallows dups and performs a check using file ingestion history from DynamoDb. Running locally to reload data set to 'false'. This will stop checking Dynamo and attach `_now()` suffix to jobId in BigQuery.
const NODE_ENV = process.env.NODE_ENV || 'staging';
const TESTING = process.env.TESTING || 'true';

// 3rd party dependencies
const moment = require('moment');
const AWS = require('aws-sdk');
AWS.config.update({ region: 'eu-west-1' });
const s3 = new AWS.S3();
const db = new AWS.DynamoDB({ apiVersion: '2012-08-10' });
const stream = require('stream');
const through2 = require('through2');
const fs = require('fs');
const { BigQuery } = require('@google-cloud/bigquery');
const JSONStream = require('JSONStream');
const es = require('event-stream');
const zlib = require('zlib');

// Local dependencies

if (TESTING === 'false') { // When deployed to AWS, TESTING must be set to 'false'.
    process.env.NODE_CONFIG_DIR = '/var/task/bq-ingest-manager/config';
}
const config = require('config');

const bigQueryCreds = require('./bq-shakhomirov-b86071c11c27.json'); // Load BigQuery key.
const { IngestionErrors, NotFoundErrors, IngestManagerError } = require('./objects/errorObject');

const pr = (txt) => { if (DEBUG === 'true') { console.log(txt); } };

const bigquery = new BigQuery({
    projectId: config.get('gcp.gcpProjectId'),
    credentials: {
        client_email: config.get('gcp.gcpClientEmail'),
        private_key: bigQueryCreds.private_key, // ideally must be retieved from KMS or private s3 bucket.
    },
});

exports.handler = async(event, context) => {

    try {

        const tables = config.get('Tables');
        const bucket = config.get('dataBucket');
        const key = config.get('key');
        pr(`BUCKET : ${bucket} TABLES: ${tables}`);
        const results = { 'passed': [], 'failed': [] };
        for (const objectCreatedEvent of event.Records) {
            const result = await processEvent(objectCreatedEvent, tables, bucket, key);
            parseInt(result.rowsInserted) > 0 ? results.passed.push(result) : results.failed.push(result);
            console.log({ result, 'pass': parseInt(result.rowsInserted) > 0 });
        }
        context.succeed((results));
    } catch (e) {
        console.log(e);
        context.done(e);
    }
};

const processEvent = async(eventRecord, tables, bucket) => {
    const now = moment();
    console.log({ 'trigger: ': `aws s3 cp s3://${eventRecord.bucket.name}/${eventRecord.object.key}` });
    pr(`SETTING: CHECK_FILE_DUPLICATES =  ${CHECK_FILE_DUPLICATES}`);
    const fileKey = eventRecord.object.key;
    const sourceBucket = eventRecord.bucket.name;
    for (const table of tables) {
        try {
            pr(`table.dryRun: ${table.dryRun}`);
            pr(`table.pipeName: ${table.pipeName}`);
            pr(`fileKey.includes(table.pipeName): ${(fileKey).replace(/_/g, '-').includes((table.pipeName).replace(/_/g, '-'))}`);
            // eslint-disable-next-line no-empty
            if (!(table.dryRun) && ((fileKey.replace(/_/g, '-')).includes((table.pipeName).replace(/_/g, '-')))) { // Normalize table name and replace all '_' chars in table name with '-'
                console.log(`table.pipeName:: ${JSON.stringify(table.pipeName)}`);
                console.log(`table.schema:: ${JSON.stringify(table.schema)}`);

                const tableExists = await checkIfTableExists(table.bigqueryName, table.schema, table.datasetId, table.partitionField);
                pr(`tableExists ? ${tableExists}`);
                if (tableExists === false) {
                    await createBigQueryTablePartitioned(table.bigqueryName, table.schema, table.datasetId, table.partitionField, table.notes);
                }

                const wasAlreadyIngested = await checkAlreadyIngested(sourceBucket, fileKey);
                pr({ 'wasAlreadyIngested': `${wasAlreadyIngested}` });

                if (wasAlreadyIngested === true) {

                    // handleDuplication(sourceBucket, fileKey);
                    throw IngestionErrors.DUPLICATION_ATTEMPT;
                }

                const jobResult = await checkSourceFileFormatAndIngest(sourceBucket, fileKey, table);
                pr({ 'jobResult': `${JSON.stringify(jobResult)}` });

                if (jobResult.status !== 'job completed') {
                    throw NotFoundErrors.JOB_RESULT_UNKNOWN;
                }
                await logSuccessfulEvent(sourceBucket, fileKey, now.format('YYYY-MM-DDTHH:mm:ss'), table.bigqueryName, jobResult.job.statistics.load.outputRows);
                pr(`Successfully [uploaded from] :: aws s3 cp s3://${sourceBucket}/${fileKey}`);
                return { 'table': table.bigqueryName, 'rowsInserted': jobResult.job.statistics.load.outputRows };

            }
        } catch (error) {
            // No need to log ingestion error as it will appeear in INFORMATION_SCHEMA but we can do if need. Something to think about in the future.
            // await logErrorEvent({ 'ERROR': `${error.errors[0].message} :: ${sourceBucket}, ${fileKey}, ${now.format('YYYY-MM-DDTHH:mm:ss')}` });
            if (error instanceof IngestManagerError) {
                return { code: error.code, message: error.message };
            }
            return error;
        }

    }

    return { 'Finished processing data from files in S3': `${bucket}/${fileKey}` };
}
;

/**
 * Checks if file format exists. Must be defined explicitly in config.json
 * Depending on that processing will be selected
 *
 * @param {String} sourceBucket - s3 bucket as datalake for firehose events, etc., i.e. bq-shakhomirov.bigquery.aws
 * @param {String} fileKey - s3 object file key, i.e. simple_transaction (integration test)
 * @param {Object} table - table definition object from config
 *
 * @returns {Promise} Promise that resolves when node stream finished
 */

const checkSourceFileFormatAndIngest = async(sourceBucket, fileKey, table) => {
    if (!(table.fileFormat.load)) {
        throw NotFoundErrors.FILE_FORMAT_NOT_FOUND;
    }
    try {
        let jobResult;
        switch (table.fileFormat.load) {
            case 'CSV':
                switch (table.fileFormat.transform.from) {
                    case 'OBJECT_STRING':
                        jobResult = await loadJsonFileAsCsvFromS3(sourceBucket, fileKey, table.bigqueryName, table.datasetId, table.fileFormat);
                        pr(`${JSON.stringify(jobResult)}`);
                        break;

                    case 'OUTER_ARRAY_JSON':
                        jobResult = await loadOuterArrayJsonFileAsCsvFromS3(sourceBucket, fileKey, table.bigqueryName, table.datasetId, table.fileFormat);
                        pr(`${JSON.stringify(jobResult)}`);
                        break;

                    default:
                        jobResult = await loadCsvFileFromS3(sourceBucket, fileKey, table.bigqueryName, table.datasetId, table.fileFormat);
                }
                break;

            case 'NLDJ':
                switch (table.fileFormat.transform.from) {
                    case 'OBJECT_STRING':
                        switch (table.fileFormat.compression) {
                            case 'gz':
                                jobResult = await loadGzJsonFileFromS3(sourceBucket, fileKey, table.bigqueryName, table.datasetId, table.fileFormat);
                                pr(`${JSON.stringify(jobResult)}`);
                                break;

                            default:
                                jobResult = await loadJsonFileFromS3(sourceBucket, fileKey, table.bigqueryName, table.datasetId, table.fileFormat);
                        }
                        break;

                    case 'OUTER_ARRAY_JSON':
                        jobResult = await loadOuterArrayJsonFileFromS3(sourceBucket, fileKey, table.bigqueryName, table.datasetId, table.fileFormat);
                        break;

                    default:
                        jobResult = await loadJsonFileFromS3(sourceBucket, fileKey, table.bigqueryName, table.datasetId, table.fileFormat);
                }
                break;

            default:
                jobResult = { 'ERROR': 'file format not defined' };
        }
        return jobResult;

    // eslint-disable-next-line no-warning-comments
    // TODO: add ingestion ERROR for wrong spec
    } catch (e) {
        console.log(`ERROR : ${e}`);
        return (e);
    }

};

/**
 * Logs successfull batch into Dynamo to avoid further duplicated attempts
 *
 * @param {String} bucket - s3 bucket as datalake for firehose events, etc., i.e. bq-shakhomirov.bigquery.aws
 * @param {String} key - s3 object file key, i.e. simple_transaction (integration test)
 * @param {String} ts - timestamp string, i.e. now.format('YYYY-MM-DDTHH:mm:ss')
 *
 * @returns {Promise} Promise that resolves when node stream finished
 */

const logSuccessfulEvent = async(bucket, key, ts, tableName, rowCnt) => {
    const fileKey = `${bucket}/${key}`;
    const params = {
        TableName: DYNAMO_SUCCESS_TABLE,
        Item: {
            'fileKey': { S: fileKey },
            'ts': { S: ts },
            'table': { S: tableName },
            'rows': { N: rowCnt },
        },
    };
    try {
        const result = await db.putItem(params).promise();
        pr(`Successfully logged to ${DYNAMO_SUCCESS_TABLE}`);
        return result;
    } catch (e) {
        console.log(e);
    }
    return `Successfully logged to ${DYNAMO_SUCCESS_TABLE}`;
};

/**
 * Checks if the file has been already ingested before
 *
 * @param {String} bucket - source file bucket
 * @param {String} key - source file key
 *
 * @returns {Promise} Promise that resolves when got result from db, i.e. true (key exists), false (foesn't exist), error (Dynamo error, 
 * keep ingesting data not knowing if it's a duplicate or not)
 */

const checkAlreadyIngested = async(bucket, key) => {
    if (CHECK_FILE_DUPLICATES === 'true') {

        const fileKey = `${bucket}/${key}`;
        const params = {
            TableName: DYNAMO_SUCCESS_TABLE,
            Key: {
                'fileKey': { S: fileKey },
            },
        };
        try {

            const result = await db.getItem(params).promise();
            pr(`Successfully getItem from Dynamo : ${DYNAMO_SUCCESS_TABLE}`);

            if (typeof result.Item !== 'undefined' && result.Item !== null) {
                return true;
            }

        } catch (e) {
            console.log(`ERROR in Dynamo. Keep ingesting: ${e}`);
            return (e);
        }
    }

    return false;

};

/**
 * Checks if BigQuery table has been already created.
 *
 * @param {String} tableId - destination BigQuery table where the file will be uploaded, comes from ./config.json
 * @param {String} schema - schema definition, i.e. "transaction_id:INT64, user_id:INT64, dt:date"
 * @param {String} datasetId - destination BigQuery dataset
 *
 * @returns {Promise} Promise that resolves when node stream finished
 */

const checkIfTableExists = async(tableId, tableSchema, datasetId) => {

    try {
        const dataset = bigquery.dataset(datasetId);
        const table = dataset.table(tableId);
        const data = await table.get();
        const apiResponse = data[1]; // const tableData = data[0];
        pr(`apiResponse: ${JSON.stringify(apiResponse)}`);

        if (typeof apiResponse !== 'undefined' && apiResponse !== null) {
            return true;
        }

    } catch (e) {
        // eslint-disable-next-line no-magic-numbers
        if (e.code === 404) {
            pr(`[checkIfTableExists] ${e.message}`);
            return false; // data loading must go on
        }
        return e; // data loading will stop
    }
    return false;

};

/**
 * Loads data from CSV file format into BigQuery in batch mode which means we don't pay for this.
 * It's memory performant as it converts batch to stream (not BigQuery streaming).
 * BigQuery streaming !== Node streaming so even though table.insert is not a streaming API in Node terms,
 * it is a streaming API in BigQuery terms
 * ? https://googleapis.dev/nodejs/bigquery/latest/Table.html#createWriteStream
 *
 * @param {String} bucket - s3 bucket as datalake for firehose events, etc., i.e. bq-shakhomirov.bigquery.aws
 * @param {String} key - s3 object file key, i.e. simple_transaction (integration test)
 * @param {String} tableId - destination BigQuery table where the file will be uploaded, comes from ./config.json
 * @param {String} datasetId - destination BigQuery dataset
 *
 * @returns {Promise} Promise that resolves when node stream finished
 */

const loadCsvFileFromS3 = async(bucket, key, tableId, datasetId, fileFormat) => {
    return new Promise((resolve, reject) => {
        const dataset = bigquery.dataset(datasetId);
        const table = dataset.table(tableId);
        const formattedFileLocation = `${bucket.replace(/\//g, '___').replace(/\./g, '__')}___${key.replace(/\//g, '___').replace(/\./g, '__')}`; // this will insert custom jobId into INFORMATION_SCHEMA after successfull load. Has to be alphanumeric incl. "_" and "-". "___" for "/" and "__" for "."
        // Let BigQuery handle file duplicates:
        const job = CHECK_FILE_DUPLICATES === 'true' ? formattedFileLocation : `${formattedFileLocation}_${moment().format('YYYYMMDDTHHmmss')}`;

        const metadata = {
            jobId: job,
            sourceFormat: 'CSV',
            writeDisposition: fileFormat.writeDisposition || 'WRITE_APPEND',
            allowJaggedRows: fileFormat.allowJaggedRows || true,
            skipLeadingRows: fileFormat.skipLeadingRows || 1,
            field_delimiter: fileFormat.delimiter,
        };

        const params = { Bucket: bucket, Key: key };
        const request = s3.getObject(params);
        const s1 = request.createReadStream();

        // parse csv and split line by line
        s1.pipe(es.split()) // ? https://stackoverflow.com/questions/16010915/parsing-huge-logfiles-in-node-js-read-in-line-by-line
            // Apply CSV transformation to each line
            .pipe(through2(function(row, enc, next) {
                if (fileFormat.transform.add === 'outer array') {
                    this.push(`[${(row).toString('utf8')}]\n`);
                    pr(`[${(row).toString('utf8')}]\n line break`);
                } else {
                    this.push(`${(row).toString('utf8')}\n`);
                }
                next();
            },
            ))

            // Create a writeStream into BigQuery table
            .pipe(table.createWriteStream(metadata))
            .on('job', () => { // (job)
            // `job` is a Job object that can be used to check the status of the
            // request.
            })
            // eslint-disable-next-line no-shadow
            .on('complete', (job) => {
                resolve({ 'job': job.metadata, 'status': 'job completed' });
            })
            .on('error', (error) => { console.log(`[ERROR]:${error}`); reject(error); });

    });
};

/**
 * Loads data from New line delimited JSON, JSON object string
 * file into BigQuery in batch mode which means we don't pay for this.
 * It's memory performant as it converts batch to stream (not BigQuery streaming).
 * BigQuery streaming !== Node streaming so even though table.insert is not a streaming API in Node terms,
 * it is a streaming API in BigQuery terms
 * ? https://googleapis.dev/nodejs/bigquery/latest/Table.html#createWriteStream
 *
 * @param {String} bucket - s3 bucket as datalake for firehose events, etc., i.e. bq-shakhomirov.bigquery.aws
 * @param {String} key - s3 object file key, i.e. simple_transaction (integration test)
 * @param {String} tableId - destination BigQuery table where the file will be uploaded, comes from ./config.json
 * @param {String} datasetId - destination BigQuery dataset
 *
 * @returns {Promise} Promise that resolves when node stream finished
 * JSONStream.stringify() will create an array, (with default options open='[\n', sep='\n,\n', close='\n]\n')
 * If you call JSONStream.stringify(false) the elements will only be seperated by a newline.
 */

const loadJsonFileFromS3 = async(bucket, key, tableId, datasetId, fileFormat) => {
    return new Promise((resolve, reject) => {
        const dataset = bigquery.dataset(datasetId);
        const table = dataset.table(tableId);
        const formattedFileLocation = `${bucket.replace(/\//g, '___').replace(/\./g, '__')}___${key.replace(/\//g, '___').replace(/\./g, '__')}`; // this will insert custom jobId into INFORMATION_SCHEMA after successfull load. Has to be alphanumeric incl. "_" and "-". "___" for "/" and "__" for "."
        // Let BigQuery handle file duplicates:
        const job = CHECK_FILE_DUPLICATES === 'true' ? formattedFileLocation : `${formattedFileLocation}_${moment().format('YYYYMMDDTHHmmss')}`;

        const metadata = {
            jobId: job,
            writeDisposition: fileFormat.writeDisposition || 'WRITE_APPEND',
            sourceFormat: 'NEWLINE_DELIMITED_JSON', // ? https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad
        };

        const params = { Bucket: bucket, Key: key }; // i.e. { Bucket: 'bq-shakhomirov.bigquery.aws', Key: 'simple_transaction' };
        // create write stream into BigQuery from ReadStream (still batchInsert operation, check quoats):
        const request = s3.getObject(params);
        const s1 = request.createReadStream();
        s1.pipe(JSONStream.parse(true))

            // .on('data', (data) => { pr(data); })
            .pipe(JSONStream.stringify(false))

            // .on('data', (data) => { pr(data); })
            .pipe(table.createWriteStream(metadata))
            .on('error', (error) => { reject(error); })
            .on('job', () => {
            // `job` is a Job object that can be used to check the status of the
            // request.
            })
            // eslint-disable-next-line no-shadow
            .on('complete', (job) => {
                resolve({ 'job': job.metadata, 'status': 'job completed' });

            // The job has completed successfully.
            });

        // .on('data', (d) => { console.log(d); ++recordsProcessed; })
        // .on('close', () => { resolve(recordsProcessed); })
        // .on('error', () => { reject(); });

    });
};

/**
 * Loads data from from Outer Array JSON , i.e. from OBJECT_STRING enclosed by '[]' to NLDJ, i.e. [{....},{....},{....}]
 *
 * @param {String} bucket - s3 bucket as datalake for firehose events, etc., i.e. bq-shakhomirov.bigquery.aws
 * @param {String} key - s3 object file key, i.e. simple_transaction (integration test)
 * @param {String} tableId - destination BigQuery table where the file will be uploaded, comes from ./config.json
 * @param {String} datasetId - destination BigQuery dataset
 *
 * @returns {Promise} Promise that resolves when node stream finished
 * JSONStream.stringify() will create an array, (with default options open='[\n', sep='\n,\n', close='\n]\n')
 * If you call JSONStream.stringify(false) the elements will only be seperated by a newline.
 */

const loadOuterArrayJsonFileFromS3 = async(bucket, key, tableId, datasetId, fileFormat) => {
    return new Promise((resolve, reject) => {
        const dataset = bigquery.dataset(datasetId);
        const table = dataset.table(tableId);
        const formattedFileLocation = `${bucket.replace(/\//g, '___').replace(/\./g, '__')}___${key.replace(/\//g, '___').replace(/\./g, '__')}`; // this will insert custom jobId into INFORMATION_SCHEMA after successfull load. Has to be alphanumeric incl. "_" and "-". "___" for "/" and "__" for "."
        // Let BigQuery handle file duplicates:
        const job = CHECK_FILE_DUPLICATES === 'true' ? formattedFileLocation : `${formattedFileLocation}_${moment().format('YYYYMMDDTHHmmss')}`;

        const metadata = {
            jobId: job,
            writeDisposition: fileFormat.writeDisposition || 'WRITE_APPEND',
            sourceFormat: 'NEWLINE_DELIMITED_JSON', // ? https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad
        };

        const params = { Bucket: bucket, Key: key }; // i.e. { Bucket: 'bq-shakhomirov.bigquery.aws', Key: 'simple_transaction' };
        // create write stream into BigQuery from ReadStream (still batchInsert operation, check quoats):
        const request = s3.getObject(params);
        const s1 = request.createReadStream();
        s1.pipe(JSONStream.parse('*')) // will parse a collection of individual JSON objects

        // .on('data', (data) => { pr(data); })
            .pipe(JSONStream.stringify(false))

            // .on('data', (data) => { pr(data); })
            .pipe(table.createWriteStream(metadata))
            .on('error', (error) => { reject(error); })
            .on('job', () => {
            // `job` is a Job object that can be used to check the status of the
            // request.
            })
            // eslint-disable-next-line no-shadow
            .on('complete', (job) => {
                resolve({ 'job': job.metadata, 'status': 'job completed' });

            // The job has completed successfully.
            });

        // .on('data', (d) => { console.log(d); ++recordsProcessed; })
        // .on('close', () => { resolve(recordsProcessed); })
        // .on('error', () => { reject(); });

    });
};

/**
 * Loads data from
 * - New line delimited JSON
 * - JSON objests string (one string)
 * file into BigQuery in batch mode which means we don't pay for this.
 * Converts JSON to CSV where each line = JSON object = SRC column type STRING
 * It's memory performant as it converts batch to stream (not BigQuery streaming).
 * BigQuery streaming !== Node streaming so even though table.insert is not a streaming API in Node terms,
 * it is a streaming API in BigQuery terms
 * ? https://googleapis.dev/nodejs/bigquery/latest/Table.html#createWriteStream
 *
 * @param {String} bucket - s3 bucket as datalake for firehose events, etc., i.e. bq-shakhomirov.bigquery.aws
 * @param {String} key - s3 object file key, i.e. simple_transaction (integration test)
 * @param {String} tableId - destination BigQuery table where the file will be uploaded, comes from ./config.json
 * @param {String} datasetId - destination BigQuery dataset
 *
 * @returns {Promise} Promise that resolves when node stream finished
 * JSONStream.stringify() will create an array, (with default options open='[\n', sep='\n,\n', close='\n]\n')
 * If you call JSONStream.stringify(false) the elements will only be seperated by a newline.
 */

const loadJsonFileAsCsvFromS3 = async(bucket, key, tableId, datasetId, fileFormat) => {
    return new Promise((resolve, reject) => {
        const dataset = bigquery.dataset(datasetId);
        const table = dataset.table(tableId);
        const formattedFileLocation = `${bucket.replace(/\//g, '___').replace(/\./g, '__')}___${key.replace(/\//g, '___').replace(/\./g, '__')}`; // this will insert custom jobId into INFORMATION_SCHEMA after successfull load. Has to be alphanumeric incl. "_" and "-". "___" for "/" and "__" for "."
        // Let BigQuery handle file duplicates:
        const job = CHECK_FILE_DUPLICATES === 'true' ? formattedFileLocation : `${formattedFileLocation}_${moment().format('YYYYMMDDTHHmmss')}`;

        const metadata = {
            jobId: job,
            writeDisposition: fileFormat.writeDisposition || 'WRITE_APPEND',
            sourceFormat: 'CSV',
            allowJaggedRows: true,
            skipLeadingRows: 0,
            field_delimiter: fileFormat.delimiter, // 'þ'
        };

        const params = { Bucket: bucket, Key: key }; // i.e. { Bucket: 'bq-shakhomirov.bigquery.aws', Key: 'simple_transaction' };
        // create write stream into BigQuery from ReadStream (still batchInsert operation, check quoats):
        const request = s3.getObject(params);
        const s1 = request.createReadStream();
        s1.pipe(JSONStream.parse(true))

            // .on('data', (data) => { pr(data); })
            .pipe(JSONStream.stringify(false))

            // .on('data', (data) => { pr(data); })
            .pipe(table.createWriteStream(metadata))
            .on('error', (error) => { reject(error); })
            .on('job', () => {
            // `job` is a Job object that can be used to check the status of the
            // request.
            })
            // eslint-disable-next-line no-shadow
            .on('complete', (job) => {
                resolve({ 'job': job.metadata, 'status': 'job completed' });

            // The job has completed successfully.
            });

        // .on('data', (d) => { console.log(d); ++recordsProcessed; })
        // .on('close', () => { resolve(recordsProcessed); })
        // .on('error', () => { reject(); });

    });
};

/**
 * Loads data from
 * - New line delimited JSON
 * from an Array of individual JSON objests
 * Converts JSON to CSV where each line = JSON object = SRC column type STRING
 *
 * @param {String} bucket - s3 bucket as datalake for firehose events, etc., i.e. bq-shakhomirov.bigquery.aws
 * @param {String} key - s3 object file key, i.e. simple_transaction (integration test)
 * @param {String} tableId - destination BigQuery table where the file will be uploaded, comes from ./config.json
 * @param {String} datasetId - destination BigQuery dataset
 *
 * @returns {Promise} Promise that resolves when node stream finished
 * JSONStream.stringify() will create an array, (with default options open='[\n', sep='\n,\n', close='\n]\n')
 * If you call JSONStream.stringify(false) the elements will only be seperated by a newline.
 */

const loadOuterArrayJsonFileAsCsvFromS3 = async(bucket, key, tableId, datasetId, fileFormat) => {
    return new Promise((resolve, reject) => {
        const dataset = bigquery.dataset(datasetId);
        const table = dataset.table(tableId);
        const formattedFileLocation = `${bucket.replace(/\//g, '___').replace(/\./g, '__')}___${key.replace(/\//g, '___').replace(/\./g, '__')}`; // this will insert custom jobId into INFORMATION_SCHEMA after successfull load. Has to be alphanumeric incl. "_" and "-". "___" for "/" and "__" for "."
        // Let BigQuery handle file duplicates:
        const job = CHECK_FILE_DUPLICATES === 'true' ? formattedFileLocation : `${formattedFileLocation}_${moment().format('YYYYMMDDTHHmmss')}`;

        const metadata = {
            jobId: job,
            writeDisposition: fileFormat.writeDisposition || 'WRITE_APPEND',
            sourceFormat: 'CSV',
            allowJaggedRows: true,
            skipLeadingRows: 0,
            field_delimiter: fileFormat.delimiter, // 'þ'
        };

        const params = { Bucket: bucket, Key: key }; // i.e. { Bucket: 'bq-shakhomirov.bigquery.aws', Key: 'simple_transaction' };
        // create write stream into BigQuery from ReadStream (still batchInsert operation, check quoats):
        const request = s3.getObject(params);
        const s1 = request.createReadStream();
        s1.pipe(JSONStream.parse('*'))

            // .on('data', (data) => { pr(data); })
            .pipe(JSONStream.stringify(false))

            // .on('data', (data) => { pr(data); })
            .pipe(table.createWriteStream(metadata))
            .on('error', (error) => { reject(error); })
            .on('job', () => {
            // `job` is a Job object that can be used to check the status of the
            // request.
            })
            // eslint-disable-next-line no-shadow
            .on('complete', (job) => {
                resolve({ 'job': job.metadata, 'status': 'job completed' });

            // The job has completed successfully.
            });

        // .on('data', (d) => { console.log(d); ++recordsProcessed; })
        // .on('close', () => { resolve(recordsProcessed); })
        // .on('error', () => { reject(); });

    });
};

/**
 * Loads data from gzipped (.gz)
 * - New line delimited JSON
 * - JSON objests string (one string)
 *
 * @param {String} bucket - s3 bucket as datalake for firehose events, etc., i.e. bq-shakhomirov.bigquery.aws
 * @param {String} key - s3 object file key, i.e. simple_transaction (integration test)
 * @param {String} tableId - destination BigQuery table where the file will be uploaded, comes from ./config.json
 * @param {String} datasetId - destination BigQuery dataset
 *
 * @returns {Promise} Promise that resolves when node stream finished
 *
 */

const loadGzJsonFileFromS3 = async(bucket, key, tableId, datasetId, fileFormat) => {
    return new Promise((resolve, reject) => {
        const dataset = bigquery.dataset(datasetId);
        const table = dataset.table(tableId);
        const formattedFileLocation = `${bucket.replace(/\//g, '___').replace(/\./g, '__')}___${key.replace(/\//g, '___').replace(/\./g, '__')}`; // this will insert custom jobId into INFORMATION_SCHEMA after successfull load. Has to be alphanumeric incl. "_" and "-". "___" for "/" and "__" for "."
        // Let BigQuery handle file duplicates:
        const job = CHECK_FILE_DUPLICATES === 'true' ? formattedFileLocation : `${formattedFileLocation}_${moment().format('YYYYMMDDTHHmmss')}`;

        const metadata = {
            jobId: job,
            writeDisposition: fileFormat.writeDisposition || 'WRITE_APPEND',
            sourceFormat: 'NEWLINE_DELIMITED_JSON', // ? https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad
        };

        const params = { Bucket: bucket, Key: key }; // i.e. { Bucket: 'bq-shakhomirov.bigquery.aws', Key: 'simple_transaction' };
        // create write stream into BigQuery from ReadStream (still batchInsert operation, check quoats):
        const request = s3.getObject(params);

        const s1 = request.createReadStream();
        s1.pipe(zlib.createGunzip())
            .pipe(JSONStream.parse(true))

            // .on('data', (data) => { pr(data); })
            .pipe(JSONStream.stringify(false))

            // .on('data', (data) => { pr(data); })
            .pipe(table.createWriteStream(metadata))
            .on('error', (error) => { reject(error); })
            .on('job', () => {
            // `job` is a Job object that can be used to check the status of the
            // request.
            })
            // eslint-disable-next-line no-shadow
            .on('complete', (job) => {
                resolve({ 'job': job.metadata, 'status': 'job completed' });

            // The job has completed successfully.
            });

        // .on('data', (d) => { console.log(d); ++recordsProcessed; })
        // .on('close', () => { resolve(recordsProcessed); })
        // .on('error', () => { reject(); });

    });
};

/**
 * creates BigQuery table with required schema and partition.
 *
 * @param {String} tableId - destination BigQuery table where the file will be uploaded, comes from ./config.json
 * @param {String} schema - schema definition, i.e. "transaction_id:INT64, user_id:INT64, dt:date"
 * @param {String} datasetId - destination BigQuery dataset
 * @param {String} partitionField - destination table partitioning, i.e. "dt"
 *
 * @returns {Promise} Promise that resolves when node stream finished
 */

const createBigQueryTablePartitioned = async(tableId, tableSchema, datasetId, partitionField, tableNotes, datasetLocation = 'US', timePartition = 'DAY') => {

    // For all options, see https://cloud.google.com/bigquery/docs/reference/v2/tables#resource

    const options = {
        // eslint-disable-next-line object-shorthand
        schema: {
            'fields': tableSchema, // ? https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema
        }, // tableSchema,
        location: datasetLocation,
        timePartitioning: {
            type: timePartition,
            field: partitionField,
        },
        description: tableNotes,
    };

    // Create a new table in the dataset
    // ? https://googleapis.dev/nodejs/bigquery/latest/Dataset.html#createTable
    const [table] = await bigquery
        .dataset(datasetId)
        .createTable(tableId, options);
    console.log(`Table ${table.id} created with partitioning: `);
    console.log(table.metadata.timePartitioning);
    return table.id;

};

/**
 * Loads JSON data into BigQuery table in BigQuery streaming mode.
 *
 * @returns {Promise} Promise that resolves when node stream finished
 */
// eslint-disable-next-line no-unused-vars
const loadTestJsonDataBigQueryStreaming = async() => {

    const datasetId = 'source';
    const tableId = 'simple_transaction';

    const jsonData = [{
        'transaction_id': 100,
        'user_id': 999,
        'dt': '2021-08-01',
    },
    {
        'transaction_id': 101,
        'user_id': 999,
        'dt': '2021-08-01',
    },
    ];

    const job = await bigquery
        .dataset(datasetId)
        .table(tableId)
        .insert(jsonData)
        .then((data) => {
            const apiResponse = data;
            console.log(`apiResponse:: ${apiResponse}`);
        })
        .catch((err) => { console.log(`err: ${err}`); });
};

/**
 * Loads raw JSON data into BigQuery table in BigQuery streaming mode.
 * @param {Number} id - insertId to reload, i.e. 2.
 * @param {Object} metadata - job metadata object, i.e. schema fields to parse from JSON and sourceFormat.
 *
 * @returns {Promise} Promise that resolves when node stream finished
 */
// eslint-disable-next-line no-unused-vars
const loadTestRawJsonDataBigQueryStreaming = async(id, metadata) => {

    const datasetId = 'source';
    const tableId = 'simple_transaction';

    // Configure the load job. For full list of options, see:
    // https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad
    // const metadata = {
    //     sourceFormat: 'NEWLINE_DELIMITED_JSON',
    //     schema: {
    //         fields: [
    //             { name: 'transaction_id', type: 'INT64' },
    //             { name: 'user_id', type: 'INT64' },
    //             { name: 'date', type: 'DATE' },
    //         ],
    //     },
    //     location: 'US',
    // };
    const jsonData = {
        'transaction_id': 333,
        'user_id': 999,

    };

    const raw = {
        insertId: id,
        json: jsonData,
    };

    const job = await bigquery
        .dataset(datasetId)
        .table(tableId)

        // .load(jsonData, metadata);
        .insert(raw, { raw: true })
        .then((data) => {
            const apiResponse = data;
            console.log(`apiResponse:: ${apiResponse}`);
        })
        .catch((err) => { console.log(`err: ${err}`); });

    // load() waits for the job to finish
    // console.log(`Job ${job.id} completed.`);
};

