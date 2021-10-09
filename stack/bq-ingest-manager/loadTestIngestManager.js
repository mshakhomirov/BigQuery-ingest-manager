/* eslint-disable promise/catch-or-return */
/* eslint-disable camelcase */
/* eslint-disable arrow-body-style */
/* eslint-disable no-use-before-define */

const DEBUG = process.env.DEBUG;
const NODE_ENV = process.env.NODE_ENV || 'staging';
const DRY_RUN = process.env.DRY_RUN || 'true';

// 3rd party dependencies
const moment = require('moment');
const AWS = require('aws-sdk');
AWS.config.update({ region: 'eu-west-1' });
const s3 = new AWS.S3();
const lambda = new AWS.Lambda();
const fs = require('fs');
const util = require('util');

// Local dependencies
const config = require('config');

const pr = (txt) => { if (DEBUG) { console.log(txt); } };

exports.handler = async(event, context) => {

    try {

        const finalPayload = { 'Records': [] };
        for (const pipeLoadEvent of event.Records) {

            // process all pipeline files that belong to selected dateframe
            const payload = await processEventRecord(pipeLoadEvent);
            pr(JSON.stringify(payload));

            // if global DRY_RUN set to 'false' then load test bq-ingest-manager service. This is batch mode which recursively invokes service with pipeline events (1 pipe = 1 invokation = many events)
            if (DRY_RUN === 'false') {
                const response = await lambda.invoke({
                    FunctionName: config.bqIngestManagerService,

                    // InvocationType: 'Event', // The API response only includes a status code. Don't wait for Lambda to fiinish. // ? https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Lambda.html#invoke-property
                    // eslint-disable-next-line no-magic-numbers
                    Payload: JSON.stringify(payload, null, 2),
                }).promise();
                console.log(response);
            }

            finalPayload.Records = finalPayload.Records.concat(payload.Records);

        }
        pr({ 'finalPayload': `${JSON.stringify(finalPayload)}` });
        await savePayload(finalPayload);

        context.succeed((`finalPayload size: ${(finalPayload.Records.length)}`));
    } catch (e) {
        console.log(e);
        context.done(e);
    }
};

/**
 * Saves final Payload
 *
 * @param {String} finalPayload - payload for bq-ingest-manager Lambda
 *
 * @returns {Promise} Promise that resolves when saved
 */

const savePayload = async(finalPayload) => {
    try {
        const writeFile = util.promisify(fs.writeFile);
        await writeFile('./test/integration/loadTestPayload.json', JSON.stringify(finalPayload));
        console.log('File saved to ./test/integration/loadTestPayload.json');

    } catch (error) {
        console.log(error);
    }
    return 'File saved to ./test/integration/loadTestPayload.json';

};

const processEventRecord = async(eventRecord) => {
    const now = moment();
    pr(`Event bucket :: ${eventRecord.bucket.name}`);
    pr(`Event fileKey :: ${eventRecord.object.key}`);
    const pipePrefix = eventRecord.object.key;
    const sourceBucket = eventRecord.bucket.name;
    const datePrefix = eventRecord.datePrefix;
    const pipeName = eventRecord.pipeName;
    pr(`eventRecord.datePrefix:: ${eventRecord.datePrefix}`);
    try {
        // const datePrefix = '2021/09/06';
        // const prefix = pipePrefix === '/' ? `${datePrefix}` : `${pipePrefix}/${datePrefix}`;
        const prefix = `${pipePrefix}${datePrefix}`;

        pr(`bucket:: ${sourceBucket}`);
        pr(`prefix:: ${prefix}`);

        if (!eventRecord.enabled) {
            // Don't generate payload:
            return { 'Records': [] };
        }

        // otherwise get folder manifest for selected pipe:
        const params = { Bucket: sourceBucket, Prefix: prefix };
        const AWSfiles = await getFilePaths(params, pipeName, []);

        AWSfiles.forEach((entry) => pr(entry));

        const payload = { 'Records': [] };

        for (const srcKey of AWSfiles) {
            const record =
                {
                    'bucket': {
                        'name': sourceBucket,
                    },
                    'object': {
                        'key': srcKey,
                    },
                };
            payload.Records.push(record);

        }
        return payload;

    } catch (error) {

        return error;
    }

}
;

/**
 * List s3 objects, handles >1000 object limitation
 * call using an initial continuationToken = null, results = []
 *
 * @param {String} params {bucket, key} - s3 bucket as datalake for firehose events, etc., i.e. bq-shakhomirov.bigquery.aws, prefix - pipe prefix + s3 object file key, i.e. simple_transaction/2021/09/06/simple_transaction_file
 * @param {String} pipeName - file key must include pipeName in order to be selected
 * @param {List} allKeys - an array or s3 file keys.
 *
 * @returns {Promise} Promise that resolves when all S3 objects collected
 */

const getFilePaths = async(params, pipeName, allKeys = []) => {
    const response = await s3.listObjectsV2(params).promise();
    response.Contents.forEach((obj) => { if ((obj.Key).includes(pipeName)) { allKeys.push(obj.Key); } });

    if (response.NextContinuationToken) {
        params.ContinuationToken = response.NextContinuationToken;
        await getFilePaths(params, pipeName, allKeys); // RECURSIVE CALL
    }
    return allKeys;
};
