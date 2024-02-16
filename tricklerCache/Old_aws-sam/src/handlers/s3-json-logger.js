'use strict';
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
import { ListObjectsV2Command, PutObjectCommand, S3Client, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import fetch, { Headers } from 'node-fetch';
import { parse } from 'csv-parse';
import { SQSClient, SendMessageCommand, } from '@aws-sdk/client-sqs';
// import 'source-map-support/register'
// import { packageJson } from '@aws-sdk/client3/package.json'
// const version = packageJson.version
const sqsClient = new SQSClient({});
const s3 = new S3Client({ region: 'us-east-1' });
let workQueuedSuccess = false;
let POSTSuccess = false;
let xmlRows = '';
let customersConfig = {};
let tcc = {};
let sqsBatchFail = {
    batchItemFailures: [
        {
            itemIdentifier: ''
        }
    ]
};
sqsBatchFail.batchItemFailures.pop();
let tcLogInfo = true;
let tcLogDebug = false;
let tcLogVerbose = false;
let tcSelectiveDebug; //call out selective debug as an option
//Of concern, very large data sets
// - as of 10/2023 CSV handled as the papaparse engine handles the record boundary,
//  Now need to solve for JSON content
//
// But how to parse each chunk for JSON content as each chunk is a
// network chunk that can land on any or no record boundary.
//      Use a JSON Parser that handles record boundary just like the CSV parser?
//      Parse out individual Updates from the JSON in the Read Stream using start/stop index
//          of the stream/content?
//
// As of 10/2023 implemented an sqs Queue and deadletter queue,
//      Multi-GB files are parsed into "99 row updates", written to an S3 "Process" bucket and
//      an entry added to the sqs Queue that will trigger a 2nd Lambda to process each 'chunk' of 99
//
// If there is an exception processing an S3 file
// Write what data can be processed as 99 updates, and simply throw the exception which will not
// delete the S3 file for later inspection and re-processing
//      Can the same file simply be reprocessed/requeued without issue?
//          Row updates would be duplicated but would that matter?
//
// ToDo: 
//Interface to View and Edit Customer Configs
//Interface to view Logs/Errors (echo cloudwatch logs?)
//
/**
 * A Lambda function to process the Event payload received from SQS - AWS Queues.
 */
export const tricklerQueueProcessorHandler = (event, context) => __awaiter(void 0, void 0, void 0, function* () {
    //
    // ToDo: Confirm SQS Queue deletes queued work
    //       
    //      Test Foreach for multiple Queue Events processing, by default SQS polls 5 events at a time unless Queue
    //          has more events to process and then multiple Lambdas and number of Events increased
    if (process.env.ProcessQueueVisibilityTimeout === undefined ||
        process.env.ProcessQueueVisibilityTimeout === '' ||
        process.env.ProcessQueueVisibilityTimeout === null) {
        tcc = yield getTricklerConfig();
    }
    if (tcc.SelectiveDebug.indexOf("_9") > -1)
        console.info(`Selective Debug 9 - Process Environment Vars: ${JSON.stringify(process.env)}`);
    if (tcc.ProcessQueueQuiesce) {
        console.info(`Work Process Queue Quiesce is in effect, no New Work will be Queued up in the SQS Process Queue.`);
        return;
    }
    if (tcc.QueueBucketPurgeCount > 0) {
        console.info(`Purge Requested, Only action will be to Purge ${tcc.QueueBucketPurge} of ${tcc.QueueBucketPurgeCount} Records. `);
        const d = yield purgeBucket(tcc.QueueBucketPurgeCount, tcc.QueueBucketPurge);
        return d;
    }
    // Backoff strategy for failed invocations
    //When an invocation fails, Lambda attempts to retry the invocation while implementing a backoff strategy.
    // The backoff strategy differs slightly depending on whether Lambda encountered the failure due to an error in
    //  your function code, or due to throttling.
    //If your function code caused the error, Lambda gradually backs off retries by reducing the amount of 
    // concurrency allocated to your Amazon SQS event source mapping.If invocations continue to fail, Lambda eventually
    //  drops the message without retrying.
    //If the invocation fails due to throttling, Lambda gradually backs off retries by reducing the amount of 
    // concurrency allocated to your Amazon SQS event source mapping.Lambda continues to retry the message until 
    // the message's timestamp exceeds your queue's visibility timeout, at which point Lambda drops the message.
    let postResult = 'false';
    console.info(`Received SQS Events Batch of ${event.Records.length} records.`);
    // event.Records.forEach((i) => {
    //     sqsBatchFail.batchItemFailures.push({ itemIdentifier: i.messageId })
    // })
    //Empty BatchFail array 
    sqsBatchFail.batchItemFailures.forEach(() => {
        sqsBatchFail.batchItemFailures.pop();
    });
    //Process this Inbound Batch 
    for (const q of event.Records) {
        // event.Records.forEach(async (i: SQSRecord) => {
        const tqm = JSON.parse(q.body);
        tqm.workKey = JSON.parse(q.body).workKey;
        //When Testing - get some actual work queued
        if (tqm.workKey === 'process_2_pura_2023_10_27T15_11_40_732Z.csv') {
            tqm.workKey = yield getAnS3ObjectforTesting('tricklercache-process');
        }
        debugger;
        console.info(`Processing Work Queue for ${tqm.workKey}`);
        if (tcc.SelectiveDebug.indexOf("_11") > -1)
            console.info(`Selective Debug 11 - SQS Events Batch Item ${JSON.stringify(q)}`);
        try {
            const work = yield getS3Work(tqm.workKey);
            if (work.length > 0) //Retreive Contents of the Work File  
             {
                postResult = yield postToCampaign(work, tqm.custconfig, tqm.updateCount);
                if (tcc.SelectiveDebug.indexOf("_8") > -1)
                    console.info(`POST Result for ${tqm.workKey}: ${postResult}`);
                if (postResult.indexOf('retry') > -1) {
                    console.warn(`Retry Marked for ${tqm.workKey} (Batch Fails: ${sqsBatchFail.batchItemFailures.length + 1}) Returning Work Item ${q.messageId} to Process Queue.`);
                    //Add to BatchFail array to Retry processing the work 
                    sqsBatchFail.batchItemFailures.push({ itemIdentifier: q.messageId });
                }
                if (postResult.toLowerCase().indexOf('unsuccessful post') > -1)
                    console.error(`Error - Unsuccesful POST for ${tqm.workKey}: ${postResult}`);
                if (postResult.toLowerCase().indexOf('successfully posted') > -1) {
                    console.info(`Work Successfully Posted to Campaign (${tqm.workKey}), Deleting Work from S3 Process Queue`);
                    const d = yield deleteS3Object(tqm.workKey, 'tricklercache-process');
                    if (d === '204')
                        console.info(`Successful Deletion of Work: ${tqm.workKey}`);
                    else
                        console.error(`Failed to Delete ${tqm.workKey}. Expected '204' but received ${d}`);
                }
                if (tcc.SelectiveDebug.indexOf("_12") > -1)
                    console.info(`Selective Debug 12 - SQS Events BatchFail \n${JSON.stringify(sqsBatchFail)}`);
            }
            else
                throw new Error(`Failed to retrieve work file (${tqm.workKey}) `);
        }
        catch (e) {
            console.error(`Exception processing a Work File (${tqm.workKey} - \n${e} \n${JSON.stringify(tqm)}`);
        }
    }
    console.info(`Processed ${event.Records.length} Work Queue records. Items Fail Count: ${sqsBatchFail.batchItemFailures.length}\nItems Failed List: ${JSON.stringify(sqsBatchFail)}`);
    return sqsBatchFail;
    //For debugging - report no fails 
    // return {
    //     batchItemFailures: [
    //         {
    //             itemIdentifier: ''
    //         }
    //     ]
    // }
});
/**
 * A Lambda function to process the Event payload received from S3.
 */
export const s3JsonLoggerHandler = (event, context) => __awaiter(void 0, void 0, void 0, function* () {
    //ToDo: 
    //  ** Currently throttled to a single event each time, Add processing of multiple Events per invocation
    //
    //  ** Process to ReParse Cache and create Work Files
    //
    // Column mapping - Inbound Column Name to Table Column Mapping
    // Type Validation - Inbound Data type validation to Mapping Type
    //
    //ToDo: Resolve the progression of these steps, currently "Completed" is logged regardless of 'completion'
    // and Delete happens regardless of 'completion' being successful or not
    //
    // INFO Started Processing inbound data(pura_2023_11_16T20_24_37_627Z.csv)
    // INFO Completed processing inbound S3 Object Stream undefined
    // INFO Successful Delete of pura_2023_11_16T20_24_37_627Z.csv(Result 204)
    //
    //When Local Testing - pull an S3 Object and so avoid the not-found error
    if (!event.Records[0].s3.object.key || event.Records[0].s3.object.key === 'devtest.csv') {
        event.Records[0].s3.object.key = yield getAnS3ObjectforTesting(event.Records[0].s3.bucket.name);
    }
    if (process.env.ProcessQueueVisibilityTimeout === undefined ||
        process.env.ProcessQueueVisibilityTimeout === '' ||
        process.env.ProcessQueueVisibilityTimeout === null) {
        tcc = yield getTricklerConfig();
    }
    if (tcc.SelectiveDebug.indexOf("_9") > -1)
        console.info(`Selective Debug 9 - Process Environment Vars: ${JSON.stringify(process.env)}`);
    if (tcc.CacheBucketPurgeCount > 0) {
        console.warn(`Purge Requested, Only action will be to Purge ${tcc.CacheBucketPurge} of ${tcc.CacheBucketPurgeCount} Records. `);
        const d = yield purgeBucket(tcc.CacheBucketPurgeCount, tcc.CacheBucketPurge);
        return d;
    }
    if (tcc.CacheBucketQuiesce) {
        console.warn(`Trickler Cache Quiesce is in effect, new S3 Files will be ignored and not processed from the S3 Cache Bucket.\nTo Process files that have arrived during a Quiesce of the Cache, use the TricklerCacheProcess(S3File) Utility.`);
        return;
    }
    console.info(`Received Batch of ${event.Records.length} S3 Events (${event.Records[0].responseElements['x-amz-request-id']}). `);
    for (const r of event.Records) {
        let key = '';
        // {
        //     const contents = await fs.readFile(file, 'utf8')
        // }
        // event.Records.forEach(async (r: S3EventRecord) => {
        key = r.s3.object.key;
        const bucket = r.s3.bucket.name;
        try {
            customersConfig = yield getCustomerConfig(key);
            console.info(`Processing inbound data for ${key}, Customer is ${customersConfig.customer}`);
            const processS3ObjectStream = yield processS3ObjectContentStream(key, bucket, customersConfig);
            if (tcc.SelectiveDebug.indexOf("_3") > -1)
                console.info(`Selective Debug 3 - Return from Process S3 Object Content Stream - ${processS3ObjectStream}`);
            //Once successful delete the original S3 Object
            const delResultCode = yield deleteS3Object(key, bucket);
            if (delResultCode !== '204')
                throw new Error(`Invalid Delete of ${key}, Expected 204 result code, received ${delResultCode}`);
            else
                console.info(`Successful Delete of ${key}  (Result ${delResultCode}) `);
            const l = `Completed processing the S3 Object Stream for ${key}`;
            console.info(l);
            console.info(`TricklerCache Processing of S3 Object ${key} Completed.`);
            return { l, key, delResultCode };
        }
        catch (e) {
            console.error(`Exception processing S3 Event (for ${key}) \n${e}`);
        }
    }
    //Check for important Config updates
    checkForTCConfigUpdates();
    return `TricklerCache Processing of Request Id ${event.Records[0].responseElements['x-amz-request-id']} Completed.`;
});
export default s3JsonLoggerHandler;
function checkForTCConfigUpdates() {
    if (tcLogDebug)
        console.info(`Checking for TricklerCache Config updates`);
    getTricklerConfig();
    if (tcc.SelectiveDebug.indexOf("_1") > -1)
        console.info(`Refreshed TricklerCache Config \n ${JSON.stringify(tcc)}`);
}
function getTricklerConfig() {
    return __awaiter(this, void 0, void 0, function* () {
        //populate env vars with tricklercache config
        const getObjectCmd = {
            Bucket: 'tricklercache-configs',
            Key: 'tricklercache_config.json',
        };
        let tc = {};
        try {
            tc = yield s3.send(new GetObjectCommand(getObjectCmd))
                .then((getConfigS3Result) => __awaiter(this, void 0, void 0, function* () {
                var _a;
                const cr = (yield ((_a = getConfigS3Result.Body) === null || _a === void 0 ? void 0 : _a.transformToString('utf8')));
                return JSON.parse(cr);
            }));
        }
        catch (e) {
            console.error(`Pulling TricklerConfig Exception \n ${e}`);
        }
        try {
            if (tc.LOGLEVEL !== undefined && tc.LOGLEVEL.toLowerCase().indexOf('debug') > -1)
                tcLogDebug = true;
            if (tc.LOGLEVEL !== undefined && tc.LOGLEVEL.toLowerCase().indexOf('verbose') > -1)
                tcLogVerbose = true;
            if (tc.SelectiveDebug !== undefined)
                tcSelectiveDebug = tc.SelectiveDebug;
            if (tc.SQS_QUEUE_URL !== undefined)
                process.env.SQS_QUEUE_URL = tc.SQS_QUEUE_URL;
            else
                throw new Error(`Tricklercache Config invalid definition: SQS_QUEUE_URL - ${tc.SQS_QUEUE_URL}`);
            if (tc.xmlapiurl != undefined)
                process.env.xmlapiurl = tc.xmlapiurl;
            else
                throw new Error(`Tricklercache Config invalid definition: xmlapiurl - ${tc.xmlapiurl}`);
            if (tc.restapiurl !== undefined)
                process.env.restapiurl = tc.restapiurl;
            else
                throw new Error(`Tricklercache Config invalid definition: restapiurl - ${tc.restapiurl}`);
            if (tc.authapiurl !== undefined)
                process.env.authapiurl = tc.authapiurl;
            else
                throw new Error(`Tricklercache Config invalid definition: authapiurl - ${tc.authapiurl}`);
            if (tc.ProcessQueueQuiesce !== undefined) {
                process.env.ProcessQueueQuiesce = tc.ProcessQueueQuiesce.toString();
            }
            else
                throw new Error(`Tricklercache Config invalid definition: ProcessQueueQuiesce - ${tc.ProcessQueueQuiesce}`);
            if (tc.ProcessQueueVisibilityTimeout !== undefined)
                process.env.ProcessQueueVisibilityTimeout = tc.ProcessQueueVisibilityTimeout.toFixed();
            else
                throw new Error(`Tricklercache Config invalid definition: ProcessQueueVisibilityTimeout - ${tc.ProcessQueueVisibilityTimeout}`);
            if (tc.ProcessQueueWaitTimeSeconds !== undefined)
                process.env.ProcessQueueWaitTimeSeconds = tc.ProcessQueueWaitTimeSeconds.toFixed();
            else
                throw new Error(`Tricklercache Config invalid definition: ProcessQueueWaitTimeSeconds - ${tc.ProcessQueueWaitTimeSeconds}`);
            if (tc.RetryQueueVisibilityTimeout !== undefined)
                process.env.RetryQueueVisibilityTimeout = tc.ProcessQueueWaitTimeSeconds.toFixed();
            else
                throw new Error(`Tricklercache Config invalid definition: RetryQueueVisibilityTimeout - ${tc.RetryQueueVisibilityTimeout}`);
            if (tc.RetryQueueInitialWaitTimeSeconds !== undefined)
                process.env.RetryQueueInitialWaitTimeSeconds = tc.RetryQueueInitialWaitTimeSeconds.toFixed();
            else
                throw new Error(`Tricklercache Config invalid definition: RetryQueueInitialWaitTimeSeconds - ${tc.RetryQueueInitialWaitTimeSeconds}`);
            if (tc.MaxBatchesWarning !== undefined)
                process.env.RetryQueueInitialWaitTimeSeconds = tc.MaxBatchesWarning.toFixed();
            else
                throw new Error(`Tricklercache Config invalid definition: MaxBatchesWarning - ${tc.MaxBatchesWarning}`);
            if (tc.CacheBucketQuiesce !== undefined) {
                process.env.CacheBucketQuiesce = tc.CacheBucketQuiesce.toString();
            }
            else
                throw new Error(`Tricklercache Config invalid definition: CacheBucketQuiesce - ${tc.CacheBucketQuiesce}`);
            if (tc.CacheBucketPurge !== undefined)
                process.env.CacheBucketPurge = tc.CacheBucketPurge;
            else
                throw new Error(`Tricklercache Config invalid definition: CacheBucketPurge - ${tc.CacheBucketPurge}`);
            if (tc.CacheBucketPurgeCount !== undefined)
                process.env.CacheBucketPurgeCount = tc.CacheBucketPurgeCount.toFixed();
            else
                throw new Error(`Tricklercache Config invalid definition: CacheBucketPurgeCount - ${tc.CacheBucketPurgeCount}`);
            if (tc.QueueBucketQuiesce !== undefined) {
                process.env.QueueBucketQuiesce = tc.QueueBucketQuiesce.toString();
            }
            else
                throw new Error(`Tricklercache Config invalid definition: QueueBucketQuiesce - ${tc.QueueBucketQuiesce}`);
            if (tc.QueueBucketPurge !== undefined)
                process.env.QueueBucketPurge = tc.QueueBucketPurge;
            else
                throw new Error(`Tricklercache Config invalid definition: QueueBucketPurge - ${tc.QueueBucketPurge}`);
            if (tc.QueueBucketPurgeCount !== undefined)
                process.env.QueueBucketPurgeCount = tc.QueueBucketPurgeCount.toFixed();
            else
                throw new Error(`Tricklercache Config invalid definition: QueueBucketPurgeCount - ${tc.QueueBucketPurgeCount}`);
        }
        catch (e) {
            throw new Error(`Exception parsing TricklerCache Config File ${e}`);
        }
        if (tc.SelectiveDebug.indexOf("_1") > -1)
            console.info(`Selective Debug 1 - Pulled tricklercache_config.json: \n${JSON.stringify(tc)}`);
        return tc;
    });
}
function getCustomerConfig(filekey) {
    return __awaiter(this, void 0, void 0, function* () {
        // Retrieve file's prefix as Customer Name
        if (!filekey)
            throw new Error(`Exception - Cannot resolve Customer Config without a valid Customer Prefix (file prefix is ${filekey})`);
        const customer = filekey.split('_')[0] + '_';
        if (customer === '_' || customer.length < 4) {
            throw new Error(`Exception: Customer cannot be determined from S3 Cache File '${filekey}'      \n      `);
        }
        let configJSON = {};
        // const configObjs = [new Uint8Array()]
        const getObjectCommand = {
            Key: `${customer}config.json`,
            Bucket: `tricklercache-configs`,
        };
        let cc = {};
        try {
            yield s3.send(new GetObjectCommand(getObjectCommand))
                .then((getConfigS3Result) => __awaiter(this, void 0, void 0, function* () {
                var _a;
                const ccr = yield ((_a = getConfigS3Result.Body) === null || _a === void 0 ? void 0 : _a.transformToString('utf8'));
                if (tcc.SelectiveDebug.indexOf("_10") > -1)
                    console.info(`Selective Debug 10 - Customers Config: \n ${ccr}`);
                configJSON = JSON.parse(ccr);
            }))
                .catch((e) => {
                const err = JSON.stringify(e);
                if (err.indexOf('NoSuchKey') > -1)
                    throw new Error(`Customer Config Not Found on S3 tricklercache-configs (${customer}config.json) \nException ${e}`);
                else
                    throw new Error(`Exception Retrieving Config from S3 tricklercache-configs (${customer}config.json) Exception ${e}`);
            });
        }
        catch (e) {
            console.error(`Exception Pulling Customer Config \n${e}`);
        }
        //Was working before adding foreach block, which killed Promise, which led to rewriting this
        //block until it was discovered foreach needed to be replaced with const x of y
        //But, as config is so small probably don't need a streamreader to get the data
        //     try
        //     {
        //         await s3.send(
        // ,
        //         )
        //             .catch((err) => {
        //                 console.error(`Exception Processing Customer Config - ${err}`)
        //                 throw new Error(`Exception (Promise-Catch) Processing Customer Config: ${err} `)
        //             })
        //             .then(async (custConfigS3Result: GetObjectCommandOutput) => {
        //                 try
        //                 {
        //                     debugger
        //                     if (custConfigS3Result.$metadata.httpStatusCode != 200)
        //                     {
        //                         let errMsg = JSON.stringify(custConfigS3Result.$metadata)
        //                         throw new Error(`Get S3 Object Command failed for ${customer}config.json \n${errMsg}`)
        //                     }
        //                     // const s3Body = custConfigS3Result.Body as NodeJS.ReadableStream
        //                     const c = await custConfigS3Result.Body?.transformToString('utf8') as string
        //                     configJSON = JSON.parse(c) as customerConfig
        //                     customersConfig = await validateCustomerConfig(configJSON)
        //                     return customersConfig as customerConfig
        //                 }
        //                 catch (e)
        //                 {
        //                     console.error(`exception in then for Cust Config: ${e}`)
        //                 }
        //             })
        // configJSON = new Promise<customerConfig>(async (resolve, reject) => {
        //     if (s3Body !== undefined)
        //     {
        //         s3Body.on('data', (chunk: Uint8Array) => {
        //             configObjs.push(chunk)
        //         })
        //         s3Body.on('error', () => {
        //             reject
        //         })
        //         s3Body.on('end', () => {
        //             let cj = {} as customerConfig
        //             let cf = Buffer.concat(configObjs).toString('utf8')
        //             try
        //             {
        //                 cj = JSON.parse(cf)
        //                 // if (tcLogDebug) console.info(
        //                 //     `Parsing Config File: ${cj.customer}, Format: ${cj.format}, Region: ${cj.region}, Pod: ${cj.pod}, List Name: ${cj.listName},List  Id: ${cj.listId}, `,
        //                 // )
        //             } catch (e)
        //             {
        //                 throw new Error(`Exception Parsing Config ${customer}config.json: ${e}`)
        //             }
        //             resolve(cj)
        // //         })
        // // }
        // }).catch(e => {
        //     throw new Error(`Exception retrieving Customer Config ${customer}config.json \n${e}`)
        // })
        // } catch (e)
        // {
        //     throw new Error(`Exception retrieving Customer Config ${customer}config.json: \n ${e}`)
        // }
        customersConfig = yield validateCustomerConfig(configJSON);
        return customersConfig;
    });
}
function validateCustomerConfig(config) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!config || config === null) {
            throw new Error('Invalid Config Content - is not defined (empty or null config)');
        }
        if (!config.customer) {
            throw new Error('Invalid Config Content - Customer is not defined');
        }
        if (!config.clientId) {
            throw new Error('Invalid Config Content - ClientId is not defined');
        }
        if (!config.clientSecret) {
            throw new Error('Invalid Config Content - ClientSecret is not defined');
        }
        if (!config.format) {
            throw new Error('Invalid Config Content - Format is not defined');
        }
        if (!config.listId) {
            throw new Error('Invalid Config Content - ListId is not defined');
        }
        if (!config.listName) {
            throw new Error('Invalid Config Content - ListName is not defined');
        }
        if (!config.pod) {
            throw new Error('Invalid Config Content - Pod is not defined');
        }
        if (!config.region) {
            throw new Error('Invalid Config Content - Region is not defined');
        }
        if (!config.refreshToken) {
            throw new Error('Invalid Config Content - RefreshToken is not defined');
        }
        if (!config.format.toLowerCase().match(/^(?:csv|json)$/gim)) {
            throw new Error("Invalid Config - Format is not 'CSV' or 'JSON' ");
        }
        if (!config.pod.match(/^(?:0|1|2|3|4|5|6|7|8|9|a|b)$/gim)) {
            throw new Error('Invalid Config - Pod is not 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, A, or B. ');
        }
        if (!config.region.toLowerCase().match(/^(?:us|eu|ap|ca)$/gim)) {
            throw new Error("Invalid Config - Region is not 'US', 'EU', CA' or 'AP'. ");
        }
        return config;
    });
}
function processS3ObjectContentStream(key, bucket, custConfig) {
    return __awaiter(this, void 0, void 0, function* () {
        let chunks = new Array();
        let batchCount = 0;
        let streamResult = '';
        let closeResult = '';
        if (tcLogDebug)
            console.info(`Processing S3 Content Stream for ${key}`);
        streamResult = yield s3
            .send(new GetObjectCommand({
            Key: key,
            Bucket: bucket,
        }))
            .then((getS3StreamResult) => __awaiter(this, void 0, void 0, function* () {
            if (tcLogDebug)
                console.info(`Get S3 Object - Object returned ${key}`);
            if (getS3StreamResult.$metadata.httpStatusCode != 200) {
                let errMsg = JSON.stringify(getS3StreamResult.$metadata);
                throw new Error(`Get S3 Object Command failed for ${key}. Result is ${errMsg}`);
            }
            let recs = 0;
            let s3ContentReadableStream = getS3StreamResult.Body;
            console.info(`S3 Content Stream Opened for ${key}, Records being received... `);
            if (custConfig.format.toLowerCase() === 'csv') {
                const csvParser = parse({
                    delimiter: ',',
                    columns: true,
                    comment: '#',
                    trim: true,
                    skip_records_with_error: true,
                });
                s3ContentReadableStream = s3ContentReadableStream.pipe(csvParser); //, { end: false })
                // .on('error', function (err) {
                //     console.error(`CSVParse(${key}) - Error ${err}`)
                //     debugger
                // })
                // .on('end', function (e: string) {
                //     console.info(`CSVParse(${key}) - OnEnd - Message: ${e} \nDebugData: ${JSON.stringify(debugData)}`)
                //     debugger
                // })
                // .on('finish', function (f: string) {
                //     console.info(`CSVParse(${key}) - OnFinish ${f}`)
                //     debugger
                // })
                // .on('close', function (c: string) {
                //     console.info(`CSVParse(${key}) - OnClose ${c}`)
                //     console.info(`Stream Closed \n${JSON.stringify(debugData)}`)
                //     debugger
                // })
                // .on('skip', async function (err) {
                //     console.info(`CSVParse(${key}) - Invalid Record \nError: ${err.code} for record ${err.lines}.\nOne possible cause is a field containing commas ',' and not properly Double-Quoted. \nContent: ${err.record} \nMessage: ${err.message} \nStack: ${err.stack} `)
                //     debugger
                // })
                // .on('data', function (f: string) {
                //     console.info(`CSVParse(${key}) - OnData ${f}`)
                //     // debugger
                // })
            }
            s3ContentReadableStream.setMaxListeners(tcc.EventEmitterMaxListeners);
            // const streamPromise = await new Promise( () =>
            s3ContentReadableStream
                .on('error', function (err) {
                return __awaiter(this, void 0, void 0, function* () {
                    chunks = [];
                    batchCount = 0;
                    const errMessage = `An error has stopped Content Parsing at record ${recs} for s3 object ${key}.\n${err}`;
                    recs = 0;
                    console.error(errMessage);
                    throw new Error(errMessage);
                });
            })
                .on('data', function (s3Chunk) {
                return __awaiter(this, void 0, void 0, function* () {
                    recs++;
                    if (recs > custConfig.updateMaxRows)
                        throw new Error(`The number of Updates in this batch Exceeds Max Row Updates allowed ${recs} in the Customers Config`);
                    if (tcLogVerbose)
                        console.info(`s3ContentStream OnData - Another chunk (ArrayLen:${chunks.length} Recs:${recs} Batch:${batchCount} from ${key} - ${JSON.stringify(s3Chunk)}`);
                    chunks.push(s3Chunk);
                    if (chunks.length > 98) {
                        batchCount++;
                        const a = chunks;
                        chunks = [];
                        if (tcLogDebug)
                            console.info(`s3ContentStream OnData Over 99 Records - Queuing Work (Batch: ${batchCount} Chunks: ${a.length}) from ${key}`);
                        const sqwResult = yield storeAndQueueWork(a, key, custConfig, batchCount);
                        if (tcLogDebug)
                            console.info(`Await of Store And Queue Work returns - ${sqwResult}`);
                    }
                });
            })
                .on('end', function (msg) {
                return __awaiter(this, void 0, void 0, function* () {
                    batchCount++;
                    const endResult = `S3 Content Stream Ended for ${key}. Processed ${recs} records as ${batchCount} batches.`;
                    console.info(endResult);
                    const d = chunks;
                    chunks = [];
                    batchCount = 0;
                    recs = 0;
                    const sqwResult = yield storeAndQueueWork(d, key, custConfig, batchCount);
                    if (tcc.SelectiveDebug.indexOf("_2") > -1)
                        console.info(`Selective Debug 2: End of Queueing Work for (${key}) Result: ${sqwResult}`);
                    return { endResult, sqwResult };
                });
            })
                .on('close', function (msg) {
                return __awaiter(this, void 0, void 0, function* () {
                    chunks = [];
                    batchCount = 0;
                    recs = 0;
                    closeResult = `S3ContentStream OnClose - S3 Content Streaming has Closed, successfully processed ${recs} records from ${key}`;
                    if (tcLogDebug)
                        console.info(closeResult);
                    console.info(`S3 Content Stream Closed for ${key}.`);
                });
            });
            // }).catch(e => {
            //     // throw new Error(`Exception Processing (Promise) S3 Get Object Content for ${key}: \n ${e}`);
            //     console.error(
            //         `Exception Processing (Await S3 Body) S3 Get Object Content for ${key}: \n ${e}`,
            //     )
            // })
            return streamResult;
        }));
        // return { s3ContentResults, workQueuedSuccess }
        // return s3ContentStream
        // return closeResult
        return streamResult;
    });
}
function storeAndQueueWork(chunks, s3Key, config, batch) {
    return __awaiter(this, void 0, void 0, function* () {
        if (batch > tcc.MaxBatchesWarning)
            console.warn(`Warning: Updates from the S3 Object (${s3Key}) are exceeding (${batch}) the Warning Limit of ${tcc.MaxBatchesWarning} Batches per Object.`);
        // throw new Error(`Updates from the S3 Object (${s3Key}) Exceed (${batch}) Safety Limit of 20 Batches of 99 Updates each. Exiting...`)
        xmlRows = convertToXMLUpdates(chunks, config);
        const key = `process_${batch}_${s3Key}`;
        if (tcLogDebug)
            console.info(`Queuing Work for ${key},  Batch - ${batch},  Records - ${chunks.length} `);
        const AddWorkToS3ProcessBucketResults = yield addWorkToS3ProcessStore(xmlRows, key);
        const AddWorkToSQSProcessQueueResults = yield addWorkToSQSProcessQueue(config, key, batch.toString(), chunks.length.toString());
        return JSON.stringify({ AddWorkToS3ProcessBucketResults, AddWorkToSQSProcessQueueResults });
    });
}
function convertToXMLUpdates(rows, config) {
    if (tcLogDebug)
        console.info(`Converting S3 Content to XML Updates. Packaging ${rows.length} rows as updates to ${config.customer}'s ${config.listName}`);
    if (tcc.SelectiveDebug.indexOf("_6") > -1)
        console.info(`Selective Debug 6 - Convert to XML Updates: ${JSON.stringify(rows)}`);
    xmlRows = `<Envelope><Body><InsertUpdateRelationalTable><TABLE_ID>${config.listId}</TABLE_ID><ROWS>`;
    let r = 0;
    rows.forEach(jo => {
        r++;
        xmlRows += `<ROW>`;
        Object.entries(jo).forEach(([key, value]) => {
            // console.info(`Record ${r} as ${key}: ${value}`)
            xmlRows += `<COLUMN name="${key}"> <![CDATA[${value}]]> </COLUMN>`;
        });
        xmlRows += `</ROW>`;
    });
    //Tidy up the XML
    xmlRows += `</ROWS></InsertUpdateRelationalTable></Body></Envelope>`;
    return xmlRows;
}
function addWorkToS3ProcessStore(queueContent, key) {
    return __awaiter(this, void 0, void 0, function* () {
        //write to the S3 Process Bucket
        if (tcc.QueueBucketQuiesce) {
            console.warn(`Work/Process Bucket Quiesce is in effect, no New Work Files will be written to the S3 Queue Bucket.`);
            return;
        }
        const s3PutInput = {
            Body: queueContent,
            Bucket: 'tricklercache-process',
            Key: key,
        };
        if (tcLogDebug)
            console.info(`Write Work to S3 Process Queue for ${key}`);
        let AddWorkToS3ProcessBucket;
        let S3ProcessBucketResult;
        try {
            yield s3
                .send(new PutObjectCommand(s3PutInput))
                .then((s3PutResult) => __awaiter(this, void 0, void 0, function* () {
                S3ProcessBucketResult = JSON.stringify(s3PutResult.$metadata.httpStatusCode, null, 2);
                if (S3ProcessBucketResult === '200') {
                    AddWorkToS3ProcessBucket = `Wrote Work File (${key}) to S3 Process Store (Result ${S3ProcessBucketResult})`;
                    if (tcc.SelectiveDebug.indexOf("_7") > -1)
                        console.info(`Selective Debug 7 - ${AddWorkToS3ProcessBucket}`);
                }
                else
                    throw new Error(`Failed to write Work File to S3 Process Store (Result ${S3ProcessBucketResult}) for ${key}`);
            }))
                .catch(err => {
                throw new Error(`PutObjectCommand Results Failed for (${key} to S3 Processing bucket: ${err}`);
            });
        }
        catch (e) {
            throw new Error(`PutObjectCommand Exception writing work(${key} to S3 Processing bucket: ${e}`);
        }
        return { AddWorkToS3ProcessBucket, S3ProcessBucketResult };
    });
}
function addWorkToSQSProcessQueue(config, key, batch, recCount) {
    return __awaiter(this, void 0, void 0, function* () {
        const sqsQMsgBody = {};
        sqsQMsgBody.workKey = key;
        sqsQMsgBody.attempts = 1;
        sqsQMsgBody.updateCount = recCount;
        sqsQMsgBody.custconfig = config;
        sqsQMsgBody.firstQueued = Date.now().toString();
        const sqsParams = {
            MaxNumberOfMessages: 1,
            QueueUrl: process.env.SQS_QUEUE_URL,
            VisibilityTimeout: parseInt(process.env.ProcessQueueVisibilityTimeout),
            WaitTimeSeconds: parseInt(process.env.ProcessQueueWaitTimeSeconds),
            MessageAttributes: {
                FirstQueued: {
                    DataType: 'String',
                    StringValue: Date.now().toString(),
                },
                Retry: {
                    DataType: 'Number',
                    StringValue: '0',
                },
            },
            MessageBody: JSON.stringify(sqsQMsgBody),
        };
        // sqsParams.MaxNumberOfMessages = 1
        // sqsParams.MessageAttributes.FirstQueued.StringValue = Date.now().toString()
        // sqsParams.MessageAttributes.Retry.StringValue = '0'
        // sqsParams.MessageBody = JSON.stringify(sqsQMsgBody)
        // sqsParams.QueueUrl = process.env.SQS_QUEUE_URL
        // sqsParams.VisibilityTimeout = parseInt(process.env.ProcessQueueVisibilityTimeout!)
        // sqsParams.WaitTimeSeconds = parseInt(process.env.ProcessQueueWaitTimeSeconds!)
        if (tcLogDebug)
            console.info(`Add Work to SQS Process Queue - SQS Params: ${JSON.stringify(sqsParams)}`);
        let SQSSendResult;
        let sqsWriteResult;
        try {
            yield sqsClient
                .send(new SendMessageCommand(sqsParams))
                .then((sqsSendMessageResult) => __awaiter(this, void 0, void 0, function* () {
                sqsWriteResult = JSON.stringify(sqsSendMessageResult.$metadata.httpStatusCode, null, 2);
                if (sqsWriteResult !== '200') {
                    throw new Error(`Failed writing to SQS Process Queue (queue URL: ${sqsParams.QueueUrl}), ${sqsQMsgBody.workKey}, SQS Params${JSON.stringify(sqsParams)})`);
                }
                SQSSendResult = JSON.stringify(sqsSendMessageResult);
                workQueuedSuccess = true;
                if (tcc.SelectiveDebug.indexOf("_8") > -1)
                    console.info(`Queued Work to SQS Process Queue (${sqsQMsgBody.workKey}) - Result: ${sqsWriteResult} `);
            }))
                .catch(err => {
                console.error(`Failed writing to SQS Process Queue (${err}) \nQueue URL: ${sqsParams.QueueUrl})\nWork to be Queued: ${sqsQMsgBody.workKey}\nSQS Params: ${JSON.stringify(sqsParams)})`);
            });
        }
        catch (e) {
            console.error(`Exception writing to SQS Process Queue - (queue URL${sqsParams.QueueUrl}), process_${sqsQMsgBody.workKey}, SQS Params${JSON.stringify(sqsParams)}) - Error: ${e}`);
        }
        return { sqsWriteResult, workQueuedSuccess, SQSSendResult };
    });
}
// async function reQueue (sqsevent: SQSEvent, queued: tcQueueMessage) {
//     const workKey = JSON.parse(sqsevent.Records[0].body).workKey
//     debugger
//     const sqsParams = {
//         MaxNumberOfMessages: 1,
//         QueueUrl: process.env.SQS_QUEUE_URL,
//         VisibilityTimeout: parseInt(process.env.ProcessQueueVisibilityTimeout!),
//         WaitTimeSeconds: parseInt(process.env.ProcessQueueWaitTimeSeconds!),
//         MessageAttributes: {
//             FirstQueued: {
//                 DataType: 'String',
//                 StringValue: Date.now().toString(),
//             },
//             Retry: {
//                 DataType: 'Number',
//                 StringValue: '0',
//             },
//         },
//         MessageBody: JSON.stringify(queued),
//     }
//     let maR = sqsevent.Records[0].messageAttributes.Retry.stringValue as string
//     let n = parseInt(maR)
//     n++
//     const r: string = n.toString()
//     sqsParams.MessageAttributes.Retry = {
//         DataType: 'Number',
//         StringValue: r,
//     }
//     // if (n > config.MaxRetryUpdate) throw new Error(`Queued Work ${workKey} has been retried more than 10 times: ${r}`)
//     const writeSQSCommand = new SendMessageCommand(sqsParams)
//     let qAdd
//     try
//     {
//         qAdd = await sqsClient.send(writeSQSCommand).then(async (sqsWriteResult: SendMessageCommandOutput) => {
//             const rr = JSON.stringify(sqsWriteResult.$metadata.httpStatusCode, null, 2)
//             if (tcLogDebug) console.info(`Process Queue - Wrote Retry Work to SQS Queue (process_${workKey} - Result: ${rr} `)
//             return JSON.stringify(sqsWriteResult)
//         })
//     } catch (e)
//     {
//         throw new Error(`ReQueue Work Exception - Writing Retry Work to SQS Queue: ${e}`)
//     }
//     return qAdd
// }
function updateDatabase() {
    return __awaiter(this, void 0, void 0, function* () {
        const update = `<Envelope>
          <Body>
                <AddRecipient>
                      <LIST_ID>13097633</LIST_ID>
                      <CREATED_FROM>2</CREATED_FROM>
                      <UPDATE_IF_FOUND>true</UPDATE_IF_FOUND>
                      <!-- <SYNC_FIELDS>
                        <SYNC_FIELD> 
                            <NAME>EMAIL</NAME>
                            <VALUE>a.bundy@555shoe.com</VALUE> 
                         </SYNC_FIELD>
                        </SYNC_FIELDS> -->
                      <COLUMN>
                            <NAME>EMAIL</NAME>
                            <VALUE>a.bundy@555shoe.com</VALUE>
                      </COLUMN>
                      <COLUMN>
                            <NAME>city</NAME>
                            <VALUE>Dallas</VALUE>
                      </COLUMN>
                      <COLUMN>
                            <NAME>Column_Nonexistent</NAME>
                            <VALUE>123-45-6789</VALUE>
                      </COLUMN>
                      <COLUMN>
                            <NAME>Street_Address</NAME>
                            <VALUE>123 New Street</VALUE>
                      </COLUMN>
                </AddRecipient>
          </Body>
    </Envelope>`;
    });
}
function getS3Work(s3Key) {
    return __awaiter(this, void 0, void 0, function* () {
        if (tcLogDebug)
            console.info(`Debug - GetS3Work Key: ${s3Key}`);
        const getObjectCmd = {
            Bucket: 'tricklercache-process',
            Key: s3Key,
        };
        let work = '';
        try {
            yield s3.send(new GetObjectCommand(getObjectCmd))
                .then((getS3Result) => __awaiter(this, void 0, void 0, function* () {
                var _a;
                work = (yield ((_a = getS3Result.Body) === null || _a === void 0 ? void 0 : _a.transformToString('utf8')));
                if (tcLogDebug)
                    console.info(`Work Pulled (${work.length} chars): ${s3Key}`);
            }));
        }
        catch (e) {
            const err = JSON.stringify(e);
            if (err.indexOf('NoSuchKey') > -1)
                throw new Error(`Work Not Found on S3 Process Queue (${s3Key}) Is an S3 Process Queue Management Policy Deleting Work before Processing can be accomplished?\nException ${e}`);
            else
                throw new Error(`Exception Retrieving Work from S3 Process Queue (${s3Key}) Exception ${e}`);
        }
        return work;
    });
}
export function getAccessToken(config) {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            const rat = yield fetch(`https://api-campaign-${config.region}-${config.pod}.goacoustic.com/oauth/token`, {
                method: 'POST',
                body: new URLSearchParams({
                    refresh_token: config.refreshToken,
                    client_id: config.clientId,
                    client_secret: config.clientSecret,
                    grant_type: 'refresh_token',
                }),
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'User-Agent': 'S3 TricklerCache GetAccessToken',
                },
            });
            const ratResp = (yield rat.json());
            if (rat.status != 200) {
                throw new Error(`Exception retrieving Access Token:   ${rat.status} - ${rat.statusText}`);
            }
            const accessToken = ratResp.access_token;
            return { accessToken }.accessToken;
        }
        catch (e) {
            throw new Error(`Exception during getAccessToken: \n ${e}`);
        }
    });
}
export function postToCampaign(xmlCalls, config, count) {
    var _a, _b, _c;
    return __awaiter(this, void 0, void 0, function* () {
        if (process.env.accessToken === undefined || process.env.accessToken === null || process.env.accessToken == '') {
            if (tcLogDebug)
                console.info(`POST to Campaign - Need AccessToken...`);
            process.env.accessToken = (yield getAccessToken(config));
            const l = process.env.accessToken.length;
            const redactAT = '.......' + process.env.accessToken.substring(l - 10, l);
            if (tcLogDebug)
                console.info(`Generated a new AccessToken: ${redactAT}`);
        }
        else {
            const l = (_b = (_a = process.env.accessToken) === null || _a === void 0 ? void 0 : _a.length) !== null && _b !== void 0 ? _b : 0;
            const redactAT = '.......' + ((_c = process.env.accessToken) === null || _c === void 0 ? void 0 : _c.substring(l - 8, l));
            if (tcLogDebug)
                console.info(`Access Token already stored: ${redactAT}`);
        }
        const myHeaders = new Headers();
        myHeaders.append('Content-Type', 'text/xml');
        myHeaders.append('Authorization', 'Bearer ' + process.env.accessToken);
        myHeaders.append('Content-Type', 'text/xml');
        myHeaders.append('Connection', 'keep-alive');
        myHeaders.append('Accept', '*/*');
        myHeaders.append('Accept-Encoding', 'gzip, deflate, br');
        let requestOptions = {
            method: 'POST',
            headers: myHeaders,
            body: xmlCalls,
            redirect: 'follow',
        };
        const host = `https://api-campaign-${config.region}-${config.pod}.goacoustic.com/XMLAPI`;
        if (tcc.SelectiveDebug.indexOf("_5") > -1)
            console.info(`Selective Debug 5 - Updates to POST are: ${xmlCalls}`);
        let postRes;
        // try
        // {
        postRes = yield fetch(host, requestOptions)
            .then(response => response.text())
            .then((result) => __awaiter(this, void 0, void 0, function* () {
            if (result.toLowerCase().indexOf('false</success>') > -1) {
                // "<Envelope><Body><RESULT><SUCCESS>false</SUCCESS></RESULT><Fault><Request/>
                //   <FaultCode/><FaultString>Invalid XML Request</FaultString><detail><error>
                //   <errorid>51</errorid><module/><class>SP.API</class><method/></error></detail>
                //    </Fault></Body></Envelope>\r\n"
                if (result.toLowerCase().indexOf('max number of concurrent') > -1) {
                    if (tcc.SelectiveDebug.indexOf("_4") > -1)
                        console.info(`Selective Debug 4 - Max Number of Concurrent Updates Fail - Marked for Retry`);
                    return 'retry';
                }
                else
                    return `Error - Unsuccessful POST of the Updates (${count}) - Response : ${result}`;
            }
            result = result.replace('\n', ' ');
            return `Successfully POSTed (${count}) Updates - Result: ${result}`;
        }))
            .catch(e => {
            if (e.toLowerCase().indexOf('econnreset') > -1)
                return 'retry';
            console.error(`Error - Unsuccessful POST of the Updates: ${e} - Set to Retry`);
            return 'retry';
        });
        // } catch (e)
        // {
        //     console.error(`Exception during POST to Campaign (AccessToken ${process.env.accessToken}) Result: ${e}`)
        // }
        return postRes;
    });
}
function deleteS3Object(s3ObjKey, bucket) {
    return __awaiter(this, void 0, void 0, function* () {
        let delRes = '';
        try {
            yield s3
                .send(new DeleteObjectCommand({
                Key: s3ObjKey,
                Bucket: bucket,
            }))
                .then((s3DelResult) => __awaiter(this, void 0, void 0, function* () {
                // if (tcLogDebug) console.info("Received the following Object: \n", data.Body?.toString());
                delRes = JSON.stringify(s3DelResult.$metadata.httpStatusCode, null, 2);
                if (tcLogDebug)
                    console.info(`Result from Delete of ${s3ObjKey}: ${delRes} `);
            }));
        }
        catch (e) {
            console.error(`Exception Processing S3 Delete Command for ${s3ObjKey}: \n ${e}`);
        }
        return delRes;
    });
}
function checkMetadata() {
    //Pull metadata for table/db defined in config
    // confirm updates match Columns
    // Log where Columns are not matching
}
function getAnS3ObjectforTesting(bucket) {
    return __awaiter(this, void 0, void 0, function* () {
        const listReq = {
            Bucket: bucket,
            MaxKeys: 11
        };
        let s3Key = '';
        // try
        // {
        yield s3.send(new ListObjectsV2Command(listReq))
            .then((s3ListResult) => __awaiter(this, void 0, void 0, function* () {
            var _a, _b;
            let i = 0;
            if (s3ListResult.Contents) {
                let kc = s3ListResult.KeyCount - 1;
                if (kc = 0)
                    throw new Error("No S3 Objects to retrieve as Test Data, exiting");
                if (kc > 10) {
                    i = Math.floor(Math.random() * (10 - 1 + 1) + 1);
                }
                if (kc = 1)
                    i = 0;
                s3Key = (_b = (_a = s3ListResult.Contents) === null || _a === void 0 ? void 0 : _a.at(i)) === null || _b === void 0 ? void 0 : _b.Key;
                // console.info(`S3 List:\n${JSON.stringify(s3ListResult.Contents)}`)
                // if (tcLogDebug)
                console.info(`TestRun (${i}) Retrieved ${s3Key} for this Test Run`);
            }
            else
                throw new Error(`No S3 Object available for Testing: ${bucket}`);
            return s3Key;
        }))
            .catch((e) => {
            console.error(`Exception on S3 List Command for Testing Objects from ${bucket}: ${e}`);
        });
        // .finally(() => {
        //     console.info(`S3 List Finally...`)
        // })
        // } catch (e)
        // {
        //     console.error(`Exception Processing S3 List Command: ${e} `)
        // }
        return s3Key;
        // return 'pura_2023_11_12T01_43_58_170Z.csv'
    });
}
function purgeBucket(count, bucket) {
    return __awaiter(this, void 0, void 0, function* () {
        const listReq = {
            Bucket: bucket,
            MaxKeys: count,
        };
        let d = 0;
        let r = '';
        try {
            yield s3.send(new ListObjectsV2Command(listReq)).then((s3ListResult) => __awaiter(this, void 0, void 0, function* () {
                var _a;
                (_a = s3ListResult.Contents) === null || _a === void 0 ? void 0 : _a.forEach((listItem) => __awaiter(this, void 0, void 0, function* () {
                    d++;
                    r = yield deleteS3Object(listItem.Key, bucket);
                    if (r !== '204')
                        console.error(`Non Successful return ( Expected 204 but received ${r} ) on Delete of ${listItem.Key}`);
                }));
            }));
        }
        catch (e) {
            console.error(`Exception Processing Purge of Bucket ${bucket}: \n${e}`);
        }
        console.info(`Deleted ${d} Objects from ${bucket}`);
        return `Deleted ${d} Objects from ${bucket}`;
    });
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiczMtanNvbi1sb2dnZXIuanMiLCJzb3VyY2VSb290IjoiL1VzZXJzL2t3bGFuZHJ5L0Ryb3Bib3gvRG9jdW1lbnRzL0EuQ2FtcGFpZ24vQ29kZUZvbGRlci9BLlRyaWNrbGVyQ2FjaGVfY29kZWNvbW1pdC90cmlja2xlckNhY2hlLyIsInNvdXJjZXMiOlsic3JjL2hhbmRsZXJzL3MzLWpzb24tbG9nZ2VyLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBLFlBQVksQ0FBQTs7Ozs7Ozs7OztBQUVaLE9BQU8sRUFDSCxvQkFBb0IsRUFDcEIsZ0JBQWdCLEVBQThCLFFBQVEsRUFDdEQsZ0JBQWdCLEVBQ2hCLG1CQUFtQixFQUV0QixNQUFNLG9CQUFvQixDQUFBO0FBRTNCLE9BQU8sS0FBSyxFQUFFLEVBQUUsT0FBTyxFQUF5QixNQUFNLFlBQVksQ0FBQTtBQUVsRSxPQUFPLEVBQUUsS0FBSyxFQUFFLE1BQU0sV0FBVyxDQUFBO0FBRWpDLE9BQU8sRUFDSCxTQUFTLEVBTVQsa0JBQWtCLEdBRXJCLE1BQU0scUJBQXFCLENBQUE7QUFFNUIsdUNBQXVDO0FBRXZDLDhEQUE4RDtBQUM5RCxzQ0FBc0M7QUFFdEMsTUFBTSxTQUFTLEdBQUcsSUFBSSxTQUFTLENBQUMsRUFBRSxDQUFDLENBQUE7QUFRbkMsTUFBTSxFQUFFLEdBQUcsSUFBSSxRQUFRLENBQUMsRUFBRSxNQUFNLEVBQUUsV0FBVyxFQUFFLENBQUMsQ0FBQTtBQUVoRCxJQUFJLGlCQUFpQixHQUFHLEtBQUssQ0FBQTtBQUM3QixJQUFJLFdBQVcsR0FBRyxLQUFLLENBQUE7QUFFdkIsSUFBSSxPQUFPLEdBQVcsRUFBRSxDQUFBO0FBdUJ4QixJQUFJLGVBQWUsR0FBRyxFQUFvQixDQUFBO0FBd0MxQyxJQUFJLEdBQUcsR0FBRyxFQUFjLENBQUE7QUFXeEIsSUFBSSxZQUFZLEdBQXNCO0lBQ2xDLGlCQUFpQixFQUFFO1FBQ2Y7WUFDSSxjQUFjLEVBQUUsRUFBRTtTQUNyQjtLQUNKO0NBQ0osQ0FBQTtBQUVE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