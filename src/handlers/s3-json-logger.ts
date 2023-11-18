'use strict'

import { PutObjectCommand, PutObjectCommandOutput, S3, S3Client, S3ClientConfig } from '@aws-sdk/client-s3'
import { GetObjectCommand, GetObjectCommandOutput, GetObjectCommandInput } from '@aws-sdk/client-s3'
import {
    DeleteObjectCommand,
    DeleteObjectCommandInput,
    DeleteObjectCommandOutput,
    DeleteObjectOutput,
    DeleteObjectRequest,
} from '@aws-sdk/client-s3'
import { ListObjectsV2Command, ListObjectsV2CommandInput, ListObjectsV2CommandOutput } from '@aws-sdk/client-s3'
import { Handler, S3Event, Context, SQSEvent, SQSRecord } from 'aws-lambda'
import fetch, { Headers, RequestInit, Response } from 'node-fetch'
// import { Writable, Readable, Stream, Duplex } from "node:stream";
import { parse } from 'csv-parse'

import {
    SQSClient,
    ReceiveMessageCommand,
    DeleteMessageBatchCommand,
    ReceiveMessageCommandOutput,
    Message,
    paginateListQueues,
    SendMessageCommand,
    SendMessageCommandOutput,
} from '@aws-sdk/client-sqs'
import { pipeline, finished } from 'stream/promises'
// import { ReadableStream, ReadableStreamDefaultReader } from 'node:stream/web'


// import 'source-map-support/register'

// import { packageJson } from '@aws-sdk/client3/package.json'
// const version = packageJson.version

// process.env.AWS_REGION = "us-east-1"
// process.env.accessToken = ''

//SQS
//ToDo: pickup config from tricklercache config
// process.env.SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/777957353822/tricklercacheQueue";

const sqsClient = new SQSClient({})


export type sqsObject = {
    bucketName: string
    objectKey: string
}

//-----------SQS


const s3 = new S3Client({ region: 'us-east-1' })

let workQueuedSuccess = false
let POSTSuccess = false

let xmlRows: string = ''

interface S3Object {
    Bucket: string
    Key: string
}

interface customerConfig {
    customer: string
    format: string // csv (w/ Headers), json (as idx'd), jsonFixed (fixed Columns), csvFixed (fixed Columns)
    listId: string
    listName: string
    colVals: { [idx: string]: string }
    columns: string[]
    pod: string // 1,2,3,4,5,6,7
    region: string // US, EU, AP
    updateMaxRows: number //Safety to avoid run away data inbound and parsing it all
    refreshToken: string // API Access
    clientId: string // API Access
    clientSecret: string // API Access
    MaxRetryUpdate: number  //Maximum times to retry an update to Campaign
}

let config = {} as customerConfig

export interface accessResp {
    access_token: string
    token_type: string
    refresh_token: string
    expires_in: number
}

export interface tcQueueMessage {
    workKey: string
    attempts: number
    updateCount: string
    custconfig: customerConfig
    firstQueued: string
}

export interface tcConfig {
    LOGLEVEL: string
    AWS_REGION: string
    SQS_QUEUE_URL: string
    xmlapiurl: string
    restapiurl: string
    authapiurl: string
    ProcessQueueQuiesce: boolean
    ProcessQueueVisibilityTimeout: number
    ProcessQueueWaitTimeSeconds: number
    RetryQueueVisibilityTimeout: number
    RetryQueueInitialWaitTimeSeconds: number
    EventEmitterMaxListeners: number
    CacheBucketQuiesce: boolean
    CacheBucketPurgeCount: number
    CacheBucketPurge: string
    QueueBucketQuiesce: boolean
    QueueBucketPurgeCount: number
    QueueBucketPurge: string
}

let tc = {} as tcConfig

export interface SQSBatchItemFails {
    batchItemFailures: [
        {
            itemIdentifier: string
        }
    ]
}


let sqsBatchFail: SQSBatchItemFails = {
    batchItemFailures: [
        {
            itemIdentifier: ''
        }
    ]
}

let tcLogInfo = true
let tcLogDebug = false
let tcLogVerbose = false


const csvParser = parse({
    delimiter: ',',
    comment: '#',
    trim: true,
    skip_records_with_error: true,
},
    // function (msg) {
    //     if (tcLogDebug) console.log(`CSVParse Function : ${msg}`)
    //     debugger
    // }
)





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

/**
 * A Lambda function to process the Event payload received from SQS - AWS Queues.
 */
export const tricklerQueueProcessorHandler: Handler = async (event: SQSEvent, context: Context) => {
    //ToDo: Confirm SQS Queue deletes queued work
    //      Add Foreach for multiple Queue Events processing, by default SQS polls 5 events at a time unless Queue
    //          has more events to process and then multiple Lambdas and number of Events increased

    //Interface to View and Edit Customer Configs
    //Interface to view Logs/Errors (echo cloudwatch logs?)
    //

    if (
        process.env.ProcessQueueVisibilityTimeout === undefined ||
        process.env.ProcessQueueVisibilityTimeout === '' ||
        process.env.ProcessQueueVisibilityTimeout === null
    )
    {
        // if (tcLogDebug) console.log(`Debug-Process Env not populated: ${JSON.stringify(process.env)}`)
        tc = await getTricklerConfig()
    }
    // else if (tcLogDebug) console.log(`Debug-Process Env already populated: ${JSON.stringify(process.env)}`)

    if (tc.ProcessQueueQuiesce) 
    {
        console.log(`Work Process Queue Quiesce is in effect, no New Work will be Queued up in the SQS Process Queue.`)
        return
    }

    if (tc.QueueBucketPurgeCount > 0)
    {
        console.log(`Purge Requested, Only action will be to Purge ${tc.QueueBucketPurge} of ${tc.QueueBucketPurgeCount} Records. `)
        const d = await purgeBucket(tc.QueueBucketPurgeCount, tc.QueueBucketPurge)
        return d
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


    let postResult: string = 'false'

    console.log(`SQS Events Batch (${event.Records.length} records) ${JSON.stringify(event)}`)
    debugger
    event.Records.forEach((i) => {
        sqsBatchFail.batchItemFailures.push({ itemIdentifier: i.messageId })
    })

    event.Records.forEach(async (i: SQSRecord) => {
        const tqm: tcQueueMessage = JSON.parse(i.body)

        tqm.workKey = JSON.parse(i.body).workKey

        //When Testing - get some actual work queued
        if (tqm.workKey === 'process_2_pura_2023_10_27T15_11_40_732Z.csv')
        {
            tqm.workKey = (await getAnS3ObjectforTesting('tricklercache-process')) as string
        }

        console.log(`Processing Work Queue for ${tqm.workKey}`)
        if (tcLogDebug) console.log(`Debug-Processing Work Queue - Work File is \n ${JSON.stringify(tqm)}`)

        debugger

        try
        {
            const work = await getS3Work(tqm.workKey)
            if (!work) throw new Error(`Failed to retrieve work file (${tqm.workKey})`)

            postResult = await postToCampaign(work, tqm.custconfig, tqm.updateCount)

            // if (postResult.postRes === 'retry')
            // {
            //     await reQueue(event, tqm)
            //     return postResult?.POSTSuccess
            // }

            if (postResult === 'retry')
            {
                console.log(`Failed to process work of ${tqm.workKey}. Result ${postResult} `)
                sqsBatchFail.batchItemFailures.push({ itemIdentifier: i.messageId })
            }
            else
                deleteS3Object(tqm.workKey, 'tricklercache-process')

        } catch (e)
        {
            console.log(`Processing Work Queue Exception: ${e}`)
        }

        debugger

        // console.log(`Processing Work Queue - Work (${tqm.workKey}), Result(${postResult})`)

    })

    if (tcLogDebug) console.log(`Processed Work Queue Batch of ${event.Records.length} records. Batch Item Fail Log: (${sqsBatchFail})`)

    // return sqsBatchFail

    return {
        batchItemFailures: [
            {
                itemIdentifier: ''
            }
        ]
    }
}


/**
 * A Lambda function to process the Event payload received from S3.
 */

export const s3JsonLoggerHandler: Handler = async (event: S3Event, context: Context) => {
    // if (tcLogDebug) console.log(`AWS-SDK Version: ${version}`)
    // if (tcLogDebug) console.log('ENVIRONMENT VARIABLES\n' + JSON.stringify(process.env, null, 2))


    //ToDo: Resolve the progression of these steps, currently "Completed" is logged regardless of 'completion'
    // and Delete happens regardless of 'completion' being successful or not
    //
    // INFO Started Processing inbound data(pura_2023_11_16T20_24_37_627Z.csv)
    // INFO Completed processing inbound S3 Object Stream undefined
    // INFO Successful Delete of pura_2023_11_16T20_24_37_627Z.csv(Result 204)
    // 


    //When Local Testing - pull an S3 Object and so avoid the not-found error
    if (!event.Records[0].s3.object.key || event.Records[0].s3.object.key === 'devtest.csv')
    {
        event.Records[0].s3.object.key = (await getAnS3ObjectforTesting(event.Records[0].s3.bucket.name)) as string
    }

    let key = event.Records[0].s3.object.key
    const bucket = event.Records[0].s3.bucket.name

    if (
        process.env.ProcessQueueVisibilityTimeout === undefined ||
        process.env.ProcessQueueVisibilityTimeout === '' ||
        process.env.ProcessQueueVisibilityTimeout === null
    )
    {
        // if (tcLogDebug) console.log(`Debug-Process Env not populated: ${JSON.stringify(process.env)}`)

        tc = await getTricklerConfig()

    } //else if (tcLogDebug) console.log(`Debug-Process Env already populated: ${JSON.stringify(process.env)}`)


    if (tc.CacheBucketPurgeCount > 0)
    {
        console.log(`Purge Requested, Only action will be to Purge ${tc.CacheBucketPurge} of ${tc.CacheBucketPurgeCount} Records. `)
        const d = await purgeBucket(tc.CacheBucketPurgeCount, tc.CacheBucketPurge)
        return d
    }

    if (tc.CacheBucketQuiesce) 
    {
        console.log(`Trickler Cache Quiesce is in effect, new S3 Files will be ignored and not processed from the S3 Cache Bucket.\nTo Process files that have arrived during a Quiesce of the Cache, use the TricklerCacheProcess(S3File) Utility.`)
        return
    }


    const customer = key.split('_')[0] + '_'
    // console.log("GetCustomerConfig: Customer string is ", customer)

    console.log(
        `Received S3 Trigger Event(${event.Records[0].responseElements['x-amz-request-id']}) Customer indicated is ${customer}`,
    )

    //Just in case we start getting multiple file triggers for whatever reason
    if (event.Records.length > 1)
        throw new Error(
            `Expecting only a single S3 Object from a Triggered S3 write of a new Object, received ${event.Records.length} Objects`,
        )

    try
    {
        config = (await getCustomerConfig(customer)) as customerConfig
    } catch (e)
    {
        throw new Error(`Exception Retrieving Config: \n ${e}`)
    }
    try
    {
        config = await validateConfig(config)
    } catch (e)
    {
        throw new Error(`Exception Validating Config ${e}`)
    }

    console.log(`Started Processing inbound data (${key})`)


    // let s3Result = { s3ContentResults: '', workQueuedSuccess: false }

    const processS3ObjectStream = await processS3ObjectContentStream(event)

    if (tcLogDebug) console.log(
        `ProcessS3ObjectContentStream - s3CacheProcessor Promise returned for ${key} Completed (Result: ${processS3ObjectStream})`
    )

    debugger
    console.log(`Completed processing inbound S3 Object Stream \n${JSON.stringify(processS3ObjectStream)}`)

    //Once successful delete the original S3 Object
    const delResultCode = await deleteS3Object(key, bucket)
    if (delResultCode !== '204') console.log(`Invalid Delete of ${key}, Expected 204 result code, received ${delResultCode}`)
    else console.log(`Successful Delete of ${key}  (Result ${delResultCode}) `)

    //Check for important Config updates each time
    checkForTCConfigUpdates()


    // //resolve streaming stopping
    // //resolve streaming stopping
    // //resolve streaming stopping

    // //When Local Testing - pull an S3 Object and so avoid the not-found error

    // event.Records[0].s3.object.key = (await getAnS3ObjectforTesting(bucket)) as string
    // key = event.Records[0].s3.object.key

    // console.log(`Round 2 - Processing of ${key} `)

    // const s3Result2 = await processS3ObjectContentStream(event)

    // if (tcLogDebug) console.log(
    //     `ProcessS3ObjectContentStream - s3CacheProcessor Promise 2 returned (${s3Result2}) for ${key} Completed (Result: ${s3Result2})`
    // )

    // //Once successful delete the original S3 Object
    // const delResultCode2 = await deleteS3Object(key, bucket)
    // if (delResultCode !== '204') console.log(`Invalid Delete of ${key}, Expected 204 result code, received ${delResultCode}`)
    // else console.log(`Successful Delete of ${key}  (Result ${delResultCode}) `)




    // //When Local Testing - pull an S3 Object and so avoid the not-found error

    // event.Records[0].s3.object.key = (await getAnS3ObjectforTesting(bucket)) as string
    // key = event.Records[0].s3.object.key

    // console.log(`Round 3 - Processing of ${key} `)
    // const s3Result3 = await processS3ObjectContentStream(event)

    // if (tcLogDebug) console.log(
    //     `ProcessS3ObjectContentStream - s3CacheProcessor Promise 3 returned (${s3Result3}) for ${key} Completed (Result: ${s3Result3})`
    // )

    // //Once successful delete the original S3 Object
    // const delResultCode3 = await deleteS3Object(key, bucket)
    // if (delResultCode !== '204') console.log(`Invalid Delete of ${key}, Expected 204 result code, received ${delResultCode}`)
    // else console.log(`Successful Delete of ${key}  (Result ${delResultCode}) `)




    // //resolve streaming stopping
    // //resolve streaming stopping
    // //resolve streaming stopping


    return `TricklerCache Processing of ${key} Successfully Completed.`
}


export default s3JsonLoggerHandler


function checkForTCConfigUpdates () {
    if (tcLogDebug) console.log(`Checking for TricklerCache Config updates`)
    getTricklerConfig()
    console.log(`Refreshed TricklerCache Config \n ${JSON.stringify(tc)}`)
}

async function getTricklerConfig () {
    //populate env vars with tricklercache config
    const getObjectCmd = {
        Bucket: 'tricklercache-configs',
        Key: 'tricklercache_config.json',
    }

    let tc = {} as tcConfig
    try
    {
        tc = await s3.send(new GetObjectCommand(getObjectCmd)).then(async (getConfigS3Result: GetObjectCommandOutput) => {
            const cr = (await getConfigS3Result.Body?.transformToString('utf8')) as string
            tc = JSON.parse(cr)
            // if (tcLogDebug) console.log(`Tricklercache Config: \n ${cr}`)
            return tc
        })
    } catch (e)
    {
        console.log(`Pulling TricklerConfig Exception \n ${e}`)
    }

    if (tcLogDebug) console.log(`Debug-Pulling tricklercache_config.json: ${JSON.stringify(tc)}`)

    try
    {
        //ToDo: Need validation of Config
        if (tc.LOGLEVEL !== undefined && tc.LOGLEVEL.toLowerCase().indexOf('debug') > -1) tcLogDebug = true
        if (tc.LOGLEVEL !== undefined && tc.LOGLEVEL.toLowerCase().indexOf('verbose') > -1) tcLogVerbose = true


        if (tc.SQS_QUEUE_URL !== undefined) process.env.SQS_QUEUE_URL = tc.SQS_QUEUE_URL
        else throw new Error(`Tricklercache Config invalid definition: SQS_QUEUE_URL - ${tc.SQS_QUEUE_URL}`)

        if (tc.xmlapiurl != undefined) process.env.xmlapiurl = tc.xmlapiurl
        else throw new Error(`Tricklercache Config invalid definition: xmlapiurl - ${tc.xmlapiurl}`)

        if (tc.restapiurl !== undefined) process.env.restapiurl = tc.restapiurl
        else throw new Error(`Tricklercache Config invalid definition: restapiurl - ${tc.restapiurl}`)

        if (tc.authapiurl !== undefined) process.env.authapiurl = tc.authapiurl
        else throw new Error(`Tricklercache Config invalid definition: authapiurl - ${tc.authapiurl}`)


        if (tc.ProcessQueueQuiesce !== undefined)
        {
            process.env.ProcessQueueQuiesce = tc.ProcessQueueQuiesce.toString()
        }
        else
            throw new Error(
                `Tricklercache Config invalid definition: ProcessQueueQuiesce - ${tc.ProcessQueueQuiesce}`,
            )

        if (tc.ProcessQueueVisibilityTimeout !== undefined)
            process.env.ProcessQueueVisibilityTimeout = tc.ProcessQueueVisibilityTimeout.toFixed()
        else
            throw new Error(
                `Tricklercache Config invalid definition: ProcessQueueVisibilityTimeout - ${tc.ProcessQueueVisibilityTimeout}`,
            )

        if (tc.ProcessQueueWaitTimeSeconds !== undefined)
            process.env.ProcessQueueWaitTimeSeconds = tc.ProcessQueueWaitTimeSeconds.toFixed()
        else
            throw new Error(
                `Tricklercache Config invalid definition: ProcessQueueWaitTimeSeconds - ${tc.ProcessQueueWaitTimeSeconds}`,
            )

        if (tc.RetryQueueVisibilityTimeout !== undefined)
            process.env.RetryQueueVisibilityTimeout = tc.ProcessQueueWaitTimeSeconds.toFixed()
        else
            throw new Error(
                `Tricklercache Config invalid definition: RetryQueueVisibilityTimeout - ${tc.RetryQueueVisibilityTimeout}`,
            )

        if (tc.RetryQueueInitialWaitTimeSeconds !== undefined)
            process.env.RetryQueueInitialWaitTimeSeconds = tc.RetryQueueInitialWaitTimeSeconds.toFixed()
        else
            throw new Error(
                `Tricklercache Config invalid definition: RetryQueueInitialWaitTimeSeconds - ${tc.RetryQueueInitialWaitTimeSeconds}`,
            )


        if (tc.CacheBucketQuiesce !== undefined)
        {
            process.env.CacheBucketQuiesce = tc.CacheBucketQuiesce.toString()
        }
        else
            throw new Error(
                `Tricklercache Config invalid definition: CacheBucketQuiesce - ${tc.CacheBucketQuiesce}`,
            )


        if (tc.CacheBucketPurge !== undefined)
            process.env.CacheBucketPurge = tc.CacheBucketPurge
        else
            throw new Error(
                `Tricklercache Config invalid definition: CacheBucketPurge - ${tc.CacheBucketPurge}`,
            )

        if (tc.CacheBucketPurgeCount !== undefined)
            process.env.CacheBucketPurgeCount = tc.CacheBucketPurgeCount.toFixed()
        else
            throw new Error(
                `Tricklercache Config invalid definition: CacheBucketPurgeCount - ${tc.CacheBucketPurgeCount}`,
            )

        if (tc.QueueBucketQuiesce !== undefined)
        {
            process.env.QueueBucketQuiesce = tc.QueueBucketQuiesce.toString()
        }
        else
            throw new Error(
                `Tricklercache Config invalid definition: QueueBucketQuiesce - ${tc.QueueBucketQuiesce}`,
            )

        if (tc.QueueBucketPurge !== undefined)
            process.env.QueueBucketPurge = tc.QueueBucketPurge
        else
            throw new Error(
                `Tricklercache Config invalid definition: QueueBucketPurge - ${tc.QueueBucketPurge}`,
            )

        if (tc.QueueBucketPurgeCount !== undefined)
            process.env.QueueBucketPurgeCount = tc.QueueBucketPurgeCount.toFixed()
        else
            throw new Error(
                `Tricklercache Config invalid definition: QueueBucketPurgeCount - ${tc.QueueBucketPurgeCount}`,
            )

    } catch (e)
    {
        throw new Error(`Exception parsing TricklerCache Config File ${e}`)
    }
    return tc
}

async function getCustomerConfig (customer: string) {
    // ToDo: Once retrieved need to validate all fields

    let configJSON = {}
    const configObjs = [new Uint8Array()]

    try
    {
        await s3
            .send(
                new GetObjectCommand({
                    Key: `${customer}config.json`,
                    Bucket: `tricklercache-configs`,
                }),
            )
            .then(async (getCustConfigS3Result: GetObjectCommandOutput) => {
                if (getCustConfigS3Result.$metadata.httpStatusCode != 200)
                {
                    let errMsg = JSON.stringify(getCustConfigS3Result.$metadata)
                    throw new Error(`Get S3 Object Command failed for ${customer}config.json \n${errMsg}`)
                }
                const s3Body = getCustConfigS3Result.Body as NodeJS.ReadableStream

                configJSON = new Promise<customerConfig>(async (resolve, reject) => {
                    if (s3Body !== undefined)
                    {
                        s3Body.on('data', (chunk: Uint8Array) => {
                            configObjs.push(chunk)
                        })
                        s3Body.on('error', () => {
                            reject
                        })
                        s3Body.on('end', () => {
                            let cj = {} as customerConfig
                            let cf = Buffer.concat(configObjs).toString('utf8')
                            try
                            {
                                cj = JSON.parse(cf)
                                // if (tcLogDebug) console.log(
                                //     `Parsing Config File: ${cj.customer}, Format: ${cj.format}, Region: ${cj.region}, Pod: ${cj.pod}, List Name: ${cj.listName},List  Id: ${cj.listId}, `,
                                // )
                            } catch (e)
                            {
                                throw new Error(`Exception Parsing Config ${customer}config.json: ${e}`)
                            }

                            resolve(cj)
                        })
                    }
                }).catch(e => {
                    throw new Error(`Exception retrieving Customer Config ${customer}config.json \n${e}`)
                })
            })
    } catch (e)
    {
        throw new Error(`Exception retrieving Customer Config ${customer}config.json: \n ${e}`)
    }

    return configJSON
}

async function validateConfig (config: customerConfig) {
    if (!config || config === null)
    {
        throw new Error('Invalid Config Content - is not defined (empty or null config)')
    }
    if (!config.customer)
    {
        throw new Error('Invalid Config Content - Customer is not defined')
    }
    if (!config.clientId)
    {
        throw new Error('Invalid Config Content - ClientId is not defined')
    }
    if (!config.clientSecret)
    {
        throw new Error('Invalid Config Content - ClientSecret is not defined')
    }
    if (!config.format)
    {
        throw new Error('Invalid Config Content - Format is not defined')
    }
    if (!config.listId)
    {
        throw new Error('Invalid Config Content - ListName is not defined')
    }
    if (!config.listName)
    {
        throw new Error('Invalid Config Content - ListName is not defined')
    }
    if (!config.pod)
    {
        throw new Error('Invalid Config Content - Pod is not defined')
    }
    if (!config.region)
    {
        throw new Error('Invalid Config Content - Region is not defined')
    }
    if (!config.refreshToken)
    {
        throw new Error('Invalid Config Content - RefreshToken is not defined')
    }

    if (!config.format.toLowerCase().match(/^(?:csv|json)$/gim))
    {
        throw new Error("Invalid Config - Format is not 'CSV' or 'JSON' ")
    }

    if (!config.pod.match(/^(?:0|1|2|3|4|5|6|7|8|9|a|b)$/gim))
    {
        throw new Error('Invalid Config - Pod is not 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, A, or B. ')
    }

    if (!config.region.toLowerCase().match(/^(?:us|eu|ap|ca)$/gim))
    {
        throw new Error("Invalid Config - Region is not 'US', 'EU', CA' or 'AP'. ")
    }

    return config as customerConfig
}


async function processS3ObjectContentStream (event: S3Event) {
    let chunks: string[] = new Array()
    let s3ContentResults = ''
    let s3ContentStream: NodeJS.ReadableStream
    let batchCount = 0
    const key = event.Records[0].s3.object.key
    const bucket = event.Records[0].s3.bucket.name

    // try
    // {
    if (tcLogDebug) console.log(`Processing S3 Content Stream for ${event.Records[0].s3.object.key}`)

    let streamResult = await s3
        .send(
            new GetObjectCommand({
                Key: key,
                Bucket: bucket,
            }),
        )
        .then(async (getS3StreamResult: GetObjectCommandOutput) => {

            if (tcLogDebug) console.log(`Get S3 Object - Object returned ${key}`)

            if (getS3StreamResult.$metadata.httpStatusCode != 200)
            {
                let errMsg = JSON.stringify(getS3StreamResult.$metadata)
                throw new Error(
                    `Get S3 Object Command failed for ${key}. Result is ${errMsg}`,
                )
            }

            let recs = 0
            const debugData = new Array()

            // await getS3StreamResult.Body

            s3ContentStream = getS3StreamResult.Body as NodeJS.ReadableStream

            if (tcLogDebug) console.log(`Get S3 Object - Records returned from ${key}`)

            if (config.format.toLowerCase() === 'csv')
            {
                s3ContentStream = s3ContentStream.pipe(csvParser, { end: false })
                    .on('error', function (err) {
                        console.log(`CSVParse - Error ${err}`)
                        debugger
                        s3ContentStream.emit('error')
                    })
                    .on('end', function (e: string) {
                        debugger
                        console.log(`CSVParse - OnEnd`)
                        s3ContentStream.emit('end')
                    })
                    .on('finish', function (f: string) {
                        console.log(`CSVParse - OnFinish ${f}`)
                        debugger
                        s3ContentStream.emit('finish')

                    })
                    .on('close', function (c: string) {
                        console.log(`CSVParse - OnClose ${c}`)
                        console.log(`Stream Closed \n${JSON.stringify(debugData)}`)
                        debugger
                        s3ContentStream.emit('close')

                    })
                    .on('skip', async function (err) {
                        debugData.push(err)
                        console.log(`CSV Parse - Invalid Record for ${key} \nError: ${err.code} for record ${err.lines}.\nOne possible cause is a field containing commas ',' and not properly Double-Quoted. \nContent: ${err.record} \nMessage: ${err.message} \nStack: ${err.stack} `)
                        debugger
                    })
                    .on('data', function (f: string) {
                        debugData.push(f)
                        // console.log(`CSVParse - OnData ${f}`)
                        // debugger
                    })
            }

            // if (tcLogDebug) console.log(`Establish stream, Paused? ${s3ContentStream.isPaused().toString()}`)

            // if (tcLogDebug) console.log(`Established CSV Parser and listeners - now s3ContentStream Listeners`)


            s3ContentStream.setMaxListeners(tc.EventEmitterMaxListeners)


            const s = await new Promise(() => s3ContentStream
                .on('error', async function (err: string) {
                    chunks = []
                    batchCount = 0
                    workQueuedSuccess = false
                    s3ContentResults = `An error has stopped Content Parsing at record ${recs} for s3 object ${key}. ${err}`

                    console.log(s3ContentResults)

                    throw new Error(s3ContentResults)
                })

                .on('data', async function (s3Chunk: string) {
                    recs++
                    if (recs > config.updateMaxRows) throw new Error(`The number of Updates in this batch Exceeds Max Row Updates allowed ${recs} `)

                    if (tcLogVerbose) console.log(`s3ContentStream OnData - Another chunk (ArrayLen:${chunks.length} Recs:${recs} Batch:${batchCount} from ${key} - ${JSON.stringify(s3Chunk)}`)

                    chunks.push(s3Chunk)

                    if (chunks.length > 98)
                    {
                        batchCount++
                        const a = chunks
                        chunks = []

                        if (tcLogDebug) console.log(`s3ContentStream OnData Over 99 Records - Queuing Work (Batch: ${batchCount} Chunks: ${a.length}) from ${key}`)

                        const sw = await storeAndQueueWork(a, key, config, batchCount)

                        if (tcLogDebug) console.log(`Await of StoreAndQueueWork returns - ${sw}`)

                    }
                })

                .on('end', async function (msg: string) {
                    batchCount++
                    const a = chunks
                    chunks = []

                    console.log(`S3 Content Stream Ended for ${key}. Processed ${recs} records`)

                    const swResult = await storeAndQueueWork(a, key, config, batchCount)

                    s3ContentResults = `S3ContentStream OnEnd (${key}) Set Work Result ${swResult}`
                    if (tcLogDebug) console.log(s3ContentResults)

                    batchCount = 0
                    const r = recs
                    recs = 0
                })

                .on('close', async function (msg: string) {
                    chunks = []

                    // if (s3ContentStream && !s3ContentStream.readable)
                    // {
                    //     // if (!isReadableEnded(stream))
                    //     //     return callback.call(stream, new ERR_STREAM_PREMATURE_CLOSE())
                    //     console.log(`OnClose: Readable - ${s3ContentStream.readable}  for ${key}`)
                    // }

                    s3ContentResults = `S3ContentStream OnClose - S3 Content Streaming has Closed, successfully processed ${recs} records from ${key}\nNow Deleting ${key}`
                    if (tcLogDebug) console.log(s3ContentResults)

                    console.log(`S3 Content Stream Closed for ${key}. Processed ${recs} records`)

                    batchCount = 0
                    const r = recs
                    recs = 0

                    return { 'close': s3ContentResults }
                })

                .on('finish', async function (msg: string) {
                    //     // CSVParse for NodeJS
                    //     // Problem:
                    //     // You are using the "finish" event and you don't have all your records.
                    //     // The "readable" event is still being called with a few records left.
                    //     // Solution:
                    //     // The parser is both a writable and a readable stream.You write data and you read records.
                    //     // Following Node.js.stream documentation, the "finish" event is from the write API and is
                    //     // emitted when the input source has flushed its data.The "end" event is from the read API
                    //     // and is emitted when there is no more data to be consumed from the stream.


                    chunks = []

                    // if (s3ContentStream && !s3ContentStream.readable)
                    // {
                    //     // if (!isReadableEnded(stream))
                    //     //     return callback.call(stream, new ERR_STREAM_PREMATURE_CLOSE())
                    //     console.log(`OnClose: Readable - ${s3ContentStream.readable}  for ${key}`)
                    // }

                    s3ContentResults = `S3ContentStream OnFinish - S3 Content Streaming has Finished, successfully processed ${recs} records from ${key}\nNow Deleting ${key}`
                    if (tcLogDebug) console.log(s3ContentResults)

                    console.log(`S3 Content Stream Finished for ${key}. Processed ${recs} records`)

                    batchCount = 0
                    const r = recs
                    recs = 0

                    return { 'finish': s3ContentResults }

                    // debugger
                    // return new Promise((resolve, reject) => {
                    //     s3ContentStream.on('error', reject)
                    //     s3ContentStream.on('close', resolve)
                })

            )

                .then(() => {

                    return "streamProcessed"

                })



            // }).catch(e => {
            //     // throw new Error(`Exception Processing (Promise) S3 Get Object Content for ${key}: \n ${e}`);
            //     console.log(
            //         `Exception Processing (Await S3 Body) S3 Get Object Content for ${key}: \n ${e}`,
            //     )
            // })



        })
    // .catch(e => {
    //     // throw new Error(`Exception Processing (Promise) S3 Get Object Content for ${key}: \n ${e}`);
    //     console.log(
    //         `Exception Processing (S3 Send Command Promise) S3 Get Object Content for ${key}: \n ${e}`,
    //     )
    // })

    // } catch (e)
    // {
    //     chunks = []
    //     batchCount = 0
    //     throw new Error(`Exception during Processing of S3 Object for ${key}: \n ${e}`)
    // }


    // await finished(s3ContentStream)
    // return { s3ContentResults, workQueuedSuccess }

    if (tcLogDebug) console.log(`Began Processing the S3Object Content Stream for ${key}`)
    debugger

    // return new Promise((resolve, reject) => {
    //     s3ContentStream.on('error', reject)
    //     s3ContentStream.on('close', resolve)
    // })

    // return { s3ContentResults, workQueuedSuccess }
    // return s3ContentStream

    return streamResult

}


async function storeAndQueueWork (chunks: string[], s3Key: string, config: customerConfig, batch: number) {

    if (batch > 20) throw new Error(`S3 Object ${s3Key} Updates exceed Safety Limit of 20 Batches  - Exiting. `)

    xmlRows = convertToXMLUpdate(chunks, config)
    const key = `process_${batch}_${s3Key}`

    if (tcLogDebug) console.log(`Queuing Work for ${key},  Batch - ${batch},  Records - ${chunks.length} `)

    const AddWorkToS3ProcessBucketResults = await addWorkToS3ProcessStore(xmlRows, key)
    const AddWorkSQSProcessQueueResults = await addWorkToSQSProcessQueue(config, key, batch.toString(), chunks.length.toString())

    return JSON.stringify({ AddWorkToS3ProcessBucketResults, AddWorkSQSProcessQueueResults })
}


function convertToXMLUpdate (rows: string[], config: customerConfig) {
    if (tcLogDebug) console.log(`Converting S3 Content to XML Updates. Packaging ${rows.length} rows as updates to ${config.customer}'s ${config.listName}`)

    xmlRows = `<Envelope><Body><InsertUpdateRelationalTable><TABLE_ID>${config.listId}</TABLE_ID><ROWS>`
    let r = 0

    rows.forEach(jo => {
        r++
        xmlRows += `<ROW>`
        Object.entries(jo).forEach(([key, value]) => {
            // console.log(`Record ${r} as ${key}: ${value}`)
            xmlRows += `<COLUMN name="${key}"> <![CDATA[${value}]]> </COLUMN>`
        })
        xmlRows += `</ROW>`
    })

    //Tidy up the XML
    xmlRows += `</ROWS></InsertUpdateRelationalTable></Body></Envelope>`

    return xmlRows
}

// async function queueUpWork (queueContent: string, key: string, batchNum: string, recCount: number) {
//     if (tcLogDebug) console.log(`Queuing Work for ${key},  Batch - ${batchNum},  Records - ${recCount} `)

//     const AddWorkToS3ProcessBucketResults = await addWorkToS3ProcessStore(queueContent, key)
//     const AddWorkSQSProcessQueueResults = await addWorkToSQSProcessQueue(config, key, batchNum, recCount.toString())

//     return JSON.stringify({ AddWorkToS3ProcessBucketResults, AddWorkSQSProcessQueueResults })
// }

async function addWorkToS3ProcessStore (queueContent: string, key: string) {
    //write to the S3 Process Bucket

    if (tc.QueueBucketQuiesce)
    {
        console.log(`Work Bucket Quiesce is in effect, no New Work Files will be written to the S3 Queue Bucket.`)
        return
    }


    const s3PutInput = {
        Body: queueContent,
        Bucket: 'tricklercache-process',
        Key: key,
    }

    if (tcLogDebug) console.log(`Write Work to S3 Process Queue for ${key}`)

    let AddWorkToS3ProcessBucket
    let S3ProcessBucketResult

    try
    {
        await s3
            .send(new PutObjectCommand(s3PutInput))
            .then(async (s3PutResult: PutObjectCommandOutput) => {
                S3ProcessBucketResult = JSON.stringify(s3PutResult.$metadata.httpStatusCode, null, 2)
                if (S3ProcessBucketResult === '200')
                {
                    AddWorkToS3ProcessBucket = `Wrote Work File (${key}) to S3 Process Store (Result ${S3ProcessBucketResult})`
                    if (tcLogDebug) console.log(`${AddWorkToS3ProcessBucket}`)
                }
                else throw new Error(`Failed to write Work File to S3 Process Store (Result ${S3ProcessBucketResult}) for ${key}`)
            })
            .catch(err => {
                throw new Error(`PutObjectCommand Results Failed for (${key} to S3 Processing bucket: ${err}`)
            })
    } catch (e)
    {
        throw new Error(`PutObjectCommand Exception writing work(${key} to S3 Processing bucket: ${e}`)
    }

    return { AddWorkToS3ProcessBucket, S3ProcessBucketResult }
}

async function addWorkToSQSProcessQueue (config: customerConfig, key: string, batch: string, recCount: string) {

    const sqsQMsgBody = {} as tcQueueMessage
    sqsQMsgBody.workKey = key
    sqsQMsgBody.attempts = 1
    sqsQMsgBody.updateCount = recCount
    sqsQMsgBody.custconfig = config
    sqsQMsgBody.firstQueued = Date.now().toString()

    const sqsParams = {
        MaxNumberOfMessages: 1,
        QueueUrl: process.env.SQS_QUEUE_URL,
        VisibilityTimeout: parseInt(process.env.ProcessQueueVisibilityTimeout!),
        WaitTimeSeconds: parseInt(process.env.ProcessQueueWaitTimeSeconds!),
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
    }


    // sqsParams.MaxNumberOfMessages = 1
    // sqsParams.MessageAttributes.FirstQueued.StringValue = Date.now().toString()
    // sqsParams.MessageAttributes.Retry.StringValue = '0'
    // sqsParams.MessageBody = JSON.stringify(sqsQMsgBody)
    // sqsParams.QueueUrl = process.env.SQS_QUEUE_URL
    // sqsParams.VisibilityTimeout = parseInt(process.env.ProcessQueueVisibilityTimeout!)
    // sqsParams.WaitTimeSeconds = parseInt(process.env.ProcessQueueWaitTimeSeconds!)

    if (tcLogDebug) console.log(`Add Work to SQS Process Queue - SQS Params: ${JSON.stringify(sqsParams)}`)

    let SQSSendResult
    let sqsWriteResult

    try
    {
        await sqsClient
            .send(new SendMessageCommand(sqsParams))
            .then(async (sqsSendMessageResult: SendMessageCommandOutput) => {
                sqsWriteResult = JSON.stringify(sqsSendMessageResult.$metadata.httpStatusCode, null, 2)

                if (sqsWriteResult !== '200')
                {
                    throw new Error(
                        `Failed writing to SQS Process Queue (queue URL: ${sqsParams.QueueUrl}), ${sqsQMsgBody.workKey}, SQS Params${JSON.stringify(sqsParams)})`,
                    )
                }
                SQSSendResult = JSON.stringify(sqsSendMessageResult)

                workQueuedSuccess = true

                if (tcLogDebug) console.log(`Wrote Work to SQS Process Queue (${sqsQMsgBody.workKey}) - Result: ${sqsWriteResult} `)
            })
            .catch(err => {
                console.log(
                    `Failed writing to SQS Process Queue (${err}) \nQueue URL: ${sqsParams.QueueUrl})\nWork to be Queued: ${sqsQMsgBody.workKey}\nSQS Params: ${JSON.stringify(sqsParams)})`,
                )
                debugger
            })
    } catch (e)
    {
        console.log(
            `Exception writing to SQS Process Queue - (queue URL${sqsParams.QueueUrl}), process_${sqsQMsgBody.workKey}, SQS Params${JSON.stringify(sqsParams)}) - Error: ${e}`,
        )
    }

    return { sqsWriteResult, workQueuedSuccess, SQSSendResult }
}

async function reQueue (sqsevent: SQSEvent, queued: tcQueueMessage) {

    const workKey = JSON.parse(sqsevent.Records[0].body).workKey
    debugger

    //ToDo: set attempts on requeue

    const sqsParams = {
        MaxNumberOfMessages: 1,
        QueueUrl: process.env.SQS_QUEUE_URL,
        VisibilityTimeout: parseInt(process.env.ProcessQueueVisibilityTimeout!),
        WaitTimeSeconds: parseInt(process.env.ProcessQueueWaitTimeSeconds!),
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
        MessageBody: JSON.stringify(queued),
    }

    let maR = sqsevent.Records[0].messageAttributes.Retry.stringValue as string
    let n = parseInt(maR)
    n++
    const r: string = n.toString()
    sqsParams.MessageAttributes.Retry = {
        DataType: 'Number',
        StringValue: r,
    }

    if (n > config.MaxRetryUpdate) throw new Error(`Queued Work ${workKey} has been retried more than 10 times: ${r}`)

    const writeSQSCommand = new SendMessageCommand(sqsParams)

    let qAdd

    try
    {
        qAdd = await sqsClient.send(writeSQSCommand).then(async (sqsWriteResult: SendMessageCommandOutput) => {
            const rr = JSON.stringify(sqsWriteResult.$metadata.httpStatusCode, null, 2)
            if (tcLogDebug) console.log(`Process Queue - Wrote Retry Work to SQS Queue (process_${workKey} - Result: ${rr} `)
            return JSON.stringify(sqsWriteResult)
        })
    } catch (e)
    {
        throw new Error(`ReQueue Work Exception - Writing Retry Work to SQS Queue: ${e}`)
    }

    return qAdd
}

async function updateDatabase () {
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
    </Envelope>`
}

async function getS3Work (s3Key: string) {
    // console.log(`GetS3Work Key: ${s3Key}`)

    const getObjectCmd = {
        Bucket: 'tricklercache-process',
        Key: s3Key,
    } as GetObjectCommandInput

    let work: string = ''
    try
    {
        await s3.send(new GetObjectCommand(getObjectCmd))
            .then(async (getS3Result: GetObjectCommandOutput) => {
                // work = JSON.stringify(b, null, 2)
                work = (await getS3Result.Body?.transformToString('utf8')) as string
                if (tcLogDebug) console.log(`Work Pulled (${work.length} chars): ${s3Key}`)
            })
    } catch (e)
    {
        const err: string = JSON.stringify(e)

        if (err.indexOf('NoSuchKey') > -1)
            throw new Error(`Failed to Retrieve Work from S3 Process Queue (${s3Key}) Exception ${e}`)
        else throw new Error(`Exception Retrieving Work from S3 Process Queue (${s3Key}) Exception ${e}`)
    }
    return work
}

export async function getAccessToken (config: customerConfig) {
    try
    {
        const rat = await fetch(`https://api-campaign-${config.region}-${config.pod}.goacoustic.com/oauth/token`, {
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
        })

        const ratResp = (await rat.json()) as accessResp
        if (rat.status != 200)
        {
            throw new Error(`Exception retrieving Access Token:   ${rat.status} - ${rat.statusText}`)
        }
        const accessToken = ratResp.access_token
        return { accessToken }.accessToken
    } catch (e)
    {
        throw new Error(`Exception in getAccessToken: \n ${e}`)
    }
}

export async function postToCampaign (xmlCalls: string, config: customerConfig, count: string) {


    //
    //For Testing only
    //
    config.pod = '2'
    config.listId = '12663209'
    xmlCalls = xmlCalls.replace('3055683', '12663209')
    //
    //For Testing only
    //



    if (process.env.accessToken === undefined || process.env.accessToken === null || process.env.accessToken == '')
    {
        if (tcLogDebug) console.log(`POST to Campaign - Need AccessToken...`)
        process.env.accessToken = (await getAccessToken(config)) as string

        const l = process.env.accessToken.length
        const redactAT = '.......' + process.env.accessToken.substring(l - 10, l)
        if (tcLogDebug) console.log(`Generated a new AccessToken: ${redactAT}`)
    } else
    {
        const l = process.env.accessToken?.length ?? 0
        const redactAT = '.......' + process.env.accessToken?.substring(l - 8, l)
        if (tcLogDebug) console.log(`Access Token already present: ${redactAT}`)
    }

    const myHeaders = new Headers()
    myHeaders.append('Content-Type', 'text/xml')
    myHeaders.append('Authorization', 'Bearer ' + process.env.accessToken)
    myHeaders.append('Content-Type', 'text/xml')
    myHeaders.append('Connection', 'keep-alive')
    myHeaders.append('Accept', '*/*')
    myHeaders.append('Accept-Encoding', 'gzip, deflate, br')

    let requestOptions: RequestInit = {
        method: 'POST',
        headers: myHeaders,
        body: xmlCalls,
        redirect: 'follow',
    }

    const host = `https://api-campaign-${config.region}-${config.pod}.goacoustic.com/XMLAPI`

    let postRes

    // try
    // {
    postRes = await fetch(host, requestOptions)
        .then(response => response.text())
        .then(result => {
            // if (tcLogDebug) console.log("POST Update Result: ", result)
            debugger

            if (result.toLowerCase().indexOf('false</success>') > -1)
            {

                // "<Envelope><Body><RESULT><SUCCESS>false</SUCCESS></RESULT><Fault><Request/>
                //   <FaultCode/><FaultString>Invalid XML Request</FaultString><detail><error>
                //   <errorid>51</errorid><module/><class>SP.API</class><method/></error></detail>
                //    </Fault></Body></Envelope>\r\n"

                if (result.indexOf('max number of concurrent') > -1) postRes = 'retry'
                else postRes = result
                // throw new Error(`Unsuccessful POST of Update - ${result}`)

            }

            POSTSuccess = true
            result = result.replace('\n', ' ')
            return `Processed ${count} Updates - Result: ${result}`
        })
        .catch(e => {
            console.log(`Exception on POST to Campaign: ${e}`)
        })
    // } catch (e)
    // {
    //     console.log(`Exception during POST to Campaign (AccessToken ${process.env.accessToken}) Result: ${e}`)
    // }

    return POSTSuccess.toString()
}

async function deleteS3Object (s3ObjKey: string, bucket: string) {

    let delRes = ''

    try
    {
        // if (tcLogDebug) console.log(`DeleteS3Object - ${s3ObjKey}`)

        await s3
            .send(
                new DeleteObjectCommand({
                    Key: s3ObjKey,
                    Bucket: bucket,
                }),
            )
            .then(async (s3DelResult: DeleteObjectCommandOutput) => {
                // if (tcLogDebug) console.log("Received the following Object: \n", data.Body?.toString());

                delRes = JSON.stringify(s3DelResult.$metadata.httpStatusCode, null, 2)

                // if (tcLogDebug) console.log(`Result from Delete of ${s3ObjKey}: ${delRes} `)
            })
    } catch (e)
    {
        console.log(`Exception Processing S3 Delete Command for ${s3ObjKey}: \n ${e}`)
    }
    return delRes
}

function checkMetadata () {
    //Pull metadata for table/db defined in config
    // confirm updates match Columns
    // Log where Columns are not matching
}

async function getAnS3ObjectforTesting (bucket: string) {
    const listReq = {
        Bucket: bucket,
        MaxKeys: 11
    } as ListObjectsV2CommandInput

    let s3Key: string = ''

    try
    {
        await s3.send(new ListObjectsV2Command(listReq))
            .then(async (s3ListResult: ListObjectsV2CommandOutput) => {
                // event.Records[0].s3.object.key  = s3ListResult.Contents?.at(0)?.Key as string
                // if (tcLogDebug) console.log("Received the following Object: \n", JSON.stringify(data.Body, null, 2));
                debugger
                if (s3ListResult.Contents)
                {
                    const i: number = Math.floor(Math.random() * (10 - 1 + 1) + 1)
                    s3Key = s3ListResult.Contents?.at(i)?.Key as string
                    console.log(`S3 List:\n${JSON.stringify(s3ListResult.Contents)}`)
                    if (tcLogDebug) console.log(`TestRun (${i}) Retrieved ${s3Key} for this Test Run`)
                }
                else throw new Error(`No S3 Object available for Testing: ${bucket}`)
            })
    } catch (e)
    {
        console.log(`Exception Processing S3 List Command: ${e} `)
    }
    return s3Key
    // return 'pura_2023_11_12T01_43_58_170Z.csv'
}



async function purgeBucket (count: number, bucket: string) {
    const listReq = {
        Bucket: bucket,
        MaxKeys: count,
    } as ListObjectsV2CommandInput

    let d = 0
    let r = ''
    try
    {
        await s3.send(new ListObjectsV2Command(listReq)).then(async (s3ListResult: ListObjectsV2CommandOutput) => {
            s3ListResult.Contents?.forEach(async (listItem) => {
                d++
                r = await deleteS3Object(listItem.Key as string, bucket)
                if (r !== '204') console.log(`Non Successful return ( Expected 204 but received ${r} ) on Delete of ${listItem.Key}`)
            })
        })
    } catch (e)
    {
        console.log(`Exception Processing Purge of Bucket ${bucket}: \n${e}`)
    }
    console.log(`Deleted ${d} Objects from ${bucket}`)
    return `Deleted ${d} Objects from ${bucket}`
}

// export const writeSQS = async (msgAttr: string, msgBody: string) => {
//     const command = new SendMessageCommand({
//         QueueUrl: sqsQueueUrl,
//         DelaySeconds: 10,
//         MessageAttributes: {
//             Title: {
//                 DataType: "String",
//                 StringValue: "The Whistler",
//             },
//         },
//         MessageBody:
//             "Information about current NY Times fiction bestseller for week of 12/11/2016.",
//     });

//     const response = await sqsClient.send(command);
//     console.log(response);
//     return response;
// };

// const sqsMessage = async (event: Event) => {

//     // while (true) {
//     try {
//         const rmc = new ReceiveMessageCommand(sqsParams);
//         const data: ReceiveMessageCommandOutput = await sqsClient.send(rmc);
//         if (data.Messages) {
//             // NOTE: Could await next call but performance is better when called async
//             
//             processReceivedMessages(data.Messages);
//         }
//     } catch (err) {
//         console.log("Error handling SQS message", err);
//     } finally {
//         console.log("SQS Waiting...");
//     }
//     // }
// }

// function processReceivedMessage(msg: Message): sqsObject[] {
//     var records: sqsObject[] = [];
//     
//     const body = JSON.parse(msg.Body!);
//     for (const rec of body.Records) {
//         if (rec.s3 === undefined || rec.s3 == "")
//             continue;
//         if (rec.s3.bucket === undefined || rec.s3.bucket == "")
//             continue;
//         if (rec.s3.bucket.name === undefined || rec.s3.bucket.name == "")
//             continue;
//         if (rec.s3.object === undefined || rec.s3.object == "")
//             continue;
//         if (rec.s3.object.key === undefined || rec.s3.object.key == "")
//             continue;

//         const record: sqsObject = {
//             bucketName: rec.s3.bucket.name,
//             objectKey: rec.s3.object.key
//         };
//         records.push(record);
//     }

//     return records;
// }

// async function processReceivedMessages(messages: Message[]) {

//     for (const msg of messages) {
//         if (msg.Body === undefined)
//             continue;

//         var records = processReceivedMessage(msg);
//         var str = JSON.stringify(records);
//         console.log(str); // TODO: Implement your logic HERE
//     }

//     try {
//         var deleteBatchParams = {
//             QueueUrl: sqsQueueUrl,
//             Entries: (messages.map((message, index) => ({
//                 Id: `${index}`,
//                 ReceiptHandle: message.ReceiptHandle,
//             })))
//         };

//         const dmbc = new DeleteMessageBatchCommand(deleteBatchParams);
//         await sqsClient.send(dmbc);
//     } catch (err) {
//         console.log("Error deleting batch messages", err);
//     }
// }

// export const sqsCount = async () => {
//     // The configuration object (`{}`) is required. If the region and credentials
//     // are omitted, the SDK uses your local configuration if it exists.

//     // You can also use `ListQueuesCommand`, but to use that command you must
//     // handle the pagination yourself. You can do that by sending the `ListQueuesCommand`
//     // with the `NextToken` parameter from the previous request.
//     const paginatedQueues = paginateListQueues({ client: sqsClient }, {});
//     const queues = [];

//     for await (const page of paginatedQueues) {
//         if (page.QueueUrls?.length) {
//             queues.push(...page.QueueUrls);
//         }
//     }

//     const suffix = queues.length === 1 ? "" : "s";

//     console.log(
//         `SQS Queues : ${queues.length} queue${suffix}`,
//     );
//     console.log(queues.map((t) => `  * ${t}`).join("\n"));
// };
