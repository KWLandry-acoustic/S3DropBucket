'use strict'

import {
    ListObjectsV2Command, ListObjectsV2CommandInput, ListObjectsV2CommandOutput,
    PutObjectCommand, PutObjectCommandOutput, S3, S3Client, S3ClientConfig,
    GetObjectCommand, GetObjectCommandOutput, GetObjectCommandInput,
    DeleteObjectCommand, DeleteObjectCommandInput, DeleteObjectCommandOutput,
    DeleteObjectOutput, DeleteObjectRequest, ObjectStorageClass, DeleteObjectsCommand, ListObjectVersionsCommand, ListObjectsCommandOutput
} from '@aws-sdk/client-s3'
import { Handler, S3Event, Context, SQSEvent, SQSRecord, S3EventRecord } from 'aws-lambda'
import fetch, { Headers, RequestInit, Response } from 'node-fetch'

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
import { close } from 'fs'

// import 'source-map-support/register'

// import { packageJson } from '@aws-sdk/client3/package.json'
// const version = packageJson.version

const sqsClient = new SQSClient({})

export type sqsObject = {
    bucketName: string
    objectKey: string
}


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
    format: string // CSV or JSON 
    listId: string
    listName: string
    listType: string
    dbKey: string
    lookupKeys: string
    createdFrom: string
    pod: string // 1,2,3,4,5,6,7
    region: string // US, EU, AP
    updateMaxRows: number //Safety to avoid run away data inbound and parsing it all
    refreshToken: string // API Access
    clientId: string // API Access
    clientSecret: string // API Access
    // map: object
    // colVals: { [idx: string]: string }
    // columns: string[]
}

let customersConfig = {} as customerConfig

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
    MaxBatchesWarning: number,
    SelectiveDebug: string,
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

let tcc = {} as tcConfig

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

sqsBatchFail.batchItemFailures.pop()

let tcLogInfo = true
let tcLogDebug = false
let tcLogVerbose = false
let tcSelectiveDebug   //call out selective debug as an option



//TODO:Of concern, very large data sets
//TODO: - as of 10/2023 CSV handled as the papaparse engine handles the record boundary,
//TODO:  Now need to solve for JSON content
//TODO:
//TODO: But how to parse each chunk for JSON content as each chunk is a
//TODO: network chunk that can land on any or no record boundary.
//TODO:      Use a JSON Parser that handles record boundary just like the CSV parser?
//TODO:      Parse out individual Updates from the JSON in the Read Stream using start/stop index
//TODO:          of the stream/content?
//TODO:
//TODO: As of 10/2023 implemented an sqs Queue and deadletter queue,
//TODO:      Multi-GB files are parsed into "99 row updates", written to an S3 "Process" bucket and
//TODO:      an entry added to the sqs Queue that will trigger a 2nd Lambda to process each 'chunk' of 99
//TODO:
//TODO: If there is an exception processing an S3 file
//TODO: Write what data can be processed as 99 updates, and simply throw the exception which will not
//TODO: delete the S3 file for later inspection and re-processing
//TODO:      Can the same file simply be reprocessed/requeued without issue?
//TODO:          Row updates would be duplicated but would that matter?
//TODO:


//TODO: Interface to View and Edit Customer Configs
//TODO: Interface to view Logs/Errors (echo cloudwatch logs?)
//




/**
 * A Lambda function to process the Event payload received from SQS - AWS Queues.
 */
export const tricklerQueueProcessorHandler: Handler = async (event: SQSEvent, context: Context) => {
    //
    //TODO: Confirm SQS Queue deletes queued work
    //       


    if (
        process.env.ProcessQueueVisibilityTimeout === undefined ||
        process.env.ProcessQueueVisibilityTimeout === '' ||
        process.env.ProcessQueueVisibilityTimeout === null
    )
    {
        tcc = await getTricklerConfig()
    }
    if (tcc.SelectiveDebug.indexOf("_9,") > -1) console.info(`Selective Debug 9 - Process Environment Vars: ${JSON.stringify(process.env)}`)




    if (tcc.ProcessQueueQuiesce) 
    {
        console.info(`Work Process Queue Quiesce is in effect, no New Work will be Queued up in the SQS Process Queue.`)
        return
    }

    if (tcc.QueueBucketPurgeCount > 0)
    {
        console.info(`Purge Requested, Only action will be to Purge ${tcc.QueueBucketPurge} of ${tcc.QueueBucketPurgeCount} Records. `)
        const d = await purgeBucket(tcc.QueueBucketPurgeCount, tcc.QueueBucketPurge)
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

    console.info(`Received SQS Events Batch of ${event.Records.length} records.`)

    // event.Records.forEach((i) => {
    //     sqsBatchFail.batchItemFailures.push({ itemIdentifier: i.messageId })
    // })

    //Empty BatchFail array 
    sqsBatchFail.batchItemFailures.forEach(() => {
        sqsBatchFail.batchItemFailures.pop()
    })

    //Process this Inbound Batch 

    for (const q of event.Records)
    {
        // event.Records.forEach(async (i: SQSRecord) => {
        const tqm: tcQueueMessage = JSON.parse(q.body)

        tqm.workKey = JSON.parse(q.body).workKey

        //When Testing - get some actual work queued
        if (tqm.workKey === 'process_2_pura_2023_10_27T15_11_40_732Z.csv')
        {
            tqm.workKey = await getAnS3ObjectforTesting('tricklercache-process')
        }

        console.info(`Processing Work Queue for ${tqm.workKey}`)
        if (tcc.SelectiveDebug.indexOf("_11,") > -1) console.info(`Selective Debug 11 - SQS Events - Processing Batch Item ${JSON.stringify(q)}`)

        try
        {
            const work = await getS3Work(tqm.workKey)
            if (work.length > 0)        //Retreive Contents of the Work File  
            {
                postResult = await postToCampaign(work, tqm.custconfig, tqm.updateCount)
                if (tcc.SelectiveDebug.indexOf("_8,") > -1) console.info(`POST Result for ${tqm.workKey}: ${postResult}`)

                if (postResult.indexOf('retry') > -1)
                {
                    console.warn(`Retry Marked for ${tqm.workKey} (Retry Report: ${sqsBatchFail.batchItemFailures.length + 1}) Returning Work Item ${q.messageId} to Process Queue.`)
                    //Add to BatchFail array to Retry processing the work 
                    sqsBatchFail.batchItemFailures.push({ itemIdentifier: q.messageId })
                    if (tcc.SelectiveDebug.indexOf("_12,") > -1) console.info(`Selective Debug 12 - Added ${tqm.workKey} to SQS Events Retry \n${JSON.stringify(sqsBatchFail)}`)
                }

                if (postResult.toLowerCase().indexOf('unsuccessful post') > -1)
                    console.error(`Error - Unsuccesful POST (Permanent Failure) for ${tqm.workKey}: \n${postResult} \n Customer: ${tqm.custconfig.customer}, Pod: ${tqm.custconfig.pod}, ListId: ${tqm.custconfig.listId} \n${work}`)

                if (postResult.toLowerCase().indexOf('successfully posted') > -1)
                {
                    console.info(`Work Successfully Posted to Campaign (${tqm.workKey}), Deleting Work from S3 Process Queue`)

                    const d: string = await deleteS3Object(tqm.workKey, 'tricklercache-process')
                    if (d === '204') console.info(`Successful Deletion of Work: ${tqm.workKey}`)
                    else console.error(`Failed to Delete ${tqm.workKey}. Expected '204' but received ${d}`)
                }


            }
            else throw new Error(`Failed to retrieve work file (${tqm.workKey}) `)

        } catch (e)
        {
            console.error(`Exception - Processing a Work File (${tqm.workKey} - \n${e} \n${JSON.stringify(tqm)}`)
        }

    }

    console.info(`Processed ${event.Records.length} Work Queue records. Items Fail Count: ${sqsBatchFail.batchItemFailures.length}\nItems Failed List: ${JSON.stringify(sqsBatchFail)}`)

    return sqsBatchFail

    //For debugging - report no fails 
    // return {
    //     batchItemFailures: [
    //         {
    //             itemIdentifier: ''
    //         }
    //     ]
    // }



}



/**
 * A Lambda function to process the Event payload received from S3.
 */

export const s3JsonLoggerHandler: Handler = async (event: S3Event, context: Context) => {


    //TODO: Currently throttled to a single event each time, Add processing of multiple Events per invocation
    //
    //TODO: Process to ReParse Cache and create Work Files
    //
    //TODO: Column mapping - Inbound Column Name to Table Column Mapping
    //TODO: Type Validation - Inbound Data type validation to Mapping Type
    //

    //TODO: Resolve the progression of these steps, currently "Completed" is logged regardless of 'completion'
    // and Delete happens regardless of 'completion' being successful or not
    //
    // INFO Started Processing inbound data(pura_2023_11_16T20_24_37_627Z.csv)
    // INFO Completed processing inbound S3 Object Stream undefined
    // INFO Successful Delete of pura_2023_11_16T20_24_37_627Z.csv(Result 204)
    //


    //When Local Testing - pull an S3 Object and so avoid the not-found error
    if (!event.Records[0].s3.object.key || event.Records[0].s3.object.key === 'devtest.csv')
    {
        event.Records[0].s3.object.key = await getAnS3ObjectforTesting(event.Records[0].s3.bucket.name)
    }


    if (
        process.env.ProcessQueueVisibilityTimeout === undefined ||
        process.env.ProcessQueueVisibilityTimeout === '' ||
        process.env.ProcessQueueVisibilityTimeout === null
    )
    {
        tcc = await getTricklerConfig()
    }
    if (tcc.SelectiveDebug.indexOf("_9,") > -1) console.info(`Selective Debug 9 - Process Environment Vars: ${JSON.stringify(process.env)}`)


    if (tcc.CacheBucketPurgeCount > 0)
    {
        console.warn(`Purge Requested, Only action will be to Purge ${tcc.CacheBucketPurge} of ${tcc.CacheBucketPurgeCount} Records. `)
        const d = await purgeBucket(tcc.CacheBucketPurgeCount, tcc.CacheBucketPurge)
        return d
    }

    if (tcc.CacheBucketQuiesce) 
    {
        console.warn(`Trickler Cache Quiesce is in effect, new S3 Files will be ignored and not processed from the S3 Cache Bucket.\nTo Process files that have arrived during a Quiesce of the Cache, use the TricklerCacheProcess(S3File) Utility.`)
        return
    }

    console.info(
        `Received S3 DropBucket Event Batch of ${event.Records.length} S3 Events (requestId: ${event.Records[0].responseElements['x-amz-request-id']}). `,
    )

    for (const r of event.Records)
    {

        let key = ''
        // {
        //     const contents = await fs.readFile(file, 'utf8')
        // }
        // event.Records.forEach(async (r: S3EventRecord) => {
        key = r.s3.object.key
        const bucket = r.s3.bucket.name


        //TODO: Resolve Duplicates Issue - S3 allows Duplicate Object Names but Delete marks all Objects of same Name Deleted. 
        //   Which causes an issue with Key Not Found after an Object of Name A is processed and deleted, then another Object of Name A comes up in a Trigger.
        const vid = r.s3.object.versionId
        const et = r.s3.object.eTag

        try
        {
            customersConfig = await getCustomerConfig(key)
            console.info(`Processing inbound data for ${key}, Customer is ${customersConfig.customer}`)
        }
        catch (e)
        {
            console.error(`Exception - Retrieving Customer Config for ${key} \n${e}`)
        }

        try
        {

            const processS3ObjectStream = await processS3ObjectContentStream(key, bucket, customersConfig)

            if (tcc.SelectiveDebug.indexOf("_3,") > -1) console.info(`Selective Debug 3 - Returned from Processing S3 Object Content Stream for ${key}. Result: ${processS3ObjectStream}`)

            console.info(`Completed processing the S3 Object ${key} of ${event.Records.length} S3 DropBox Event Records`)

            // return { l, key }

        } catch (e)
        {
            console.error(`Exception - Processing S3 Object Content Stream for ${key} \n${e}`)
        }

        try
        {
            //Once successful delete the original S3 Object
            const delResultCode = await deleteS3Object(key, bucket)

            if (delResultCode !== '204') throw new Error(`Invalid Delete of ${key}, Expected 204 result code, received ${delResultCode}`)
            else console.info(`Successful Delete of ${key}  (Result ${delResultCode}) `)
        }
        catch (e)
        {
            console.error(`Exception - Deleting S3 Object after successful processing of the Content Stream for ${key} \n${e}`)
        }
    }

    //Check for important Config updates
    checkForTCConfigUpdates()

    console.info(`Completing S3 DropBucket Processing of Request Id ${event.Records[0].responseElements['x-amz-request-id']}`)

    return `S3 DropBucket Processing of Request Id ${event.Records[0].responseElements['x-amz-request-id']} Completed.`
}


export default s3JsonLoggerHandler



function checkForTCConfigUpdates () {
    if (tcLogDebug) console.info(`Checking for TricklerCache Config updates`)
    getTricklerConfig()
    if (tcc.SelectiveDebug.indexOf("_1,") > -1) console.info(`Refreshed TricklerCache Config \n ${JSON.stringify(tcc)}`)
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
        tc = await s3.send(new GetObjectCommand(getObjectCmd))
            .then(async (getConfigS3Result: GetObjectCommandOutput) => {
                const cr = (await getConfigS3Result.Body?.transformToString('utf8')) as string
                return JSON.parse(cr)
            })
    } catch (e)
    {
        console.error(`Exception - Pulling TricklerConfig \n ${e}`)
    }

    try
    {

        if (tc.LOGLEVEL !== undefined && tc.LOGLEVEL.toLowerCase().indexOf('debug') > -1) tcLogDebug = true
        if (tc.LOGLEVEL !== undefined && tc.LOGLEVEL.toLowerCase().indexOf('verbose') > -1) tcLogVerbose = true

        if (tc.SelectiveDebug !== undefined) tcSelectiveDebug = tc.SelectiveDebug


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


        if (tc.MaxBatchesWarning !== undefined)
            process.env.RetryQueueInitialWaitTimeSeconds = tc.MaxBatchesWarning.toFixed()
        else
            throw new Error(
                `Tricklercache Config invalid definition: MaxBatchesWarning - ${tc.MaxBatchesWarning}`,
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
        throw new Error(`Exception - Parsing TricklerCache Config File ${e}`)
    }

    if (tc.SelectiveDebug.indexOf("_1,") > -1) console.info(`Selective Debug 1 - Pulled tricklercache_config.json: \n${JSON.stringify(tc)}`)

    return tc
}

async function getCustomerConfig (filekey: string) {

    // Retrieve file's prefix as Customer Name
    if (!filekey) throw new Error(`Exception - Cannot resolve Customer Config without a valid Customer Prefix (file prefix is ${filekey})`)


    const customer = filekey.split('_')[0] + '_'

    if (customer === '_' || customer.length < 4)
    {
        throw new Error(`Exception - Customer cannot be determined from S3 Cache File '${filekey}'      \n      `)
    }

    let configJSON = {} as customerConfig
    // const configObjs = [new Uint8Array()]

    const getObjectCommand = {
        Key: `${customer}config.json`,
        Bucket: `tricklercache-configs`,
    }

    let cc = {} as customerConfig

    try
    {
        await s3.send(new GetObjectCommand(getObjectCommand))
            .then(async (getConfigS3Result: GetObjectCommandOutput) => {
                const ccr = await getConfigS3Result.Body?.transformToString('utf8') as string

                if (tcc.SelectiveDebug.indexOf("_10,") > -1) console.info(`Selective Debug 10 - Customers Config: \n ${ccr}`)

                configJSON = JSON.parse(ccr)
            })
            .catch((e) => {

                const err: string = JSON.stringify(e)

                if (err.indexOf('NoSuchKey') > -1)
                    throw new Error(`Exception - Customer Config Not Found on S3 tricklercache-configs (${customer}config.json) \nException ${e}`)
                else throw new Error(`Exception - Retrieving Config from S3 tricklercache-configs (${customer}config.json) \nException ${e}`)

            })
    } catch (e)
    {
        console.error(`Exception - Pulling Customer Config \n${e}`)
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
    //                 console.error(`Exception - Processing Customer Config - ${err}`)
    //                 throw new Error(`Exception - (Promise-Catch) Processing Customer Config: ${err} `)
    //             })

    //             .then(async (custConfigS3Result: GetObjectCommandOutput) => {
    //                 try
    //                 {
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
    //                     console.error(`Exception - in then for Cust Config: ${e}`)
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
    //                 throw new Error(`Exception - Parsing Config ${customer}config.json: ${e}`)
    //             }

    //             resolve(cj)
    // //         })
    // // }
    // }).catch(e => {
    //     throw new Error(`Exception - retrieving Customer Config ${customer}config.json \n${e}`)
    // })


    // } catch (e)
    // {
    //     throw new Error(`Exception - retrieving Customer Config ${customer}config.json: \n ${e}`)
    // }

    customersConfig = await validateCustomerConfig(configJSON)
    return customersConfig as customerConfig
}

async function validateCustomerConfig (config: customerConfig) {
    if (!config || config === null)
    {
        throw new Error('Invalid Config - empty or null config')
    }
    if (!config.customer)
    {
        throw new Error('Invalid Config - Customer is not defined')
    }
    if (!config.clientId)
    {
        throw new Error('Invalid Config - ClientId is not defined')
    }
    if (!config.clientSecret)
    {
        throw new Error('Invalid Config - ClientSecret is not defined')
    }
    if (!config.format)
    {
        throw new Error('Invalid Config - Format is not defined')
    }
    if (!config.listId)
    {
        throw new Error('Invalid Config - ListId is not defined')
    }
    if (!config.listName)
    {
        throw new Error('Invalid Config - ListName is not defined')
    }
    if (!config.pod)
    {
        throw new Error('Invalid Config - Pod is not defined')
    }
    if (!config.region)
    {
        throw new Error('Invalid Config - Region is not defined')
    }
    if (!config.refreshToken)
    {
        throw new Error('Invalid Config - RefreshToken is not defined')
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

    if (!config.listType)
    {
        throw new Error('Invalid Config - ListType is not defined')
    }

    if (!config.listType.toLowerCase().match(/^(?:relational|dbkeyed|dbnonkeyed)$/gim))
    {
        throw new Error("Invalid Config - ListType must be either 'Relational', 'DBKeyed' or 'DBNonKeyed'. ")
    }

    if (config.listType.toLowerCase() == 'dbkeyed' && !config.dbKey)
    {
        throw new Error("Invalid Config - Update set as Database Keyed but DBKey is not defined. ")
    }

    if (config.listType.toLowerCase() == 'dbnonkeyed' && !config.lookupKeys)
    {
        throw new Error("Invalid Config - Update set as Database NonKeyed but lookupKeys is not defined. ")
    }

    return config as customerConfig
}


async function processS3ObjectContentStream (key: string, bucket: string, custConfig: customerConfig) {
    let chunks: string[] = new Array()
    let batchCount = 0
    let streamResult = ""


    if (tcLogDebug) console.info(`Processing S3 Content Stream for ${key}`)

    const processS3Object = await s3.send(
        new GetObjectCommand({
            Key: key,
            Bucket: bucket,
        }),
    )
        .then(async (getS3StreamResult: GetObjectCommandOutput) => {

            if (tcLogDebug) console.info(`Get S3 Object - Object returned ${key}`)

            if (getS3StreamResult.$metadata.httpStatusCode != 200)
            {
                let errMsg = JSON.stringify(getS3StreamResult.$metadata)
                throw new Error(
                    `Get S3 Object Command failed for ${key}. Result is ${errMsg}`,
                )
            }

            let recs = 0

            let s3ContentReadableStream = getS3StreamResult.Body as NodeJS.ReadableStream

            if (custConfig.format.toLowerCase() === 'csv')
            {
                const csvParser = parse({
                    delimiter: ',',
                    columns: true,
                    comment: '#',
                    trim: true,
                    skip_records_with_error: true,
                },
                    // function (msg) {
                    //     if (tcLogDebug) console.info(`CSVParse Function : ${msg}`)
                    // }
                )
                s3ContentReadableStream = s3ContentReadableStream.pipe(csvParser) //, { end: false })
                // .on('error', function (err) {
                //     console.error(`CSVParser(${key}) - Error ${err}`)
                // })
                // .on('end', function (e: string) {
                //     console.info(`CSVParser(${key}) - OnEnd - Message: ${e} \nDebugData: ${JSON.stringify(debugData)}`)
                // })
                // .on('finish', function (f: string) {
                //     console.info(`CSVParser(${key}) - OnFinish ${f}`)
                // })
                // .on('close', function (c: string) {
                //     console.info(`CSVParser(${key}) - OnClose ${c}`)
                //     console.info(`Stream Closed \n${JSON.stringify(debugData)}`)
                // })
                // .on('skip', async function (err) {
                //     console.info(`CSVParse(${key}) - Invalid Record \nError: ${err.code} for record ${err.lines}.\nOne possible cause is a field containing commas ',' and not properly Double-Quoted. \nContent: ${err.record} \nMessage: ${err.message} \nStack: ${err.stack} `)
                // })
                // .on('data', function (f: string) {
                //     console.info(`CSVParse(${key}) - OnData ${f}`)
                //     
                // })
            }

            s3ContentReadableStream.setMaxListeners(tcc.EventEmitterMaxListeners)

            if (custConfig.format.toLowerCase() === 'json')
            {
                //Placeholder 
            }


            console.info(`S3 Content Stream Opened for ${key}`)

            // try
            // {
            // let streamPromiseResult = await new Promise(() => {
            s3ContentReadableStream
                .on('error', async function (err: string) {
                    chunks = []
                    batchCount = 0

                    const errMessage = `An error has stopped Content Parsing at record ${recs} for s3 object ${key}.\n${err}`
                    recs = 0

                    console.error(errMessage)
                    throw new Error(errMessage)
                })

                .on('data', async function (s3Chunk: string) {
                    recs++
                    if (recs > custConfig.updateMaxRows) throw new Error(`The number of Updates in this batch Exceeds Max Row Updates allowed ${recs} in the Customers Config`)

                    if (tcc.SelectiveDebug.indexOf("_13,") > -1) console.info(`Selective Debug 13 - s3ContentStream OnData - Another chunk (ArrayLen:${chunks.length} Recs:${recs} Batch:${batchCount} from ${key} - ${JSON.stringify(s3Chunk)}`)

                    chunks.push(s3Chunk)

                    if (chunks.length > 98)
                    {
                        batchCount++

                        const a = chunks
                        chunks = []

                        const sqwResult = await storeAndQueueWork(a, key, custConfig, batchCount)

                        if (tcc.SelectiveDebug.indexOf("_2,") > -1) console.info(`Selective Debug 2: Content Stream OnData - Store And Queue Work for ${key} of Batch ${batchCount} of ${a.length} records, Result: \n${JSON.stringify(sqwResult)}`)

                    }

                })

                .on('end', async function (msg: string) {
                    batchCount++

                    const streamEndResult = `S3 Content Stream Ended for ${key}. Processed ${recs} records as ${batchCount} batches.`
                    // "S3 Content Stream Ended for pura_2024_01_22T18_02_45_204Z.csv. Processed 33 records as 1 batches."
                    if (tcLogDebug) console.info(streamEndResult)
                    console.info(`OnEnd - Debug:  ${streamEndResult}`)
                    // streamResult += `\n${streamEndResult}`

                    const d = chunks
                    chunks = []

                    const storeQueueResult = await storeAndQueueWork(d, key, custConfig, batchCount)
                    // "{\"AddWorkToS3ProcessBucketResults\":{\"AddWorkToS3ProcessBucket\":\"Wrote Work File (process_0_pura_2024_01_22T18_02_46_119Z_csv.xml) to S3 Processing Bucket (Result 200)\",\"S3ProcessBucketResult\":\"200\"},\"AddWorkToSQSProcessQueueResults\":{\"sqsWriteResult\":\"200\",\"workQueuedSuccess\":true,\"SQSSendResult\":\"{\\\"$metadata\\\":{\\\"httpStatusCode\\\":200,\\\"requestId\\\":\\\"e70fba06-94f2-5608-b104-e42dc9574636\\\",\\\"attempts\\\":1,\\\"totalRetryDelay\\\":0},\\\"MD5OfMessageAttributes\\\":\\\"0bca0dfda87c206313963daab8ef354a\\\",\\\"MD5OfMessageBody\\\":\\\"940f4ed5927275bc93fc945e63943820\\\",\\\"MessageId\\\":\\\"cf025cb3-dce3-4564-89a5-23dcae86dd42\\\"}\"}}"
                    if (tcLogDebug) console.info(`Store and Queue Work Result: ${storeQueueResult}`)

                    if (tcc.SelectiveDebug.indexOf("_2,") > -1) console.info(`Selective Debug 2: Content Stream End - Store and Queue Work for (${key}) of Batch ${batchCount} of ${d.length} records Result: \n${JSON.stringify(storeQueueResult)}`)

                    batchCount = 0
                    recs = 0

                    streamResult += `\nStore and Queue Work Result: ${storeQueueResult}`

                    return streamResult
                })

                .on('close', async function (msg: string) {

                    streamResult += `\nS3 Content Stream Closed for ${key}`
                    console.info(streamResult)

                    chunks = []
                    batchCount = 0
                    recs = 0

                    // return streamResult
                })

            // })
            //     .then((streamResult ) => {
            //         debugger
            //         console.info(`Promise then`)
            //         // streamPromiseResult = streamResult
            //         // return streamPromiseResult as string
            //         return streamResult as string
            //     })
            //     .catch(e => {
            //         // throw new Error(`Exception - Processing Readable Stream Promise for S3 Get Object Content for ${key}: \n ${e}`);
            //         console.error(
            //             `Exception - Processing (Await s3 Content Readable Stream) S3 Get Object Content for ${key}: \n ${e}`,
            //         )
            //     })
            // } catch (e)
            // {
            //     console.error(`Exception - StreamResult \n${e}`)
            // }


            return streamResult
        })
        .catch(e => {
            console.error(`Exception -  Process S3 Object Content Stream for ${key}: \n ${e}`)
            throw new Error(`Exception - Process S3 Object Content Stream for ${key}: \n ${e}`)
        })

    return processS3Object
    // })

}


async function storeAndQueueWork (chunks: string[], s3Key: string, config: customerConfig, batch: number) {

    if (batch > tcc.MaxBatchesWarning) console.warn(`Warning: Updates from the S3 Object (${s3Key}) are exceeding (${batch}) the Warning Limit of ${tcc.MaxBatchesWarning} Batches per Object.`)
    // throw new Error(`Updates from the S3 Object (${s3Key}) Exceed (${batch}) Safety Limit of 20 Batches of 99 Updates each. Exiting...`)

    if (customersConfig.listType.toLowerCase() === 'dbkeyed' ||
        customersConfig.listType.toLowerCase() === 'dbnonkeyed')
    {
        xmlRows = convertJSONToXML_DBUpdates(chunks, config)
    }

    if (customersConfig.listType.toLowerCase() === 'relational')
    {
        xmlRows = convertJSONToXML_RTUpdates(chunks, config)
    }


    let key = s3Key.replace('.', '_')
    key = `process_${batch}_${key}.xml`


    if (tcLogDebug) console.info(`Queuing Work for ${s3Key} - ${key}. (Batch ${batch} of ${chunks.length} records) `)

    const AddWorkToS3ProcessBucketResults = await addWorkToS3ProcessStore(xmlRows, key)
    //     {
    //         AddWorkToS3ProcessBucket: "Wrote Work File (process_0_pura_2024_01_22T18_02_46_119Z_csv.xml) to S3 Processing Bucket (Result 200)",
    //         S3ProcessBucketResult: "200",
    // }

    const AddWorkToSQSProcessQueueResults = await addWorkToSQSProcessQueue(config, key, batch.toString(), chunks.length.toString())
    //     {
    //         sqsWriteResult: "200",
    //         workQueuedSuccess: true,
    //         SQSSendResult: "{\"$metadata\":{\"httpStatusCode\":200,\"requestId\":\"e70fba06-94f2-5608-b104-e42dc9574636\",\"attempts\":1,\"totalRetryDelay\":0},\"MD5OfMessageAttributes\":\"0bca0dfda87c206313963daab8ef354a\",\"MD5OfMessageBody\":\"940f4ed5927275bc93fc945e63943820\",\"MessageId\":\"cf025cb3-dce3-4564-89a5-23dcae86dd42\"}",
    // }

    return { AddWorkToS3ProcessBucketResults, AddWorkToSQSProcessQueueResults }
}


function convertJSONToXML_RTUpdates (rows: string[], config: customerConfig) {
    if (tcLogDebug) console.info(`Converting S3 Content to XML RT Updates. Packaging ${rows.length} rows as updates to ${config.customer}'s ${config.listName}`)

    if (tcc.SelectiveDebug.indexOf("_6,") > -1) console.info(`Selective Debug 6 - Convert to XML Updates: ${JSON.stringify(rows)}`)

    xmlRows = `<Envelope><Body><InsertUpdateRelationalTable><TABLE_ID>${config.listId}</TABLE_ID><ROWS>`
    let r = 0

    rows.forEach(jo => {
        r++
        xmlRows += `<ROW>`
        Object.entries(jo).forEach(([key, value]) => {
            // console.info(`Record ${r} as ${key}: ${value}`)
            xmlRows += `<COLUMN name="${key}"> <![CDATA[${value}]]> </COLUMN>`
        })
        xmlRows += `</ROW>`
    })

    //Tidy up the XML
    xmlRows += `</ROWS></InsertUpdateRelationalTable></Body></Envelope>`

    return xmlRows
}

function convertJSONToXML_DBUpdates (rows: string[], config: customerConfig) {

    if (tcLogDebug) console.info(`Converting S3 Content to XML DB Updates. Packaging ${rows.length} rows as updates to ${config.customer}'s ${config.listName}`)

    if (tcc.SelectiveDebug.indexOf("_16,") > -1) console.info(`Selective Debug 16 - Convert JSON to XML DB Updates, JSON: ${JSON.stringify(rows)}`)

    // <AddRecipient>
    // <CREATED_FROM>0</CREATED_FROM>
    // <UPDATE_IF_FOUND>true</UPDATE_IF_FOUND>
    // <SYNC_FIELDS>
    // <SYNC_FIELD> 
    // <COLUMN> 
    // <NAME> <VALUE>

    xmlRows = `<Envelope><Body>`
    let r = 0

    rows.forEach(jsonObj => {
        r++
        xmlRows += `<AddRecipient><LIST_ID>${config.listId}</LIST_ID><CREATED_FROM>0</CREATED_FROM><UPDATE_IF_FOUND>true</UPDATE_IF_FOUND>`

        // If Keyed, then Column that is the key must be present in Column Set
        // If Not Keyed must use Lookup Fields
        // Use SyncFields as 'Lookup" values,
        //   Columns hold the Updates while SyncFields hold the 'lookup' values.

        //Only needed on non-keyed(In Campaign use DB -> Settings -> LookupKeys to find what fields are Lookup Keys)
        if (config.dbKey.toLowerCase() === 'dbnonkeyed')
        {
            const lk = config.lookupKeys.split(',')

            xmlRows += `<SYNC_FIELDS>`
            lk.forEach(k => {
                debugger
                xmlRows += `<SYNC_FIELD><NAME>${k}</NAME><VALUE> <![CDATA[${lk}]]></VALUE > </SYNC_FIELD>`
            })

            xmlRows += `</SYNC_FIELDS>`
        }

        if (config.dbKey.toLowerCase() === 'dbkeyed')
        {
            //Placeholder
        }

        Object.entries(jsonObj).forEach(([key, value]) => {
            // console.info(`Record ${r} as ${key}: ${value}`)
            xmlRows += `<COLUMN><NAME>${key}</NAME><VALUE>${value}</VALUE></COLUMN>`
        })

        xmlRows += `</AddRecipient>`
    })

    xmlRows += `</Body></Envelope>`

    return xmlRows
}

async function updateDatabase () {

    const update = `<Envelope>
          <Body>
                <AddRecipient>
                      <LIST_ID>${customersConfig.listId}</LIST_ID>
                      <CREATED_FROM>${customersConfig.createdFrom}</CREATED_FROM>
                      <UPDATE_IF_FOUND>true</UPDATE_IF_FOUND>
                      ${customersConfig.lookupKeys}
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







async function addWorkToS3ProcessStore (queueContent: string, key: string) {
    //write to the S3 Process Bucket

    if (tcc.QueueBucketQuiesce)
    {
        console.warn(`Work/Process Bucket Quiesce is in effect, no New Work Files will be written to the S3 Queue Bucket.`)
        return
    }


    const s3PutInput = {
        Body: queueContent,
        Bucket: 'tricklercache-process',
        Key: key,
    }

    if (tcLogDebug) console.info(`Write Work to S3 Process Queue for ${key}`)

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
                    AddWorkToS3ProcessBucket = `Wrote Work File (${key}) to S3 Processing Bucket (Result ${S3ProcessBucketResult})`
                    if (tcc.SelectiveDebug.indexOf("_7,") > -1) console.info(`Selective Debug 7 - ${AddWorkToS3ProcessBucket}`)
                }
                else throw new Error(`Failed to write Work File to S3 Process Store (Result ${S3ProcessBucketResult}) for ${key}`)
            })
            .catch(err => {
                throw new Error(`PutObjectCommand Results Failed for (${key} to S3 Processing bucket: ${err}`)
            })
    } catch (e)
    {
        throw new Error(`Exception - Put Object Command for writing work(${key} to S3 Processing bucket: ${e}`)
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

    if (tcLogDebug) console.info(`Add Work to SQS Process Queue - SQS Params: ${JSON.stringify(sqsParams)}`)

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

                if (tcc.SelectiveDebug.indexOf("_8,") > -1) console.info(`Selective Debug 8 - Queued Work to SQS Process Queue (${sqsQMsgBody.workKey}) - Result: ${sqsWriteResult} `)
            })
            .catch(err => {
                console.error(
                    `Failed writing to SQS Process Queue (${err}) \nQueue URL: ${sqsParams.QueueUrl})\nWork to be Queued: ${sqsQMsgBody.workKey}\nSQS Params: ${JSON.stringify(sqsParams)})`,
                )
            })
    } catch (e)
    {
        console.error(
            `Exception - Writing to SQS Process Queue - (queue URL${sqsParams.QueueUrl}), process_${sqsQMsgBody.workKey}, SQS Params${JSON.stringify(sqsParams)}) - Error: ${e}`,
        )
    }

    return { "SQSWriteResult": sqsWriteResult, "SQSQueued_Metadata": SQSSendResult }
}

// async function reQueue (sqsevent: SQSEvent, queued: tcQueueMessage) {

//     const workKey = JSON.parse(sqsevent.Records[0].body).workKey

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




async function getS3Work (s3Key: string) {

    if (tcLogDebug) console.info(`Debug - GetS3Work Key: ${s3Key}`)

    const getObjectCmd = {
        Bucket: 'tricklercache-process',
        Key: s3Key,
    } as GetObjectCommandInput

    let work: string = ''
    try
    {
        await s3.send(new GetObjectCommand(getObjectCmd))
            .then(async (getS3Result: GetObjectCommandOutput) => {
                work = (await getS3Result.Body?.transformToString('utf8')) as string
                if (tcLogDebug) console.info(`Work Pulled (${work.length} chars): ${s3Key}`)
            })
    } catch (e)
    {
        const err: string = JSON.stringify(e)

        if (err.indexOf('NoSuchKey') > -1)
            throw new Error(`Exception - Work Not Found on S3 Process Queue (${s3Key}) Is an S3 Process Queue Management Policy Deleting Work before Processing can be accomplished? \n${e}`)
        else throw new Error(`Exception - Retrieving Work from S3 Process Queue for ${s3Key}. \n ${e}`)
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
            throw new Error(`Exception - Retrieving Access Token:   ${rat.status} - ${rat.statusText}`)
        }
        const accessToken = ratResp.access_token
        return { accessToken }.accessToken
    } catch (e)
    {
        throw new Error(`Exception - On GetAccessToken: \n ${e}`)
    }
}

export async function postToCampaign (xmlCalls: string, config: customerConfig, count: string) {

    if (process.env.accessToken === undefined || process.env.accessToken === null || process.env.accessToken == '')
    {
        if (tcLogDebug) console.info(`POST to Campaign - Need AccessToken...`)
        process.env.accessToken = (await getAccessToken(config)) as string

        const l = process.env.accessToken.length
        const redactAT = '.......' + process.env.accessToken.substring(l - 10, l)
        if (tcLogDebug) console.info(`Generated a new AccessToken: ${redactAT}`)
    } else
    {
        const l = process.env.accessToken?.length ?? 0
        const redactAT = '.......' + process.env.accessToken?.substring(l - 8, l)
        if (tcLogDebug) console.info(`Access Token already stored: ${redactAT}`)
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


    if (tcc.SelectiveDebug.indexOf("_5,") > -1) console.info(`Selective Debug 5 - Updates to POST are: ${xmlCalls}`)

    let postRes

    // try
    // {
    postRes = await fetch(host, requestOptions)
        .then(response => response.text())
        .then(async (result) => {

            if (result.toLowerCase().indexOf('false</success>') > -1)
            {
                if (
                    result.toLowerCase().indexOf('max number of concurrent') > -1
                )
                {
                    if (tcc.SelectiveDebug.indexOf("_4,") > -1) console.info(`Selective Debug 4 - Max Number of Concurrent Updates Fail - Marked for Retry`)
                    return 'retry'
                }
                else return `Error - Unsuccessful POST of the Updates (${count}) - Response : ${result}`
            }

            result = result.replace('\n', ' ')
            return `Successfully POSTed (${count}) Updates - Result: ${result}`
        })
        .catch(e => {
            if (e.toLowerCase().indexOf('econnreset') > -1) return 'retry'
            console.error(`Error - Unsuccessful POST of the Updates: ${e} - Set to Retry`)
            return 'retry'
        })
    // } catch (e)
    // {
    //     console.error(`Exception - On POST to Campaign (AccessToken ${process.env.accessToken}) Result: ${e}`)
    // }

    return postRes
}

async function deleteS3Object (s3ObjKey: string, bucket: string) {

    let delRes = ''

    // debugger

    // const listObjectCommand = {
    //     Bucket: bucket, // required
    //     // KeyMarker: s3ObjKey,
    //     Prefix: s3ObjKey,
    //     // versionId: ver,
    //     IfMatch: entity
    // }
    // // console.info("Debug: ", entity, "Debug: ", ver)
    // const loc = new ListObjectVersionsCommand(listObjectCommand)
    // const locResponse = await s3.send(loc)
    //     .then(async (listResult: ListObjectsV2CommandOutput) => {
    //         console.info(`ListObject Response: ${JSON.stringify(listResult)}`)
    //         debugger
    //     })

    // debugger


    try
    {
        await s3
            .send(
                new DeleteObjectCommand({
                    Key: s3ObjKey,
                    Bucket: bucket
                }),
            )
            .then(async (s3DelResult: DeleteObjectCommandOutput) => {
                // if (tcLogDebug) console.info("Received the following Object: \n", data.Body?.toString());

                delRes = JSON.stringify(s3DelResult.$metadata.httpStatusCode, null, 2)

                if (tcLogDebug) console.info(`Result from Delete of ${s3ObjKey}: ${delRes} `)
            })
    } catch (e)
    {
        console.error(`Exception - Attempting S3 Delete Command for ${s3ObjKey}: \n ${e} `)
    }
    return delRes as string
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

    // try
    // {
    await s3.send(new ListObjectsV2Command(listReq))
        .then(async (s3ListResult: ListObjectsV2CommandOutput) => {

            let i: number = 0

            if (s3ListResult.Contents)
            {
                let kc: number = s3ListResult.KeyCount as number - 1
                if (kc = 0) throw new Error("No S3 Objects to retrieve as Test Data, exiting")
                if (kc > 10)
                {
                    i = Math.floor(Math.random() * (10 - 1 + 1) + 1)
                }
                if (kc = 1) i = 0
                s3Key = s3ListResult.Contents?.at(i)?.Key as string
                // console.info(`S3 List: \n${ JSON.stringify(s3ListResult.Contents) } `)
                // if (tcLogDebug)
                console.info(`TestRun(${i}) Retrieved ${s3Key} for this Test Run`)

            }
            else throw new Error(`No S3 Object available for Testing: ${bucket} `)

            return s3Key
        })
        .catch((e) => {
            console.error(`Exception - On S3 List Command for Testing Objects from ${bucket}: ${e} `)
        })
    // .finally(() => {
    //     console.info(`S3 List Finally...`)
    // })
    // } catch (e)
    // {
    //     console.error(`Exception - Processing S3 List Command: ${ e } `)
    // }


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
                if (r !== '204') console.error(`Non Successful return (Expected 204 but received ${r} ) on Delete of ${listItem.Key} `)
            })
        })
    } catch (e)
    {
        console.error(`Exception - Attempting Purge of Bucket ${bucket}: \n${e} `)
    }
    console.info(`Deleted ${d} Objects from ${bucket} `)
    return `Deleted ${d} Objects from ${bucket} `
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
//     console.info(response);
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
//         console.info("Error handling SQS message", err);
//     } finally {
//         console.info("SQS Waiting...");
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
//         console.info(str); 
//     }

//     try {
//         var deleteBatchParams = {
//             QueueUrl: sqsQueueUrl,
//             Entries: (messages.map((message, index) => ({
//                 Id: `${ index } `,
//                 ReceiptHandle: message.ReceiptHandle,
//             })))
//         };

//         const dmbc = new DeleteMessageBatchCommand(deleteBatchParams);
//         await sqsClient.send(dmbc);
//     } catch (err) {
//         console.error("Error deleting batch messages", err);
//     }
// }

// export const sqsCount = async () => {
//     // The configuration object (`{ } `) is required. If the region and credentials
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

//     console.info(
//         `SQS Queues: ${ queues.length } queue${ suffix } `,
//     );
//     console.info(queues.map((t) => `  * ${ t } `).join("\n"));
// };
