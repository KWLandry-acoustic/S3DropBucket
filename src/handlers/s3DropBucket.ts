'use strict'

import {
    ListObjectsV2Command, ListObjectsV2CommandInput, ListObjectsV2CommandOutput,
    PutObjectCommand, PutObjectCommandOutput, S3, S3Client, S3ClientConfig,
    GetObjectCommand, GetObjectCommandOutput, GetObjectCommandInput,
    DeleteObjectCommand, DeleteObjectCommandInput, DeleteObjectCommandOutput,
    DeleteObjectOutput, DeleteObjectRequest, ObjectStorageClass, DeleteObjectsCommand,
    ListObjectVersionsCommand, ListObjectsCommandOutput, CopyObjectCommand
} from '@aws-sdk/client-s3'

import { FirehoseClient, PutRecordCommand, PutRecordCommandInput, PutRecordCommandOutput } from "@aws-sdk/client-firehose"

import { SchedulerClient, ListSchedulesCommand, ListSchedulesCommandInput } from '@aws-sdk/client-scheduler' // ES Modules import

import { Handler, S3Event, Context, SQSEvent, SQSRecord, S3EventRecord } from 'aws-lambda'

import fetch, { Headers, RequestInit, Response } from 'node-fetch'


// import { JSONParser } from '@streamparser/json'
// const jsonParser = new JSONParser()
//json-node Stream compatible package
import { JSONParser, Tokenizer, TokenParser } from '@streamparser/json-node'
// import JSONParserTransform from '@streamparser/json-node/jsonparser.js'


import { parse } from 'csv-parse'

import { transform } from 'stream-transform'

import jsonpath from 'jsonpath'

import {
    SQSClient,
    ReceiveMessageCommand,
    DeleteMessageBatchCommand,
    ReceiveMessageCommandOutput,
    Message,
    paginateListQueues,
    SendMessageCommand,
    SendMessageCommandOutput,
    BatchEntryIdsNotDistinct,
} from '@aws-sdk/client-sqs'

import sftpClient, { ListFilterFunction } from 'ssh2-sftp-client'
import { FileResultCallback } from '@babel/core'
import { freemem } from 'os'
import { env } from 'node:process'
import { keyBy } from 'lodash'

//For when needed to reference Lambda execution environment /tmp folder 
// import { ReadStream, close } from 'fs'



let testS3Key: string
let testS3Bucket: string
testS3Bucket = "tricklercache-configs"
// testS3Key = "TestData/cloroxweather_99706.csv"
// testS3Key = "TestData/visualcrossing_00213.csv"
// testS3Key = "TestData/pura_2024_02_26T05_53_26_084Z.json"
// testS3Key = "TestData/pura_2024_02_25T00_00_00_090Z.json"
// testS3Key = "TestData/pura_S3DropBucket_Aggregator-8-2024-03-19-16-42-48-46e884aa-8c6a-3ff9-8d32-c329395cf311.json"
testS3Key = "pura_S3DropBucket_Aggregator-8-2024-03-23-09-23-55-123cb0f9-9552-3303-a451-a65dca81d3c4_json_update_53_99.xml"


let vid: string
let et: string
let tqmVid: string

const sqsClient = new SQSClient({})

export type sqsObject = {
    bucketName: string
    objectKey: string
}

//ToDo: Make AWS Region Config Option for portability/infrastruct dedication
const s3 = new S3Client({ region: 'us-east-1' })

const SFTPClient = new sftpClient()


let localTesting = false

let xmlRows: string = ''

interface S3Object {
    Bucket: string
    Key: string
}

interface customerConfig {
    Customer: string
    format: string // CSV or JSON 
    updates: string // singular or bulk
    listId: string
    listName: string
    listType: string
    DBKey: string
    LookupKeys: string
    pod: string // 1,2,3,4,5,6,7
    region: string // US, EU, AP
    updateMaxRows: number //Safety to avoid run away data inbound and parsing it all
    refreshToken: string // API Access
    clientId: string // API Access
    clientSecret: string // API Access
    sftp: {
        "user": string
        "password": string
        "filepattern": string
        "schedule": string
    }
    transforms: {
        jsonMap: { [key: string]: string },
        csvMap: { [key: string]: string },
        ignore: string[],
        script: string[]
    }
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
    versionId: string
    attempts: number
    updateCount: string
    custconfig: customerConfig
    lastQueued: string
}

export interface tcConfig {
    LOGLEVEL: string
    AWS_REGION: string
    s3DropBucket: string
    s3DropBucketWorkBucket: string
    s3DropBucketWorkQueue: string
    xmlapiurl: string
    restapiurl: string
    authapiurl: string
    MaxBatchesWarning: number,
    SelectiveDebug: string,
    ProcessQueueQuiesce: boolean
    prefixFocus: string,
    // ProcessQueueVisibilityTimeout: number
    // ProcessQueueWaitTimeSeconds: number
    // RetryQueueVisibilityTimeout: number
    // RetryQueueInitialWaitTimeSeconds: number
    EventEmitterMaxListeners: number
    S3DropBucketQuiesce: boolean
    S3DropBucketMaintHours: number,
    S3DropBucketProcessQueueMaintHours: number,
    S3DropBucketPurgeCount: number
    S3DropBucketPurge: string
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


export interface processS3ObjectStreamResult {
    OnDataReadStreamException: string,
    OnDataStoreQueueResult: object,
    OnEndStoreS3QueueResult: {
        AddWorkToS3ProcessBucketResults: {
            versionId: string,
            S3ProcessBucketResult: string,
            AddWorkToS3ProcessBucket: object
        },
        AddWorkToSQSProcessQueueResults: {
            SQSWriteResult: string,
            AddWorkToSQSQueueResult: object
        },
        StoreQueueWorkException: string
        StoreS3WorkException: string
    },
    OnEndStreamEndResult: string,
    OnEndRecordStatus: string,
    OnEndNoRecordsException: string,
    ProcessS3ObjectStreamCatch: string,
    OnClose_Result: string,
    StreamReturnLocation: string,
    PutToFireHoseAggregatorResult: string,
    PutToFirehoseException: string,
    OnEnd_PutToFireHoseAggregator: string,
    DeleteResult: string
}


let processS3ObjectStreamResolution: processS3ObjectStreamResult = {
    OnClose_Result: '',
    OnEndStoreS3QueueResult: {
        AddWorkToS3ProcessBucketResults: {
            versionId: '',
            S3ProcessBucketResult: '',
            AddWorkToS3ProcessBucket: {}
        },
        AddWorkToSQSProcessQueueResults: {
            SQSWriteResult: '',
            AddWorkToSQSQueueResult: {}
        },
        StoreQueueWorkException: '',
        StoreS3WorkException: '',
    },
    PutToFireHoseAggregatorResult: '',
    OnEnd_PutToFireHoseAggregator: '',
    PutToFirehoseException: '',
    ProcessS3ObjectStreamCatch: '',
    OnDataReadStreamException: '',
    OnEndRecordStatus: '',
    OnEndStreamEndResult: '',
    StreamReturnLocation: '',
    DeleteResult: '',
    OnDataStoreQueueResult: {},
    OnEndNoRecordsException: '',
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




/**
 * A Lambda function to process the Event payload received from S3.
 */

export const s3DropBucketHandler: Handler = async (event: S3Event, context: Context) => {

    if (event.Records[0].s3.object.key.indexOf('AggregationError') > -1) return ""

    if (

        process.env["EventEmitterMaxListeners"] === undefined ||
        process.env["EventEmitterMaxListeners"] === '' ||
        process.env["EventEmitterMaxListeners"] === null
    )
    {
        tcc = await getValidateTricklerConfig()
    }

    if (tcc.SelectiveDebug.indexOf("_9,") > -1) console.info(`Selective Debug 9 - Process Environment Vars: ${JSON.stringify(process.env)}`)


    if (event.Records[0].s3.object.key.indexOf('Aggregator') > -1)
    {
        if (tcc.SelectiveDebug.indexOf("_25,") > -1) console.info(`Selective Debug 25 - Processing an Aggregated File ${event.Records[0].s3.object.key}`)

    }


    //When Local Testing - pull an S3 Object and so avoid the not-found error
    if (!event.Records[0].s3.object.key || event.Records[0].s3.object.key === 'devtest.csv')
    {
        if (testS3Key !== undefined && testS3Key !== null &&
            testS3Bucket !== undefined && testS3Bucket !== null)
        {
            event.Records[0].s3.object.key = testS3Key
            event.Records[0].s3.bucket.name = testS3Bucket
        }
        else
        {
            event.Records[0].s3.object.key = await getAnS3ObjectforTesting(event.Records[0].s3.bucket.name) ?? ""
        }
        localTesting = true
    }
    else
    {
        testS3Key = ''
        testS3Bucket = ''
        localTesting = false
    }


    if (tcc.S3DropBucketPurgeCount > 0)
    {
        console.warn(`Purge Requested, Only action will be to Purge ${tcc.S3DropBucketPurge} of ${tcc.S3DropBucketPurgeCount} Records. `)
        const d = await purgeBucket(Number(tcc.S3DropBucketPurgeCount!), tcc.S3DropBucketPurge!)
        return d
    }

    if (tcc.S3DropBucketQuiesce)
    {
        if (!localTesting)
        {
            console.warn(`Trickler Cache Quiesce is in effect, new S3 Files will be ignored and not processed from the S3 Cache Bucket.\nTo Process files that have arrived during a Quiesce of the Cache, use the aws cli command to copy the files from the DropBucket to the DropBucket to drive the object creation event to the Lambda function.`)
            return
        }
    }

    console.info(
        `Received S3DropBucket Event Batch. There are ${event.Records.length} S3DropBucket Event Records in this batch. (Event Id: ${event.Records[0].responseElements['x-amz-request-id']}).`,
    )

    //Future: Left this for possible switch of the Trigger as an SQS Trigger of an S3 Write, 
    // Drive higher concurrency in each Lambda invocation by running batches of 10 files written at a time(SQS Batch) 
    for (const r of event.Records)
    {
        let key = ''
        let bucket = ''


        // {
        //     const contents = await fs.readFile(file, 'utf8')
        // }
        // event.Records.forEach(async (r: S3EventRecord) => {


        key = r.s3.object.key
        bucket = r.s3.bucket.name

        if (!key.startsWith(tcc.prefixFocus))
        {
            console.warn(`PrefixFocus is configured, File Name ${key} does not fall within focus restricted by the configured PrefixFocus ${tcc.prefixFocus}`)
            return
        }


        //ToDo: Resolve Duplicates Issue - S3 allows Duplicate Object Names but Delete marks all Objects of same Name Deleted. 
        //   Which causes an issue with Key Not Found after an Object of Name A is processed and deleted, then another Object of Name A comes up in a Trigger.

        vid = r.s3.object.versionId ?? ""
        et = r.s3.object.eTag ?? ""

        try
        {
            customersConfig = await getCustomerConfig(key)
            console.info(`Processing inbound data for ${customersConfig.Customer} - ${key} \nS3 Object Details: VersionID: ${vid}, ETag: ${et}`)
        }
        catch (e)
        {
            throw new Error(`Exception - Retrieving Customer Config for ${key} \n${e}`)
        }


        // get test files from the testdata folder of the tricklercache-configs/TestData/ bucket
        if (testS3Key && testS3Key !== null)
        {
            key = testS3Key
            bucket = 'S3DropBucket'
        }
        if (testS3Bucket && testS3Bucket !== null)
        {
            bucket = testS3Bucket
        }


        //ToDo: Refactor messaging to reference Object properties versus stringifying and adding additional overhead
        //ToDo: Refactor to be consistent in using the response object across all processes
        //ToDo: Refactor messaging to be more consistent across all processes
        // {
        //     "OnEnd_StreamEndResult": "S3 Content Stream Ended for pura_2024_03_04T20_42_23_797Z.json. Processed 1 records as 1 batches.",
        //         "OnClose_Result": "S3 Content Stream Closed for pura_2024_03_04T20_42_23_797Z.json",
        //             "OnEnd_PutToFireHoseAggregator": "{}",
        //                 "ReturnLocation": "...End of ReadStream Promise"
        // }
        // DeleteResult: "Successful Delete of pura_2024_03_04T20_42_23_797Z.json  (Result 204)"

        try
        {
            processS3ObjectStreamResolution = await processS3ObjectContentStream(key, vid, bucket, customersConfig)
                .then(async (res) => {
                    let delResultCode

                    console.info(`Completed processing all records of the S3 Object ${key}. ${res.OnEndRecordStatus}`)

                    //Don't delete the test data
                    if (localTesting) key = 'TestData/S3Object_DoNotDelete'
                    //Do not delete in order to Capture Testing Data
                    // if (key.toLowerCase().indexOf('aggregat') > -1) key = 'TestData/S3Object_DoNotDelete'


                    if ((res.PutToFireHoseAggregatorResult = "200") ||
                        (res.OnEndStoreS3QueueResult.AddWorkToS3ProcessBucketResults.S3ProcessBucketResult === "200") &&
                        res.OnEndStoreS3QueueResult.AddWorkToSQSProcessQueueResults.SQSWriteResult === "200")
                    {
                        try
                        {
                            //Once File successfully processed delete the original S3 Object
                            delResultCode = await deleteS3Object(key, vid, bucket)

                            if (delResultCode !== '204')
                            {
                                res = { ...res, DeleteResult: JSON.stringify(delResultCode) }
                                throw new Error(`Unsuccessful Delete of ${key}, Expected 204 result code, received ${delResultCode}`)
                            } else
                            {
                                const dr = `Successful Delete of ${key}  (Result ${delResultCode})`
                                res = { ...res, DeleteResult: `Successful Delete of ${key}  (Result ${JSON.stringify(delResultCode)})` }
                            }
                        }
                        catch (e)
                        {
                            console.error(`Exception - Deleting S3 Object after successful processing of the Content Stream for ${key} \n${e}`)
                        }
                    }

                    return res
                })
                .catch((e: any) => {
                    const r = `Exception - Process S3 Object Stream exception \n${e}`
                    console.error(r)
                    processS3ObjectStreamResolution = { ...processS3ObjectStreamResolution, ProcessS3ObjectStreamCatch: r }
                    return processS3ObjectStreamResolution
                })
        } catch (e)
        {
            console.error(`Exception - Processing S3 Object Content Stream for ${key} \n${e}`)
        }

        if (tcc.SelectiveDebug.indexOf("_3,") > -1) console.info(`Selective Debug 3 - Returned from Processing S3 Object Content Stream for ${key}. Result: ${JSON.stringify(processS3ObjectStreamResolution)}`)

    }


    if (event.Records[0].s3.bucket.name && tcc.S3DropBucketMaintHours > 0)
    {
        const maintenance = await maintainS3DropBucket(customersConfig)

        if (tcc.SelectiveDebug.indexOf("_26,") > -1)
        {
            const l = maintenance[0] as number
            if (l > 0) console.info(`Selective Debug 26 - Files sent to ReProcess: \n${maintenance}`)
            else console.info(`Selective Debug 26 - No files found to Reprocess`)
        }
    }

    //Check for important Config updates (which caches the config in Lambdas long-running cache)
    checkForTCConfigUpdates()

    console.info(`Completing S3 DropBucket Processing of Request Id ${event.Records[0].responseElements['x-amz-request-id']}`)
    if (tcc.SelectiveDebug.indexOf("_20,") > -1) console.info(`Selective Debug 20 - \n${processS3ObjectStreamResolution}`)

    return JSON.stringify(processS3ObjectStreamResolution)
}

export default s3DropBucketHandler



async function processS3ObjectContentStream (key: string, version: string, bucket: string, custConfig: customerConfig) {

    let batchCount = 0
    let chunks: string[] = []

    let processS3ObjectResults: processS3ObjectStreamResult = {
        OnClose_Result: '',
        OnEndStoreS3QueueResult: {
            AddWorkToS3ProcessBucketResults: {
                versionId: '',
                S3ProcessBucketResult: '',
                AddWorkToS3ProcessBucket: {}
            },
            AddWorkToSQSProcessQueueResults: {
                SQSWriteResult: '',
                AddWorkToSQSQueueResult: {}
            },
            StoreQueueWorkException: '',
            StoreS3WorkException: '',
        },
        OnDataStoreQueueResult: {},
        OnEnd_PutToFireHoseAggregator: '',
        PutToFireHoseAggregatorResult: '',
        PutToFirehoseException: '',
        ProcessS3ObjectStreamCatch: '',
        OnEndStreamEndResult: '',
        OnEndRecordStatus: '',
        OnDataReadStreamException: '',
        OnEndNoRecordsException: '',
        StreamReturnLocation: '',
        DeleteResult: '',
    }


    if (tcLogDebug) console.info(`Processing S3 Content Stream for ${key}`)

    let s3C: GetObjectCommandInput
    if (version !== '')
    {
        s3C = {
            Key: key,
            Bucket: bucket,
            VersionId: version
        }
    }
    else
    {
        s3C = {
            Key: key,
            Bucket: bucket
        }
    }

    let streamResult = processS3ObjectResults

    processS3ObjectResults = await s3.send(new GetObjectCommand(s3C)
    )
        .then(async (getS3StreamResult: GetObjectCommandOutput) => {

            if (getS3StreamResult.$metadata.httpStatusCode != 200)
            {
                const errMsg = JSON.stringify(getS3StreamResult.$metadata)
                throw new Error(`Get S3 Object Command failed for ${key}. Result is ${errMsg}`)
            }

            let recs = 0

            let s3ContentReadableStream = getS3StreamResult.Body as NodeJS.ReadableStream

            if (key.indexOf('aggregate_') < 0 && custConfig.format.toLowerCase() === 'csv')
            {
                const csvParser = parse({
                    delimiter: ',',
                    columns: true,
                    comment: '#',
                    trim: true,
                    skip_records_with_error: true,
                })

                const t = transform(function (data) {
                    return JSON.stringify(data) + '\n'
                })


                s3ContentReadableStream = s3ContentReadableStream.pipe(csvParser).pipe(t)


                //#region
                // s3ContentReadableStream = s3ContentReadableStream.pipe(csvParser), { end: false })
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
                //#region
            }


            //Placeholder - Everything should be JSON by the time we get here 
            if (custConfig.format.toLowerCase() === 'json')
            {
            }

            //Options to Handling Large JSON Files
            // Send the JSON objects formatted without newlines and use a newline as the delimiter.
            // Send the JSON objects concatenated with a record separator control character as the delimiter.
            // Send the JSON objects concatenated with no delimiters and rely on a streaming parser to extract them.
            // Send the JSON objects prefixed with their length and rely on a streaming parser to extract them.

            /** 
            //The following are what make up StreamJSON JSONParser but can be broken out to process data more granularly
            //Might be helpful in future capabilities 

            const jsonTZParser = new Tokenizer({
                "stringBufferSize": 64,
                "numberBufferSize": 64,
                "separator": "",
                "emitPartialTokens": false
            })
            // {
            //     paths: <string[]>,
            //     keepStack: <boolean>, // whether to keep all the properties in the stack
            //     separator: <string>, // separator between object. For example `\n` for nd-js. If left empty or set to undefined, the token parser will end after parsing the first object. To parse multiple object without any delimiter just set it to the empty string `''`.
            //     emitPartialValues: <boolean>, // whether to emit values mid-parsing.
            // }
            const jsonTParser = new TokenParser(
                {
                    // paths: [''],                //JSONPath partial support see docs.
                    keepStack: true,            // whether to keep all the properties in the stack
                    separator: '',              // separator between object. For example `\n` for nd-js. If left empty or set to undefined, the token parser will end after parsing the first object. To parse multiple object without any delimiter just set it to the empty string `''`.
                    emitPartialValues: false,   // whether to emit values mid-parsing.
                })
            */




            // {
            //  stringBufferSize: <number>, // set to 0 to don't buffer. Min valid value is 4.
            //  numberBufferSize: <number>, // set to 0 to don't buffer.
            //  separator: <string>, // separator between object. For example `\n` for nd-js.
            //  emitPartialTokens: <boolean> // whether to emit tokens mid-parsing.
            // }

            if (key.indexOf('aggregate_') > -1) console.info(`Begin Stream Parsing aggregate file ${key}`)


            const jsonParser = new JSONParser({
                // numberBufferSize: 64,        //64, //0, //undefined, // set to 0 to don't buffer.
                stringBufferSize: undefined,        //64, //0, //undefined,
                separator: '\n',               // separator between object. For example `\n` for nd-js.
                paths: ['$'],              //ToDo: Possible data transform oppty
                keepStack: false,
                emitPartialTokens: false    // whether to emit tokens mid-parsing.
            })               //, { objectMode: true })


            //At this point, all data, whether a json file or a csv file parsed by csvparser should come through
            //  as a series of Objects (One at a time in the Stream) with a line break after each Object.
            //   {data:data, data:data, .....}
            //   {data:data, data:data, .....}
            //   {data:data, data:data, .....}
            //
            // Later, OnData processing populates an Array with each line/Object, so the
            // final format will be an Array of Objects:
            // [ {data:data, data:data, .....}
            //  { data: data, data: data, .....}
            //  { data: data, data: data, .....} ]
            //


            // s3ContentReadableStream = s3ContentReadableStream.pipe(t).pipe(jsonParser)
            s3ContentReadableStream = s3ContentReadableStream.pipe(jsonParser)

            s3ContentReadableStream.setMaxListeners(Number(tcc.EventEmitterMaxListeners))


            // const readStream = await new Promise(async (resolve, reject) => {
            await new Promise(async (resolve, reject) => {

                let d: string

                s3ContentReadableStream
                    .on('error', async function (err: string) {
                        const errMessage = `An error has stopped Content Parsing at record ${recs} for s3 object ${key}.\n${err}`
                        console.error(errMessage)
                        chunks = []
                        batchCount = 0
                        recs = 0

                        throw new Error(`Error on Readable Stream for s3DropBucket Object ${key}. \nError Message: ${errMessage}`)
                    })
                    .on('data', async function (s3Chunk: { key: string, parent: object, stack: object, value: object }) {
                        recs++
                        let sqwResult: {} = {}

                        d = JSON.stringify(s3Chunk.value)

                        chunks.push(d)

                        if (key.toLowerCase().indexOf('aggregat') < 0
                            && recs > custConfig.updateMaxRows) throw new Error(`The number of Updates in this batch Exceeds Max Row Updates allowed ${recs} in the Customers Config. S3 Object ${key} will not be deleted to allow for review and possible restaging.`)

                        if (tcc.SelectiveDebug.indexOf("_13,") > -1) console.info(`Selective Debug 13 - s3ContentStream OnData - Another chunk (Num of Entries:${Object.values(s3Chunk).length} Recs:${recs} Batch:${batchCount} from ${key} - ${d}`)

                        try
                        {
                            //Singular Update files will not reach 99 updates in a single file
                            //Aggregate(d) Files will have > 99 updates in each file 
                            if (chunks.length > 98)
                            {
                                batchCount++

                                const updates: string[] = []

                                while (chunks.length > 0 && updates.length < 100)
                                {
                                    const c = chunks.pop() ?? ""
                                    updates.push(c)
                                }

                                sqwResult = await storeAndQueueWork(updates, key, custConfig, updates.length, batchCount)
                            }

                            if (tcc.SelectiveDebug.indexOf("_2,") > -1) console.info(`Selective Debug 2: Content Stream OnData - Store And Queue Work for ${key} of ${batchCount + 1} Batches of ${Object.values(d).length} records, Result: \n${JSON.stringify(sqwResult)}`)

                            streamResult = { ...streamResult, OnDataStoreQueueResult: sqwResult }

                        } catch (e)
                        {
                            console.error(`Exception - Read Stream OnData Processing for ${key} \n${e}`)
                            streamResult = { ...streamResult, OnDataReadStreamException: `Exception - Read Stream OnData Processing for ${key} \n${e}` }
                        }
                    })

                    .on('end', async function () {

                        if (recs < 1 && chunks.length < 1)
                        {
                            streamResult = {
                                ...streamResult, OnEndNoRecordsException: `Exception - No records returned from parsing file. Check the content as well as the configured file format (${custConfig.format}) matches the content of the file.`
                            }
                            // console.error(`Exception - ${JSON.stringify(streamResult)}`)
                            throw new Error(`Exception - onEnd ${JSON.stringify(streamResult)}`)
                        }

                        let sqwResult

                        //Next Process Step is Queue Work or Aggregate Small single Update files into larger Update files to improve Campaign Update performance.
                        try
                        {
                            // if (aggregate file and chunks not 0)


                            if ((chunks.length > 0) &&
                                (custConfig.updates.toLowerCase() === 'bulk') ||
                                key.toLowerCase().indexOf('aggregat') > 0)
                            {
                                batchCount++
                                recs = chunks.length
                                sqwResult = await storeAndQueueWork(chunks, key, custConfig, chunks.length, batchCount)

                                // streamResult.OnEndStoreS3QueueResult = sqwResult 
                                Object.assign(streamResult.OnEndStoreS3QueueResult, sqwResult)
                                // streamResult = { ...streamResult, OnEndStoreS3QueueResult: sqwResult }

                            }

                            if (chunks.length > 0 &&
                                custConfig.updates.toLowerCase() === 'singular' &&
                                key.toLowerCase().indexOf('aggregat') < 0)
                            {
                                batchCount++
                                recs = chunks.length

                                let pfhRes

                                try 
                                {
                                    pfhRes = await putToFirehose(chunks, key, custConfig.Customer)
                                        .then((res) => {

                                            // const su = ` Singular Update (${key}) put to Firehose aggregator pipe - \n${JSON.stringify(res)} \n${batchCount} Batches of ${chunks.length} records - Result: \n${JSON.stringify(res)}`

                                            streamResult = {
                                                ...streamResult, ...res
                                            }
                                            return streamResult
                                        })

                                } catch (e)
                                {
                                    console.error(`Exception - PutToFirehose Call - \n${e} `)
                                    streamResult = { ...streamResult, PutToFirehoseException: `Exception - PutToFirehose \n${e} ` }
                                    return streamResult
                                }
                            }

                        } catch (e)
                        {
                            const sErr = `Exception - ReadStream OnEnd Processing - \n${e} `
                            // console.error(sErr)
                            return { ...streamResult, OnEndStreamResult: sErr }
                        }

                        const streamEndResult = `S3 Content Stream Ended for ${key}.Processed ${recs} records as ${batchCount} batches.`
                        // "S3 Content Stream Ended for pura_2024_01_22T18_02_45_204Z.csv. Processed 33 records as 1 batches."
                        // console.info(`OnEnd - Stream End Result: ${ streamEndResult } `)
                        streamResult = {
                            ...streamResult, OnEndStreamEndResult: streamEndResult, OnEndRecordStatus: `Processed ${recs} records as ${batchCount} batches.`
                        }

                        if (tcc.SelectiveDebug.indexOf("_2,") > -1) console.info(`Selective Debug 2: Content Stream OnEnd for (${key}) - Store and Queue Work of ${batchCount + 1} Batches of ${d.length} records - Result: \n${JSON.stringify(streamResult)} `)

                        chunks = []
                        batchCount = 0
                        recs = 0

                        resolve({ ...streamResult })

                        // return streamResult
                    })

                    .on('close', async function () {

                        streamResult = { ...streamResult, OnClose_Result: `S3 Content Stream Closed for ${key}` }

                    })

                console.info(`S3 Content Stream Opened for ${key}`)

            })
                .then((r) => {
                    return { ...streamResult, ReturnLocation: "ReadStream Then Clause." }
                })
                .catch(e => {
                    const err = `Exception - ReadStream(catch) - Process S3 Object Content Stream for ${key}.\nResults: ${JSON.stringify(streamResult)}.\n${e} `
                    console.error(err)
                    throw new Error(err)
                })

            // return { ...readStream, ReturnLocation: `...End of ReadStream Promise` }
            return { ...streamResult, ReturnLocation: `...End of ReadStream Promise` }
        })
        .catch(e => {
            // console.error(`Exception(error) - Process S3 Object Content Stream for ${ key }.\nResults: ${ JSON.stringify(streamResult) }.\n${ e } `)
            throw new Error(`Exception(throw) - Process S3 Object Content Stream for ${key}.\nResults: ${JSON.stringify(streamResult)}.\n${e} `)
        })

    return processS3ObjectResults

}


async function putToFirehose (S3Obj: string[], key: string, cust: string) {

    const client = new FirehoseClient()

    // S3DropBucket_Aggregator 
    // S3DropBucket_FireHoseStream

    let putFirehoseResp: {} = {}

    try
    {
        for (const fo in S3Obj)
        {
            const j = JSON.parse(S3Obj[fo])

            Object.assign(j, { "Customer": cust })

            const f = Buffer.from(JSON.stringify(j), 'utf-8')

            const fc = {
                DeliveryStreamName: "S3DropBucket_Aggregator",
                Record: {
                    Data: f
                }
            } as PutRecordCommandInput

            const fireCommand = new PutRecordCommand(fc)
            let fr = {}

            try
            {
                let res: PutRecordCommandOutput

                putFirehoseResp = await client.send(fireCommand)
                    .then((res) => {

                        if (tcc.SelectiveDebug.indexOf('_22,') > -1) console.info(`Put to Firehose Aggregator for ${key} - \n${JSON.stringify(fc)} \nResult: ${JSON.stringify(res)} `)

                        if (res.$metadata.httpStatusCode === 200)
                        {
                            //     PutToFireHoseAggregatorResult: '',
                            //     OnEnd_PutToFireHoseAggregator: '',
                            //     PutToFirehoseException: '',
                            fr = { ...fr, PutToFirehoseAggregatorResult: `${res.$metadata.httpStatusCode} ` }
                            fr = { ...fr, OnEnd_PutToFireHoseAggregator: `Successful Put to Firehose Aggregator for ${key}. \n${JSON.stringify(res)} \n${res.RecordId} ` }
                        }
                        else
                        {
                            fr = { ...fr, PutToFirehoseAggregatorResult: `UnSuccessful Put to Firehose Aggregator for ${key} \n ${JSON.stringify(res)} ` }
                        }
                        return fr
                    })
                    .catch((e) => {
                        console.error(`Exception - Put to Firehose Aggregator(Promise -catch) for ${key} \n${e} `)
                        fr = { ...fr, PutToFirehoseException: `Exception - Put to Firehose Aggregator for ${key} \n${e} ` }
                        return fr
                    })
            } catch (e)
            {
                console.error(`Exception - PutToFirehose \n${e} `)
            }

        }

        // return putFirehoseResp

    } catch (e)
    {
        console.error(`Exception - Put to Firehose Aggregator(try-catch) for ${key} \n${e} `)
    }

    return putFirehoseResp
}

/**
 * A Lambda function to process the Event payload received from SQS - AWS Queues.
 */
export const S3DropBucketQueueProcessorHandler: Handler = async (event: SQSEvent, context: Context) => {

    //Populate Config Options in process.env as a means of Caching the config across invocations occurring within 15 secs of each other.
    if (
        process.env["ProcessQueueVisibilityTimeout"] === undefined ||
        process.env["ProcessQueueVisibilityTimeout"] === '' ||
        process.env["ProcessQueueVisibilityTimeout"] === null
    )
    {
        tcc = await getValidateTricklerConfig()
    }

    console.info(`S3 DropBucket Work Processor Selective Debug Set is: ${tcc.SelectiveDebug!} `)

    if (tcc.SelectiveDebug.indexOf("_9,") > -1) console.info(`Selective Debug 9 - Process Environment Vars: ${JSON.stringify(process.env)} `)


    if (tcc.ProcessQueueQuiesce) 
    {
        console.info(`Work Process Queue Quiesce is in effect, no New Work will be Queued up in the SQS Process Queue.`)
        return
    }

    if (tcc.QueueBucketPurgeCount > 0)
    {
        console.info(`Purge Requested, Only action will be to Purge ${tcc.QueueBucketPurge} of ${tcc.QueueBucketPurgeCount} Records. `)
        const d = await purgeBucket(Number(process.env["QueueBucketPurgeCount"]!), process.env["QueueBucketPurge"]!)
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

    if (tcc.SelectiveDebug.indexOf("_4,") > -1) console.info(`Selective Debug 4 - Received ${event.Records.length} Work Queue Records.Records are: \n${JSON.stringify(event)} `)

    //Empty BatchFail array 
    sqsBatchFail.batchItemFailures.forEach(() => {
        sqsBatchFail.batchItemFailures.pop()
    })

    let tqm: tcQueueMessage = {
        workKey: '',
        versionId: '',
        attempts: 0,
        updateCount: '',
        custconfig: {
            Customer: '',
            format: '',
            updates: '',
            listId: '',
            listName: '',
            listType: '',
            DBKey: '',
            LookupKeys: '',
            pod: '',
            region: '',
            updateMaxRows: 0,
            refreshToken: '',
            clientId: '',
            clientSecret: '',
            sftp: {
                user: '',
                password: '',
                filepattern: '',
                schedule: ''
            },
            transforms: {
                jsonMap: {},
                csvMap: {},
                ignore: [],
                script: []
            }
        },
        lastQueued: ''
    }

    //Process this Inbound Batch 
    for (const q of event.Records)
    {
        // event.Records.forEach(async (i: SQSRecord) => {
        tqm = JSON.parse(q.body)

        // tqm.workKey = JSON.parse(q.body).workKey

        //When Testing (Launch config has pre-stored payload) - get some actual work queued
        if (tqm.workKey === 'process_2_pura_2023_10_27T15_11_40_732Z.csv')
        {
            tqm.workKey = await getAnS3ObjectforTesting(tcc.s3DropBucketWorkBucket!) ?? ""
            localTesting = true
        }
        else
        {
            testS3Key = ''
            testS3Bucket = ''
            localTesting = false
        }

        console.info(`Processing Work off the Queue - ${tqm.workKey} (versionId: ${tqm.versionId})`)
        if (tcc.SelectiveDebug.indexOf("_11,") > -1) console.info(`Selective Debug 11 - SQS Events - Processing Batch Item ${JSON.stringify(q)} `)

        try
        {
            const work = await getS3Work(tqm.workKey, tqm.versionId, tcc.s3DropBucketWorkQueue)

            if (work.length > 0)        //Retrieve Contents of the Work File  
            {

                postResult = await postToCampaign(work, tqm.custconfig, tqm.updateCount)

                if (tcc.SelectiveDebug.indexOf("_8,") > -1) console.info(`Selective Debug 8 - POST Result for ${tqm.workKey}(versionId: ${tqm.versionId}): ${postResult} `)

                if (postResult.indexOf('retry') > -1)
                {
                    console.warn(`Retry Marked for ${tqm.workKey}(versionId: ${tqm.versionId})(Retry Report: ${sqsBatchFail.batchItemFailures.length + 1}) Returning Work Item ${q.messageId} to Process Queue.`)
                    //Add to BatchFail array to Retry processing the work 
                    sqsBatchFail.batchItemFailures.push({ itemIdentifier: q.messageId })
                    if (tcc.SelectiveDebug.indexOf("_12,") > -1) console.info(`Selective Debug 12 - Added ${tqm.workKey} (versionId: ${tqm.versionId}) to SQS Events Retry \n${JSON.stringify(sqsBatchFail)} `)
                }

                else if (postResult.toLowerCase().indexOf('unsuccessful post') > -1)
                {
                    console.error(`Error - Unsuccesful POST(Hard Failure) for ${tqm.workKey}(versionId: ${tqm.versionId}): \n${postResult} \n Customer: ${tqm.custconfig.Customer}, Pod: ${tqm.custconfig.pod}, ListId: ${tqm.custconfig.listId} `)
                }
                else
                {
                    if (postResult.toLowerCase().indexOf('partially succesful') > -1)
                    {
                        console.info(`Most Work was Successfully Posted to Campaign, exceptions are: \n${postResult}`)
                    }

                    else if (postResult.toLowerCase().indexOf('successfully posted') > -1)
                    {
                        console.info(`Work Successfully Posted to Campaign - ${tqm.custconfig.listName} from (${tqm.workKey} - versionId: ${tqm.versionId}), will now Delete the Work from the S3 Process Queue`)
                    }

                    //Delete the Work file
                    const d: string = await deleteS3Object(tqm.workKey, tqm.versionId, tcc.s3DropBucketWorkBucket!)
                    if (d === '204') console.info(`Successful Deletion of Work: ${tqm.workKey} (versionId: ${tqm.versionId})`)
                    else console.error(`Failed to Delete ${tqm.workKey} (versionId: ${tqm.versionId}). Expected '204' but received ${d} `)

                }
            }
            else throw new Error(`Failed to retrieve work file(${tqm.workKey}) `)

        } catch (e)
        {
            console.error(`Exception - Processing a Work File(${tqm.workKey} off the Work Queue - \n${e}} `)
        }

    }

    console.info(`Processed ${event.Records.length} Work Queue records. Items Retry Count: ${sqsBatchFail.batchItemFailures.length} \nItems Retry List: ${JSON.stringify(sqsBatchFail)} `)

    if (tcc.S3DropBucketProcessQueueMaintHours > 0)
    {
        const maintenance = await maintainS3DropBucketQueueBucket(tqm.custconfig)

        if (tcc.SelectiveDebug.indexOf("_27,") > -1)
        {
            const l = maintenance[0] as number
            if (l > 0) console.info(`Selective Debug 27 - Work Files ReQueued: \n${maintenance}`)
            else console.info(`Selective Debug 27 - No Work files found to ReQueue`)
        }

    }




    //ToDo: Complete the Final Processing Outcomes messaging for Queue Processing 
    // if (tcc.SelectiveDebug.indexOf("_21,") > -1) console.info(`Selective Debug 21 - \n${ JSON.stringify(processS3ObjectStreamResolution) } `)

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



export const s3DropBucketSFTPHandler: Handler = async (event: SQSEvent, context: Context) => {

    // const sftpClient = new sftpClient()

    if (
        process.env["ProcessQueueVisibilityTimeout"] === undefined ||
        process.env["ProcessQueueVisibilityTimeout"] === '' ||
        process.env["ProcessQueueVisibilityTimeout"] === null
    )
    {
        tcc = await getValidateTricklerConfig()
    }

    console.info(`S3 Dropbucket SFTP Processor Selective Debug Set is: ${tcc.SelectiveDebug!} `)

    if (tcc.SelectiveDebug.indexOf("_9,") > -1) console.info(`Selective Debug 9 - Process Environment Vars: ${JSON.stringify(process.env)} `)


    console.info(`SFTP  Received Event: ${JSON.stringify(event)} `)
    //Existing Event Emit at every 1 minute 

    //For all work defined confirm a Scheduler2 Event exists for that work
    // If it does not exist Create a Scheduler2 event for the defined Schedule.
    //Write all existing Scheduler2 Events as a Log Entry
    //
    //Process all Scheduler2 Events in the current batch of events
    //  Scheduler2 Events are
    // {
    //     "version": "0",
    //         "id": "d565d36f-a484-46ca-8ca8-ff01feb2c827",
    //         "detail-type": "Scheduled Event",
    //         "source": "aws.scheduler",
    //         "account": "777957353822",
    //         "time": "2024-02-19T15:11:32Z",
    //         "region": "us-east-1",
    //          "resources": [
    //              "arn:aws:scheduler:us-east-1:777957353822:schedule/default/s3DropBucketSFTPFunctionComplexScheduleEvent"
    //          ],
    //       "detail": "{}"
    // }
    //



    // const client = new SchedulerClient(config)
    const client = new SchedulerClient()

    const input = {
        // GroupName: "STRING_VALUE",
        NamePrefix: "STRING_VALUE",
        // State: "STRING_VALUE",
        NextToken: "STRING_VALUE",
        MaxResults: Number("int"),
    } as ListSchedulesCommandInput

    const command = new ListSchedulesCommand(input)

    const response = await client.send(command)
    // { // ListSchedulesOutput
    //   NextToken: "STRING_VALUE",
    //   Schedules: [ // ScheduleList // required
    //     { // ScheduleSummary
    //       Arn: "STRING_VALUE",
    //       Name: "STRING_VALUE",
    //       GroupName: "STRING_VALUE",
    //       State: "STRING_VALUE",
    //       CreationDate: new Date("TIMESTAMP"),
    //       LastModificationDate: new Date("TIMESTAMP"),
    //       Target: { // TargetSummary
    //         Arn: "STRING_VALUE", // required
    //       },
    //     },
    //   ],
    // };







    return







    console.info(`Received SFTP SQS Events Batch of ${event.Records.length} records.`)

    if (tcc.SelectiveDebug.indexOf("_4,") > -1) console.info(`Selective Debug 4 - Received ${event.Records.length} SFTP Queue Records.Records are: \n${JSON.stringify(event)} `)




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

        //General Plan:
        // Process SQS Events being emitted for Check FTP Directory for Customer X
        // Process SQS Events being emitted for 
        // Check  configs to emit another round of SQS Events for the next round of FTP Work.



        // event.Records.forEach(async (i: SQSRecord) => {
        const tqm: tcQueueMessage = JSON.parse(q.body)

        tqm.workKey = JSON.parse(q.body).workKey

        //When Testing - get some actual work queued
        if (tqm.workKey === 'process_2_pura_2023_10_27T15_11_40_732Z.csv')
        {
            tqm.workKey = await getAnS3ObjectforTesting(tcc.s3DropBucket!) ?? ""
        }

        console.info(`Processing Work Queue for ${tqm.workKey}`)
        if (tcc.SelectiveDebug.indexOf("_11,") > -1) console.info(`Selective Debug 11 - SQS Events - Processing Batch Item ${JSON.stringify(q)} `)

        // try
        // {
        //     const work = await getS3Work(tqm.workKey, "tricklercache-process")
        //     if (work.length > 0)        //Retreive Contents of the Work File  
        //     {
        //         postResult = await postToCampaign(work, tqm.custconfig, tqm.updateCount)
        //         if (tcc.SelectiveDebug.indexOf("_8,") > -1) console.info(`Selective Debug 8 - POST Result for ${ tqm.workKey }: ${ postResult } `)

        //         if (postResult.indexOf('retry') > -1)
        //         {
        //             console.warn(`Retry Marked for ${ tqm.workKey }(Retry Report: ${ sqsBatchFail.batchItemFailures.length + 1 }) Returning Work Item ${ q.messageId } to Process Queue.`)
        //             //Add to BatchFail array to Retry processing the work 
        //             sqsBatchFail.batchItemFailures.push({ itemIdentifier: q.messageId })
        //             if (tcc.SelectiveDebug.indexOf("_12,") > -1) console.info(`Selective Debug 12 - Added ${ tqm.workKey } to SQS Events Retry \n${ JSON.stringify(sqsBatchFail) } `)
        //         }

        //         if (postResult.toLowerCase().indexOf('unsuccessful post') > -1)
        //             console.error(`Error - Unsuccesful POST(Hard Failure) for ${ tqm.workKey }: \n${ postResult } \n Customer: ${ tqm.custconfig.Customer }, Pod: ${ tqm.custconfig.pod }, ListId: ${ tqm.custconfig.listId } \n${ work } `)

        //         if (postResult.toLowerCase().indexOf('successfully posted') > -1)
        //         {
        //             console.info(`Work Successfully Posted to Campaign(${ tqm.workKey }), Deleting Work from S3 Process Queue`)

        //             const d: string = await deleteS3Object(tqm.workKey, 'tricklercache-process')
        //             if (d === '204') console.info(`Successful Deletion of Work: ${ tqm.workKey } `)
        //             else console.error(`Failed to Delete ${ tqm.workKey }. Expected '204' but received ${ d } `)
        //         }


        //     }
        //     else throw new Error(`Failed to retrieve work file(${ tqm.workKey }) `)

        // } catch (e)
        // {
        //     console.error(`Exception - Processing a Work File(${ tqm.workKey } - \n${ e } \n${ JSON.stringify(tqm) }`)
        // }

    }

    console.info(`Processed ${event.Records.length} SFTP Requests.Items Fail Count: ${sqsBatchFail.batchItemFailures.length}\nItems Failed List: ${JSON.stringify(sqsBatchFail)}`)

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


async function sftpConnect (options: { host: any; port: any; username?: string; password?: string }) {
    console.info(`Connecting to ${options.host}: ${options.port}`)
    try
    {
        // await sftpClient.connect(options)
    } catch (err)
    {
        console.info('Failed to connect:', err)
    }
}

async function sftpDisconnect () {
    // await sftpClient.end()
}

async function sftpListFiles (remoteDir: string, fileGlob: ListFilterFunction) {
    console.info(`Listing ${remoteDir} ...`)
    let fileObjects: sftpClient.FileInfo[] = []
    try
    {
        // fileObjects = await sftpClient.list(remoteDir, fileGlob)
    } catch (err)
    {
        console.info('Listing failed:', err)
    }

    const fileNames = []

    for (const file of fileObjects)
    {
        if (file.type === 'd')
        {
            console.info(`${new Date(file.modifyTime).toISOString()} PRE ${file.name}`)
        } else
        {
            console.info(`${new Date(file.modifyTime).toISOString()} ${file.size} ${file.name}`)
        }

        fileNames.push(file.name)
    }

    return fileNames
}

async function sftpUploadFile (localFile: string, remoteFile: string) {
    console.info(`Uploading ${localFile} to ${remoteFile} ...`)
    try
    {
        // await sftpClient.put(localFile, remoteFile)
    } catch (err)
    {
        console.error('Uploading failed:', err)
    }
}

async function sftpDownloadFile (remoteFile: string, localFile: string) {
    console.info(`Downloading ${remoteFile} to ${localFile} ...`)
    try
    {
        // await sftpClient.get(remoteFile, localFile)
    } catch (err)
    {
        console.error('Downloading failed:', err)
    }
}

async function sftpDeleteFile (remoteFile: string) {
    console.info(`Deleting ${remoteFile}`)
    try
    {
        // await sftpClient.delete(remoteFile)
    } catch (err)
    {
        console.error('Deleting failed:', err)
    }
}


async function checkForTCConfigUpdates () {
    if (tcLogDebug) console.info(`Checking for S3DropBucket Config updates`)
    tcc = await getValidateTricklerConfig()

    if (tcc.SelectiveDebug.indexOf("_1,") > -1) console.info(`Refreshed S3DropBucket Queue Config \n ${JSON.stringify(tcc)} `)
}

async function getValidateTricklerConfig () {

    //Article notes that Lambda runs faster referencing process.env vars, lets see.  
    //Did not pan out, with all the issues with conversions needed to actually use as primary reference, can't see it being faster
    //Using process.env as a useful reference store, especially for accessToken, good across invocations
    //Validate then populate env vars with S3DropBucket config


    const getObjectCmd = {
        Bucket: 'tricklercache-configs',
        Key: 'tricklercache_config.jsonc',
    }

    let tcr
    let tc = {} as tcConfig
    try
    {
        tc = await s3.send(new GetObjectCommand(getObjectCmd))
            .then(async (getConfigS3Result: GetObjectCommandOutput) => {

                tcr = (await getConfigS3Result.Body?.transformToString('utf8')) as string

                //Parse comments out of the json before parse
                tcr = tcr.replaceAll(new RegExp(/[^:](\/\/.*(,|$|")?)/g), '')
                tcr = tcr.replaceAll(' ', '')
                tcr = tcr.replaceAll('\n', '')

                return JSON.parse(tcr)
            })
    } catch (e)
    {
        console.error(`Exception - Pulling TricklerConfig \n${tcr} \n${e} `)
    }

    try
    {

        if (tc.LOGLEVEL !== undefined && tc.LOGLEVEL.toLowerCase().indexOf('debug') > -1)
        {
            tcLogDebug = true
            process.env.tcLogDebug = "true"
        }

        if (tc.LOGLEVEL !== undefined && tc.LOGLEVEL.toLowerCase().indexOf('verbose') > -1)
        {
            tcLogVerbose = true
            process.env["tcLogVerbose"] = "true"
        }

        if (tc.SelectiveDebug !== undefined) process.env["SelectiveDebug"] = tc.SelectiveDebug


        if (!tc.s3DropBucket || tc.s3DropBucket === "")
        {
            throw new Error(`Exception - S3 DropBucket Configuration is not correct: ${tc.s3DropBucket}.`)
        }
        else process.env["s3DropBucket"] = tc.s3DropBucket

        if (!tc.s3DropBucketWorkBucket || tc.s3DropBucketWorkBucket === "")
        {
            throw new Error(`Exception - S3 DropBucket Work Bucket Configuration is not correct: ${tc.s3DropBucketWorkBucket} `)
        }
        else process.env["s3DropBucketWorkBucket"] = tc.s3DropBucketWorkBucket

        if (!tc.s3DropBucketWorkQueue || tc.s3DropBucketWorkQueue === "")
        {
            throw new Error(`Exception - S3 DropBucket Work Queue Configuration is not correct: ${tc.s3DropBucketWorkQueue} `)
        }
        else process.env["s3DropBucketWorkQueue"] = tc.s3DropBucketWorkQueue


        // if (tc.SQS_QUEUE_URL !== undefined) tcc.SQS_QUEUE_URL = tc.SQS_QUEUE_URL
        // else throw new Error(`S3DropBucket Config invalid definition: SQS_QUEUE_URL - ${ tc.SQS_QUEUE_URL } `)

        if (tc.xmlapiurl != undefined) process.env["xmlapiurl"] = tc.xmlapiurl
        else throw new Error(`S3DropBucket Config invalid definition: xmlapiurl - ${tc.xmlapiurl} `)

        if (tc.restapiurl !== undefined) process.env["restapiurl"] = tc.restapiurl
        else throw new Error(`S3DropBucket Config invalid definition: restapiurl - ${tc.restapiurl} `)

        if (tc.authapiurl !== undefined) process.env["authapiurl"] = tc.authapiurl
        else throw new Error(`S3DropBucket Config invalid definition: authapiurl - ${tcc.authapiurl} `)


        if (tc.ProcessQueueQuiesce !== undefined)
        {
            process.env["ProcessQueueQuiesce"] = tc.ProcessQueueQuiesce.toString()
        }
        else
            throw new Error(
                `S3DropBucket Config invalid definition: ProcessQueueQuiesce - ${tc.ProcessQueueQuiesce} `,
            )

        //deprecated in favor of using AWS interface to set these on the queue
        // if (tc.ProcessQueueVisibilityTimeout !== undefined)
        //     process.env.ProcessQueueVisibilityTimeout = tc.ProcessQueueVisibilityTimeout.toFixed()
        // else
        //     throw new Error(
        //         `S3DropBucket Config invalid definition: ProcessQueueVisibilityTimeout - ${ tc.ProcessQueueVisibilityTimeout } `,
        //     )

        // if (tc.ProcessQueueWaitTimeSeconds !== undefined)
        //     process.env.ProcessQueueWaitTimeSeconds = tc.ProcessQueueWaitTimeSeconds.toFixed()
        // else
        //     throw new Error(
        //         `S3DropBucket Config invalid definition: ProcessQueueWaitTimeSeconds - ${ tc.ProcessQueueWaitTimeSeconds } `,
        //     )

        // if (tc.RetryQueueVisibilityTimeout !== undefined)
        //     process.env.RetryQueueVisibilityTimeout = tc.ProcessQueueWaitTimeSeconds.toFixed()
        // else
        //     throw new Error(
        //         `S3DropBucket Config invalid definition: RetryQueueVisibilityTimeout - ${ tc.RetryQueueVisibilityTimeout } `,
        //     )

        // if (tc.RetryQueueInitialWaitTimeSeconds !== undefined)
        //     process.env.RetryQueueInitialWaitTimeSeconds = tc.RetryQueueInitialWaitTimeSeconds.toFixed()
        // else
        //     throw new Error(
        //         `S3DropBucket Config invalid definition: RetryQueueInitialWaitTimeSeconds - ${ tc.RetryQueueInitialWaitTimeSeconds } `,
        //     )


        if (tc.MaxBatchesWarning !== undefined)
            process.env["RetryQueueInitialWaitTimeSeconds"] = tc.MaxBatchesWarning.toFixed()
        else
            throw new Error(
                `S3DropBucket Config invalid definition: MaxBatchesWarning - ${tc.MaxBatchesWarning} `,
            )


        if (tc.S3DropBucketQuiesce !== undefined)
        {
            process.env["DropBucketQuiesce"] = tc.S3DropBucketQuiesce.toString()
        }
        else
            throw new Error(
                `S3DropBucket Config invalid definition: DropBucketQuiesce - ${tc.S3DropBucketQuiesce} `,
            )


        if (tc.S3DropBucketMaintHours != undefined)
        {
            process.env["DropBucketMaintHours"] = tc.S3DropBucketMaintHours.toString()
        }
        else tc.S3DropBucketMaintHours = -1

        if (tc.S3DropBucketProcessQueueMaintHours != undefined)
        {
            process.env["DropBucketProcessQueueMaintHours"] = tc.S3DropBucketProcessQueueMaintHours.toString()
        }
        else tc.S3DropBucketProcessQueueMaintHours = -1

        if (tc.S3DropBucketPurge !== undefined)
            process.env["DropBucketPurge"] = tc.S3DropBucketPurge
        else
            throw new Error(
                `S3DropBucket Config invalid definition: DropBucketPurge - ${tc.S3DropBucketPurge} `,
            )

        if (tc.S3DropBucketPurgeCount !== undefined)
            process.env["DropBucketPurgeCount"] = tc.S3DropBucketPurgeCount.toFixed()
        else
            throw new Error(
                `S3DropBucket Config invalid definition: DropBucketPurgeCount - ${tc.S3DropBucketPurgeCount} `,
            )

        if (tc.QueueBucketQuiesce !== undefined)
        {
            process.env["QueueBucketQuiesce"] = tc.QueueBucketQuiesce.toString()
        }
        else
            throw new Error(
                `S3DropBucket Config invalid definition: QueueBucketQuiesce - ${tc.QueueBucketQuiesce} `,
            )

        if (tc.QueueBucketPurge !== undefined)
            process.env["QueueBucketPurge"] = tc.QueueBucketPurge
        else
            throw new Error(
                `S3DropBucket Config invalid definition: QueueBucketPurge - ${tc.QueueBucketPurge} `,
            )

        if (tc.QueueBucketPurgeCount !== undefined)
            process.env["QueueBucketPurgeCount"] = tc.QueueBucketPurgeCount.toFixed()
        else
            throw new Error(
                `S3DropBucket Config invalid definition: QueueBucketPurgeCount - ${tc.QueueBucketPurgeCount} `,
            )

        if (tc.prefixFocus !== undefined && tc.prefixFocus != "")
        {
            process.env["TricklerProcessPrefix"] = tc.prefixFocus
            console.warn(`A Prefix Focus has been configured.Only DropBucket Objects with the prefix "${tc.prefixFocus}" will be processed.`)
        }

    } catch (e)
    {
        throw new Error(`Exception - Parsing S3DropBucket Config File ${e} `)
    }

    if (tc.SelectiveDebug.indexOf("_1,") > -1) console.info(`Selective Debug 1 - Pulled tricklercache_config.jsonc: \n${JSON.stringify(tc)} `)

    return tc
}

async function getCustomerConfig (filekey: string) {

    // Retrieve file's prefix as Customer Name
    if (!filekey) throw new Error(`Exception - Cannot resolve Customer Config without a valid Customer Prefix(file prefix is ${filekey})`)

    if (filekey.indexOf('/') > -1)
    {
        filekey = filekey.split('/').at(-1) ?? filekey
    }

    const customer = filekey.split('_')[0] + '_'

    if (customer === '_' || customer.length < 4)
    {
        throw new Error(`Exception - Customer cannot be determined from S3 Cache File '${filekey}'      \n      `)
    }

    let configJSON = {} as customerConfig
    // const configObjs = [new Uint8Array()]

    const getObjectCommand = {
        Key: `${customer}config.jsonc`,
        Bucket: 'tricklercache-configs'
    }

    let ccr

    try
    {
        await s3.send(new GetObjectCommand(getObjectCommand))
            .then(async (getConfigS3Result: GetObjectCommandOutput) => {
                ccr = await getConfigS3Result.Body?.transformToString('utf8') as string

                if (tcc.SelectiveDebug.indexOf("_10,") > -1) console.info(`Selective Debug 10 - Customers Config: \n ${ccr} `)

                //Parse comments out of the json before parse
                ccr = ccr.replaceAll(new RegExp(/[^:](\/\/.*(,|$|")?)/g), '')
                ccr = ccr.replaceAll(' ', '')
                ccr = ccr.replaceAll('\n', '')

                configJSON = JSON.parse(ccr)

            })
            .catch((e) => {

                const err: string = JSON.stringify(e)

                if (err.indexOf('specified key does not exist') > -1)
                    throw new Error(`Exception - Customer Config ${customer}config.jsonc does not exist on tricklercache-configs bucket \nException ${e} `)

                if (err.indexOf('NoSuchKey') > -1)
                    throw new Error(`Exception - Customer Config Not Found(${customer}config.jsonc) on tricklercache-configs\nException ${e} `)

                throw new Error(`Exception - Retrieving Config(${customer}config.jsonc) from tricklercache-configs \nException ${e} `)

            })
    } catch (e)
    {
        throw new Error(`Exception - Pulling Customer Config \n${ccr} \n${e} `)
    }

    customersConfig = await validateCustomerConfig(configJSON)
    return customersConfig as customerConfig
}

async function validateCustomerConfig (config: customerConfig) {
    if (!config || config === null)
    {
        throw new Error('Invalid Config - empty or null config')
    }

    if (!config.Customer)
    {
        throw new Error('Invalid Config - Customer is not defined')
    }
    else if (config.Customer.length < 4 ||
        !config.Customer.endsWith('_')
    )
    {
        throw new Error(`Invalid Config - Customer string is not valid, must be at least 3 characters and a trailing underscore, '_'`)
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
    if (!config.updates)
    {
        throw new Error('Invalid Config - Updates is not defined')
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


    if (!config.updates.toLowerCase().match(/^(?:singular|bulk)$/gim))
    {
        throw new Error("Invalid Config - Updates is not 'Singular' or 'Bulk' ")
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

    if (config.listType.toLowerCase() == 'dbkeyed' && !config.DBKey)
    {
        throw new Error("Invalid Config - Update set as Database Keyed but DBKey is not defined. ")
    }

    if (config.listType.toLowerCase() == 'dbnonkeyed' && !config.LookupKeys)
    {
        throw new Error("Invalid Config - Update set as Database NonKeyed but lookupKeys is not defined. ")
    }

    if (!config.sftp) { config.sftp = { user: "", password: "", filepattern: "", schedule: "" } }

    if (config.sftp.user && config.sftp.user !== '') { }
    if (config.sftp.password && config.sftp.password !== '') { }
    if (config.sftp.filepattern && config.sftp.filepattern !== '') { }
    if (config.sftp.schedule && config.sftp.schedule !== '') { }

    if (!config.transforms)
    {
        Object.assign(config, { "transforms": {} })
    }
    if (!config.transforms.jsonMap)
    {
        Object.assign(config.transforms, { jsonMap: { "none": "" } })
    }
    if (!config.transforms.csvMap)
    {
        Object.assign(config.transforms, { csvMap: { "none": "" } })
    }
    if (!config.transforms.ignore)
    {
        Object.assign(config.transforms, { ignore: [] })
    }
    if (!config.transforms.script)
    {
        Object.assign(config.transforms, { script: "" })
    }

    // if (Object.keys(config.transform[0].jsonMap)[0].indexOf('none'))
    if (!config.transforms.jsonMap.hasOwnProperty("none"))  
    {
        let tmpMap: { [key: string]: string } = {}
        // let tmpmap2: Record<string, string> = {}
        const jm = config.transforms.jsonMap as unknown as { [key: string]: string }
        for (const m in jm)
        {
            try
            {
                const p = jm[m]
                const v = jsonpath.parse(p)
                tmpMap[m] = jm[m]
                // tmpmap2.m = jm.m
            }
            catch (e)
            {
                console.error(`Invalid JSONPath defined in Customer config: ${m}: "${m}", \nInvalid JSONPath - ${e} `)
            }
        }
        config.transforms.jsonMap = tmpMap
    }

    return config as customerConfig
}


async function storeAndQueueWork (chunks: string[], s3Key: string, config: customerConfig, recs: number, batch: number) {

    if (batch > tcc.MaxBatchesWarning) console.warn(`Warning: Updates from the S3 Object(${s3Key}) are exceeding(${batch}) the Warning Limit of ${tcc.MaxBatchesWarning} Batches per Object.`)
    // throw new Error(`Updates from the S3 Object(${ s3Key }) Exceed(${ batch }) Safety Limit of 20 Batches of 99 Updates each.Exiting...`)

    //Customers marked as "Singular" update files are not transformed, but sent to Firehose before this, 
    //  therefore need to transform Aggregate files as well as files marked as "Bulk"
    chunks = transforms(chunks, config)

    if (customersConfig.listType.toLowerCase() === 'dbkeyed' ||
        customersConfig.listType.toLowerCase() === 'dbnonkeyed')
    {
        xmlRows = convertJSONToXML_DBUpdates(chunks, config)
    }

    if (customersConfig.listType.toLowerCase() === 'relational')
    {
        xmlRows = convertJSONToXML_RTUpdates(chunks, config)
    }

    if (s3Key.indexOf('TestData') > -1)
    {
        s3Key = s3Key.split('/').at(-1) ?? s3Key
    }

    let key = s3Key.replace('.', '_')
    key = `${key}_update_${batch}_${recs}.xml`

    if (tcLogDebug) console.info(`Queuing Work for ${s3Key} - ${key}. (Batch ${batch} of ${Object.values(chunks).length} records)`)

    let AddWorkToS3ProcessBucketResults
    let AddWorkToSQSProcessQueueResults
    let v = ''

    try
    {
        AddWorkToS3ProcessBucketResults = await addWorkToS3ProcessStore(xmlRows, key)
            .then((res) => {
                // const l = { ...AddWorkToS3ProcessBucketResults } //as typeof sqw.OnEndStoreQueueResult.AddWorkToS3ProcessBucketResults
                // v = l.AddWorkToS3ProcessBucketResults.versionId

                return res
            })

    } catch (e)
    {
        const sqwError = `Exception - StoreAndQueueWork Add work to S3 Bucket exception \n${e} `
        console.error(sqwError)
        return { StoreS3WorkException: sqwError, StoreQueueWorkException: '', AddWorkToS3ProcessBucketResults, AddWorkToSQSProcessQueueResults }
    }

    v = AddWorkToS3ProcessBucketResults.versionId ?? ''

    try
    {
        AddWorkToSQSProcessQueueResults = await addWorkToSQSProcessQueue(config, key, v, batch.toString(), chunks.length.toString())
            .then((res) => {
                return res
            })
        //     {
        //         sqsWriteResult: "200",
        //         workQueuedSuccess: true,
        //         SQSSendResult: "{\"$metadata\":{\"httpStatusCode\":200,\"requestId\":\"e70fba06-94f2-5608-b104-e42dc9574636\",\"attempts\":1,\"totalRetryDelay\":0},\"MD5OfMessageAttributes\":\"0bca0dfda87c206313963daab8ef354a\",\"MD5OfMessageBody\":\"940f4ed5927275bc93fc945e63943820\",\"MessageId\":\"cf025cb3-dce3-4564-89a5-23dcae86dd42\"}",
        // }
    } catch (e)
    {
        const sqwError = `Exception - StoreAndQueueWork Add work to SQS Queue exception \n${e} `
        console.error(sqwError)
        return { StoreQueueWorkException: sqwError, StoreS3WorkException: '' }
    }

    if (tcc.SelectiveDebug.indexOf("_15,") > -1) console.info(`Selective Debug 15 - Results of Store and Queue of Updates - Add to Proces Bucket: ${JSON.stringify(AddWorkToS3ProcessBucketResults)} \n Add to Process Queue: ${JSON.stringify(AddWorkToSQSProcessQueueResults)} `)

    return { AddWorkToS3ProcessBucketResults, AddWorkToSQSProcessQueueResults }

}

function convertJSONToXML_RTUpdates (updates: string[], config: customerConfig) {

    if (updates.length < 1)
    {
        throw new Error(`Exception - Convert JSON to XML for RT - No Updates (${updates.length}) were passed to process. Customer ${config.Customer} `)
    }


    xmlRows = `<Envelope> <Body> <InsertUpdateRelationalTable> <TABLE_ID> ${config.listId} </TABLE_ID><ROWS>`

    let r = 0
    debugger
    for (const jo in updates)
    {
        const j = JSON.parse(updates[jo])
        r++
        xmlRows += `<ROW>`
        // Object.entries(jo).forEach(([key, value]) => {
        for (const l in j)
        {
            // console.info(`Record ${r} as ${key}: ${value}`)
            xmlRows += `<COLUMN name="${l}"> <![CDATA[${j[l]}]]> </COLUMN>`
        }
        xmlRows += `</ROW>`
    }

    //Tidy up the XML
    xmlRows += `</ROWS></InsertUpdateRelationalTable></Body></Envelope>`

    if (tcLogDebug) console.info(`Converting S3 Content to XML RT Updates. Packaging ${Object.values(updates).length} rows as updates to ${config.Customer}'s ${config.listName}`)
    if (tcc.SelectiveDebug.indexOf("_6,") > -1) console.info(`Selective Debug 6 - JSON to be converted to XML RT Updates: ${JSON.stringify(updates)}`)
    if (tcc.SelectiveDebug.indexOf("_17,") > -1) console.info(`Selective Debug 17 - XML from JSON for RT Updates: ${xmlRows}`)


    return xmlRows
}

function convertJSONToXML_DBUpdates (updates: string[], config: customerConfig) {

    if (updates.length < 1)
    {
        throw new Error(`Exception - Convert JSON to XML for DB - No Updates (${updates.length}) were passed to process. Customer ${config.Customer} `)
    }


    xmlRows = `<Envelope><Body>`
    let r = 0

    try
    {
        for (const jo in updates)
        {
            r++
            const j = JSON.parse(updates[jo])
            const s = JSON.stringify(j)

            xmlRows += `<AddRecipient><LIST_ID>${config.listId}</LIST_ID><CREATED_FROM>0</CREATED_FROM><UPDATE_IF_FOUND>true</UPDATE_IF_FOUND>`

            // If Keyed, then Column that is the key must be present in Column Set
            // If Not Keyed must use Lookup Fields
            // Use SyncFields as 'Lookup" values,
            //   Columns hold the Updates while SyncFields hold the 'lookup' values.


            //Only needed on non-keyed(In Campaign use DB -> Settings -> LookupKeys to find what fields are Lookup Keys)
            if (config.listType.toLowerCase() === 'dbnonkeyed')
            {
                const lk = config.LookupKeys.split(',')

                xmlRows += `<SYNC_FIELDS>`
                lk.forEach(k => {
                    k = k.trim()
                    const sf = `<SYNC_FIELD><NAME>${k}</NAME><VALUE><![CDATA[${j[k]}]]></VALUE></SYNC_FIELD>`
                    xmlRows += sf
                })

                xmlRows += `</SYNC_FIELDS>`
            }

            if (config.listType.toLowerCase() === 'dbkeyed')
            {
                //Placeholder
                //Don't need to do anything with DBKey, it's superfluous but documents the keys of the keyed DB
            }

            for (const pv in j)
            {
                xmlRows += `<COLUMN><NAME>${pv}</NAME><VALUE><![CDATA[${j[pv]}]]></VALUE></COLUMN>`
            }


            //CRM Lead Source Update 
            //Todo: CRM Lead Source as a config option
            xmlRows += `<COLUMN><NAME>CRM Lead Source</NAME><VALUE><![CDATA[S3DropBucket]]></VALUE></COLUMN>`

            xmlRows += `</AddRecipient>`

            // })
        }
    } catch (e)
    {
        console.error(`Exception - ConvertJSONtoXML_DBUpdates - \n${e}`)
    }

    xmlRows += `</Body></Envelope>`

    if (tcLogDebug) console.info(`Converting S3 Content to XML DB Updates. Packaging ${Object.values(updates).length} rows as updates to ${config.Customer}'s ${config.listName}`)
    if (tcc.SelectiveDebug.indexOf("_16,") > -1) console.info(`Selective Debug 16 - JSON to be converted to XML DB Updates: ${JSON.stringify(updates)}`)
    if (tcc.SelectiveDebug.indexOf("_17,") > -1) console.info(`Selective Debug 17 - XML from JSON for DB Updates: ${xmlRows}`)

    return xmlRows
}



function transforms (chunks: string[], config: customerConfig) {

    //Apply Transforms


    //Clorox Weather Data
    //Add dateday column
    if (config.Customer.toLowerCase().indexOf('cloroxweather_') > -1)
    {
        let t: typeof chunks = []

        const days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

        for (const l of chunks)
        {
            const j = JSON.parse(l)
            const d = j.datetime ?? ""
            if (d !== "")
            {
                const dt = new Date(d)
                const day = { "dateday": days[dt.getDay()] }

                Object.assign(j, day)
                t.push(JSON.stringify(j))
            }
        }
        if (t.length !== chunks.length)
        {
            throw new Error(`Error - Transform - Applying Clorox Custom Transform returns fewer records (${t.length}) than initial set ${chunks.length}`)
        }
        else chunks = t
    }



    //Apply Ignore
    // "Ignore": [ //Ignore column if it exists in the data
    //     "Col_BA",
    //     "Col_BB",
    //     "Col_BC"
    // ],
    if (config.transforms.ignore.length > 0)
    {
        let i: typeof chunks = []
        try
        {
            for (const l of chunks)
            {
                const jo = JSON.parse(l)
                for (const ig of config.transforms.ignore)
                {
                    // const { [keyToRemove]: removedKey, ...newObject } = originalObject;
                    // const { [ig]: , ...i } = jo
                    delete jo[ig]
                    i.push(JSON.stringify(jo))
                }
            }
        }
        catch (e)
        {
            console.error(`Exception - Transform - Applying Ignore - \n${e}`)
        }
        if (i.length !== chunks.length)
        {
            throw new Error(`Error - Transform - Applying Ignore returns fewer records ${i.length} than initial set ${chunks.length}`)
        }
        else chunks = i
    }


    //Apply the JSONMap - 
    //  JSONPath statements
    //      "jsonMap": {
    //          "email": "$.uniqueRecipient",
    //              "zipcode": "$.context.traits.address.postalCode"
    //      },

    if (Object.keys(config.transforms.jsonMap).indexOf('none') < 0)
    {
        let r: typeof chunks = []
        try
        {
            for (const l of chunks)
            {
                const jo = JSON.parse(l)
                const jmr = applyJSONMap(jo, config.transforms.jsonMap)
                r.push(JSON.stringify(jmr))
            }
        } catch (e)
        {
            console.error(`Exception - Transform - Applying JSONMap \n${e}`)
        }

        if (r.length !== chunks.length)
        {
            throw new Error(`Error - Transform - Applying JSONMap returns fewer records (${r.length}) than initial set ${chunks.length}`)
        }
        else chunks = r
    }

    //Apply CSVMap
    // "csvMap": { //Mapping when processing CSV files
    //       "Col_AA": "COL_XYZ", //Write Col_AA with data from Col_XYZ in the CSV file
    //       "Col_BB": "COL_MNO",
    //       "Col_CC": "COL_GHI",

    //       "Col_DD": 1, //Write Col_DD with data from the 1st column of data in the CSV file.
    //       "Col_DE": 2,
    //       "Col_DF": 3
    // },
    if (Object.keys(config.transforms.csvMap).indexOf('none') < 0)
    {
        let c: typeof chunks = []
        try
        {
            for (const l of chunks)
            {
                const jo = JSON.parse(l)

                const map = config.transforms.csvMap as { [key: string]: string }
                Object.entries(map).forEach(([k, v]) => {
                    if (typeof v !== "number") jo[k] = jo[v] ?? ""
                    else
                    {
                        debugger
                        const vk = Object.keys(jo)[v]
                        // const vkk = vk[v]
                        jo[k] = jo[vk] ?? ""

                    }
                })
                c.push(JSON.stringify(jo))
            }
        } catch (e)
        {
            console.error(`Exception - Transforms - Applying CSVMap \n${e}`)
        }
        if (c.length !== chunks.length)
        {
            throw new Error(`Error - Transform - Applying CSVMap returns fewer records (${c.length}) than initial set ${chunks.length}`)
        }
        else chunks = c
    }

    return chunks
}

function applyJSONMap (jsonObj: object, map: { [key: string]: string }) {



    // j.forEach((o: object) => {
    //     const a = applyJSONMap([o], config.transforms[0].jsonMap)
    //     am.push(a)
    //     chunks = am
    // })

    Object.entries(map).forEach(([k, v]) => {
        try
        {
            let j = jsonpath.value(jsonObj, v)
            if (!j)
            {
                console.error(`Data not Found for JSONPath statement ${k}: ${v},  \nTarget Data: \n${JSON.stringify(jsonObj)} `)
            }
            else
            {
                // Object.assign(jsonObj, { [k]: jsonpath.value(jsonObj, v) })
                Object.assign(jsonObj, { [k]: j })
            }
        } catch (e)
        {
            console.error(`Error parsing data for JSONPath statement ${k} ${v}, ${e} \nTarget Data: \n${JSON.stringify(jsonObj)} `)
        }

        // const a1 = jsonpath.parse(value)
        // const a2 = jsonpath.parent(s3Chunk, value)
        // const a3 = jsonpath.paths(s3Chunk, value)
        // const a4 = jsonpath.query(s3Chunk, value)
        // const a6 = jsonpath.value(s3Chunk, value)

        //Confirms Update was accomplished 
        // const j = jsonpath.query(s3Chunk, v)
        // console.info(`${ j } `)
    })
    return jsonObj
}


async function addWorkToS3ProcessStore (queueUpdates: string, key: string) {
    //write to the S3 Process Bucket

    // let s3p = {
    //     AddWorkToS3ProcessBucketResults: {
    //         versionId: '',
    //         S3ProcessBucketResult: '',
    //         AddWorkToS3ProcessBucket: {}
    //     }
    // }

    if (tcc.QueueBucketQuiesce)
    {
        console.warn(`Work/Process Bucket Quiesce is in effect, no New Work Files are being written to the S3 Queue Bucket. This work file is for ${key}`)
        return { versionId: '', S3ProcessBucketResult: '', AddWorkToS3ProcessBucket: 'In Quiesce' }
    }


    const s3PutInput = {
        Body: queueUpdates,
        Bucket: tcc.s3DropBucketWorkBucket,
        Key: key,
    }

    if (tcLogDebug) console.info(`Write Work to S3 Process Queue for ${key}`)

    let s3ProcessBucketResult
    let addWorkToS3ProcessBucket

    try
    {
        addWorkToS3ProcessBucket = await s3.send(new PutObjectCommand(s3PutInput))
            .then(async (s3PutResult: PutObjectCommandOutput) => {

                if (s3PutResult.$metadata.httpStatusCode !== 200)
                {
                    throw new Error(`Failed to write Work File to S3 Process Store (Result ${s3PutResult}) for ${key} of ${queueUpdates.length} characters`)
                }
                return s3PutResult
            })
            .catch(err => {
                throw new Error(`PutObjectCommand Results Failed for (${key} of ${queueUpdates.length} characters) to S3 Processing bucket: ${err}`)
                //return {StoreS3WorkException: err}
            })
    } catch (e)
    {
        throw new Error(`Exception - Put Object Command for writing work(${key} to S3 Processing bucket: ${e}`)
        // return { StoreS3WorkException: e }
    }

    if (tcc.SelectiveDebug.indexOf("_7,") > -1) console.info(`Selective Debug 7 - ${addWorkToS3ProcessBucket}`)

    s3ProcessBucketResult = JSON.stringify(addWorkToS3ProcessBucket.$metadata.httpStatusCode, null, 2)

    const vd = addWorkToS3ProcessBucket.VersionId ?? ""

    const m = `Wrote Work File (${key} (versionId: ${vd}) of ${queueUpdates.length} characters) to S3 Processing Bucket (Result ${s3ProcessBucketResult}`

    return {
        versionId: vd,
        AddWorkToS3ProcessBucket: addWorkToS3ProcessBucket,
        S3ProcessBucketResult: s3ProcessBucketResult
    }
}


async function addWorkToSQSProcessQueue (config: customerConfig, key: string, versionId: string, batch: string, recCount: string) {

    if (tcc.QueueBucketQuiesce)
    {
        console.warn(`Work/Process Bucket Quiesce is in effect, no New Work Files are being written to the SQS Queue of S3 Work Bucket. This work file is for ${key}`)
        return { versionId: '', S3ProcessBucketResult: '', AddWorkToS3ProcessBucket: 'In Quiesce' }
    }

    const sqsQMsgBody = {} as tcQueueMessage
    sqsQMsgBody.workKey = key
    sqsQMsgBody.versionId = versionId
    sqsQMsgBody.attempts = 1
    sqsQMsgBody.updateCount = recCount
    sqsQMsgBody.custconfig = config
    sqsQMsgBody.lastQueued = Date.now().toString()

    const sqsParams = {
        MaxNumberOfMessages: 1,
        QueueUrl: tcc.s3DropBucketWorkQueue,
        //Defer to setting these on the Queue in AWS SQS Interface
        // VisibilityTimeout: parseInt(tcc.ProcessQueueVisibilityTimeout),
        // WaitTimeSeconds: parseInt(tcc.ProcessQueueWaitTimeSeconds),
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

    if (tcLogDebug) console.info(`Add Work to SQS Process Queue - SQS Params: ${JSON.stringify(sqsParams)}`)

    let sqsSendResult
    let sqsWriteResult

    try
    {
        await sqsClient.send(new SendMessageCommand(sqsParams))
            .then((sqsSendMessageResult: SendMessageCommandOutput) => {
                sqsWriteResult = JSON.stringify(sqsSendMessageResult.$metadata.httpStatusCode, null, 2)
                if (sqsWriteResult !== '200')
                {
                    const storeQueueWorkException =
                        `Failed writing to SQS Process Queue (queue URL: ${sqsParams.QueueUrl}), ${sqsQMsgBody.workKey}, SQS Params${JSON.stringify(sqsParams)})`

                    return { StoreQueueWorkException: storeQueueWorkException }
                }
                sqsSendResult = sqsSendMessageResult

                if (tcc.SelectiveDebug.indexOf("_14,") > -1) console.info(`Selective Debug 14 - Queued Work to SQS Process Queue (${sqsQMsgBody.workKey}) - Result: ${sqsWriteResult} `)

                return sqsSendMessageResult
            })
            .catch(err => {
                const storeQueueWorkException =
                    `Failed writing to SQS Process Queue (${err}) \nQueue URL: ${sqsParams.QueueUrl})\nWork to be Queued: ${sqsQMsgBody.workKey}\nSQS Params: ${JSON.stringify(sqsParams)}`
                return { StoreQueueWorkException: storeQueueWorkException }
            })
    } catch (e)
    {
        console.error(
            `Exception - Writing to SQS Process Queue - (queue URL${sqsParams.QueueUrl}), ${sqsQMsgBody.workKey}, SQS Params${JSON.stringify(sqsParams)}) - Error: ${e}`,
        )
    }

    return {
        SQSWriteResult: sqsWriteResult,
        AddWorkToSQSQueueResult: sqsSendResult
    }
}



// Deprecated in favor of AWS CLI commands
//
// async function requeueWork (customer: string) {
//     const cc = await getCustomerConfig(customer)

//     const bucket = tcc.s3DropBucketWorkBucket

//     const listReq = {
//         Bucket: bucket,
//         MaxKeys: 1000,
//         ifMatch: customer
//     } as ListObjectsV2CommandInput

//     let q = 0
//     let isTruncated: boolean | unknown = true

//     try
//     {
//         while (isTruncated)
//         {
//             await s3.send(new ListObjectsV2Command(listReq))
//                 .then(async (s3ListResult: ListObjectsV2CommandOutput) => {

//                     listReq.ContinuationToken = s3ListResult?.NextContinuationToken
//                     isTruncated = s3ListResult?.IsTruncated

//                     s3ListResult.Contents?.forEach(async (listItem) => {
//                         q++
//                         const r = await addWorkToSQSProcessQueue(cc, listItem.Key as string, vid, "", "")
//                         if (r.SQSWriteResult !== '200') console.error(`Non Successful return, received ${r} ) on ReQueue of ${listItem.Key} `)
//                     })
//                 })
//         }
//     } catch (e)
//     {
//         console.error(`Exception - While Requeuing ${customer} Updates from Process bucket: \n${e} `)
//     }

//     console.info(`Requeued ${q} Updates of ${customer} from Process bucket`)

//     return `Requeued ${q} Updates of ${customer} from Process bucket`
// }




// async function reQueue (sqsevent: SQSEvent, queued: tcQueueMessage) {

//     const workKey = JSON.parse(sqsevent.Records[0].body).workKey

//     const sqsParams = {
//         MaxNumberOfMessages: 1,
//         QueueUrl: tcc.SQS_QUEUE_URL,
//         VisibilityTimeout: parseInt(tcc.ProcessQueueVisibilityTimeout!),
//         WaitTimeSeconds: parseInt(tcc.ProcessQueueWaitTimeSeconds!),
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




async function getS3Work (s3Key: string, version: string, bucket: string) {

    if (tcLogDebug) console.info(`Debug - GetS3Work Key: ${s3Key}`)

    let getObjectCmd
    if (version !== "")
    {
        getObjectCmd = {
            Bucket: bucket,
            Key: s3Key,
            VersionId: version
        } as GetObjectCommandInput
    }
    else
    {
        getObjectCmd = {
            Bucket: bucket,
            Key: s3Key
        } as GetObjectCommandInput
    }

    let work: string = ''
    try
    {
        await s3.send(new GetObjectCommand(getObjectCmd))
            .then(async (getS3Result: GetObjectCommandOutput) => {
                work = (await getS3Result.Body?.transformToString('utf8')) as string
                if (tcLogDebug) console.info(`Work Pulled (${work.length} chars): ${s3Key} (versionId: ${version})`)
            })
    } catch (e)
    {
        const err: string = JSON.stringify(e)

        if (err.indexOf('NoSuchKey') > -1)
            throw new Error(`Exception - Work Not Found on S3 Process Queue (${s3Key} (versionId: ${version})) Work will not be marked for Retry. \n${e}`)
        else throw new Error(`Exception - Retrieving Work from S3 Process Queue for ${s3Key} (versionId: ${version}). \n ${e}`)
    }
    return work
}

async function saveS3Work (s3Key: string, body: string, bucket: string) {

    if (tcLogDebug) console.info(`Debug - SaveS3Work Key: ${s3Key}`)

    const putObjectCmd = {
        Bucket: bucket,
        Key: s3Key,
        Body: body
        // ContentLength: Number(`${body.length}`),
    } as GetObjectCommandInput

    let saveS3: string = ''
    try
    {
        await s3.send(new PutObjectCommand(putObjectCmd))
            .then(async (getS3Result: GetObjectCommandOutput) => {
                saveS3 = (await getS3Result.Body?.transformToString('utf8')) as string
                if (tcLogDebug) console.info(`Work Saved (${saveS3.length} chars): ${s3Key}`)
            })
    } catch (e)
    {
        throw new Error(`Exception - Saving Work for ${s3Key}. \n ${e}`)
    }
    return saveS3
}

async function deleteS3Object (s3ObjKey: string, version: string, bucket: string) {

    let delRes = ''

    //  
    //ToDo: Attempt to uniquely delete one object with a duplicate name of another object (Yep, that's a thing in S3)
    //
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
    //
    //     })
    //
    //

    let s3D
    if (version !== '')
    {
        s3D = {
            Key: s3ObjKey,
            Bucket: bucket,
            VersionId: version
        }
    }
    else
    {
        s3D = {
            Key: s3ObjKey,
            Bucket: bucket
        }
    }
    const d = new DeleteObjectCommand(s3D)

    // d.setMatchingETagConstraints(Collections.singletonList(et));

    try
    {
        await s3.send(d)
            .then(async (s3DelResult: DeleteObjectCommandOutput) => {
                delRes = JSON.stringify(s3DelResult.$metadata.httpStatusCode, null, 2)
            })
            .catch((e) => {
                console.error(`Exception - Attempting S3 Delete Command for ${s3ObjKey}: \n ${e} `)
                return delRes
            })
    } catch (e)
    {
        console.error(`Exception - Attempting S3 Delete Command for ${s3ObjKey}: \n ${e} `)
    }
    return delRes
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
                'User-Agent': 'S3DropBucket GetAccessToken',
            },
        })

        const ratResp = (await rat.json()) as accessResp
        if (rat.status != 200)
        {
            const err = ratResp as unknown as { "error": string, "error_description": string }
            console.error(`Problem retrieving Access Token (${rat.status}) Error: ${err.error} \nDescription: ${err.error_description}`)
            //  {
            //  error: "invalid_client",
            //  error_description: "Unable to find matching client for 1d99f8d8-0897-4090-983a-c517cc54032e",
            //  }

            throw new Error(`Problem - Retrieving Access Token:   ${rat.status} - ${err.error}  - \n${err.error_description}`)
        }
        const accessToken = ratResp.access_token
        return { accessToken }.accessToken
    } catch (e)
    {
        throw new Error(`Exception - On GetAccessToken: \n ${e}`)
    }
}

export async function postToCampaign (xmlCalls: string, config: customerConfig, count: string) {

    const c = config.Customer

    //Store AccessToken in process.env vars for reference across invocations, save requesting it repeatedly
    if (process.env[`${c}_accessToken`] === undefined ||
        process.env[`${c}_accessToken`] === null ||
        process.env[`${c}_accessToken`] == '')
    {
        process.env[`${c}_accessToken`] = (await getAccessToken(config)) as string
        const at = process.env[`${c}_accessToken"`] ?? ''
        const l = at.length
        const redactAT = '.......' + at.substring(l - 10, l)
        if (tcLogDebug) console.info(`Generated a new AccessToken: ${redactAT}`)
    } else
    {
        const at = process.env["accessToken"] ?? ''
        const l = at.length
        const redactAT = '.......' + at.substring(l - 8, l)
        if (tcLogDebug) console.info(`Access Token already stored: ${redactAT}`)
    }



    const myHeaders = new Headers()
    myHeaders.append('Content-Type', 'text/xml')
    myHeaders.append('Authorization', 'Bearer ' + process.env[`${c}_accessToken`])
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
        .then(response => response.text()
        )
        .then(async (result) => {

            // console.error(`Debug POST Response: ${result}`)
            let faults: string[] = []

            if (result.toLowerCase().indexOf('false</success>') > -1)
            {

                //Todo: Add this error in: 
                // ERROR	Debug POST Response: <Envelope>
                //  <Body> <RESULT>
                //  <SUCCESS> true < /SUCCESS>
                //  < FAILURES >
                //  <FAILURE failure_type="permanent" description = "All key columns are not present" >


                if (
                    result.toLowerCase().indexOf('max number of concurrent') > -1 ||
                    result.toLowerCase().indexOf('access token has expired') > -1 ||
                    result.toLowerCase().indexOf('Error saving row') > -1
                    // result.toLowerCase().indexOf('max number of concurrent') > -1 ||
                )
                {
                    console.error(`Temporary Failure - POST of the Updates - Marked for Retry. \n${result}`)
                    return 'retry'
                }
                else if (result.toLowerCase().indexOf('<FaultString><![CDATA[') > -1)
                {
                    const f = result.split('<FaultString>')
                    for (const fl in f)
                    {
                        faults.push(fl)
                    }
                    return "Partially Succesful - \nJSON.stringify(faults)"
                }
                else return `Error - Unsuccessful POST of the Updates (${count}) - Response : ${result}`
            }


            // <FAILURES>
            // <FAILURE failure_type="permanent" description = "There is no column name" >
            // </FAILURE>
            // < /FAILURES>
            if (result.toLowerCase().indexOf("<failure ") > -1)
            {
                let msg = ''

                const m = result.match(/<FAILURE (.*)>$/gim)

                if (m && m?.length > 0)
                {
                    for (const l in m)
                    {
                        // "<FAILURE failure_type=\"permanent\" description=\"There is no column name\">"
                        l.replace("There is no column", "There is no column = ")
                        msg += l
                    }

                    console.error(`Unsuccessful POST of the Updates (${count}) - \nFailure Msg: ${JSON.stringify(msg)}`)
                    return `Error - Unsuccessful POST of the Updates (${count}) - \nFailure Msg: ${JSON.stringify(msg)}`
                }
            }

            result = result.replace('\n', ' ')
            return `Successfully POSTed (${count}) Updates - Result: ${result}`
        })
        .catch(e => {
            console.error(`Error - Temporary failure to POST the Updates - Marked for Retry. ${e}`)
            if (e.indexOf('econnreset') > -1)
            {
                console.error(`Error - Temporary failure to POST the Updates - Marked for Retry. ${e}`)
                return 'retry'
            }
            else
            {
                console.error(`Error - Unsuccessful POST of the Updates: ${e}`)
                throw new Error(`Exception - Unsuccessful POST of the Updates \n${e}`)
                // return 'Unsuccessful POST of the Updates'
            }
        })
    // } catch (e)
    // {
    //     console.error(`Exception - On POST to Campaign (AccessToken ${process.env[`${c}_accessToken`]}) Result: ${e}`)
    // }

    return postRes
}


function checkMetadata () {
    //Pull metadata for table/db defined in config
    // confirm updates match Columns
    //ToDo:  Log where Columns are not matching
}

async function getAnS3ObjectforTesting (bucket: string) {

    let s3Key: string = ''

    if (testS3Key !== null) return

    const listReq = {
        Bucket: bucket,
        MaxKeys: 101,
        Prefix: tcc.prefixFocus
    } as ListObjectsV2CommandInput

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

                while (s3Key.toLowerCase().indexOf('aggregat') > -1)
                {
                    i++
                    s3Key = s3ListResult.Contents?.at(i)?.Key as string
                }

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

function saveSampleJSON (body: string) {

    const path = "Saved/"
    saveS3Work(`${path}sampleJSON_${Date.now().toString()}.json`, body, 'tricklercache-configs')
    // s3://tricklercache-configs/Saved/
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
                r = await deleteS3Object(listItem.Key as string, "", bucket)
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




async function maintainS3DropBucket (cust: customerConfig) {

    const bucket = tcc.s3DropBucket

    const listReq = {
        Bucket: bucket,
        MaxKeys: 1000,
        Prefix: `cust.Customer`
    } as ListObjectsV2CommandInput

    const reProcess: string[] = []

    await s3.send(new ListObjectsV2Command(listReq))
        .then(async (s3ListResult: ListObjectsV2CommandOutput) => {

            // 3,600,000 millisecs = 1 hour
            const a = 3600000 * tcc.S3DropBucketMaintHours  //Older Than X Hours 

            const d: Date = new Date()

            try
            {
                for (const o in s3ListResult.Contents)
                {
                    const n = parseInt(o)
                    const s3d: Date = new Date(s3ListResult.Contents[n].LastModified ?? new Date())
                    const df = d.getTime() - s3d.getTime()
                    const dd = s3d.setHours(-tcc.S3DropBucketMaintHours)
                    if (df > a) 
                    {
                        const obj = s3ListResult.Contents[n]
                        const k = obj.Key

                        // const updatedMetadata =
                        await s3.send(
                            new CopyObjectCommand({
                                Bucket: bucket,
                                Key: k,
                                CopySource: `${bucket}/${k}`,
                                MetadataDirective: 'COPY',
                                CopySourceIfUnmodifiedSince: new Date(dd)
                            })
                        )
                            .then((res) => {
                                // console.info(`${JSON.stringify(res)}`)
                                reProcess.push(k + " --> " + JSON.stringify(res))
                            })
                    }
                }
            } catch (e)
            {
                throw new Error(`Exception - Maintain S3DropBucket - List Results processing - \n${e}`)
            }
        })
        .catch((e) => {
            console.error(`Exception - On S3 List Command for Maintaining Objects on ${bucket}: ${e} `)
        })

    debugger
    const l = reProcess.length
    return [l, reProcess]
}


async function maintainS3DropBucketQueueBucket (config: customerConfig) {  //, key: string, versionId: string, batch: string, recCount: string) {

    const bucket = tcc.s3DropBucketWorkBucket

    const listReq = {
        Bucket: bucket,
        MaxKeys: 1000
    } as ListObjectsV2CommandInput

    const reQueue: string[] = []

    await s3.send(new ListObjectsV2Command(listReq))
        .then(async (s3ListResult: ListObjectsV2CommandOutput) => {

            // 3,600,000 millisecs = 1 hour
            const a = 3600000 * tcc.S3DropBucketProcessQueueMaintHours  //Older Than X Hours 

            const d: Date = new Date()

            try
            {

                for (const o in s3ListResult.Contents)
                {
                    const n = parseInt(o)
                    const s3d: Date = new Date(s3ListResult.Contents[n].LastModified ?? new Date())
                    const df = d.getTime() - s3d.getTime()
                    const dd = s3d.setHours(-tcc.S3DropBucketMaintHours)

                    let rm: string[] = []
                    if (df > a) 
                    {

                        const obj = s3ListResult.Contents[n]
                        const key = obj.Key ?? ""
                        let versionId = ''
                        let batch = ''
                        let updates = ''

                        const r1 = new RegExp(/json_update_(.*)_/g)
                        let rm = r1.exec(key) ?? ""
                        batch = rm[1]

                        const r2 = new RegExp(/json_update_.*?_(.*)\./g)
                        rm = r2.exec(key) ?? ""
                        updates = rm[1]

                        debugger

                        //build SQS Entry
                        const qa = await addWorkToSQSProcessQueue(config, key, versionId, batch, updates)

                        reQueue.push("ReQueue Work" + key + " --> " + JSON.stringify(qa))

                    }
                }
            } catch (e)
            {
                throw new Error(`Exception - Maintain S3DropBucket - List Results processing - \n${e}`)
            }
        })
        .catch((e) => {
            console.error(`Exception - On S3 List Command for Maintaining Objects on ${bucket}: ${e} `)
        })

    debugger
    const l = reQueue.length
    return [l, reQueue]

}

