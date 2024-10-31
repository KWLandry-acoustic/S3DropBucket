/* eslint-disable no-debugger */
"use strict"

import {
  ListObjectsV2CommandInput,
  ListObjectsV2CommandOutput,
  PutObjectCommandOutput,
  GetObjectCommandOutput,
  GetObjectCommandInput,
  DeleteObjectCommandOutput,
} from "@aws-sdk/client-s3"

import {
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
  GetObjectCommand,
  DeleteObjectCommand,
  //CopyObjectCommand,
} from "@aws-sdk/client-s3"

import {
  SQSClient,
  SendMessageCommand,
  SendMessageCommandOutput,
} from "@aws-sdk/client-sqs"

import {
  FirehoseClient,
  PutRecordCommand,
  PutRecordCommandInput,
  PutRecordCommandOutput,
} from "@aws-sdk/client-firehose"

import {
  SchedulerClient,
  ListSchedulesCommand,
  ListSchedulesCommandInput,
} from "@aws-sdk/client-scheduler" 

import {
  Handler,
  S3Event,
  Context,
  SQSEvent,
  // SQSRecord,
  // S3EventRecord,
} from "aws-lambda"

//import fetch from 'node-fetch'
//import {Headers, RequestInit, Response} from 'node-fetch'
//import fetch, {FetchHttpHandler} from "@smithy/fetch-http-handler"


//json-node = Stream compatible package
//import {JSONParser, Tokenizer, TokenParser} from '@streamparser/json-node'
import {JSONParser} from '@streamparser/json-node'
//import JSONParser from "@streamparser/json-node"
//import {transform} from '@streamparser/json-node'
import {transform} from "stream-transform"

import { v4 as uuidv4 } from "uuid"

import { parse } from "csv-parse"

import jsonpath from "jsonpath"

import sftpClient, {ListFilterFunction} from "ssh2-sftp-client"
//import {type sftpClientOptions} from "ssh2-sftp-client/lib/typescript/sftp-client"
//import {type sftpClientError} from "ssh2-sftp-client/lib/typescript/sftp-client"
//import {type sftpClientEvent} from "ssh2-sftp-client/lib/typescript/sftp-client"
//import {type sftpClientEventError} from "ssh2-sftp-client/lib/typescript/sftp-client"
//import {type sftpClientEventClose} from "ssh2-sftp-client/lib/typescript/sftp-client"
//import {type sftpClientEventData} from "ssh2-sftp-client/lib/typescript/sftp-client"
//import {type sftpClientEventEnd} from "ssh2-sftp-client/lib/typescript/sftp-client"
//import {type sftpClientEventOpen} from "ssh2-sftp-client/lib/typescript/sftp-client"
//import {type sftpClientEventRead} from "ssh2-sftp-client/lib/typescript/sftp-client"
//import {type sftpClientEventWrite} from "ssh2-sftp-client/lib/typescript/sftp-client"
//import {type sftpClientEventRename} from "ssh2-sftp-client/lib/typescript/sftp-client"
//import {type sftpClientEventCreate} from "ssh2-sftp-client/lib/typescript/sftp-client"
//import {type sftpClientEventDelete} from "ssh2-sftp-client/lib/typescript/sftp-client"
//import {type sftpClientEventStat} from "ssh2-sftp-client/lib/typescript/sftp-client"


//Useful for internal process references
//import { type FileResultCallback } from "@babel/core"
//import { freemem } from "os"
//import { env } from "node:process"
//import { keyBy } from "lodash"
//import { setUncaughtExceptionCaptureCallback } from "process"
//import { type LargeNumberLike } from "crypto"

const s3db_version = `3.3.003 - ${new Date().toUTCString()}`
console.info(s3db_version)

let s3 = {} as S3Client

const SFTPClient = new sftpClient()


//For when needed to reference Lambda execution environment /tmp folder
// import { ReadStream, close } from 'fs'

const sqsClient = new SQSClient({})

export type sqsObject = {
  bucketName: string
  objectKey: string
}

let localTesting = false

let chunks: object[]

let xmlRows = ""
let batchCount = 0
let recs = 0

interface customerConfig {
  Customer: string
  format: string // CSV or JSON
  separator: string
  updates: string // singular or Multiple (default) (also 'bulk' as legacy)
  listId: string
  listName: string
  listType: string
  DBKey: string
  LookupKeys: string
  pod: string // 1,2,3,4,5,6,7,8,9,A,B
  region: string // US, EU, AP
  updateMaxRows: number //Safety to avoid run away data inbound and parsing it all
  refreshToken: string // API Access
  clientId: string // API Access
  clientSecret: string // API Access
  sftp: {
    user: string
    password: string
    filepattern: string
    schedule: string
  }
  transforms: {
    jsonMap: { [key: string]: string }
    csvMap: { [key: string]: string }
    ignore: string[]
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

export interface s3dbQueueMessage {
  workKey: string
  versionId: string
  marker: string
  attempts: number
  batchCount: string
  updateCount: string
  custconfig: customerConfig
  lastQueued: string
}

export interface s3dbConfig {
  LOGLEVEL: string
  AWS_REGION: string
  S3DropBucket: string
  S3DropBucketWorkBucket: string
  S3DropBucketWorkQueue: string
  S3DropBucketLog: boolean //Future: Firehose Aggregator Bucket
  S3DropBucketLogBucket: string //Future: Firehose Aggregator Bucket
  S3DropBucketConfigs: string
  xmlapiurl: string
  restapiurl: string
  authapiurl: string
  jsonSeparator: string
  MaxBatchesWarning: number
  SelectiveDebug: string
  WorkQueueQuiesce: boolean
  prefixFocus: string
  // WorkQueueVisibilityTimeout: number
  // WorkQueueWaitTimeSeconds: number
  // RetryQueueVisibilityTimeout: number
  // RetryQueueInitialWaitTimeSeconds: number
  EventEmitterMaxListeners: number
  S3DropBucketQuiesce: boolean
  S3DropBucketMaintHours: number
  S3DropBucketMaintLimit: number
  S3DropBucketMaintConcurrency: number
  S3DropBucketWorkQueueMaintHours: number
  S3DropBucketWorkQueueMaintLimit: number
  S3DropBucketWorkQueueMaintConcurrency: number
  S3DropBucketPurgeCount: number
  S3DropBucketPurge: string
  QueueBucketQuiesce: boolean
  WorkQueueBucketPurgeCount: number
  WorkQueueBucketPurge: string
}

let S3DBConfig = {} as s3dbConfig

export interface SQSBatchItemFails {
  batchItemFailures: [
    {
      itemIdentifier: string
    }
  ]
}

export interface processS3ObjectStreamResult {
  Key: string
  Processed: string
  OnDataBatchingResult: string
  OnDataStoreAndQueueWorkResult: {
    StoreAndQueueWorkResult: {
      AddWorkToS3WorkBucketResults: {
        versionId: string
        S3ProcessBucketResult: string
        AddWorkToS3ProcessBucket: string
      }
      AddWorkToSQSWorkQueueResults: {
        SQSWriteResult: string
        AddToSQSQueue: string
      }
      StoreQueueWorkException: string
      StoreS3WorkException: string
    }
  }
  OnEndStreamEndResult: {
    StoreAndQueueWorkResult: {
      AddWorkToS3WorkBucketResults: {
        versionId: string
        S3ProcessBucketResult: string
        AddWorkToS3ProcessBucket: string
      }
      AddWorkToSQSWorkQueueResults: {
        SQSWriteResult: string
        AddToSQSQueue: string
      }
      StoreQueueWorkException: string
      StoreS3WorkException: string
    }
  }
  StreamEndResult: string
  StreamException: string
  OnEndRecordStatus: string
  OnDataReadStreamException: string
  OnEndNoRecordsException: string
  ProcessS3ObjectStreamCatch: string
  OnClose_Result: string
  StreamReturnLocation: string
  PutToFireHoseAggregatorResult: string
  PutToFireHoseException: string
  OnEnd_PutToFireHoseAggregator: string
  DeleteResult: string
}

let processS3ObjectStreamResolution: processS3ObjectStreamResult = {
  Key: "",
  Processed: "",
  OnDataBatchingResult: "",
  OnDataStoreAndQueueWorkResult: {
    StoreAndQueueWorkResult: {
      AddWorkToS3WorkBucketResults: {
        versionId: "",
        S3ProcessBucketResult: "",
        AddWorkToS3ProcessBucket: "",
      },
      AddWorkToSQSWorkQueueResults: {
        SQSWriteResult: "",
        AddToSQSQueue: "",
      },
      StoreQueueWorkException: "",
      StoreS3WorkException: "",
    },
  },
  OnEndStreamEndResult: {
    StoreAndQueueWorkResult: {
      AddWorkToS3WorkBucketResults: {
        versionId: "",
        S3ProcessBucketResult: "",
        AddWorkToS3ProcessBucket: "",
      },
      AddWorkToSQSWorkQueueResults: {
        SQSWriteResult: "",
        AddToSQSQueue: "",
      },
      StoreQueueWorkException: "",
      StoreS3WorkException: "",
    },
  },
  StreamEndResult: "",
  StreamException: "",
  OnEndRecordStatus: "",
  OnDataReadStreamException: "",
  OnEndNoRecordsException: "",
  ProcessS3ObjectStreamCatch: "",
  OnClose_Result: "",
  StreamReturnLocation: "",
  PutToFireHoseAggregatorResult: "",
  PutToFireHoseException: "",
  OnEnd_PutToFireHoseAggregator: "",
  DeleteResult: "",
}

const sqsBatchFail: SQSBatchItemFails = {
  batchItemFailures: [
    {
      itemIdentifier: "",
    },
  ],
}

sqsBatchFail.batchItemFailures.pop()

let vid = ""
let et = ""

let s3dbLogDebug = false
//let s3dbLogVerbose = false
//let s3dbLogNormal = false

//For local testing
let testS3Key: string
let testS3Bucket: string
testS3Bucket = "s3dropbucket-configs"

// testS3Key = "TestData/visualcrossing_00213.csv"
// testS3Key = "TestData/pura_2024_02_25T00_00_00_090Z.json"

//testS3Key = "TestData/pura_S3DropBucket_Aggregator-8-2024-03-23-09-23-55-123cb0f9-9552-3303-a451-a65dca81d3c4_json_update_53_99.xml"
//testS3Key = "TestData/alerusrepsignature_sampleformatted_json_update_1_1.xml"
//testS3Key = "TestData/alerusrepsignature_advisors_2_json_update-3c74bfb2-1997-4653-bd8e-73bf030b4f2d_26_14.xml"
//  Core - Key Set of Test Datasets
//testS3Key = "TestData/cloroxweather_99706.csv"
//testS3Key = "TestData/pura_S3DropBucket_Aggregator-8-2024-03-19-16-42-48-46e884aa-8c6a-3ff9-8d32-c329395cf311.json"
//testS3Key = "TestData/pura_2024_02_26T05_53_26_084Z.json"
//testS3Key = "TestData/alerusrepsignature_sample.json"
//testS3Key = "TestData/alerusrepsignature_advisors.json"
//testS3Key = "TestData/alerusrepsignature_sampleformatted.json"
//testS3Key = "TestData/alerusrepsignature_sample - min.json"
//testS3Key = "TestData/alerusreassignrepsignature_advisors.json"
//testS3Key = "TestData/Funding_Circle_Limited_CampaignRelationalTable1_2024_10_08T10_16_49_700Z.json"
//testS3Key = "TestData/Funding_Circle_Limited_CampaignDatabase1_2024_10_08T09_52_13_903Z.json"
//testS3Key = "TestData/alerusrepsignature_advisors.json"
//testS3Key = "TestData/alerusreassignrepsignature_advisors.json"
//testS3Key = "TestData/KingsfordWeather_00210.csv"
//testS3Key = "TestData/KingsfordWeather_00211.csv"
testS3Key = "TestData/KingsfordWeather_00212.csv"

/**
 * A Lambda function to process the Event payload received from S3.
 */

export const s3DropBucketHandler: Handler = async (
  event: S3Event,
  context: Context
) => {

  //Ignore Aggregation Error Files
  if (event.Records[0].s3.object.key.indexOf("AggregationError") > -1) return ""

  //ToDo: Make AWS Region Config Option for portability/infrastruct dedication
  //const s3 = new S3Client( {region: 'us-east-1'} )
  s3 = new S3Client({ region: process.env.s3DropBucketRegion })

  if (
    process.env["EventEmitterMaxListeners"] === undefined ||
    process.env["EventEmitterMaxListeners"] === "" ||
    process.env["EventEmitterMaxListeners"] === null
  ) {
    S3DBConfig = await getValidateS3DropBucketConfig()
  }

  if (S3DBConfig.SelectiveDebug.indexOf("_98,") > -1)
    console.info(
      `Selective Debug 98 - S3DropBucket Options: ${JSON.stringify(S3DBConfig)} `
    )
  if (S3DBConfig.SelectiveDebug.indexOf("_99,") > -1)
    console.info(
      `Selective Debug 99 - S3DropBucket Logging Options: ${S3DBConfig.SelectiveDebug!} `
    )
  if (S3DBConfig.SelectiveDebug.indexOf("_909,") > -1)
    console.info(
      `Selective Debug 909 - Environment Vars: ${JSON.stringify(process.env)} `
    )

  if (event.Records[0].s3.object.key.indexOf("Aggregator") > -1) {
    if (S3DBConfig.SelectiveDebug.indexOf("_925,") > -1)
      console.info(
        `Selective Debug 925 - Processing an Aggregated File ${event.Records[0].s3.object.key}`
      )
  }

  
  //When Local Testing - pull an S3 Object and so avoid the not-found error
  if (
    !event.Records[0].s3.object.key ||
    event.Records[0].s3.object.key === "devtest.csv"
  ) {
    if (
      testS3Key !== undefined &&
      testS3Key !== null &&
      testS3Bucket !== undefined &&
      testS3Bucket !== null
    ) {
      event.Records[0].s3.object.key = testS3Key
      event.Records[0].s3.bucket.name = testS3Bucket
    } else {
      event.Records[0].s3.object.key =
        (await getAnS3ObjectforTesting(event.Records[0].s3.bucket.name)) ?? ""
    }
    localTesting = true
  } else {
    testS3Key = ""
    testS3Bucket = ""
    localTesting = false
  }

  //if (S3DBConfig.S3DropBucketPurgeCount > 0) {
  //  console.warn(
  //    `Purge Requested, Only action will be to Purge ${S3DBConfig.S3DropBucketPurge} of ${S3DBConfig.S3DropBucketPurgeCount} Records. `
  //  )
  //  const d = await purgeBucket(
  //    Number(S3DBConfig.S3DropBucketPurgeCount!),
  //    S3DBConfig.S3DropBucketPurge!
  //  )
  //  return d
  //}

  if (S3DBConfig.S3DropBucketQuiesce) {
    if (!localTesting) {
      console.warn(
        `S3DropBucket Quiesce is in effect, new Files from ${S3DBConfig.S3DropBucket} will be ignored and not processed. \nTo Process files that have arrived during a Quiesce of the Cache, reference the S3DropBucket Guide appendix for AWS cli commands.`
      )
      return
    }
  }

  if (S3DBConfig.SelectiveDebug.indexOf("_100,") > -1)
    console.info(
      `(100) Received S3DropBucket Event Batch.There are ${event.Records.length} S3DropBucket Event Records in this batch. (Event Id: ${event.Records[0].responseElements["x-amz-request-id"]}, \nContext: ${context}).`
    )

  //Future: Left this for possible switch of the Trigger from S3 being an SQS Trigger of an S3 Write,
  // Drive higher concurrency in each Lambda invocation by running batches of 10 files written at a time(SQS Batch)
  for (const r of event.Records) {
    const key = r.s3.object.key ?? ""
    const bucket = r.s3.bucket.name ?? ""

    if (S3DBConfig.prefixFocus !== "" && key.indexOf(S3DBConfig.prefixFocus) < 0) {
      //console.warn( `PrefixFocus is configured, File Name ${ key } does not fall within focus restricted by the configured PrefixFocus ${ tcc.prefixFocus }` )
      return
    }

    //ToDo: Resolve Duplicates Issue - S3 allows Duplicate Object Names but Delete marks all Objects of same Name Deleted.
    //   Which causes an issue with Key Not Found after an Object of Name A is processed and deleted, then another Object of Name A comes up in a Trigger.

    vid = r.s3.object.versionId ?? ""
    et = r.s3.object.eTag ?? ""

    try {
      customersConfig = await getCustomerConfig(key)
    } catch (e) {
      console.error(`Exception - Pulling Customer Config \n${e} `)
      break
    }

    //Initial work out for writing logs to S3 Bucket
    /*
    try {
      if (key.indexOf("S3DropBucket-LogsS3DropBucket_Aggregator") > -1)
        console.warn(`Warning -- Found Invalid Aggregator File Name - ${key} \nVersionID: ${vid}, \neTag: ${et}`)
      if (S3DBConfig.SelectiveDebug.indexOf("_101,") > -1)
        console.info(
          `(101) Processing inbound data for ${customersConfig.Customer} - ${key}`
        )
    } catch (e) {
      throw new Error(
        `Exception - Retrieving Customer Config for ${key} \n${e}`
      )
    }
    */

    //ReQueue .xml files - in lieu of requeing through config, have work files (....xml) moved 
    // to the S3DropBucket bucket and drive an object creation event to the handler
    /*
    try {
      if (key.indexOf(".xml") > -1) {
        console.warn(`Warning -- Found Invalid Aggregator File Name - ${key} \nVersionID: ${vid}, \neTag: ${et}`)

        await getS3Work(key, bucket)
          .then(async (work) => {
            const workSet = work.split("</Envelope>")
            await packageUpdates(
              workSet,
              key,
              customersConfig
            )
        })

        if (S3DBConfig.SelectiveDebug.indexOf("_101,") > -1)
          console.info(
            `(101) Processing inbound data for ${customersConfig.Customer} - ${key} \nVersionID: ${vid}, \neTag: ${et}`
          )
      }
    } catch (e) {
      throw new Error(
        `Exception - ReQueing Work from ${bucket} for ${key} \nVersionID: ${vid}, \neTag: ${et} \n${e}`
      )
    }
    */  

    batchCount = 0
    recs = 0

    try {
      processS3ObjectStreamResolution = await processS3ObjectContentStream(
        key,
        bucket,
        customersConfig
      )
        .then(async (res) => {
          let delResultCode
          let streamResults = processS3ObjectStreamResolution

          streamResults.Key = key
          streamResults.Processed = res.OnEndRecordStatus

          console.info(
            `Completed Processing Content Stream - ${streamResults.Key} ${streamResults.Processed}`
          )

          if (S3DBConfig.SelectiveDebug.indexOf("_103,") > -1)
            console.info(
              `(103) Completed processing all records of the S3 Object ${key} \nVersionID: ${vid}, \neTag: ${et}. \nStatus: ${res.OnEndRecordStatus}`
            )

          //Don't delete the test data
          if (localTesting)
          {
            streamResults= {...streamResults, DeleteResult: `Test Data - Not Deleting ${key}`}
            return streamResults
          }
          // if (key.toLowerCase().indexOf('aggregat') > -1) key = 'TestData/S3Object_DoNotDelete'

          if (
            res?.PutToFireHoseAggregatorResult === "200" ||
            (res.OnEndStreamEndResult.StoreAndQueueWorkResult
              .AddWorkToS3WorkBucketResults?.S3ProcessBucketResult === "200" &&
              res.OnEndStreamEndResult.StoreAndQueueWorkResult
                .AddWorkToSQSWorkQueueResults?.SQSWriteResult === "200")
          ) {
            try {
              //Once File successfully processed delete the original S3 Object
              delResultCode = await deleteS3Object(key, bucket).catch((e) => {
                console.error(`Exception - DeleteS3Object - ${e}`)
              })

              if (delResultCode !== "204") {
                streamResults = {
                  ...streamResults,
                  DeleteResult: JSON.stringify(delResultCode),
                }
                if (S3DBConfig.SelectiveDebug.indexOf("_104,") > -1)
                  console.error(
                    `Processing Successful, but Unsuccessful Delete of ${key}, Expected 204 result code, received ${delResultCode}`
                  )
              } else {
                if (S3DBConfig.SelectiveDebug.indexOf("_104,") > -1)
                  console.info(
                    `(104) Processing Successful, Delete of ${key} Successful (Result ${delResultCode}).`
                  )
                streamResults = {
                  ...streamResults,
                  DeleteResult: `Successful Delete of ${key}  (Result ${JSON.stringify(
                    delResultCode
                  )})`,
                }
              }
            } catch (e) {
              console.error(
                `Exception - Deleting S3 Object after successful processing of the Content Stream for ${key} \n${e}`
              )
            }
          }

          return streamResults
        })
        .catch((e) => {
          const err = `Exception - Process S3 Object Stream Catch - \n${e}`
          console.error(err)
          processS3ObjectStreamResolution = {
            ...processS3ObjectStreamResolution,
            ProcessS3ObjectStreamCatch: err,
          }
          return processS3ObjectStreamResolution
        })
    } catch (e) {
      console.error(
        `Exception - Processing S3 Object Content Stream for ${key} \n${e}`
      )
    }

    if (S3DBConfig.SelectiveDebug.indexOf("_903,") > -1)
      console.info(
        `Selective Debug 903 - Returned from Processing S3 Object Content Stream for ${key}. Result: ${JSON.stringify(
          processS3ObjectStreamResolution
        )}`
      )
  }

  //Check for important Config updates (which caches the config in Lambdas long-running cache)
  checkForS3DBConfigUpdates()

  //if (event.Records[0].s3.bucket.name && S3DBConfig.S3DropBucketMaintHours > 0) {
  //  const maintenance = await maintainS3DropBucket(customersConfig)

  //  const l = maintenance[0] as number
  //  // console.info( `- ${ l } File(s) met criteria and are marked for reprocessing ` )

  //  if (S3DBConfig.SelectiveDebug.indexOf("_926,") > -1) {
  //    const filesProcessed = maintenance[1]
  //    if (l > 0)
  //      console.info(
  //        `Selective Debug 926 - ${l} Files met criteria and are returned for Processing: \n${filesProcessed}`
  //      )
  //    else
  //      console.info(
  //        `Selective Debug 926 - No files met the criteria to return for Reprocessing`
  //      )
  //  }
  //}

  //Need to protect against the Result String becoming excessively long
  
  const n = new Date().toISOString()
  let objectStreamResolution =
    n + "  -  " + JSON.stringify(processS3ObjectStreamResolution) + "\n\n"
  const osrl = objectStreamResolution.length

  if (osrl > 10000) {
    //100K Characters
    objectStreamResolution = `Excessive Length of ProcessS3ObjectStreamResolution: ${osrl} Truncated: \n ${objectStreamResolution.substring(
      0,
      1000
    )} ... ${objectStreamResolution.substring(osrl - 1000, osrl)}`

    if (S3DBConfig.SelectiveDebug.indexOf("_920,") > -1)
      console.warn(`Selective Debug 920 - \n ${JSON.stringify(objectStreamResolution)} `)
    return objectStreamResolution
  }
  const k = processS3ObjectStreamResolution.Key
  const p = processS3ObjectStreamResolution.Processed

  if (S3DBConfig.SelectiveDebug.indexOf("_105,") > -1)
    console.info(
      `(105) Completing S3DropBucket Processing of Request Id ${event.Records[0].responseElements["x-amz-request-id"]} for ${k} \n${p}`
    )

  if (S3DBConfig.SelectiveDebug.indexOf("_920,") > -1)
    console.info(`Selective Debug 920 - \n${JSON.stringify(objectStreamResolution)}`)

  //
  //Possible Logging Option - Using Firehose
  //
 /*
 if (S3DBConfig.S3DropBucketLog) {
    const logMsg: object[] = [{osr}]      //need to check this one, 
    debugger
    const logKey = `S3DropBucket_Log_${new Date()
      .toISOString()
      .replace(/:/g, "_")}`
    const fireHoseLog = await putToFirehose(
      logMsg,
      logKey,
      "S3DropBucket_Logs_"
    )
    console.info(
      `Write S3DropBucket Log to FireHose aggregation - ${JSON.stringify(
        fireHoseLog
      )}`
    )
  }
  */

  debugger

  //Done with logging the results of this pass, empty the object for the next processing cycle
  processS3ObjectStreamResolution = {} as processS3ObjectStreamResult

  return objectStreamResolution   //Return the results logging object
}

export default s3DropBucketHandler

async function processS3ObjectContentStream(
  key: string,
  bucket: string,
  custConfig: customerConfig
) {
  if (s3dbLogDebug) console.info(`Processing S3 Content Stream for ${key}`)

  const s3C: GetObjectCommandInput = {
    Key: key,
    Bucket: bucket,
  }

  let streamResult: processS3ObjectStreamResult =
    processS3ObjectStreamResolution


  // processS3ObjectResults
  processS3ObjectStreamResolution = await s3
    .send(new GetObjectCommand(s3C))
    .then(async (getS3StreamResult: GetObjectCommandOutput) => {
      if (getS3StreamResult.$metadata.httpStatusCode != 200)
      {
        const errMsg = JSON.stringify(getS3StreamResult.$metadata)
        //throw new Error( `Get S3 Object Command failed for ${ key }. Result is ${ errMsg }` )
        return {
          ...streamResult,
          ProcessS3ObjectStreamError: `Get S3 Object Command failed for ${key}. Result is ${errMsg}`,
        }
      }

      let s3ContentReadableStream = getS3StreamResult.Body as NodeJS.ReadableStream

      const t = transform(function (data) {
        //"The \"chunk\" argument must be of type string or an instance of Buffer or Uint8Array. Received an instance of Object"
        //Future - Opportunity for Transforms
        let r
        if (Buffer.isBuffer(data)) r = data.toString("utf8")
        else r = JSON.stringify(data) + "\n"
        return r
      })

      if (
        key.indexOf("aggregate_") < 0 &&
        custConfig.format.toLowerCase() === "csv"
      )
      {
        const csvParser = parse({
          delimiter: ",",
          columns: true,
          comment: "#",
          trim: true,
          skip_records_with_error: true,
        })

        s3ContentReadableStream = s3ContentReadableStream
          .pipe(csvParser)
          .pipe(t)

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
      } else
      {
        s3ContentReadableStream = s3ContentReadableStream.pipe(t)
      }

      //Placeholder - Everything should be JSON by the time we get here
      if (custConfig.format.toLowerCase() === "json")
      {
        if (S3DBConfig.SelectiveDebug.indexOf("____,") > -1)  //Future Logging Opportunity
          console.info(` `)
      }

      //Notes: Options to Handling Large JSON Files
      // Send the JSON objects formatted without newlines and use a newline as the delimiter.
      // Send the JSON objects concatenated with a record separator control character as the delimiter.
      // Send the JSON objects concatenated with no delimiters and rely on a streaming parser to extract them.
      // Send the JSON objects prefixed with their length and rely on a streaming parser to extract them.

      /** 
            //The following are what make up StreamJSON JSONParser but can be broken out to process data more granularly
            //Might be helpful in future capabilities 

            const jsonTzerParser = new Tokenizer({
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

      //if ( key.indexOf( 'aggregate_' ) > -1 ) console.info( `Begin Stream Parsing aggregate file ${ key }` )

      let jsonSep = S3DBConfig.jsonSeparator
      if (custConfig.separator && key.indexOf("aggregate_") < 0)
        jsonSep = custConfig.separator

      const jsonParser = new JSONParser({
        // numberBufferSize: 64,        //64, //0, //undefined, // set to 0 to don't buffer.
        stringBufferSize: undefined,    //64, //0, //undefined,
        //separator: '\n',              // separator between object. For example `\n` for nd-js.
        //separator: `''`,              // separator between object. For example `\n` for nd-js.
        separator: jsonSep,             // separator between object. For example `\n` for nd-js.
        paths: ["$"],                   //ToDo: Possible data transform oppty
        keepStack: false,               //Save Memory, tradeoff not complete info
        emitPartialTokens: false,       // whether to emit tokens mid-parsing.
      })

      //At this point, all data, whether a json file or a csv file parsed by csvparser should come through
      //  as a series of Objects (One at a time in the Stream) with a line break after each Object.
      //   {data:data, data:data, .....}
      //   {data:data, data:data, .....}
      //   {data:data, data:data, .....}
      //
      // Later, OnData processing populates an Array with each line/Object, so the
      // final format will be an Array of Objects:
      // [ {data:data, data:data, .....},
      //  { data: data, data: data, .....},
      //  { data: data, data: data, .....}, ... ]
      //

      // s3ContentReadableStream = s3ContentReadableStream.pipe(t).pipe(jsonParser)
      s3ContentReadableStream = s3ContentReadableStream.pipe(jsonParser)
      //s3ContentReadableStream = s3ContentReadableStream.pipe(tJsonParser)


      s3ContentReadableStream.setMaxListeners(
        Number(S3DBConfig.EventEmitterMaxListeners)
      )

      chunks = []
      batchCount = 0
      recs = 0
      let packageResult

      // const readStream = await new Promise(async (resolve, reject) => {
      await new Promise( (resolve, reject) => {
        s3ContentReadableStream
          .on("error", async function (err: string) {
            const errMessage = `An error has stopped Content Parsing at record ${recs++} for s3 object ${key}. Separator is ${jsonSep}.\n${err} \n${chunks}`

            console.error(errMessage)

            chunks = []
            batchCount = 0
            recs = 0

            streamResult = {
              ...streamResult,
              StreamException: `s3ContentReadableStreamErrorMessage ${JSON.stringify(
                errMessage
              )}`,
            }
            reject(errMessage)
            throw new Error(
              `Error on Readable Stream for s3DropBucket Object ${key}.\nError Message: ${errMessage} `
            )
          })
          .on(
            "data",
            async function (s3Chunk: {
              key: string
              parent: object
              stack: object
              value: object
            }) {
              if (
                key.toLowerCase().indexOf("aggregat") < 0 &&
                recs > custConfig.updateMaxRows
              )
                throw new Error(
                  `The number of Updates in this batch (${recs}) Exceeds Max Row Updates allowed in the Customers Config (${custConfig.updateMaxRows}).  ${key} will not be deleted from ${S3DBConfig.S3DropBucket} to allow for review and possible restaging.`
                )

              if (S3DBConfig.SelectiveDebug.indexOf("_913,") > -1)
                console.info(
                  `Selective Debug 913 - s3ContentStream OnData - Another Batch ${
                    batchCount + 1
                  } of ${
                    Object.values(s3Chunk).length
                  } Updates read from ${key} `
                )

              try {
                const oa = s3Chunk.value

                //What's possible
                //  {} a single Object - Pura
                //  [{},{},{}] An Array of Objects - Alerus
                //  A list of Objects - (Aggregated files) Line delimited
                //      [{}
                //      {}
                //       ...
                //      {}]
                // An array of Strings (CSV Parsed)
                //  [{"key1":"value1","key2":"value2"},{"key1":"value1","key2":"value2"},...]
                //
                //Build a consistent Object of an Array of Objects
                // [{},{},{},...]

                if (Array.isArray(oa)) {
                  for (const a in oa) {
                    let e = oa[a]
                    if (typeof e === "string") {
                      e = JSON.parse(e)
                    }
                    chunks.push(e)
                  }
                } else {
                  //chunks.push( JSON.stringify( oa ) )
                  chunks.push(oa)
                }
              } catch (e) {
                console.error(
                  `Exception - ReadStream-OnData - Chunk aggregation for ${key} \nBatch ${batchCount} of ${recs} Updates. \n${e} `
                )
                streamResult = {
                  ...streamResult,
                  OnDataReadStreamException: `Exception - First Catch - ReadStream-OnData Processing for ${key} \nBatch ${batchCount} of ${recs} Updates. \n${e} `,
                }
              }

              try {
                //Update files with Singular updates per file do not reach 99 updates in a single file,
                // these will fall through to the OnEnd processing.
                //Aggregate(d) Files with Multiple updates per file can have > 99 updates in each file so
                //  those will need to be chunked up into 99 updates each and stored as Work files.

                while (chunks.length > 98) {
                  //if ( chunks.length > 9 )
                  packageResult = await packageUpdates(chunks, key, custConfig)

                  streamResult = {
                    ...streamResult,
                    OnDataBatchingResult: JSON.stringify(packageResult),
                  }
                }
              } catch (e) {
                console.error(
                  `Exception - ReadStream-OnData - Batch Packaging for ${key} \nBatch ${batchCount} of ${recs} Updates. \n${e} `
                )
                streamResult = {
                  ...streamResult,
                  OnDataReadStreamException: `Exception - Second Catch - ReadStream-OnData - Batch Packaging for ${key} \nBatch ${batchCount} of ${recs} Updates. \n${e} `,
                }
              }
            }
          )

          .on("end", async function () {
            if (recs < 1 && chunks.length < 1) {
              streamResult = {
                ...streamResult,
                OnEndNoRecordsException: `Exception - No records returned from parsing file.Check the content as well as the configured file format(${custConfig.format}) matches the content of the file.`,
              }
              // console.error(`Exception - ${ JSON.stringify(streamResult) } `)
              //throw new Error( `Exception - onEnd ${ JSON.stringify( streamResult ) } ` )
              return streamResult
            }
            debugger;
            //Next Process Step is Queue Work or Aggregate Small single Update files into larger Update files to improve Campaign Update performance.
            try {
              // if Not an Aggregate file, there are chunks to process, and Multiple option)
              if (
                (chunks.length > 0 &&
                  custConfig.updates.toLowerCase() === "multiple") ||
                key.toLowerCase().indexOf("aggregat") > 0
              ) {
                packageResult = await packageUpdates(
                  chunks,
                  key,
                  custConfig
                ).then((res) => {
                  //console.info( `Return Await PackageResult from PackageUpdates: ${ JSON.stringify( res ) }` )
                  return res
                })

               /* streamResult = {
                  ...streamResult,
                  processS3ObjectStreamResult: packageResult.StoreAndQueueWorkResult
                }*/
              }

              // if (this is an aggregate file and chunks are not 0)
              if (
                chunks.length > 0 &&
                custConfig.updates.toLowerCase() === "singular" &&
                key.toLowerCase().indexOf("aggregat") < 0
              ) {
 

                try {
                  await putToFirehose(
                    chunks,
                    key,
                    custConfig.Customer
                  ).then((res) => {
                    const fRes = res as {
                      OnEnd_PutToFireHoseAggregator: string
                      PutToFireHoseAggregatorResult: string
                      PutToFireHoseException: string
                    }

                    streamResult = {
                      ...streamResult,
                      ...fRes,
                    }
                    return streamResult
                  })
                } catch (e) {
                  console.error(`Exception - PutToFirehose Call - \n${e} `)
                  streamResult = {
                    ...streamResult,
                    PutToFireHoseException: `Exception - PutToFirehose \n${e} `,
                  }
                  return streamResult
                }
              }
            } catch (e) {
              const sErr = `Exception - ReadStream OnEnd Processing - \n${e} `
              // console.error(sErr)
              return { ...streamResult, OnEndStreamResult: sErr }
            }

            const streamEndResult = `S3 Content Stream Ended for ${key}.Processed ${recs} records as ${batchCount} batches.`

            streamResult = {
              ...streamResult,
              StreamEndResult: streamEndResult,
              OnEndRecordStatus: `Processed ${recs} records as ${batchCount} batches.`,
            }

            if (S3DBConfig.SelectiveDebug.indexOf("_902,") > -1)
              console.info(
                `Selective Debug 902: Content Stream OnEnd for (${key}) - Store and Queue Work of ${batchCount} Batches of ${recs} records - Stream Result: \n${JSON.stringify(
                  streamResult
                )} `
              )

            //chunks = []
            //batchCount = 0
            //recs = 0

            resolve({ ...streamResult })

            // return streamResult
          })

          .on("close", async function () {
            streamResult = {
              ...streamResult,
              OnClose_Result: `S3 Content Stream Closed for ${key}`,
            }
          })

        if (S3DBConfig.SelectiveDebug.indexOf("_102,") > -1)
          console.info(`(102) S3 Content Stream Opened for ${key}`)
      })
        .then((r) => {
          return {
            ...streamResult,
            ReturnLocation: `ReadStream Then Clause.\n${r} `,
          }
        })
        .catch((e) => {
          const err = `Exception - ReadStream(catch) - Process S3 Object Content Stream for ${key}. \n${e} `
          //console.error( err )
          //throw new Error( err )
          return {...streamResult, OnCloseReadStreamException: `${err}`}
        })

      // return { ...readStream, ReturnLocation: `...End of ReadStream Promise` }
      return {
        ...streamResult,
        ReturnLocation: `...End of ReadStream Promise`,
      }
    })
    .catch((e) => {
      // console.error(`Exception(error) - Process S3 Object Content Stream for ${ key }.\nResults: ${ JSON.stringify(streamResult) }.\n${ e } `)
      //throw new Error( `Exception(throw) - ReadStream - For ${ key }.\nResults: ${ JSON.stringify( streamResult ) }.\n${ e } ` )
      return {
        ...streamResult,
        ReadStreamException: `Exception(throw) - ReadStream - For ${key}.\nResults: ${JSON.stringify(
          streamResult
        )}.\n${e} `,
      }
    })

  // return processS3ObjectResults
  return processS3ObjectStreamResolution
}

async function putToFirehose(chunks: object[], key: string, cust: string) {
  const client = new FirehoseClient()

  // S3DropBucket_Aggregator
  // S3DropBucket_Log

  let fireHoseStream = "S3DropBucket_Aggregator"
  if (cust === "S3DropBucket_Logs_") fireHoseStream = "S3DropBucket_Log"

  let putFirehoseResp: object = {}

  try {
    for (const j in chunks) {
      //const jo = chunks[j]

      //const j = JSON.parse( chunks[ fhchunk ] )

      //if (cust !== "S3DropBucket_Log_")
        //cust = Object.assign(jo, { Customer: cust })
      cust = j
      
      //const fd = Buffer.from(JSON.stringify(jo), "utf-8")
      const fd = Buffer.from(JSON.stringify(j), "utf-8")

      const fp = {
        DeliveryStreamName: fireHoseStream,
        Record: {
          Data: fd,
        },
      } as PutRecordCommandInput

      const fireCommand = new PutRecordCommand(fp)
      let firehosePutResult: object

      try {
        putFirehoseResp = await client
          .send(fireCommand)
          .then((res: PutRecordCommandOutput) => {
            if (fireHoseStream !== "S3DropBucket_Log") {
              if (S3DBConfig.SelectiveDebug.indexOf("_922,") > -1)
                console.info(
                  `Selective Debug 922 - Put to Firehose Aggregator for ${key} - \n${JSON.stringify(
                    fd
                  )} \nResult: ${JSON.stringify(res)} `
                )

              if (res.$metadata.httpStatusCode === 200) {
                firehosePutResult = {
                  ...firehosePutResult,
                  PutToFireHoseAggregatorResult: `${res.$metadata.httpStatusCode}`,
                }
                firehosePutResult = {
                  ...firehosePutResult,
                  OnEnd_PutToFireHoseAggregator: `Successful Put to Firehose Aggregator for ${key}.\n${JSON.stringify(
                    res
                  )} \n${res.RecordId} `,
                }
              } else {
                firehosePutResult = {
                  ...firehosePutResult,
                  PutToFireHoseAggregatorResult: `UnSuccessful Put to Firehose Aggregator for ${key} \n ${JSON.stringify(
                    res
                  )} `,
                }
              }
            }
            return firehosePutResult
          })
          .catch((e) => {
            console.error(
              `Exception - Put to Firehose Aggregator(Promise -catch) for ${key} \n${e} `
            )
            firehosePutResult = {
              ...firehosePutResult,
              PutToFireHoseException: `Exception - Put to Firehose Aggregator for ${key} \n${e} `,
            }
            return firehosePutResult
          })
      } catch (e) {
        console.error(`Exception - PutToFirehose \n${e} `)
      }
    }

    return putFirehoseResp
  } catch (e) {
    console.error(
      `Exception - Put to Firehose Aggregator(try-catch) for ${key} \n${e} `
    )
  }
}

/**
 * A Lambda function to process the Event payload received from SQS - AWS Queues.
 */
export const S3DropBucketQueueProcessorHandler: Handler = async (
  event: SQSEvent,
  context: Context
) => {
  //ToDo: Build aggregate results and outcomes block
  s3 = new S3Client({ region: process.env.s3DropBucketRegion })

  //Populate Config Options in process.env as a means of Caching the config across invocations occurring within 15 secs of each other.
  //If an obscure config does not exist in process.env then we need to get them all
  if (
    process.env["WorkQueueVisibilityTimeout"] === undefined ||
    process.env["WorkQueueVisibilityTimeout"] === "" ||
    process.env["WorkQueueVisibilityTimeout"] === null
  ) {
    S3DBConfig = await getValidateS3DropBucketConfig()
  }

  if (S3DBConfig.SelectiveDebug.indexOf("_98,") > -1)
    console.info(
      `Selective Debug 98 - S3DropBucket Options: ${JSON.stringify(S3DBConfig)} `
    )
  if (S3DBConfig.SelectiveDebug.indexOf("_99,") > -1)
    console.info(
      `Selective Debug 99 - S3DropBucket Logging Options: ${S3DBConfig.SelectiveDebug!} `
    )
  if (S3DBConfig.SelectiveDebug.indexOf("_909,") > -1)
    console.info(
      `Selective Debug 909 - Environment Vars: ${JSON.stringify(process.env)} `
    )

  if (S3DBConfig.WorkQueueQuiesce) {
    console.info(
      `WorkQueue Quiesce is in effect, no New Work will be Queued up in the SQS Process Queue.`
    )
    return
  }

  //if (S3DBConfig.WorkQueueBucketPurgeCount > 0) {
  //  console.info(
  //    `Purge Requested, Only action will be to Purge ${S3DBConfig.WorkQueueBucketPurge} of ${S3DBConfig.WorkQueueBucketPurgeCount} Records. `
  //  )
  //  const d = await purgeBucket(
  //    Number(process.env["WorkQueueBucketPurgeCount"]!),
  //    process.env["WorkQueueBucketPurge"]!
  //  )
  //  return d
  //}

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

  let custconfig: customerConfig

  
  let postResult: string = "false"

  if (S3DBConfig.SelectiveDebug.indexOf("_506,") > -1)
    console.info(
      `(506) Received SQS Events Batch of ${event.Records.length} records. \nContext: ${context}`
    )

  if (S3DBConfig.SelectiveDebug.indexOf("_904,") > -1)
    console.info(
      `Selective Debug 904 - Received ${
        event.Records.length
      } Work Queue Records. The set of Records are: \n${JSON.stringify(event)} `
    )

  //Empty BatchFail array
  sqsBatchFail.batchItemFailures.forEach(() => {
    sqsBatchFail.batchItemFailures.pop()
  })

  let tqm: s3dbQueueMessage = {
    workKey: "",
    versionId: "",
    marker: "",
    attempts: 0,
    batchCount: "",
    updateCount: "",
    lastQueued: "",
    custconfig: {
      Customer: "",
      format: "",
      separator: "",
      updates: "",
      listId: "",
      listName: "",
      listType: "",
      DBKey: "",
      LookupKeys: "",
      pod: "",
      region: "",
      updateMaxRows: 0,
      refreshToken: "",
      clientId: "",
      clientSecret: "",
      sftp: {
        user: "",
        password: "",
        filepattern: "",
        schedule: "",
      },
      transforms: {
        jsonMap: {},
        csvMap: {},
        ignore: [],
        script: [],
      },
    },
  }

  //Process this Inbound Batch
  for (const q of event.Records) {
    tqm = JSON.parse(q.body)

    //When Testing locally  (Launch config has pre-stored payload) - get some actual work queued
    if (tqm.workKey === "") {
      tqm.workKey =
        (await getAnS3ObjectforTesting(S3DBConfig.S3DropBucketWorkBucket)) ?? ""
    }

    if (tqm.workKey === "devtest.xml") {
      //tqm.workKey = await getAnS3ObjectforTesting( tcc.s3DropBucketWorkBucket! ) ?? ""
      tqm.workKey = testS3Key
      S3DBConfig.S3DropBucketWorkBucket = testS3Bucket
      localTesting = true
    } else {
      testS3Key = ""
      testS3Bucket = ""
      localTesting = false
    }
    //
    //

    if (S3DBConfig.SelectiveDebug.indexOf("_507,") > -1)
      console.info(`(507) Processing Work off the Queue - ${tqm.workKey}`)

    if (S3DBConfig.SelectiveDebug.indexOf("_911,") > -1)
      console.info(
        `Selective Debug 911 - Processing a Batch Item. SQS Event Message: ${JSON.stringify(
          q
        )}`
      )

    custconfig = await getCustomerConfig(tqm.custconfig.Customer) as customerConfig

    try {
      const work = await getS3Work(tqm.workKey, S3DBConfig.S3DropBucketWorkBucket)

      if (work.length > 0) {
        //Retrieve Contents of the Work File
        if (S3DBConfig.SelectiveDebug.indexOf("_512,") > -1)
          console.info(
            `Selective Debug 512 - S3 Retrieve results for Work file ${
              tqm.workKey
            }: ${JSON.stringify(work)}`
          )

        if (custconfig.listType.toLowerCase() === 'referenceset')
          postResult = await postToConnect(
            work,
            custconfig as customerConfig,
            tqm.updateCount
          )
        if (custconfig.listType.toLowerCase() === 'relational' || custconfig.listType.toLowerCase() === 'dbkeyed' || custconfig.listType.toLowerCase() === 'dbnonkeyed')
        postResult = await postToCampaign(
          work,
          custconfig as customerConfig,
          tqm.updateCount
        )

        //  postResult can contain:
        //retry
        //unsuccessful post
        //partially successful
        //successfully posted

        if (S3DBConfig.SelectiveDebug.indexOf("_908,") > -1)
          console.info(
            `Selective Debug 908 - POST Result for ${tqm.workKey}: ${postResult} `
          )

        if (postResult.indexOf("retry") > -1) {
          console.warn(
            `Retry Marked for ${tqm.workKey}. Returning Work Item ${
              q.messageId
            } to Process Queue (Total Retry Count: ${
              sqsBatchFail.batchItemFailures.length + 1
            }). \n${postResult} `
          )
          //Add to BatchFail array to Retry processing the work
          sqsBatchFail.batchItemFailures.push({ itemIdentifier: q.messageId })

          if (S3DBConfig.SelectiveDebug.indexOf("_509,") > -1)
            console.warn(
              `(509) - ${
                tqm.workKey
              } added back to Queue for Retry \n${JSON.stringify(
                sqsBatchFail
              )} `
            )
        } else if (postResult.toLowerCase().indexOf("unsuccessful post") > -1) {
          console.error(
            `Error - Unsuccessful POST (Hard Failure) for ${tqm.workKey}: \n${postResult}\nCustomer: ${custconfig.Customer}, ListId: ${custconfig.listId} ListName: ${custconfig.listName} `
          )
        } else {
          if (postResult.toLowerCase().indexOf("partially successful") > -1) {
            if (S3DBConfig.SelectiveDebug.indexOf("_508,") > -1)
              console.info(
                `(508) Most Work was Successfully Posted to Campaign (work file (${tqm.workKey}, updated ${tqm.custconfig.listName} from ${tqm.workKey}, however there were some exceptions: \n${postResult} `
              )
          } else if (
            postResult.toLowerCase().indexOf("successfully posted") > -1
          ) {
            if (S3DBConfig.SelectiveDebug.indexOf("_508,") > -1)
              console.info(
                `(508) Work Successfully Posted to Campaign (work file (${tqm.workKey}, updated ${tqm.custconfig.listName} from ${tqm.workKey}, \n${postResult} \nThe Work will be deleted from the S3 Process Queue`
              )
          }

          //Delete the Work file
          const d = await deleteS3Object(
            tqm.workKey,
            S3DBConfig.S3DropBucketWorkBucket
          )
          if (d === "204") {
            if (S3DBConfig.SelectiveDebug.indexOf("_924,") > -1)
              console.info(
                `Selective Debug 924 - Successful Deletion of Queued Work file: ${tqm.workKey}`
              )
          } else if (S3DBConfig.SelectiveDebug.indexOf("_924,") > -1)
            console.error(
              `Selective Debug 924 - Failed to Delete ${tqm.workKey}. Expected '204' but received ${d} `
            )

          if (S3DBConfig.SelectiveDebug.indexOf("_511,") > -1)
            console.info(
              `(511) Processed ${tqm.updateCount} Updates from ${tqm.workKey}`
            )

          if (S3DBConfig.SelectiveDebug.indexOf("_510,") > -1)
            console.info(
              `(510) Processed ${
                event.Records.length
              } Work Queue Events. Posted: ${postResult}. \nItems Retry Count: ${
                sqsBatchFail.batchItemFailures.length
              } \nItems Retry List: ${JSON.stringify(sqsBatchFail)} `
            )
        }
      } else throw new Error(`Failed to retrieve work file(${tqm.workKey}) `)
    } catch (e) {
      console.error(
        `Exception - Processing a Work File (${tqm.workKey} off the Work Queue - \n${e}} `
      )
    }
  }

  //let maintenance: (number | string[])[] = []
  //if (S3DBConfig.S3DropBucketWorkQueueMaintHours > 0) {
  //  try {
  //    maintenance = (await maintainS3DropBucketQueueBucket()) ?? [0, ""]

  //    if (S3DBConfig.SelectiveDebug.indexOf("_927,") > -1) {
  //      const l = maintenance[0] as number
  //      if (l > 0)
  //        console.info(
  //          `Selective Debug 927 - ReQueued Work Files: \n${maintenance} `
  //        )
  //      else
  //        console.info(
  //          `Selective Debug 927 - No Work files met criteria to ReQueue`
  //        )
  //    }
  //  } catch (e) {
  //    debugger
  //  }
  //}

  if (S3DBConfig.SelectiveDebug.indexOf("_912,") > -1)
    console.info(
      `Selective Debug 912 - ${
        tqm.workKey
      } returned to Work Queue for Retry \n${JSON.stringify(sqsBatchFail)} `
    )

  //ToDo: For Queue Processing - Complete the Final Processing Outcomes messaging for Queue Processing
  // if (tcc.SelectiveDebug.indexOf("_921,") > -1) console.info(`Selective Debug 921 - \n${ JSON.stringify(processS3ObjectStreamResolution) } `)

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

export const s3DropBucketSFTPHandler: Handler = async (
  event: SQSEvent,
  context: Context
) => {
  // const SFTPClient = new sftpClient()

  if (
    process.env["WorkQueueVisibilityTimeout"] === undefined ||
    process.env["WorkQueueVisibilityTimeout"] === "" ||
    process.env["WorkQueueVisibilityTimeout"] === null
  )
  {
    S3DBConfig = await getValidateS3DropBucketConfig()
  }

  console.info(
    `S3 Dropbucket SFTP Processor Selective Debug Set is: ${S3DBConfig.SelectiveDebug!} `
  )

  if (S3DBConfig.SelectiveDebug.indexOf("_98,") > -1)
    console.info(
      `Selective Debug 98 - Process Environment Vars: ${JSON.stringify(
        process.env
      )} `
    )

  console.info(`SFTP  Received Event: ${JSON.stringify(event)}.\nContext: ${context} `)
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
    .then((res): string => {
      console.info(res)
      return response
    })
    .catch((err) => {
      console.error(
        `Error - Failed to retrieve SFTP Scheduler2 Events: ${err} `
      )
      return err
    })
  
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

  
  console.info(
    `Received SFTP SQS Events Batch of ${event.Records.length} records.`
  )

  if (S3DBConfig.SelectiveDebug.indexOf("_4,") > -1)
    console.info(
      `Selective Debug 4 - Received ${event.Records.length
      } SFTP Queue Records.Records are: \n${JSON.stringify(event)} `
    )

  

  //This test provides avoiding the following code until we can get the rest of the refactoring completed
  if (new Date().getTime() > 0) return

  SFTPClient.uploadDir('srcBucket', 'destBucket')


  sftpDeleteFile('remotefilepath')
  sftpDownloadFile('remoteFile', 'localFile')
  sftpUploadFile('localFile', 'remoteFile') 
  sftpListFiles('remoteDir', {} as ListFilterFunction)
  sftpDisconnect()
  sftpConnect(
    {
      host: '',
      port: '',
      username: '',
      password: ''
    })



  // event.Records.forEach((i) => {
  //     sqsBatchFail.batchItemFailures.push({ itemIdentifier: i.messageId })
  // })

  //Empty BatchFail array
  sqsBatchFail.batchItemFailures.forEach(() => {
    sqsBatchFail.batchItemFailures.pop()
  })

  //Process this Inbound Batch
  for (const q of event.Records) {
    //General Plan:
    // Process SQS Events being emitted for Check FTP Directory for Customer X
    // Process SQS Events being emitted for
    // Check  configs to emit another round of SQS Events for the next round of FTP Work.

    // event.Records.forEach(async (i: SQSRecord) => {
    const tqm: s3dbQueueMessage = JSON.parse(q.body)

    tqm.workKey = JSON.parse(q.body).workKey

    //When Testing - get some actual work queued
    if (tqm.workKey === "process_2_pura_2023_10_27T15_11_40_732Z.csv") {
      tqm.workKey = (await getAnS3ObjectforTesting(S3DBConfig.S3DropBucket!)) ?? ""
    }

    console.info(`Processing Work Queue for ${tqm.workKey}`)
    if (S3DBConfig.SelectiveDebug.indexOf("_11,") > -1)
      console.info(
        `Selective Debug 11 - SQS Events - Processing Batch Item ${JSON.stringify(
          q
        )} `
      )

    // try
    // {
    //     const work = await getS3Work(tqm.workKey, "s3dropbucket-process")
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

    //             const d: string = await deleteS3Object(tqm.workKey, 's3dropbucket-process')
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

  console.info(
    `Processed ${event.Records.length} SFTP Requests.Items Fail Count: ${
      sqsBatchFail.batchItemFailures.length
    }\nItems Failed List: ${JSON.stringify(sqsBatchFail)}`
  )

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

async function sftpConnect(options: {
  host: string
  port: string
  username?: string
  password?: string
}) {
  console.info(`Connecting to ${options.host}: ${options.port}`)
  try {
    // await SFTPClient.connect(options)
  } catch (err) {
    console.info("Failed to connect:", err)
  }
}

async function sftpDisconnect() {
  // await SFTPClient.end()
}

async function sftpListFiles(remoteDir: string, fileGlob: ListFilterFunction) {
  console.info(`Listing ${remoteDir} ...`)
  let fileObjects: sftpClient.FileInfo[] = []
  try {
     fileObjects = await SFTPClient.list(remoteDir, fileGlob)
  } catch (err) {
    console.info("Listing failed:", err)
  }

  const fileNames = []

  for (const file of fileObjects) {
    if (file.type === "d") {
      console.info(
        `${new Date(file.modifyTime).toISOString()} PRE ${file.name}`
      )
    } else {
      console.info(
        `${new Date(file.modifyTime).toISOString()} ${file.size} ${file.name}`
      )
    }

    fileNames.push(file.name)
  }

  return fileNames
}

async function sftpUploadFile(localFile: string, remoteFile: string) {
  console.info(`Uploading ${localFile} to ${remoteFile} ...`)
  try {
    // await SFTPClient.put(localFile, remoteFile)
  } catch (err) {
    console.error("Uploading failed:", err)
  }
}

async function sftpDownloadFile(remoteFile: string, localFile: string) {
  console.info(`Downloading ${remoteFile} to ${localFile} ...`)
  try {
    // await SFTPClient.get(remoteFile, localFile)
  } catch (err) {
    console.error("Downloading failed:", err)
  }
}

async function sftpDeleteFile(remoteFile: string) {
  console.info(`Deleting ${remoteFile}`)
  try {
    // await SFTPClient.delete(remoteFile)
  } catch (err) {
    console.error("Deleting failed:", err)
  }
}

async function checkForS3DBConfigUpdates() {
  if (s3dbLogDebug) console.info(`Checking for S3DropBucket Config updates`)
  S3DBConfig = await getValidateS3DropBucketConfig()
  
  //Populate/Refresh Customer Config List 


  if (S3DBConfig.SelectiveDebug.indexOf("_901,") > -1)
    console.info(
      `Selective Debug 901 - Refreshed S3DropBucket Queue Config \n ${JSON.stringify(
        S3DBConfig
      )} `
    )
}

async function getValidateS3DropBucketConfig() {
  //Article notes that Lambda runs faster referencing process.env vars, lets see.
  //Did not pan out, with all the issues with conversions needed to actually use as primary reference, can't see it being faster
  //Using process.env as a useful reference store, especially for accessToken, good across invocations
  //Validate then populate env vars with S3DropBucket config

  //const cf = process.env.S3DropBucketConfigFile
  //const cb = process.env.S3DropBucketConfigBucket

  let getObjectCmd: GetObjectCommandInput = {
    Bucket: undefined,
    Key: undefined,
  }


  if (!process.env.S3DropBucketConfigBucket)
    process.env.S3DropBucketConfigBucket = "s3dropbucket-configs"
  if (!process.env.S3DropBucketConfigFile)
    process.env.S3DropBucketConfigFile = "s3dropbucket_config.jsonc"
  getObjectCmd = {
    Bucket: process.env.S3DropBucketConfigBucket,
    Key: process.env.S3DropBucketConfigFile,
  }

  //if ( process.env.S3DropBucketConfigFile && process.env.S3DropBucketConfigBucket )
  //{
  //    getObjectCmd = {
  //        Bucket: process.env.S3DropBucketConfigBucket,
  //        Key: process.env.S3DropBucketConfigFile
  //    }
  //}
  //else
  //{
  //    getObjectCmd = {
  //        Bucket: process.env.S3DropBucketConfigBucket,    //'s3dropbucket-configs',
  //        Key: 's3dropbucket_config.jsonc',
  //    }
  //}

  let s3dbcr
  let s3dbc = {} as s3dbConfig

  try {
    s3dbc = await s3
      .send(new GetObjectCommand(getObjectCmd))
      .then(async (getConfigS3Result: GetObjectCommandOutput) => {
        s3dbcr = (await getConfigS3Result.Body?.transformToString(
          "utf8"
        )) as string

        //Parse comments out of the json before returning parsed config json
        s3dbcr = s3dbcr.replaceAll(new RegExp(/[^:](\/\/.*(,|$|")?)/g), "")
        s3dbcr = s3dbcr.replaceAll(" ", "")
        s3dbcr = s3dbcr.replaceAll("\n", "")

        return JSON.parse(s3dbcr)
      })
  } catch (e) {
    console.error(
      `Exception - Pulling S3DropBucket Config File (bucket:${getObjectCmd.Bucket}  key:${getObjectCmd.Key}) \nResult: ${s3dbcr} \nException: \n${e} `
    )
    return {} as s3dbConfig
  }

  try {
    if (
      s3dbc.LOGLEVEL !== undefined &&
      s3dbc.LOGLEVEL.toLowerCase().indexOf("debug") > -1
    ) {
      s3dbLogDebug = true
      process.env.s3dbLogDebug = "true"
    }

    //if (
    //  s3dbc.LOGLEVEL !== undefined &&
    //  s3dbc.LOGLEVEL.toLowerCase().indexOf("verbose") > -1
    //) {
    //  s3dbLogVerbose = true
    //  process.env["s3dbLogVerbose"] = "true"
    //}

    if (s3dbc.SelectiveDebug !== undefined)
      process.env["SelectiveDebug"] = s3dbc.SelectiveDebug
    //Perhaps validate that the string contains commas and underscores as needed,

    if (!s3dbc.S3DropBucket || s3dbc.S3DropBucket === "") {
      throw new Error(
        `Exception - S3DropBucket Configuration is not correct: ${s3dbc.S3DropBucket}.`
      )
    } else process.env["s3DropBucket"] = s3dbc.S3DropBucket

    if (!s3dbc.S3DropBucketConfigs || s3dbc.S3DropBucketConfigs === "") {
      throw new Error(
        `Exception -S3DropBucketConfigs definition is not correct: ${s3dbc.S3DropBucketConfigs}.`
      )
    } else process.env["s3DropBucketConfigs"] = s3dbc.S3DropBucketConfigs

    if (!s3dbc.S3DropBucketWorkBucket || s3dbc.S3DropBucketWorkBucket === "") {
      throw new Error(
        `Exception - S3DropBucket Work Bucket Configuration is not correct: ${s3dbc.S3DropBucketWorkBucket} `
      )
    } else process.env["s3DropBucketWorkBucket"] = s3dbc.S3DropBucketWorkBucket

    if (!s3dbc.S3DropBucketWorkQueue || s3dbc.S3DropBucketWorkQueue === "") {
      throw new Error(
        `Exception -S3DropBucket Work Queue Configuration is not correct: ${s3dbc.S3DropBucketWorkQueue} `
      )
    } else process.env["s3DropBucketWorkQueue"] = s3dbc.S3DropBucketWorkQueue

    // if (tc.SQS_QUEUE_URL !== undefined) tcc.SQS_QUEUE_URL = tc.SQS_QUEUE_URL
    // else throw new Error(`S3DropBucket Config invalid definition: SQS_QUEUE_URL - ${ tc.SQS_QUEUE_URL } `)

    if (s3dbc.xmlapiurl != undefined) process.env["xmlapiurl"] = s3dbc.xmlapiurl
    else
      throw new Error(
        `S3DropBucket Config invalid definition: xmlapiurl - ${s3dbc.xmlapiurl} `
      )

    if (s3dbc.restapiurl !== undefined)
      process.env["restapiurl"] = s3dbc.restapiurl
    else
      throw new Error(
        `S3DropBucket Config invalid definition: restapiurl - ${s3dbc.restapiurl} `
      )

    if (s3dbc.authapiurl !== undefined)
      process.env["authapiurl"] = s3dbc.authapiurl
    else
      throw new Error(
        `S3DropBucket Config invalid definition: authapiurl - ${S3DBConfig.authapiurl} `
      )

    //Default Seperator
    if (s3dbc.jsonSeparator !== undefined) {
      if (s3dbc.jsonSeparator.toLowerCase() === "null")
        s3dbc.jsonSeparator = `''`
      if (s3dbc.jsonSeparator.toLowerCase() === "empty")
        s3dbc.jsonSeparator = `""`
      if (s3dbc.jsonSeparator.toLowerCase() === "\n") s3dbc.jsonSeparator = "\n"
    } else s3dbc.jsonSeparator = "\n"
    process.env["jsonSeparator"] = s3dbc.jsonSeparator

    if (s3dbc.WorkQueueQuiesce !== undefined) {
      process.env["WorkQueueQuiesce"] = s3dbc.WorkQueueQuiesce.toString()
    } else
      throw new Error(
        `S3DropBucket Config invalid definition: WorkQueueQuiesce - ${s3dbc.WorkQueueQuiesce} `
      )

    //deprecated in favor of using AWS interface to set these on the queue
    // if (tc.WorkQueueVisibilityTimeout !== undefined)
    //     process.env.WorkQueueVisibilityTimeout = tc.WorkQueueVisibilityTimeout.toFixed()
    // else
    //     throw new Error(
    //         `S3DropBucket Config invalid definition: WorkQueueVisibilityTimeout - ${ tc.WorkQueueVisibilityTimeout } `,
    //     )

    // if (tc.WorkQueueWaitTimeSeconds !== undefined)
    //     process.env.WorkQueueWaitTimeSeconds = tc.WorkQueueWaitTimeSeconds.toFixed()
    // else
    //     throw new Error(
    //         `S3DropBucket Config invalid definition: WorkQueueWaitTimeSeconds - ${ tc.WorkQueueWaitTimeSeconds } `,
    //     )

    // if (tc.RetryQueueVisibilityTimeout !== undefined)
    //     process.env.RetryQueueVisibilityTimeout = tc.WorkQueueWaitTimeSeconds.toFixed()
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

    if (s3dbc.MaxBatchesWarning !== undefined)
      process.env["RetryQueueInitialWaitTimeSeconds"] =
        s3dbc.MaxBatchesWarning.toFixed()
    else
      throw new Error(
        `S3DropBucket Config invalid definition: MaxBatchesWarning - ${s3dbc.MaxBatchesWarning} `
      )

    if (s3dbc.S3DropBucketQuiesce !== undefined) {
      process.env["DropBucketQuiesce"] = s3dbc.S3DropBucketQuiesce.toString()
    } else
      throw new Error(
        `S3DropBucket Config invalid definition: DropBucketQuiesce - ${s3dbc.S3DropBucketQuiesce} `
      )

    if (s3dbc.S3DropBucketMaintHours !== undefined) {
      process.env["DropBucketMaintHours"] =
        s3dbc.S3DropBucketMaintHours.toString()
    } else s3dbc.S3DropBucketMaintHours = -1

    if (s3dbc.S3DropBucketMaintLimit !== undefined) {
      process.env["DropBucketMaintLimit"] =
        s3dbc.S3DropBucketMaintLimit.toString()
    } else s3dbc.S3DropBucketMaintLimit = 0

    if (s3dbc.S3DropBucketMaintConcurrency !== undefined) {
      process.env["DropBucketMaintConcurrency"] =
        s3dbc.S3DropBucketMaintConcurrency.toString()
    } else s3dbc.S3DropBucketMaintLimit = 1

    if (s3dbc.S3DropBucketWorkQueueMaintHours !== undefined) {
      process.env["DropBucketWorkQueueMaintHours"] =
        s3dbc.S3DropBucketWorkQueueMaintHours.toString()
    } else s3dbc.S3DropBucketWorkQueueMaintHours = -1

    if (s3dbc.S3DropBucketWorkQueueMaintLimit !== undefined) {
      process.env["DropBucketWorkQueueMaintLimit"] =
        s3dbc.S3DropBucketWorkQueueMaintLimit.toString()
    } else s3dbc.S3DropBucketWorkQueueMaintLimit = 0

    if (s3dbc.S3DropBucketWorkQueueMaintConcurrency !== undefined) {
      process.env["DropBucketWorkQueueMaintConcurrency"] =
        s3dbc.S3DropBucketWorkQueueMaintConcurrency.toString()
    } else s3dbc.S3DropBucketWorkQueueMaintConcurrency = 1

    if (s3dbc.S3DropBucketLog !== undefined) {
      process.env["S3DropBucketLog"] = s3dbc.S3DropBucketLog.toString()
    } else s3dbc.S3DropBucketLog = false

    if (s3dbc.S3DropBucketLogBucket !== undefined) {
      process.env["S3DropBucketLogBucket"] =
        s3dbc.S3DropBucketLogBucket.toString()
    } else s3dbc.S3DropBucketLogBucket = ""

    if (s3dbc.S3DropBucketWorkQueueMaintConcurrency !== undefined) {
      process.env["DropBucketWorkQueueMaintConcurrency"] =
        s3dbc.S3DropBucketWorkQueueMaintConcurrency.toString()
    } else s3dbc.S3DropBucketWorkQueueMaintConcurrency = 1

    if (s3dbc.S3DropBucketPurge !== undefined)
      process.env["DropBucketPurge"] = s3dbc.S3DropBucketPurge
    else
      throw new Error(
        `S3DropBucket Config invalid definition: DropBucketPurge - ${s3dbc.S3DropBucketPurge} `
      )

    if (s3dbc.S3DropBucketPurgeCount !== undefined)
      process.env["DropBucketPurgeCount"] =
        s3dbc.S3DropBucketPurgeCount.toFixed()
    else
      throw new Error(
        `S3DropBucket Config invalid definition: DropBucketPurgeCount - ${s3dbc.S3DropBucketPurgeCount} `
      )

    if (s3dbc.QueueBucketQuiesce !== undefined) {
      process.env["QueueBucketQuiesce"] = s3dbc.QueueBucketQuiesce.toString()
    } else
      throw new Error(
        `S3DropBucket Config invalid definition: QueueBucketQuiesce - ${s3dbc.QueueBucketQuiesce} `
      )

    if (s3dbc.WorkQueueBucketPurge !== undefined)
      process.env["WorkQueueBucketPurge"] = s3dbc.WorkQueueBucketPurge
    else
      throw new Error(
        `S3DropBucket Config invalid definition: WorkQueueBucketPurge - ${s3dbc.WorkQueueBucketPurge} `
      )

    if (s3dbc.WorkQueueBucketPurgeCount !== undefined)
      process.env["WorkQueueBucketPurgeCount"] =
        s3dbc.WorkQueueBucketPurgeCount.toFixed()
    else
      throw new Error(
        `S3DropBucket Config invalid definition: WorkQueueBucketPurgeCount - ${s3dbc.WorkQueueBucketPurgeCount} `
      )

    if (s3dbc.prefixFocus !== undefined && s3dbc.prefixFocus != "") {
      process.env["S3DropBucketFocusPrefix"] = s3dbc.prefixFocus
      console.warn(
        `A Prefix Focus has been configured. Only S3DropBucket Objects with the prefix "${s3dbc.prefixFocus}" will be processed.`
      )
    }
  } catch (e) {
    throw new Error(`Exception - Parsing S3DropBucket Config File ${e} `)
  }

  if (s3dbc.SelectiveDebug.indexOf("_901,") > -1)
    console.info(
      `Selective Debug 901 - Pulled s3dropbucket_config.jsonc: \n${JSON.stringify(
        s3dbc
      )} `
    )

  return s3dbc
}

async function getCustomerConfig(filekey: string) {
  
  // Retrieve file's prefix as the Customer Name
  if (!filekey)
    throw new Error(
        `Exception - Cannot resolve Customer Config without a valid Customer Prefix in filename (filename is ${filekey})`
    )

  while (filekey.indexOf("/") > -1) {
    //remove any folders from name
    filekey = filekey.split("/").at(-1) ?? filekey
  }

  //Check for timestamp - if timestamp - remove timestamp (remove underscores) 
  const r = new RegExp(/\d{4}_\d{2}_\d{2}T.*Z.*/, "gm")
  if (filekey.match(r)) {
    filekey = filekey.replace(r, "") //remove timestamp from name
  }

  if (filekey.indexOf('_') > -1)
  {
    filekey = filekey.substring(0, filekey.lastIndexOf("_") + 1)
    }
  
  //  const customer = filekey.split('_')[0] + '_'      //initial treatment, get prefix up to first underscore

    
  //Should be left with customername, data flow and trailing underscore
  if (!filekey.endsWith('_'))
  {
    throw new Error(
      `Exception - Cannot resolve Customer Config without a valid Customer Prefix (filename is ${filekey})`
    )
    
  }

  if (filekey === "_" || filekey.length < 4) {
    throw new Error(
      `Exception - Cannot resolve Customer Config without a valid Customer Prefix in filename (filename is ${filekey})`
   )
  }

  //const customer = filekey.split('_')[0] + '_'      //initial treatment, get prefix up to first underscore


  //ToDo: Need to change this up to getting a listing of all configs and matching up against filename,
  //  allowing Configs to match on filename regardless of case
  //  populate 'process.env.S3DropBucketConfigsList' and walk over it to match config to filename
  const customer = filekey  //.toLowerCase()
  
  let configJSON = {} as customerConfig
  // const configObjs = [new Uint8Array()]

  const getObjectCommand = {
    Key: `${customer}config.jsonc`,
    //Bucket: 's3dropbucket-configs'
    Bucket: process.env.S3DropBucketConfigBucket,
  }

  let ccr

  try {
    await s3
      .send(new GetObjectCommand(getObjectCommand))
      .then(async (getConfigS3Result: GetObjectCommandOutput) => {
        ccr = (await getConfigS3Result.Body?.transformToString(
          "utf8"
        )) as string

        if (S3DBConfig.SelectiveDebug.indexOf("_910,") > -1)
          console.info(
            `Selective Debug 910 - Customers (${customer}) Config: \n ${ccr} `
          )

        //Parse comments out of the json before parse
        ccr = ccr.replaceAll(new RegExp(/[^:](\/\/.*(,|$|")?)/g), "")
        ccr = ccr.replaceAll("\n", "")

        configJSON = JSON.parse(ccr)
      })
      .catch((e) => {
        const err: string = JSON.stringify(e)

        if (err.indexOf("specified key does not exist") > -1)
          throw new Error(
            `Exception - Customer Config - ${customer}config.jsonc does not exist on ${S3DBConfig.S3DropBucketConfigs} bucket \nException ${e} `
          )

        if (err.indexOf("NoSuchKey") > -1)
          throw new Error(
            `Exception - Customer Config Not Found(${customer}config.jsonc) on ${S3DBConfig.S3DropBucketConfigs}. \nException ${e} `
          )

        throw new Error(
          `Exception - Retrieving Config (${customer}config.jsonc) from ${S3DBConfig.S3DropBucketConfigs} \nException ${e} `
        )
      })
  } catch (e) {
    debugger
    throw new Error(`Exception - Pulling Customer Config \n${ccr} \n${e} `)
  }

  customersConfig = await validateCustomerConfig(configJSON)
  return customersConfig as customerConfig
}

async function validateCustomerConfig(config: customerConfig) {
  if (!config || config === null) {
    throw new Error("Invalid  CustomerConfig - empty or null config")
  }

  if (!config.Customer) {
    throw new Error("Invalid  CustomerConfig - Customer is not defined")
  } else if (config.Customer.length < 4 || !config.Customer.endsWith("_")) {
    throw new Error(
      `Invalid  CustomerConfig - Customer string is not valid, must be at least 3 characters and a trailing underscore, '_'`
    )
  }

  if (!config.clientId) {
    throw new Error("Invalid Customer Config - ClientId is not defined")
  }
  if (!config.clientSecret) {
    throw new Error("Invalid Customer Config - ClientSecret is not defined")
  }
  if (!config.format) {
    throw new Error("Invalid Customer Config - Format is not defined")
  }
  if (!config.updates) {
    throw new Error("Invalid Customer Config - Updates is not defined")
  }

  if (!config.listId) {
    throw new Error("Invalid Customer Config - ListId is not defined")
  }
  if (!config.listName) {
    throw new Error("Invalid Customer Config - ListName is not defined")
  }
  if (!config.pod) {
    throw new Error("Invalid Customer Config - Pod is not defined")
  }
  if (!config.region) {
    //Campaign POD Region
    throw new Error("Invalid Customer Config - Region is not defined")
  }
  if (!config.refreshToken) {
    throw new Error("Invalid Customer Config - RefreshToken is not defined")
  }

  if (!config.format.toLowerCase().match(/^(?:csv|json)$/gim)) {
    throw new Error("Invalid Customer Config - Format is not 'CSV' or 'JSON' ")
  }

  if (!config.separator) {
    //see: https://www.npmjs.com/package/@streamparser/json-node
    //JSONParser / Node separator option: null = `''` empty = '', otherwise a separator eg. '\n'
    config.separator = "\n"
  }

  //Customer specific separator
  if (config.separator.toLowerCase() === "null") config.separator = `''`
  if (config.separator.toLowerCase() === "empty") config.separator = `""`
  if (config.separator.toLowerCase() === "\n") config.separator = "\n"

  if (!config.updates.toLowerCase().match(/^(?:singular|multiple|bulk)$/gim))
  {
    throw new Error(
      "Invalid Customer Config - Updates is not 'Singular' or 'Multiple' "
    )
  }
  //Remove legacy config value
  if (config.updates.toLowerCase() === "bulk") config.updates = "Multiple"

  if (!config.pod.match(/^(?:0|1|2|3|4|5|6|7|8|9|a|b)$/gim)) {
    throw new Error(
      "Invalid Customer Config - Pod is not 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, A, or B. "
    )
  }

  //
  //    XMLAPI Endpoints
  //Pod 1 - https://api-campaign-us-1.goacoustic.com/XMLAPI
  //Pod 2 - https://api-campaign-us-2.goacoustic.com/XMLAPI
  //Pod 3 - https://api-campaign-us-3.goacoustic.com/XMLAPI
  //Pod 4 - https://api-campaign-us-4.goacoustic.com/XMLAPI
  //Pod 5 - https://api-campaign-us-5.goacoustic.com/XMLAPI
  //Pod 6 - https://api-campaign-eu-1.goacoustic.com/XMLAPI
  //Pod 7 - https://api-campaign-ap-2.goacoustic.com/XMLAPI
  //Pod 8 - https://api-campaign-ca-1.goacoustic.com/XMLAPI
  //Pod 9 - https://api-campaign-us-6.goacoustic.com/XMLAPI
  //Pod A - https://api-campaign-ap-1.goacoustic.com/XMLAPI
  //pod B - https://api-campaign-ap-3.goacoustic.com/XMLAPI

  switch (config.pod.toLowerCase()) {
    case "6":
      config.pod = "1"
      break
    case "7":
      config.pod = "2"
      break
    case "8":
      config.pod = "1"
      break
    case "9":
      config.pod = "6"
      break
    case "a":
      config.pod = "1"
      break
    case "b":
      config.pod = "3"
      break

    default:
      break
  }

  if (!config.region.toLowerCase().match(/^(?:us|eu|ap|ca)$/gim)) {
    throw new Error(
      "Invalid Customer Config - Region is not 'US', 'EU', CA' or 'AP'. "
    )
  }

  if (!config.listType) {
    throw new Error("Invalid Customer Config - ListType is not defined")
  }

  if (
    !config.listType
      .toLowerCase()
      .match(/^(?:relational|dbkeyed|dbnonkeyed)$/gim)
  ) {
    throw new Error(
      "Invalid Customer Config - ListType must be either 'Relational', 'DBKeyed' or 'DBNonKeyed'. "
    )
  }

  if (config.listType.toLowerCase() == "dbkeyed" && !config.DBKey) {
    throw new Error(
      "Invalid Customer Config - Update set as Database Keyed but DBKey is not defined. "
    )
  }

  if (config.listType.toLowerCase() == "dbnonkeyed" && !config.LookupKeys) {
    throw new Error(
      "Invalid Customer Config - Update set as Database NonKeyed but lookupKeys is not defined. "
    )
  }

  if (!config.sftp) {
    config.sftp = { user: "", password: "", filepattern: "", schedule: "" }
  }

  if (config.sftp.user && config.sftp.user !== "")
  {
    console.info(`SFTP User: ${config.sftp.user}`)
  }
  if (config.sftp.password && config.sftp.password !== "")
  {
    console.info(`SFTP Pswd: ${config.sftp.password}`)
  }
  if (config.sftp.filepattern && config.sftp.filepattern !== "")
  {
    console.info(`SFTP File Pattern: ${config.sftp.filepattern}`)

  }
  if (config.sftp.schedule && config.sftp.schedule !== "")
  {
    console.info(`SFTP Schedule: ${config.sftp.schedule}`)

  }

  if (!config.transforms) {
    Object.assign(config, { transforms: {} })
  }
  if (!config.transforms.jsonMap) {
    Object.assign(config.transforms, { jsonMap: {} })
  }
  if (!config.transforms.csvMap) {
    Object.assign(config.transforms, { csvMap: {} })
  }
  if (!config.transforms.ignore) {
    Object.assign(config.transforms, { ignore: [] })
  }
  if (!config.transforms.script) {
    Object.assign(config.transforms, { script: "" })
  }

  // if (Object.keys(config.transform[0].jsonMap)[0].indexOf('none'))
  if (!config.transforms.jsonMap)
  {
    const tmpMap: { [key: string]: string } = {}
    const jm = config.transforms.jsonMap as unknown as {
      [key: string]: string
    }
    for (const m in jm) {
      try {
        const p = jm[m]
        const v = jsonpath.parse(p)  //checking for parse exception highlighting invalid jsonpath
        tmpMap[m] = jm[m]
        // tmpmap2.m = jm.m
        if (S3DBConfig.SelectiveDebug.indexOf("_930,") > -1)
        {
          console.info(`Selective Debug 930 - Validate Customer Config - transforms - JSONPath - ${JSON.stringify(v)}`)
        }
        
      } catch (e) {
        console.error(
          `Invalid JSONPath defined in Customer config: ${m}: "${m}", \nInvalid JSONPath - ${e} `
        )
      }
    }
    config.transforms.jsonMap = tmpMap
  }

  return config as customerConfig
}

async function packageUpdates(
  workSet: object[],
  key: string,
  custConfig: customerConfig
) {
  
  let updates: object[] = []
  let sqwResult: object = {}

  try {
    //Process everything out of the Global var Chunks array
    //Need to pull that scope down to something a little more local
    while (chunks.length > 0) {
      updates = [] as object[]
      //Stand out 100 updates at a time, 
      while (chunks.length > 0 && updates.length < 100) {
        const c = chunks.pop() ?? {}
        updates.push(c)
      }

      sqwResult = await storeAndQueueWork(updates, key, custConfig).then(
        (res) => {
          //console.info( `Debug Await StoreAndQueueWork Result: ${ JSON.stringify( res ) }` )

          return res
        }
      )

      //console.info( `Debug sqwResult ${ JSON.stringify( sqwResult ) }` )
    }

    if (S3DBConfig.SelectiveDebug.indexOf("_918,") > -1)
      console.info(
        `Selective Debug 918: PackageUpdates StoreAndQueueWork for ${key}. \nFor a total of ${recs} Updates in ${batchCount} Batches.  Result: \n${JSON.stringify(
          sqwResult
        )} `
      )
  } catch (e) {
    debugger
    console.error(`Exception - packageUpdates for ${key} \n${e} `)
    sqwResult = {
      ...sqwResult,
      StoreQueueWorkException: `Exception - PackageUpdates StoreAndQueueWork for ${key} \nBatch ${batchCount} of ${recs} Updates. \n${e} `,
    }
  }

  return { StoreAndQueueWorkResult: sqwResult }
}

async function storeAndQueueWork(
  updates: object[],
  s3Key: string,
  config: customerConfig
) {
  batchCount++

  if (batchCount > S3DBConfig.MaxBatchesWarning)
    console.warn(
      `Warning: Updates from the S3 Object(${s3Key}) are exceeding(${batchCount}) the Warning Limit of ${S3DBConfig.MaxBatchesWarning} Batches per Object.`
    )
  // throw new Error(`Updates from the S3 Object(${ s3Key }) Exceed(${ batch }) Safety Limit of 20 Batches of 99 Updates each.Exiting...`)

  const updateCount = updates.length

  //Customers marked as "Singular" updates files are not transformed, but sent to Firehose prior to getting here.
  //  therefore if Aggregate file, or files config'd as "Multiple" updates, then need to perform Transforms
  try
  {
    //Apply Transforms, if any, 
    updates = transforms(updates, config)
  } catch (e) {
    throw new Error(`Exception - Transforms - ${e}`)
  }

  if (
    customersConfig.listType.toLowerCase() === "dbkeyed" ||
    customersConfig.listType.toLowerCase() === "dbnonkeyed"
  ) {
    xmlRows = convertJSONToXML_DBUpdates(updates, config)
  }

  if (customersConfig.listType.toLowerCase() === "relational") {
    xmlRows = convertJSONToXML_RTUpdates(updates, config)
  }

  if (s3Key.indexOf("TestData") > -1) {
    //strip /testdata folder from key
    s3Key = s3Key.split("/").at(-1) ?? s3Key
  }

  let key = s3Key

  while (key.indexOf("/") > -1) {
    key = key.split("/").at(-1) ?? key
  }

  key = key.replace(".", "_")

  key = `${key}-update-${batchCount}-${updateCount}-${uuidv4()}.xml`

  //if ( Object.values( updates ).length !== recs )
  //{
  //    if ( tcc.SelectiveDebug.indexOf( "_900," ) > -1 ) console.error( `(900) Recs Count ${ recs } does not reflect Updates Count ${ Object.values( updates ).length } ` )
  //}

  if (s3dbLogDebug)
    console.info(
      `Queuing Work File ${key} for ${s3Key}. Batch ${batchCount} of ${updateCount} records)`
    )

  let addWorkToS3WorkBucketResult
  let addWorkToSQSWorkQueueResult
  const v = ""

  try {
    addWorkToS3WorkBucketResult = await addWorkToS3WorkBucket(xmlRows, key)
      .then((res) => {
        return res //{"AddWorktoS3Results": res}
      })
      .catch((err) => {
        console.error(`Exception - AddWorkToS3WorkBucket ${err}`)
      })
  } catch (e) {
    const sqwError = `Exception - StoreAndQueueWork Add work to S3 Bucket exception \n${e} `
    console.error(sqwError)
    return {
      StoreS3WorkException: sqwError,
      StoreQueueWorkException: "",
      AddWorkToS3WorkBucketResults: JSON.stringify(addWorkToS3WorkBucketResult),
    }
  }

  // v = AddWorkToS3WorkBucketResults.versionId ?? ''
  const marker = "Initially Queued on " + new Date()

  try {
    addWorkToSQSWorkQueueResult = await addWorkToSQSWorkQueue(
      config,
      key,
      v,
      batchCount,
      updates.length.toString(),
      marker
    ).then((res) => {
      return res
      //     {
      //         sqsWriteResult: "200",
      //         workQueuedSuccess: true,
      //         SQSSendResult: "{\"$metadata\":{\"httpStatusCode\":200,\"requestId\":\"e70fba06-94f2-5608-b104-e42dc9574636\",\"attempts\":1,\"totalRetryDelay\":0},\"MD5OfMessageAttributes\":\"0bca0dfda87c206313963daab8ef354a\",\"MD5OfMessageBody\":\"940f4ed5927275bc93fc945e63943820\",\"MessageId\":\"cf025cb3-dce3-4564-89a5-23dcae86dd42\"}",
      // }
    })
  } catch (e) {
    const sqwError = `Exception - StoreAndQueueWork Add work to SQS Queue exception \n${e} `
    console.error(sqwError)
    return { StoreQueueWorkException: sqwError, StoreS3WorkException: "" }
  }

  if (S3DBConfig.SelectiveDebug.indexOf("_915,") > -1)
    console.info(
      `Selective Debug 915 - Results of Store and Queue of Updates - Add to Process Bucket: ${JSON.stringify(
        addWorkToS3WorkBucketResult
      )} \n Add to Process Queue: ${JSON.stringify(
        addWorkToSQSWorkQueueResult
      )} `
    )

  return {
    AddWorkToS3WorkBucketResults: addWorkToS3WorkBucketResult,
    AddWorkToSQSWorkQueueResults: addWorkToSQSWorkQueueResult,
  }
}

function convertJSONToXML_RTUpdates(updates: object[], config: customerConfig) {
  if (updates.length < 1) {
    throw new Error(
      `Exception - Convert JSON to XML for RT - No Updates(${updates.length}) were passed to process into XML. Customer ${config.Customer} `
    )
  }

  xmlRows = `<Envelope> <Body> <InsertUpdateRelationalTable> <TABLE_ID> ${config.listId} </TABLE_ID><ROWS>`

  let r = 0

  for (const upd in updates) {
    recs++

    //const updAtts = JSON.parse( updates[ upd ] )
    const updAtts = updates[upd]

    r++
    xmlRows += `<ROW>`
    // Object.entries(jo).forEach(([key, value]) => {
        
    type ua = keyof typeof updAtts

    for (const uv in updAtts) {
            const uVal: ua = uv as ua
      // console.info(`Record ${r} as ${key}: ${value}`)
      xmlRows += `<COLUMN name="${uVal}"> <![CDATA[${updAtts[uVal]}]]> </COLUMN>`
    }

    xmlRows += `</ROW>`
  }

  //Tidy up the XML
  xmlRows += `</ROWS></InsertUpdateRelationalTable></Body></Envelope>`

  if (s3dbLogDebug)
    console.info(
      `Converting ${r} updates to XML RT Updates. Packaged ${
        Object.values(updates).length
      } rows as updates to ${config.Customer}'s ${config.listName}`
    )
  if (S3DBConfig.SelectiveDebug.indexOf("_906,") > -1)
    console.info(
      `Selective Debug 906 - JSON to be converted to XML RT Updates(${
        config.Customer
      } - ${config.listName}): ${JSON.stringify(updates)}`
    )
  if (S3DBConfig.SelectiveDebug.indexOf("_917,") > -1)
    console.info(
      `Selective Debug 917 - XML from JSON for RT Updates (${config.Customer} - ${config.listName}): ${xmlRows}`
    )

  return xmlRows
}

function convertJSONToXML_DBUpdates(updates: object[], config: customerConfig) {
  if (updates.length < 1) {
    throw new Error(
      `Exception - Convert JSON to XML for DB - No Updates (${updates.length}) were passed to process. Customer ${config.Customer} `
    )
  }

  xmlRows = `<Envelope><Body>`
  let r = 0

  try {
    for ( const upd in updates) {
      r++

      const updAtts = updates[upd]
      //const s = JSON.stringify(updAttr )

      xmlRows += `<AddRecipient><LIST_ID>${config.listId}</LIST_ID><CREATED_FROM>0</CREATED_FROM><UPDATE_IF_FOUND>true</UPDATE_IF_FOUND>`

      // If Keyed, then Column that is the key must be present in Column Set
      // If Not Keyed must add Lookup Fields
      // Use SyncFields as 'Lookup" values, Columns hold the Updates while SyncFields hold the 'lookup' values.

      //Only needed on non-keyed(In Campaign use DB -> Settings -> LookupKeys to find what fields are Lookup Keys)
      if (config.listType.toLowerCase() === "dbnonkeyed") {
        const lk = config.LookupKeys.split(",")

        xmlRows += `<SYNC_FIELDS>`
        for (let k in lk) {
          //lk.forEach( k => {
          k = k.trim()
          const sf = `<SYNC_FIELD><NAME>${k}</NAME><VALUE><![CDATA[${upd[k]}]]></VALUE></SYNC_FIELD>`
          xmlRows += sf
        } //)

        xmlRows += `</SYNC_FIELDS>`
      }

      //
      if (config.listType.toLowerCase() === "dbkeyed") {
        //Placeholder
        //Don't need to do anything with DBKey, it's superfluous but documents the keys of the keyed DB
      }

      
      type ua = keyof typeof updAtts

      for (const uv in updAtts)
      {
        const uVal: ua = uv as ua
        xmlRows += `<COLUMN><NAME>${uVal}</NAME><VALUE><![CDATA[${updAtts[uVal]}]]></VALUE></COLUMN>`

      }

      xmlRows += `</AddRecipient>`
    }
  } catch (e) {
    console.error(`Exception - ConvertJSONtoXML_DBUpdates - \n${e}`)
  }

  xmlRows += `</Body></Envelope>`

  if (s3dbLogDebug)
    console.info(
      `Converting ${r} updates to XML DB Updates. Packaging ${
        Object.values(updates).length
      } rows as updates to ${config.Customer}'s ${config.listName}`
    )
  if (S3DBConfig.SelectiveDebug.indexOf("_916,") > -1)
    console.info(
      `Selective Debug 916 - JSON to be converted to XML DB Updates: ${JSON.stringify(
        updates
      )}`
    )
  if (S3DBConfig.SelectiveDebug.indexOf("_917,") > -1)
    console.info(
      `Selective Debug 917 - XML from JSON for DB Updates: ${xmlRows}`
    )

  return xmlRows
}

function transforms(updates: object[], config: customerConfig) {
  //Apply Transforms

  //Clorox/Kingsford Weather Data
  //Add dateday column

  // Prep to add transform in Config file:
  //Get Method to apply - const method = config.transforms.method (if "dateDay") )
  //Get column to update - const column = config.transforms.methods[0].updColumn
  //Get column to reference const refColumn = config.transforms.method.refColumn

  if (config.Customer.toLowerCase().indexOf("kingsfordweather_") > -1 || config.Customer.toLowerCase().indexOf("cloroxweather_") > -1)
  {
    const t: typeof updates = []

    const days = [
      "Sunday",
      "Monday",
      "Tuesday",
      "Wednesday",
      "Thursday",
      "Friday",
      "Saturday",
    ]

    debugger

    let d: string
    for (const jo of updates)
    {
      if ("datetime" in jo)
      {
        d = jo.datetime as string
        if (d !== "")
        {
          const dt = new Date(d)
          const day = {dateday: days[dt.getDay()]}
          Object.assign(jo, day)
          t.push(jo)
        }
      }
   }
      if (t.length !== updates.length)
      {
        throw new Error(
          `Error - Transform - Applying Clorox Custom Transform returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      } else updates = t
 
  }
    //Apply the JSONMap -
    //  JSONPath statements
    //      "jsonMap": {
    //          "email": "$.uniqueRecipient",
    //              "zipcode": "$.context.traits.address.postalCode"
    //      },

    //Need failsafe test of empty object jsonMap has no transforms. 
    if (Object.keys(config.transforms.jsonMap).length > 0)
    {
      const r: typeof updates = []
      try
      {
        let jmr
        for (const jo of updates)
        {
          //const jo = JSON.parse( l )
          jmr = applyJSONMap(jo, config.transforms.jsonMap)
          r.push(jmr)
        }
      } catch (e)
      {
        console.error(`Exception - Transform - Applying JSONMap \n${e}`)
      }

      if (r.length !== updates.length)
      {
        throw new Error(
          `Error - Transform - Applying JSONMap returns fewer records (${r.length}) than initial set ${updates.length}`
        )
      } else updates = r
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

    if (Object.keys(config.transforms.csvMap).length > 0)
    {
      const c: typeof updates = []
      try
      {
        for (const jo of updates)
        {
          //const jo = JSON.parse( l )

          const map = config.transforms.csvMap as {[key: string]: string}
          Object.entries(map).forEach(([key, val]) => {

            //type dk = keyof typeof jo
            //const k: dk = ig as dk
            //delete jo[k]
            type kk = keyof typeof jo
            type vv = keyof typeof jo
            const k: kk = key as kk
            const v: vv = val as vv
            jo[k] = jo[v]

            //if (typeof v !== "number") jo[k] = jo[v] ?? ""    //Number because looking to use Column Index (column 1) as reference instead of Column Name.
            //else
            //{
            //  const vk = Object.keys(jo)[v]
            //  // const vkk = vk[v]
            //  jo[k] = jo[vk] ?? ""
            //}
          
          })
          c.push(jo)
        }
      } catch (e)
      {
        console.error(`Exception - Transforms - Applying CSVMap \n${e}`)
      }
      if (c.length !== updates.length)
      {
        throw new Error(
          `Error - Transform - Applying CSVMap returns fewer records (${c.length}) than initial set ${updates.length}`
        )
      } else updates = c
    }

    // Ignore must be last to take advantage of cleaning up any extraneous columns after previous transforms
    if (config.transforms.ignore.length > 0)
    {
      const i: typeof updates = []  //start an ignore processed update set
      try
      {
        for (const jo of updates)
        {
          //const jo = JSON.parse( l )


          for (const ig of config.transforms.ignore)
          {
          type dk = keyof typeof jo
          const k: dk = ig as dk
          delete jo[k]
          }
          i.push(jo)
        }
      } catch (e)
      {
        console.error(`Exception - Transform - Applying Ignore - \n${e}`)
      }

      if (i.length !== updates.length)
      {
        throw new Error(
          `Error - Transform - Applying Ignore returns fewer records ${i.length} than initial set ${updates.length}`
        )
      } else updates = i
    }
  
  return updates
}

function applyJSONMap(jsonObj: object, map: { [key: string]: string }) {
  // j.forEach((o: object) => {
  //     const a = applyJSONMap([o], config.transforms[0].jsonMap)
  //     am.push(a)
  //     chunks = am
  // })

  Object.entries(map).forEach(([k, v]) => {
    try {
      const j = jsonpath.value(jsonObj, v)
      if (!j) {
        if (S3DBConfig.SelectiveDebug.indexOf("_930,") > -1)
          console.warn(
            `Selective Debug 930 - Warning: Data not Found for JSONPath statement ${k}: ${v},  \nTarget Data: \n${JSON.stringify(
              jsonObj
            )} `
          )
      } else {
        Object.assign(jsonObj, { [k]: j })
      }
    } catch (e) {
      console.error(
        `Error parsing data for JSONPath statement ${k} ${v}, ${e} \nTarget Data: \n${JSON.stringify(
          jsonObj
        )} `
      )
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

async function addWorkToS3WorkBucket(queueUpdates: string, key: string) {
  if (S3DBConfig.QueueBucketQuiesce) {
    console.warn(
      `Work/Process Bucket Quiesce is in effect, no New Work Files are being written to the S3 Queue Bucket. This work file is for ${key}`
    )
    return {
      versionId: "",
      S3ProcessBucketResult: "",
      AddWorkToS3ProcessBucket: "In Quiesce",
    }
  }

  const s3WorkPutInput = {
    Body: queueUpdates,
    Bucket: S3DBConfig.S3DropBucketWorkBucket,
    Key: key,
  }

  if (s3dbLogDebug) console.info(`Write Work to S3 Process Queue for ${key}`)

  let s3ProcessBucketResult = ""
  let addWorkToS3ProcessBucket

  try {
    addWorkToS3ProcessBucket = await s3
      .send(new PutObjectCommand(s3WorkPutInput))
      .then(async (s3PutResult: PutObjectCommandOutput) => {
        if (s3PutResult.$metadata.httpStatusCode !== 200) {
          throw new Error(
            `Failed to write Work File to S3 Process Store (Result ${s3PutResult}) for ${key} of ${queueUpdates.length} characters`
          )
        }
        return s3PutResult
      })
      .catch((err) => {
        throw new Error(
          `PutObjectCommand Results Failed for (${key} of ${queueUpdates.length} characters) to S3 Processing bucket (${S3DBConfig.S3DropBucketWorkBucket}): \n${err}`
        )
        //return {StoreS3WorkException: err}
      })
  } catch (e) {
    throw new Error(
      `Exception - Put Object Command for writing work(${key} to S3 Processing bucket(${S3DBConfig.S3DropBucketWorkBucket}): \n${e}`
    )
    // return { StoreS3WorkException: e }
  }

  s3ProcessBucketResult = JSON.stringify(
    addWorkToS3ProcessBucket.$metadata.httpStatusCode,
    null,
    2
  )

  const vidString = addWorkToS3ProcessBucket.VersionId ?? ""

  if (S3DBConfig.SelectiveDebug.indexOf("_907,") > -1)
    console.info(
      `Selective Debug 907 - Added Work File ${key} to Work Bucket (${
        S3DBConfig.S3DropBucketWorkBucket
      }) \n${JSON.stringify(addWorkToS3ProcessBucket)}`
    )

  const aw3pbr = {
    versionId: vidString,
    AddWorkToS3ProcessBucket: JSON.stringify(addWorkToS3ProcessBucket),
    S3ProcessBucketResult: s3ProcessBucketResult,
  }

  return aw3pbr
}

async function addWorkToSQSWorkQueue(
  config: customerConfig,
  key: string,
  versionId: string,
  batch: number,
  recCount: string,
  marker: string
) {
  if (S3DBConfig.QueueBucketQuiesce) {
    console.warn(
      `Work/Process Bucket Quiesce is in effect, no New Work Files are being written to the SQS Queue of S3 Work Bucket. This work file is for ${key}`
    )
    return {
      versionId: "",
      S3ProcessBucketResult: "",
      AddWorkToS3ProcessBucket: "In Quiesce",
    }
  }

  const sqsQMsgBody = {} as s3dbQueueMessage
  sqsQMsgBody.workKey = key
  sqsQMsgBody.versionId = versionId
  sqsQMsgBody.marker = marker
  sqsQMsgBody.attempts = 1
  sqsQMsgBody.batchCount = batch.toString()
  sqsQMsgBody.updateCount = recCount
  sqsQMsgBody.custconfig = config
  sqsQMsgBody.lastQueued = Date.now().toString()

  const sqsParams = {
    MaxNumberOfMessages: 1,
    QueueUrl: S3DBConfig.S3DropBucketWorkQueue,
    //Defer to setting these on the Queue in AWS SQS Interface
    // VisibilityTimeout: parseInt(tcc.WorkQueueVisibilityTimeout),
    // WaitTimeSeconds: parseInt(tcc.WorkQueueWaitTimeSeconds),
    MessageAttributes: {
      FirstQueued: {
        DataType: "String",
        StringValue: Date.now().toString(),
      },
      Retry: {
        DataType: "Number",
        StringValue: "0",
      },
    },
    MessageBody: JSON.stringify(sqsQMsgBody),
  }

  let sqsSendResult
  let sqsWriteResult

  try {
    await sqsClient
      .send(new SendMessageCommand(sqsParams))
      .then((sqsSendMessageResult: SendMessageCommandOutput) => {
        sqsWriteResult = JSON.stringify(
          sqsSendMessageResult.$metadata.httpStatusCode,
          null,
          2
        )
        if (sqsWriteResult !== "200") {
          const storeQueueWorkException = `Failed writing to SQS Process Queue (queue URL: ${
            sqsParams.QueueUrl
          }), ${sqsQMsgBody.workKey}, SQS Params${JSON.stringify(sqsParams)})`

          return { StoreQueueWorkException: storeQueueWorkException }
        }
        sqsSendResult = sqsSendMessageResult

        if (S3DBConfig.SelectiveDebug.indexOf("_914,") > -1)
          console.info(
            `Selective Debug 914 - Queued Work to SQS Process Queue (${sqsQMsgBody.workKey}) - Result: ${sqsWriteResult} `
          )

        return sqsSendMessageResult
      })
      .catch((err) => {
        debugger
        const storeQueueWorkException = `Failed writing to SQS Process Queue (${err}) \nQueue URL: ${
          sqsParams.QueueUrl
        })\nWork to be Queued: ${
          sqsQMsgBody.workKey
        }\nSQS Params: ${JSON.stringify(sqsParams)}`
        console.error(
          `Failed to Write to SQS Process Queue. \n${storeQueueWorkException}`
        )
        return { StoreQueueWorkException: storeQueueWorkException }
      })
  } catch (e) {
    console.error(
      `Exception - Writing to SQS Process Queue - (queue URL${
        sqsParams.QueueUrl
      }), ${sqsQMsgBody.workKey}, SQS Params${JSON.stringify(
        sqsParams
      )}) - Error: ${e}`
    )
  }

  if (S3DBConfig.SelectiveDebug.indexOf("_907,") > -1)
    console.info(
      `Selective Debug 907 - Queued Work ${key} (${recCount} updates) to the Work Queue (${
        S3DBConfig.S3DropBucketWorkQueue
      }) \nSQS Params: \n${JSON.stringify(
        sqsParams
      )} \nresults: \n${JSON.stringify({
        SQSWriteResult: sqsWriteResult,
        AddToSQSQueue: JSON.stringify(sqsSendResult),
      })}`
    )

  return {
    SQSWriteResult: sqsWriteResult,
    AddToSQSQueue: JSON.stringify(sqsSendResult),
  }
}

async function getS3Work(s3Key: string, bucket: string) {
  if (s3dbLogDebug) console.info(`Debug - GetS3Work Key: ${s3Key}`)

  const getObjectCmd = {
    Bucket: bucket,
    Key: s3Key,
  } as GetObjectCommandInput

  
  let work: string = ""

  try {
    await s3
      .send(new GetObjectCommand(getObjectCmd))
      .then(async (getS3Result: GetObjectCommandOutput) => {
        work = (await getS3Result.Body?.transformToString("utf8")) as string
        if (s3dbLogDebug)
          console.info(`Work Pulled (${work.length} chars): ${s3Key}`)
      })
  } catch (e) {
    const err: string = JSON.stringify(e)
    if (err.toLowerCase().indexOf("nosuchkey") > -1)
      throw new Error(
        `Exception - Work Not Found on S3 Process Queue (${s3Key}. Work will not be marked for Retry. \n${e}`
      )
    else
      throw new Error(
        `Exception - Retrieving Work from S3 Process Queue for ${s3Key}.  \n ${e}`
      )
  }
  return work
}

//async function saveS3Work(s3Key: string, body: string, bucket: string) {
//  if (s3dbLogDebug) console.info(`Debug - SaveS3Work Key: ${s3Key}`)

//  const putObjectCmd = {
//    Bucket: bucket,
//    Key: s3Key,
//    Body: body,
//    // ContentLength: Number(`${body.length}`),
//  } as GetObjectCommandInput

  
//  let saveS3: string = ""

//  try {
//    await s3
//      .send(new PutObjectCommand(putObjectCmd))
//      .then(async (getS3Result: GetObjectCommandOutput) => {
//        saveS3 = (await getS3Result.Body?.transformToString("utf8")) as string
//        if (s3dbLogDebug)
//          console.info(`Work Saved (${saveS3.length} chars): ${s3Key}`)
//      })
//  } catch (e) {
//    throw new Error(`Exception - Saving Work for ${s3Key}. \n ${e}`)
//  }
//  return saveS3
//}

async function deleteS3Object(s3ObjKey: string, bucket: string) {
  let delRes = ""

  const s3D = {
    Key: s3ObjKey,
    Bucket: bucket,
  }

  const d = new DeleteObjectCommand(s3D)

  try {
    await s3
      .send(d)
      .then(async (s3DelResult: DeleteObjectCommandOutput) => {
        delRes = JSON.stringify(s3DelResult.$metadata.httpStatusCode, null, 2)
      })
      .catch((e) => {
        console.error(
          `Exception - Attempting S3 Delete Command for ${s3ObjKey}: \n ${e} `
        )
        return delRes
      })
  } catch (e) {
    console.error(
      `Exception - Attempting S3 Delete Command for ${s3ObjKey}: \n ${e} `
    )
  }
  return delRes
}

export async function getAccessToken(config: customerConfig) {
  try {
    const rat = await fetch(
      `https://api-campaign-${config.region}-${config.pod}.goacoustic.com/oauth/token`,
      {
        method: "POST",
        body: new URLSearchParams({
          refresh_token: config.refreshToken,
          client_id: config.clientId,
          client_secret: config.clientSecret,
          grant_type: "refresh_token",
        }),
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          "User-Agent": "S3DropBucket GetAccessToken",
        },
      }
    )

    const ratResp = (await rat.json()) as accessResp

    if (rat.status != 200) {
      const err = ratResp as unknown as {
        error: string
        error_description: string
      }
      console.error(
        `Problem retrieving Access Token (${rat.status}) Error: ${err.error} \nDescription: ${err.error_description}`
      )
      //  {
      //  error: "invalid_client",
      //  error_description: "Unable to find matching client for 1d99f8d8-0897-4090-983a-c517cc54032e",
      //  }

      throw new Error(
        `Problem - Retrieving Access Token:   ${rat.status} - ${err.error}  - \n${err.error_description}`
      )
    }
    const accessToken = ratResp.access_token
    return { accessToken }.accessToken
  } catch (e) {
    throw new Error(`Exception - On GetAccessToken: \n ${e}`)
  }
}

export async function postToCampaign(
  xmlCalls: string,
  config: customerConfig,
  count: string
) {
  const c = config.Customer

  //Store AccessToken in process.env vars for reference across invocations, save requesting it repeatedly
  if (
    process.env[`${c}_accessToken`] === undefined ||
    process.env[`${c}_accessToken`] === null ||
    process.env[`${c}_accessToken`] === ""
  ) {
    process.env[`${c}_accessToken`] = (await getAccessToken(config)) as string
    const at = process.env[`${c}_accessToken"`] ?? ""
    const l = at.length
    const redactAT = "......." + at.substring(l - 10, l)
    if (s3dbLogDebug) console.info(`Generated a new AccessToken: ${redactAT}`)
  } else {
    const at = process.env["accessToken"] ?? ""
    const l = at.length
    const redactAT = "......." + at.substring(l - 8, l)
    if (s3dbLogDebug) console.info(`Access Token already stored: ${redactAT}`)
  }

  const myHeaders = new Headers()
  myHeaders.append("Content-Type", "text/xml")
  myHeaders.append("Authorization", "Bearer " + process.env[`${c}_accessToken`])
  myHeaders.append("Content-Type", "text/xml")
  myHeaders.append("Connection", "keep-alive")
  myHeaders.append("Accept", "*/*")
  myHeaders.append("Accept-Encoding", "gzip, deflate, br")

  const requestOptions: RequestInit = {
    method: "POST",
    headers: myHeaders,
    body: xmlCalls,
    redirect: "follow",
  }

  const host = `https://api-campaign-${config.region}-${config.pod}.goacoustic.com/XMLAPI`

  if (S3DBConfig.SelectiveDebug.indexOf("_905,") > -1)
    console.info(`Selective Debug 905 - Updates to POST are: ${xmlCalls}`)

  
  let postRes: string = ""

  // try
  // {
  postRes = await fetch(host, requestOptions)
    .then((response) => response.text())
    .then(async (result) => {
      // console.error(`Debug POST Response: ${result}`)
      const faults: string[] = []

      //const f = result.split( /<FaultString><!\[CDATA\[(.*)\]\]/g )

      //Add this fail
      //<RESULT>
      //    <SUCCESS>false</SUCCESS>
      //    < /RESULT>
      //    < Fault >
      //    <Request/>
      //    < FaultCode />
      //    <FaultString> <![ CDATA[ Local part of Email Address is Blocked.]]> </FaultString>
      //        < detail >
      //        <error>
      //        <errorid> 121 < /errorid>
      //        < module />
      //        <class> SP.Recipients < /class>
      //        < method />
      //        </error>
      //        < /detail>
      //        < /Fault>

      if (
        result.toLowerCase().indexOf("max number of concurrent") > -1 ||
        result.toLowerCase().indexOf("access token has expired") > -1 ||
        result.toLowerCase().indexOf("Error saving row") > -1
      ) {
        console.warn(
          `Temporary Failure - POST Updates - Marked for Retry. \n${result}`
        )
        return "retry"
      } else if (result.indexOf("<FaultString><![CDATA[") > -1) {
        const f = result.split(/<FaultString><!\[CDATA\[(.*)\]\]/g)
        if (f && f?.length > 0) {
          for (const fl in f) {
            faults.push(f[fl])
          }
        }
        debugger
        console.warn(
          `Partially Successful POST of the Updates (${
            f.length
          } FaultStrings on ${count} updates) - \nResults\n ${JSON.stringify(
            faults
          )}`
        )
        return `Partially Successful - (${
          f.length
        } FaultStrings on ${count} updates) \n${JSON.stringify(faults)}`
      }

      //Add this Fail
      //    //<SUCCESS> true < /SUCCESS>
      //    //    < FAILURES >
      //    //    <FAILURE failure_type="permanent" description = "There is no column registeredAdvisorTitle" >
      //    const m = result.match( /<FAILURE failure_(.*)"/gm )
      else if (result.indexOf("<FAILURE  failure_type") > -1) {
        let msg = ""

        const m = result.match(/<FAILURE (.*)>$/g)

        if (m && m?.length > 0) {
          for (const l in m) {
            // "<FAILURE failure_type=\"permanent\" description=\"There is no column name\">"
            //Actual message is ambiguous, changing it to read less confusingly:
            l.replace("There is no column ", "There is no column named ")
            msg += l
          }

          console.error(
            `Unsuccessful POST of the Updates (${
              m.length
            } of ${count}) - \nFailure Msg: ${JSON.stringify(msg)}`
          )
          return `Error - Unsuccessful POST of the Updates (${
            m.length
          } of ${count}) - \nFailure Msg: ${JSON.stringify(msg)}`
        }
      }

      result = result.replace("\n", " ")
      return `Successfully POSTed (${count}) Updates - Result: ${result}`
    })
    .catch((e) => {
      //console.error( `Error - Temporary failure to POST the Updates - Marked for Retry. ${ e }` )
      if (e.indexOf("econnreset") > -1) {
        console.error(
          `Error - Temporary failure to POST the Updates - Marked for Retry. ${e}`
        )
        return "retry"
      } else {
        console.error(`Error - Unsuccessful POST of the Updates: ${e}`)
        //throw new Error( `Exception - Unsuccessful POST of the Updates \n${ e }` )
        return "Unsuccessful POST of the Updates"
      }
    })

  //retry
  //unsuccessful post
  //partially successful
  //successfully posted

  return postRes
}

async function postToConnect(updates: string, custconfig: customerConfig, updateCount: string) {
  //get access token
  //post to connect
  //return result
  console.info(`${updates}, ${custconfig}, ${updateCount}`)

  return "postToConnect"
}






//function checkMetadata() {
//  //Pull metadata for table/db defined in config
//  // confirm updates match Columns
//  //ToDo:  Log where Columns are not matching
//}

async function getAnS3ObjectforTesting(bucket: string) {
  let s3Key: string = ""

  if (testS3Key !== null) return

  const listReq = {
    Bucket: bucket,
    MaxKeys: 101,
    Prefix: S3DBConfig.prefixFocus,
  } as ListObjectsV2CommandInput

  await s3
    .send(new ListObjectsV2Command(listReq))
    .then(async (s3ListResult: ListObjectsV2CommandOutput) => {
      let i: number = 0

      if (s3ListResult.Contents) {
        const kc: number = (s3ListResult.KeyCount as number) - 1
        if ((kc == 0))
          throw new Error("No S3 Objects to retrieve as Test Data, exiting")

        if (kc > 10) {
          i = Math.floor(Math.random() * (10 - 1 + 1) + 1)
        }
        if ((kc == 1)) i = 0
        s3Key = s3ListResult.Contents?.at(i)?.Key as string

        while (s3Key.toLowerCase().indexOf("aggregat") > -1) {
          i++
          s3Key = s3ListResult.Contents?.at(i)?.Key as string
        }

        // console.info(`S3 List: \n${ JSON.stringify(s3ListResult.Contents) } `)
        // if (tcLogDebug)
        console.info(`TestRun(${i}) Retrieved ${s3Key} for this Test Run`)
      } else throw new Error(`No S3 Object available for Testing: ${bucket} `)

      return s3Key
    })
    .catch((e) => {
      console.error(
        `Exception - On S3 List Command for Testing Objects from ${bucket}: ${e} `
      )
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

//function saveSampleJSON(body: string) {
//  const path = "Saved/"
//  saveS3Work(
//    `${path}sampleJSON_${Date.now().toString()}.json`,
//    body,
//    S3DBConfig.S3DropBucketConfigs
//  )
//  // s3://s3dropbucket-configs/Saved/
//}

//async function purgeBucket(count: number, bucket: string) {
//  const listReq = {
//    Bucket: bucket,
//    MaxKeys: count,
//  } as ListObjectsV2CommandInput

//  let d = 0
//  let r = ""
//  try {
//    await s3
//      .send(new ListObjectsV2Command(listReq))
//      .then(async (s3ListResult: ListObjectsV2CommandOutput) => {
//        s3ListResult.Contents?.forEach(async (listItem) => {
//          d++
//          r = await deleteS3Object(listItem.Key as string, bucket)
//          if (r !== "204")
//            console.error(
//              `Non Successful return (Expected 204 but received ${r} ) on Delete of ${listItem.Key} `
//            )
//        })
//      })
//  } catch (e) {
//    console.error(`Exception - Attempting Purge of Bucket ${bucket}: \n${e} `)
//  }
//  console.info(`Deleted ${d} Objects from ${bucket} `)
//  return `Deleted ${d} Objects from ${bucket} `
//}

//async function maintainS3DropBucket(cust: customerConfig) {
  
//  cust.LookupKeys //future
  
//  const bucket = S3DBConfig.S3DropBucket
//  let limit = 0
//  if (bucket.indexOf("-process") > -1)
//    limit = S3DBConfig.S3DropBucketWorkQueueMaintLimit
//  else limit = S3DBConfig.S3DropBucketMaintLimit

//  let ContinuationToken: string | undefined
//  const reProcess: string[] = []
//  //const deleteSource = false
//  const concurrency = S3DBConfig.S3DropBucketMaintConcurrency

//  const copyFile = async (sourceKey: string) => {
//    // const targetKey = sourceKey.replace(sourcePrefix, targetPrefix)

//    await s3
//      .send(
//        new CopyObjectCommand({
//          Bucket: bucket,
//          Key: sourceKey,
//          CopySource: `${bucket}/${sourceKey}`,
//          MetadataDirective: "COPY",
//          // CopySourceIfUnmodifiedSince: dd
//        })
//      )
//      .then((res) => {
//        return res
//      })
//      .catch((err) => {
//        // console.error(`Error - Maintain S3DropBucket - Copy of ${sourceKey} \n${e}`)
//        reProcess.push(
//          `Copy Error on ${sourceKey}  -->  \n${JSON.stringify(err)}`
//        )
//        if (err !== undefined && err !== "" && err.indexOf("NoSuchKey") > -1)
//          reProcess.push(
//            `S3DropBucket Maintenance - Reprocess Error - MaintainS3DropBucket - File Not Found(${sourceKey} \nException ${err} `
//          )
//        else
//          reProcess.push(
//            `S3DropBucket Maintenance - Reprocess Error for ${sourceKey} --> \n${JSON.stringify(
//              err
//            )}`
//          )
//      })

//    // if ( deleteSource )
//    // {
//    //     await s3.send(
//    //         new DeleteObjectCommand( {
//    //             Bucket: bucket,
//    //             Key: sourceKey,
//    //         } ),
//    //     )
//    //         .then( ( res ) => {
//    //             // console.info(`${JSON.stringify(res)}`)
//    //             reProcess.push( `Delete of ${ sourceKey }  -->  \n${ JSON.stringify( res ) }` )
//    //         } )
//    //         .catch( ( e ) => {
//    //             console.error( `Error - Maintain S3DropBucket - Delete of ${ sourceKey } \n${ e }` )
//    //             reProcess.push( `Delete Error on ${ sourceKey }  -->  \n${ JSON.stringify( e ) }` )
//    //         } )
//    // }

//    return reProcess
//  }

//  const d: Date = new Date()
//  // 3,600,000 millisecs = 1 hour
//  const a = 3600000 * S3DBConfig.S3DropBucketMaintHours //Older Than X Hours

//  do {
//    const { Contents = [], NextContinuationToken } = await s3.send(
//      new ListObjectsV2Command({
//        Bucket: bucket,
//        //Prefix: cust.Customer,
//        ContinuationToken,
//        //MaxKeys: limit,
//      })
//    )

//    const lastMod = Contents.map(({ LastModified }) => LastModified as Date)
//    const sourceKeys = Contents.map(({ Key }) => Key)

//    await Promise.all(
//      new Array(concurrency).fill(null).map(async () => {
//        while (sourceKeys.length) {
//          const key = sourceKeys.pop() ?? ""
//          const mod = lastMod.pop() as Date

//          const s3d: Date = new Date(mod)
//          const df = d.getTime() - s3d.getTime()
//          // const dd = new Date(s3d.setHours(-tcc.S3DropBucketMaintHours))

//          if (df > a) {
//            const rp = await copyFile(key)
//            reProcess.push(`Copy of ${key}  -->  \n${JSON.stringify(rp)}`)
//            if (reProcess.length >= limit) ContinuationToken = ""
//          }
//        }
//      })
//    )

//    ContinuationToken = NextContinuationToken ?? ""
//  } while (ContinuationToken)

//  console.info(
//    `S3DropBucketMaintenance - Copy Log: ${JSON.stringify(reProcess)}`
//  )

//  const l = reProcess.length
//  return [l, reProcess]
//}

//async function maintainS3DropBucketQueueBucket() {
//  const bucket = S3DBConfig.S3DropBucketWorkBucket
//  let ContinuationToken: string | undefined
//  const reQueue: string[] = []
//  // let deleteSource = false
//  const concurrency = S3DBConfig.S3DropBucketWorkQueueMaintConcurrency

//  // if ( new Date().getTime() > 0 ) return

//  const d: Date = new Date()
//  // 3,600,000 millisecs = 1 hour
//  const age = 3600000 * S3DBConfig.S3DropBucketMaintHours //Older Than X Hours

//  do {
//    const { Contents = [], NextContinuationToken } = await s3.send(
//      new ListObjectsV2Command({
//        Bucket: bucket,
//        //Prefix: config.Customer,   //Need some option to limit millions of records
//        ContinuationToken,
//      })
//    )

//    const lastMod = Contents.map(({ LastModified }) => LastModified as Date)
//    const sourceKeys = Contents.map(({ Key }) => Key)

//    console.info(
//      `S3DropBucket Maintenance - Processing ${Contents.length} records`
//    )

//    await Promise.all(
//      new Array(concurrency).fill(null).map(async () => {
//        while (sourceKeys.length) {
//          const key = sourceKeys.pop() ?? ""
//          const mod = lastMod.pop() as Date

//          const cc = await getCustomerConfig(key)

//          const s3d: Date = new Date(mod)
//          const tdf = d.getTime() - s3d.getTime()
//          // const dd = new Date(s3d.setHours(-tcc.S3DropBucketMaintHours))

//          if (tdf > age) {
//            let batch = ""
//            let updates = ""

//            const r1 = new RegExp(/json_update_(.*)_/g)
//            let rm = r1.exec(key) ?? ""
//            batch = rm[1]

//            const r2 = new RegExp(/json_update_.*?_(.*)\./g)
//            rm = r2.exec(key) ?? ""
//            updates = rm[1]

//            const marker = "ReQueued on: " + new Date()
//            try {
//              //build and write SQS Entry
//              const qa = await addWorkToSQSWorkQueue(
//                cc,
//                key,
//                "",
//                parseInt(batch),
//                updates,
//                marker
//              )
//              console.info(
//                `Return from Maintenance - AddWorkToSQSWorkQueue: ${JSON.stringify(
//                  qa
//                )}`
//              )
//            } catch (e) {
//              debugger
//            }
//          }
//        }
//      })
//    )

//    ContinuationToken = NextContinuationToken ?? ""
//  } while (ContinuationToken)

//  // const listReq = {
//  //     Bucket: bucket,
//  //     MaxKeys: 1000,
//  //     Prefix: `config.Customer`,
//  //     ContinuationToken,
//  // } as ListObjectsV2CommandInput

//  // await s3.send( new ListObjectsV2Command( listReq ) )
//  //     .then( async ( s3ListResult: ListObjectsV2CommandOutput ) => {

//  //         // 3,600,000 millisecs = 1 hour
//  //         const a = 3600000 * tcc.S3DropBucketWorkQueueMaintHours  //Older Than X Hours

//  //         const d: Date = new Date()

//  //         try
//  //         {
//  //             for ( const o in s3ListResult.Contents )
//  //             {
//  //                 const n = parseInt( o )
//  //                 const s3d: Date = new Date( s3ListResult.Contents[ n ].LastModified ?? new Date() )
//  //                 const df = d.getTime() - s3d.getTime()
//  //                 const dd = s3d.setHours( -tcc.S3DropBucketMaintHours )

//  //                 let rm: string[] = []
//  //                 if ( df > a )
//  //                 {

//  //                     const obj = s3ListResult.Contents[ n ]
//  //                     const key = obj.Key ?? ""
//  //                     let versionId = ''
//  //                     let batch = ''
//  //                     let updates = ''

//  //                     const r1 = new RegExp( /json_update_(.*)_/g )
//  //                     let rm = r1.exec( key ) ?? ""
//  //                     batch = rm[ 1 ]

//  //                     const r2 = new RegExp( /json_update_.*?_(.*)\./g )
//  //                     rm = r2.exec( key ) ?? ""
//  //                     updates = rm[ 1 ]

//  //                     const marker = "ReQueued on: " + new Date()

//  //                     debugger

//  //                     //build SQS Entry
//  //                     const qa = await addWorkToSQSWorkQueue( config, key, versionId, batch, updates, marker )

//  //                     reQueue.push( "ReQueue Work" + key + " --> " + JSON.stringify( qa ) )

//  //                 }
//  //             }
//  //         } catch ( e )
//  //         {
//  //             throw new Error( `Exception - Maintain S3DropBucket - List Results processing - \n${ e }` )
//  //         }
//  //     } )
//  //     .catch( ( e ) => {
//  //         console.error( `Exception - On S3 List Command for Maintaining Objects on ${ bucket }: ${ e } ` )
//  //     } )

//  debugger

//  const l = reQueue.length
//  return [l, reQueue]
//}
