/* eslint-disable no-debugger */
"use strict"

/*
| description : S3DropBucket - Process data from files dropped onto S3 Bucket(s) into Campaign/Connect 
| version  :  3.3.60
| author   :  KW Landry (kip.landry@acoustic.co)
| copyright:  (c) 2024 by ISC
| created  :  11/24/2024 18:35:06
| updated  :  12/19/2024 18:35:06
+---------------------------------------------------------------------------- */

import {version, builddate, description} from '../../package.json'

process.env["S3DropBucketPackageVersion"] = version
process.env["S3DropBucketPackageBuildDate"] = builddate
process.env["S3DropBucketPackageDescription"] = description


//const s3dbVersion = `S3DropBucket Version: 3.3.33 ( ${new Date().toUTCString()} )`
console.info(`S3DB Version: ${version} from Build:  ${builddate} \nDescription: ${description}`)


//ToDo: refactor: break out each lambda function and common functions into separate modules
//ToDo: Create Tests 

import {
  S3Client,
  PutObjectCommand,
  ListObjectsCommand,
  ListObjectsCommandInput,
  ListObjectsCommandOutput,
  GetObjectCommand,
  DeleteObjectCommand,
  PutObjectCommandOutput,
  GetObjectCommandOutput,
  GetObjectCommandInput,
  DeleteObjectCommandOutput,
  InventoryConfigurationFilterSensitiveLog
} from "@aws-sdk/client-s3"

import {NodeHttpHandler} from "@smithy/node-http-handler"
import https from "https";  


import {
  DeleteMessageCommand,
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

//import {transform} from '@streamparser/json-node'
import {transform} from "stream-transform"

import {v4 as uuidv4} from "uuid"

import {parse} from "csv-parse"

import jsonpath from "jsonpath"

import {Ajv} from "ajv"  //Ajv JSON schema validator

import sftpClient, {ListFilterFunction} from "ssh2-sftp-client"
import {config} from 'process'



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
//import { keyBy } from "lodash"
//import { setUncaughtExceptionCaptureCallback } from "process"
//import { type LargeNumberLike } from "crypto"

const awsRegion = process.env.AWS_REGION
const envRegion = process.env.S3DropBucketRegion


let smithyReqHandler = new NodeHttpHandler({      //currently using only on S3 Client
  socketAcquisitionWarningTimeout: 5_000,
  requestTimeout: 3_000,
  httpsAgent: new https.Agent({
    keepAlive: true,
    maxSockets: 100
  })
})



let s3 = {} as S3Client

const fh_Client = new FirehoseClient({region: process.env.S3DropBucketRegion})

const SFTPClient = new sftpClient()


//For when needed to reference Lambda execution environment /tmp folder
// import { ReadStream, close } from 'fs'

const sqsClient = new SQSClient({region: process.env.S3DropBucketRegion})

export type sqsObject = {
  bucketName: string
  objectKey: string
}



let localTesting = false

let chunksGlobal: object[]

let xmlRows = ""
let mutationRows = ""
let batchCount = 0
let recs = 0

export interface MasterCustomerConfig {
  $schema: string
  selectivelogging: string
  Customer: string
  format: string
  separator: string
  updates: "Multiple" | "Singular"
  updatemaxrows: number
  pod: string
  region: string
  targetupdate: string
  updatetype: string
  listid: string
  listname: string
  clientid: string
  clientsecret: string
  refreshtoken: string
  datasetid: string
  subscriptionid: string
  x_api_key: string
  x_acoustic_region: string
  transforms: {
    contactid: string
    contactkey: string
    addressablefields: {
      statement: {
        addressable: Array<{
          field: string
          eq: string
        }>
      }
    }
    consent: {
      statement: {
        channels: Array<{
          channel: string
          status: string
        }>
      } | {[key: string]: string}
    }
    audienceupdate: {
      [key: string]: string
    }
    methods: {
      daydate: string
      date_iso1806_type: {
        [key: string]: string
      }
      phone_number_type: {
        [key: string]: string
      }
      string_to_number_type: {
        [key: string]: string
      }
    }
    jsonmap: {
      [key: string]: string
    }
    csvmap: {
      [key: string]: string
    }
    ignore: string[]
  }
}

export interface CustomerConfig {
  customer: string
  format: string      // CSV or JSON
  separator: string
  updates: string     // singular or Multiple (default)
  targetupdate: string
  listid: string
  listname: string
  updatetype: string
  dbkey: string
  lookupkeys: []      //ToDo: Changeup to an Array-Strings per Customer Config Schema
  updateKey: string
  pod: string             // 1,2,3,4,5,6,7,8,9,A,B
  region: string          // US, EU, AP
  updatemaxrows: number   //Safety to avoid run away data inbound and parsing it all
  refreshtoken: string    // API Access
  clientid: string        // API Access
  clientsecret: string    // API Access
  datasetid: string
  subscriptionid: string
  x_api_key: string
  x_acoustic_region: string
  selectivelogging: string
  sftp: {
    user: string
    password: string
    filepattern: string
    schedule: string
  }
  transforms: {
    contactid: string
    contactkey: string
    addressablefields: {[key: string]: string}
    consent: {[key: string]: string}
    audienceupdate: {[key: string]: string}
    methods: {
      daydate: string       //ToDo: refactor as multiple properties config
      date_iso1806_type: {[key: string]: string}
      phone_number_type: {[key: string]: string}
      string_to_number_type: {[key: string]: string}
      method2: {[key: string]: string}
    }
    jsonmap: {[key: string]: string}
    csvmap: {[key: string]: string}
    ignore: string[]
  }
}

let customersConfig: CustomerConfig = {
  customer: "",
  format: "",
  separator: "",
  updates: "",
  targetupdate: "",
  listid: "",
  listname: "",
  updatetype: "",
  dbkey: "",
  lookupkeys: [],
  updateKey: "",
  pod: "",
  region: "",
  updatemaxrows: 0,
  refreshtoken: "",
  clientid: "",
  clientsecret: "",
  datasetid: "",
  subscriptionid: "",
  x_api_key: "",
  x_acoustic_region: "",
  selectivelogging: "",
  sftp: {
    user: "",
    password: "",
    filepattern: "",
    schedule: ""
  },
  transforms: {
    contactid: "",
    contactkey: "",
    addressablefields: {"": ""},
    consent: {"": ""},
    audienceupdate: {"": ""},
    methods: {
      "daydate": "",
      "date_iso1806_type": {"": ""},
      "phone_number_type": {"": ""},
      "string_to_number_type": {"": ""},
      "method2": {"": ""}
    },
    jsonmap: {},
    csvmap: {},
    ignore: []   //always last
  }
}

export interface AccessRequest {
  access_token: string
  token_type: string
  refresh_token: string
  expires_in: number
}

export interface S3DBQueueMessage {
  workKey: string
  versionId: string
  marker: string
  attempts: number
  batchCount: string
  updateCount: string
  custconfig: CustomerConfig
  lastQueued: string
}

export interface S3DBConfig {
  aws_region: string
  connectapiurl: string
  xmlapiurl: string
  restapiurl: string
  authapiurl: string
  s3dropbucket: string
  s3dropbucket_workbucket: string
  s3dropbucket_workqueue: string
  s3dropbucket_firehosestream: string
  s3dropbucket_loglevel: string
  s3dropbucket_log: boolean //future: firehose aggregator bucket
  s3dropbucket_logbucket: string //future: firehose aggregator bucket
  s3dropbucket_configs: string
  s3dropbucket_jsonseparator: string
  s3dropbucket_maxbatcheswarning: number
  s3dropbucket_selectivelogging: string
  s3dropbucket_workqueuequiesce: boolean
  s3dropbucket_prefixfocus: string
  s3dropbucket_eventemittermaxlisteners: number
  s3dropbucket_quiesce: boolean
  s3dropbucket_mainthours: number
  s3dropbucket_maintlimit: number
  s3dropbucket_maintconcurrency: number
  s3dropbucket_workqueuemainthours: number
  s3dropbucket_workqueuemaintlimit: number
  s3dropbucket_workqueuemaintconcurrency: number
  s3dropbucket_purgecount: number
  s3dropbucket_purge: string
  s3dropbucket_queuebucketquiesce: boolean
  s3dropbucket_workqueuebucketpurgecount: number
  s3dropbucket_workqueuebucketpurge: string
  s3dropbucket_sftp: boolean
}


interface S3DropBucketConfig {
  $schema: string
  aws_region: string
  xmlapiurl: string
  restapiurl: string
  authapiurl: string
  connectapiurl: string

  // Logging configuration
  s3dropbucket_logbucket: string
  s3dropbucket_log: 'CloudWatch' | 'S3'

  // Bucket configurations
  s3dropbucket: string
  s3dropbucket_workbucket: string
  s3dropbucket_configs: string

  // Queue configuration
  s3dropbucket_workqueue: string

  // Firehose configuration
  s3dropbucket_firehosestream: string

  // Quiesce settings
  s3dropbucket_quiesce: boolean
  s3dropbucket_workqueuequiesce: boolean
  s3dropbucket_queuebucketquiesce: boolean

  // Maintenance settings for incoming files
  s3dropbucket_mainthours: number
  s3dropbucket_maintlimit: number
  s3dropbucket_maintconcurrency: number

  // Work queue maintenance settings
  s3dropbucket_workqueuemainthours: number
  s3dropbucket_workqueuemaintlimit: number
  s3dropbucket_workqueuemaintconcurrency: number

  // Optional properties that might be present in the config
  s3dropbucket_selectivelogging?: string
  s3dropbucket_loglevel?: 'ALL' | 'NONE' | 'NORMAL'
  s3dropbucket_purgecount?: number
  s3dropbucket_purge?: string
  s3dropbucket_workqueuebucketpurgecount?: number
  s3dropbucket_workqueuebucketpurge?: string
  s3dropbucket_sftp?: boolean
  s3dropbucket_jsonseparator?: string
  s3dropbucket_eventemittermaxlisteners?: number
  s3dropbucket_maxbatcheswarning?: number
  s3dropbucket_prefixfocus?: string
}



let S3DBConfig = {} as S3DBConfig

export interface SQSBatchItemFails {
  batchItemFailures: [
    {
      itemIdentifier: string
    }
  ]
}

interface AddWorkToS3Results {
  versionId: string
  S3ProcessBucketResultStatus: string
  AddWorkToS3ProcessBucket: string
}

interface SQSResults {
  SQSWriteResultStatus: string
  AddToSQSQueue: string
}

interface StoreAndQueueWorkResults {
  AddWorkToS3WorkBucketResults: AddWorkToS3Results
  AddWorkToSQSWorkQueueResults: SQSResults
  PutToFireHoseAggregatorResult: string
  PutToFireHoseAggregatorResultDetails: string
  PutToFireHoseException: string
}

interface StoreAndQueueDataWorkResults {
  AddWorkToS3WorkBucketResults: AddWorkToS3Results
  AddWorkToSQSWorkQueueResults: SQSResults
}


interface ProcessS3ObjectStreamResult {
  Key: string
  Processed: string
  ReadStreamEndResult: string
  ReadStreamException: string
  OnEndRecordStatus: string
  OnDataReadStreamException: string
  OnEndNoRecordsException: string
  ProcessS3ObjectStreamCatch: string
  OnClose_Result: string
  StreamReturnLocation: string
  OnDataResult: {
    StoreAndQueueWorkResult: StoreAndQueueDataWorkResults
  }
  OnEndStreamEndResult: {
    StoreAndQueueWorkResult: StoreAndQueueWorkResults
  }
  DeleteResult: string
}

let ProcessS3ObjectStreamOutcome: ProcessS3ObjectStreamResult = {
  Key: '',
  Processed: '',
  ReadStreamEndResult: '',
  ReadStreamException: '',
  OnEndRecordStatus: '',
  OnDataReadStreamException: '',
  OnEndNoRecordsException: '',
  ProcessS3ObjectStreamCatch: '',
  OnClose_Result: '',
  StreamReturnLocation: '',
  OnDataResult: {
    StoreAndQueueWorkResult: {
      AddWorkToS3WorkBucketResults: {
        versionId: '',
        S3ProcessBucketResultStatus: '',
        AddWorkToS3ProcessBucket: ''
      },
      AddWorkToSQSWorkQueueResults: {
        SQSWriteResultStatus: '',
        AddToSQSQueue: ''
      }
    }
  },
  OnEndStreamEndResult: {
    StoreAndQueueWorkResult: {
      AddWorkToS3WorkBucketResults: {
        versionId: '',
        S3ProcessBucketResultStatus: '',
        AddWorkToS3ProcessBucket: ''
      },
      AddWorkToSQSWorkQueueResults: {
        SQSWriteResultStatus: '',
        AddToSQSQueue: ''
      },
      PutToFireHoseAggregatorResult: '',
      PutToFireHoseAggregatorResultDetails: '',
      PutToFireHoseException: ''
    }
  },
  DeleteResult: ''
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

//let s3dbLogDebug = false
//let s3dbLogVerbose = false
//let s3dbLogNormal = false

//For local testing
let testS3Key: string
let testS3Bucket: string
testS3Bucket = "s3dropbucket-configs"
//testS3Bucket = "s3dropbucket-process"

const testdata = ""

// testS3Key = "TestData/visualcrossing_00213.csv"
// testS3Key = "TestData/pura_2024_02_25T00_00_00_090Z.json"

//testS3Key = "TestData/pura_S3DropBucket_Aggregator-8-2024-03-23-09-23-55-123cb0f9-9552-3303-a451-a65dca81d3c4_json_update_53_99.xml"
//  Core - Key Set of Test Datasets
//testS3Key = "TestData/cloroxweather_99706.csv"
//testS3Key = "TestData/pura_S3DropBucket_Aggregator-8-2024-03-19-16-42-48-46e884aa-8c6a-3ff9-8d32-c329395cf311.json"
//testS3Key = "TestData/pura_2024_02_26T05_53_26_084Z.json"
//testS3Key = "TestData/alerusreassignrepsignature_advisors.json"
//testS3Key = "TestData/Funding_Circle_Limited_CampaignDatabase1_2024_11_12T11_20_56_317Z.json"
//testS3Key = "TestData/Funding_Circle_Limited_CampaignDatabase1_2024_11_28T22_16_03_400Z_json-update-1-1-0a147575-2123-44ff-a7bf-d12b0a0d839f.xml"
//testS3Key = "TestData/Funding_Circle_Limited_CampaignRelationalTable1_2024_10_08T10_16_49_700Z.json"
//testS3Key = "TestData/Funding_Circle_Limited_CampaignDatabase1_2025_02_04T16_06_39_169Z.json"
//testS3Key = "TestData/Funding_Circle_Limited_CampaignRelationalTable1_2025_02_04T16_11_50_592Z.json"
//testS3Key = "TestData/Funding_Circle_Limited_CampaignDatabase1_2024_10_08T09_52_13_903Z.json"

//testS3Key = "TestData/Funding_Circle_Limited_CampaignRelationalTable1_S3DropBucket_Aggregator-2-2025-02-04-16-03-43-f73b015e-43f7-3d2d-8223-dc2a91a89222.json"

//testS3Key = "TestData/alerusrepsignature_advisors.json"
//testS3Key = "TestData/alerusreassignrepsignature_advisors.json"
//testS3Key = "TestData/KingsfordWeather_00210.csv"
//testS3Key = "TestData/KingsfordWeather_00211.csv"

//testS3Key = "TestData/MasterCustomer_Sample1.json"
//testS3Key = "MasterCustomer_Sample1-json-update-1-6-c436dca1-6ec9-4c8b-bc78-6e1d774591ca.json"
//testS3Key = "MasterCustomer_Sample1-json-update-1-6-0bb2f92e-3344-41e6-af95-90f5d32a73dc.json"

//testS3Key = "TestData/KingsfordWeather_S3DropBucket_Aggregator-10-2025-01-09-19-29-39-da334f11-53a4-31cc-8c9f-8b417725560b.json"
//testS3Key = "TestData/Funding_Circle_Limited_CampaignDatabase1_2025_02_28T19_19_26_268Z.json"
//testS3Key = "TestData/alerusrepsignature_advisors-mar232025.json"
testS3Key = "TestData/MasterCustomer_Sample-mar232025.json"
//testS3Key = "TestData/KingsfordWeather_S3DropBucket_Aggregator-10-2025-03-26-09-10-22-dab1ccdf-adbc-339d-8993-b41d27696a3d.json"
//testS3Key = "TestData/MasterCustomer_Sample-mar232025-json-update-10-25-000d4919-2865-49fa-a5ac-32999a583f0a.json"
//testS3Key = "TestData/MasterCustomer_Sample-Queued-json-update-10-25-000d4919-2865-49fa-a5ac-32999a583f0a.json"





/**
 * A Lambda function to process the Event payload received from S3.
 */

export const s3DropBucketHandler: Handler = async (
  event: S3Event,
  context: Context
) => {

  try
  {

    //Ignore Aggregation Error Files created by FireHose process
    if (event.Records[0].s3.object.key.toLowerCase().indexOf("aggregationerror") > -1) return ""
    
    //Ignore any Metadata Files created
    if (event.Records[0].s3.object.key.toLowerCase().indexOf("metadata") > -1) return ""

    //If Local Testing - set up to pull an S3 Object and so avoid the not-found error
    if (
      typeof event.Records[0].s3.object.key !== "undefined" &&
      event.Records[0].s3.object.key === "devtest.csv"
    )
    {
      if (
        testS3Key !== undefined &&
        testS3Key !== null &&
        testS3Bucket !== undefined &&
        testS3Bucket !== null
      )
      {
        event.Records[0].s3.object.key = testS3Key
        event.Records[0].s3.bucket.name = testS3Bucket
      } else
      {
        event.Records[0].s3.object.key =
          (await getAnS3ObjectforTesting(event.Records[0].s3.bucket.name)) ?? ""
      }
      localTesting = true
    } else
    {
      testS3Key = ""
      testS3Bucket = ""
      localTesting = false
    }
    
    
    if (typeof process.env.AWS_REGION !== "undefined") 
    {
      s3 = new S3Client({
        region: process.env.AWS_REGION,         //{region: 'us-east-1'})
        requestHandler: smithyReqHandler
      })
      
      //const client = new SQSClient({
      //  credentials: {accessKeyId, secretAccessKey},
      //  endpoint,
      //  region,
      //  requestHandler: new NodeHttpHandler({
      //    connectionTimeout,
      //    socketTimeout,
      //    httpAgent: new http.Agent({
      //      keepAlive,
      //      maxFreeSockets,
      //      maxSockets,
      //      timeout,
      //    }),
      //  }),
      //});
    }
    else
      if (typeof process.env.S3DropBucketRegion !== "undefined" && process.env.S3DropBucketRegion?.length > 6)
      {
        s3 = new S3Client({
          region: process.env.S3DropBucketRegion,         //{region: 'us-east-1'})
          requestHandler: smithyReqHandler
        })
      }
      else 
      {
        S3DB_Logging("warn", "96", `AWS Region can not be determined. Region returns as:${process.env.AWS_REGION}`)
        throw new Error(`AWS Region can not be determined. Region returns as:${process.env.AWS_REGION}`)
      }

    const s3ClientRegion = await s3.config.region()   //Future check when deploying new regions

    if (
      process.env["EventEmitterMaxListeners"] === undefined ||
      process.env["EventEmitterMaxListeners"] === "" ||
      process.env["EventEmitterMaxListeners"] === null
    )
    {
      S3DBConfig = await getValidateS3DropBucketConfig()

      S3DB_Logging("info", "901", `Parsed S3DropBucket Config:  process.env.S3DropBucketConfigFile: \n${JSON.stringify(S3DBConfig)} `)

    }

    S3DB_Logging("info", "97", `Environment Vars: ${JSON.stringify(process.env)} `)
    S3DB_Logging("info", "98", `S3DropBucket Configuration: ${JSON.stringify(S3DBConfig)} `)
    S3DB_Logging("info", "99", `S3DropBucket Logging Options(process.env): ${process.env.S3DropBucketSelectiveLogging} `)


    if (event.Records[0].s3.object.key.indexOf("S3DropBucket_Aggregator") > -1)
    {
      S3DB_Logging("info", "947", `Processing an Aggregated File ${event.Records[0].s3.object.key}`)
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

    if (S3DBConfig.s3dropbucket_quiesce)
    {
      if (!localTesting)
      {
        S3DB_Logging("warn", "923", `S3DropBucket Quiesce is in effect, new Files from ${S3DBConfig.s3dropbucket} will be ignored and not processed. \nTo Process files that have arrived during a Quiesce of the Cache, reference the S3DropBucket Guide appendix for AWS cli commands.`)
        return
      }
    }
    S3DB_Logging("info", "500", `Received S3DropBucket Event Batch. There are ${event.Records.length} S3DropBucket Event Records in this batch. (Event Id: ${event.Records[0].responseElements["x-amz-request-id"]}, \nContext: ${JSON.stringify(context)} \n${JSON.stringify(event.Records)}).`
    )

    //Future: Left this for possible switch of the Trigger from S3 being an SQS Trigger of an S3 Write,
    // Drive higher concurrency in each Lambda invocation by running batches of 10 files written at a time(SQS Batch)
    for (const r of event.Records)
    {
      let key = r.s3.object.key
      const bucket = r.s3.bucket.name

      if (process.env.S3DropBucketPrefixFocus !== undefined && process.env.S3DropBucketPrefixFocus !== "" && process.env.S3DropBucketPrefixFocus.length > 3 && !key.startsWith(process.env.S3DropBucketPrefixFocus))
      {
        S3DB_Logging("warn", "937", `PrefixFocus is configured, File Name ${key} does not fall within focus restricted by the configured PrefixFocus ${process.env.S3DropBucketPrefixFocus}`)

        return
      }

      //ToDo: Resolve Duplicates Issue - S3 allows Duplicate Object Names but Delete marks all Objects of same Name Deleted.
      //   Which causes an issue with Key Not Found after an Object of Name A is processed and deleted, then another Object of Name A comes up in a Trigger.

      vid = r.s3.object.versionId ?? ""
      et = r.s3.object.eTag ?? ""

      try
      {
        //if (key.indexOf("S3DropBucket_Aggregator") > -1)
        //{
        //  key = key.replace("S3DropBucket_Aggregator-", "S3DropBucketAggregator-")
        //  S3DB_Logging("info", "", `Aggregator File key reformed: ${key}`)
        //}      

        customersConfig = await getFormatCustomerConfig(key) as CustomerConfig

      } catch (e)
      {
        debugger //catch

        S3DB_Logging("exception", "", `Exception - Awaiting Customer Config (${key}) \n${e} `)
        break
      }

      //Initial work out for writing logs to S3 Bucket
      /*
      try {
        if (key.indexOf("S3DropBucket-LogsS3DropBucket_Aggregator") > -1)
          console.warn(`Warning -- Found Invalid Aggregator File Name - ${key} \nVersionID: ${vid}, \neTag: ${et}`)
        if (S3DBConfig.SelectiveLogging.indexOf("_101,") > -1)
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
  
          if (S3DBConfig.SelectiveLogging.indexOf("_101,") > -1)
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
      
      try
      {
        ProcessS3ObjectStreamOutcome = await processS3ObjectContentStream(
          key,
          bucket,
          customersConfig
        )
          .then(async (streamRes) => {

            let delResultCode

            streamRes.Key = key
            streamRes.Processed = streamRes.OnEndRecordStatus
            
            const recordProcessingOutcome = `Processing Outcome: ${streamRes.OnEndRecordStatus}
            As:
            ${JSON.stringify(streamRes.OnEndStreamEndResult?.StoreAndQueueWorkResult) ?? "Not Found"}
            Wrote Work To Work Bucket: ${streamRes?.OnEndStreamEndResult?.StoreAndQueueWorkResult?.AddWorkToS3WorkBucketResults?.S3ProcessBucketResultStatus ?? "Not Found"}
            Queued Work To Work Queue: ${streamRes?.OnEndStreamEndResult?.StoreAndQueueWorkResult?.AddWorkToSQSWorkQueueResults?.SQSWriteResultStatus ?? "Not Found"} 
              Or: 
            Put To Firehose: ${streamRes.OnEndStreamEndResult.StoreAndQueueWorkResult?.PutToFireHoseAggregatorResult ?? "Not Found"}. 
            `
          
            S3DB_Logging("info", "503", `Completed processing all records of the S3 Object ${key} \neTag: ${et}. \nStatus: ${recordProcessingOutcome}`)


            //Don't delete the test data
            if (localTesting)
            {
              S3DB_Logging("info", "504", `Processing Complete for ${key} (Test Data - Not Deleting). \n${JSON.stringify(streamRes)}`)
              streamRes = {...streamRes, DeleteResult: `Processing Complete for ${key} (Test Data - Not Deleting)`}
              return streamRes
            }

            //check object to assure no reference errors as the results either came through PutFirehose or Stream so 
            // all of the object may not be filled in,
            //ToDo: refactor ProcessS3ObjectStreamResolution to dynamically add status sections rather than presets

            //if (typeof streamRes.PutToFireHoseAggregatorResult === "undefined")
            //{
            //  S3DB_Logging("info", "504", `Processing Complete for ${key}. Stream Result PutToFireHoseAggregatorResult Not Complete.  \n${JSON.stringify(streamRes)}`)
            //  //streamRes.PutToFireHoseAggregatorResult = 
            //}

            //if (typeof streamRes?.OnEndStreamEndResult?.StoreAndQueueWorkResult === "undefined")
            //{
            //  S3DB_Logging("info", "504", `Processing Complete for ${key}. Stream Result StoreAndQueueWorkResult Not Complete.  \n${JSON.stringify(streamRes)}`)
            //  //streamRes.OnEndStreamEndResult =    //ProcessS3ObjectStreamOutcome.OnEndStreamEndResult
            //}

            //if (typeof streamRes?.OnEndStreamEndResult?.StoreAndQueueWorkResult?.AddWorkToS3WorkBucketResults?.S3ProcessBucketResult === "undefined")
            //{
            //  S3DB_Logging("info", "504", `Processing Complete for ${key}. Stream Result PutToFireHoseAggregatorResult Not Complete.  \n${JSON.stringify(streamRes)}`)
            //  //streamRes?.OnEndStreamEndResult?.StoreAndQueueWorkResult?.AddWorkToS3WorkBucketResults?.S3ProcessBucketResult
            //}

            //if (typeof streamRes?.OnEndStreamEndResult?.StoreAndQueueWorkResult?.AddWorkToSQSWorkQueueResults?.SQSWriteResultStatus === "undefined")
            //{
            //  S3DB_Logging("info", "504", `Processing Complete for ${key}. Stream Result AddWorkToSQSWorkQueueResults?.SQSWriteResultStatus Not Complete.  \n${JSON.stringify(streamRes)}`)
            //  //streamRes?.OnEndStreamEndResult?.StoreAndQueueWorkResult?.AddWorkToSQSWorkQueueResults?.SQSWriteResultStatus 
            //}


            if (
              (typeof streamRes.OnEndStreamEndResult.StoreAndQueueWorkResult.PutToFireHoseAggregatorResult !== "undefined" &&
                streamRes.OnEndStreamEndResult.StoreAndQueueWorkResult.PutToFireHoseAggregatorResult === "200") ||

              (typeof streamRes?.OnEndStreamEndResult?.StoreAndQueueWorkResult?.AddWorkToS3WorkBucketResults?.S3ProcessBucketResultStatus !== "undefined" &&
                streamRes?.OnEndStreamEndResult?.StoreAndQueueWorkResult?.AddWorkToS3WorkBucketResults?.S3ProcessBucketResultStatus === "200") &&

              (typeof streamRes?.OnEndStreamEndResult?.StoreAndQueueWorkResult?.AddWorkToSQSWorkQueueResults?.SQSWriteResultStatus !== "undefined" &&
                streamRes?.OnEndStreamEndResult?.StoreAndQueueWorkResult?.AddWorkToSQSWorkQueueResults?.SQSWriteResultStatus === "200")
            )
            {
              try
              {
                //Once File successfully processed, delete the original S3 Object
                delResultCode = await deleteS3Object(key, bucket)
                  .catch((e) => {
                    debugger //catch

                    S3DB_Logging("exception", "", `Exception - DeleteS3Object - ${e}`)
                  })

                if (delResultCode !== "204")
                {
                  streamRes = {
                    ...streamRes,
                    DeleteResult: JSON.stringify(delResultCode)
                  }
                  S3DB_Logging("error", "504", `Processing Successful, but Unsuccessful Delete of ${key}, Expected 204 result code, received ${delResultCode} \n\n${JSON.stringify(streamRes)}`
                  )
                } else
                {
                  streamRes = {
                    ...streamRes,
                    DeleteResult: `Successful Delete of ${key}  (Result ${JSON.stringify(delResultCode)})`
                  }
                  S3DB_Logging("info", "504", `Processing Successful, Successful Delete of ${key} (Result ${delResultCode}). \n${JSON.stringify(streamRes)}`)
                }
              } catch (e)
              {
                debugger //catch

                S3DB_Logging("exception", "", `Exception - Deleting S3 Object after successful processing of the Content Stream for ${key} \n${e} \n\n${JSON.stringify(streamRes)}`)
              }
            }
            else
            {
              S3DB_Logging("error", "504", `Processing Complete for ${key} however Status indicates to not Delete the file. \n(Result ${JSON.stringify(streamRes)})`)
              streamRes = {
                ...streamRes,
                DeleteResult: `Processing Complete for ${key} however Status indicates to not Delete the file.  (Result ${JSON.stringify(streamRes)})`
              }
            }

            return streamRes
          })
          .catch((e) => {

            debugger //catch

            const err = `Exception - Process S3 Object Stream Catch - \n${e} \nStack: \n${e.stack}`

            ProcessS3ObjectStreamOutcome = {
              ...ProcessS3ObjectStreamOutcome,
              ProcessS3ObjectStreamCatch: err,
            }

            S3DB_Logging("exception", "", JSON.stringify(ProcessS3ObjectStreamOutcome))

            return ProcessS3ObjectStreamOutcome
          })
      } catch (e)
      {
        debugger //catch

        S3DB_Logging("exception", "", `Exception - Processing S3 Object Content Stream for ${key} \n${e}`)
      }



      S3DB_Logging("info", "903", `Returned from Processing S3 Object Content Stream for ${key}. Result: ${JSON.stringify(ProcessS3ObjectStreamOutcome)}`)

      //Check for important Config updates (which caches the config in Lambdas long-running cache)
      try
      {
        S3DBConfig = await getValidateS3DropBucketConfig()
        S3DB_Logging("info", "901", `Checked and Refreshed S3DropBucket Config \n ${JSON.stringify(S3DBConfig)} `)

      } catch (e)
      {
        debugger //catch

        S3DB_Logging("exception", "", `Exception refreshing S3DropBucket Config: ${e}`)
      }

      //deprecated - left for future revisit
      //if (event.Records[0].s3.bucket.name && S3DBConfig.S3DropBucketMaintHours > 0) {
      //  const maintenance = await maintainS3DropBucket(customersConfig)

      //  const l = maintenance[0] as number
      //  // console.info( `- ${ l } File(s) met criteria and are marked for reprocessing ` )

      //  if (S3DBConfig.SelectiveLogging.indexOf("_926,") > -1) {
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
    }

    //Need to protect against the Result String becoming excessively long

    const n = new Date().toISOString()
    let objectStreamResolution = n + "  -  " + JSON.stringify(ProcessS3ObjectStreamOutcome) + "\n\n"
    const osrl = objectStreamResolution.length

    if (osrl > 10000)
    {
      //100K Characters
      objectStreamResolution = `Excessive Length of ProcessS3ObjectStreamResolution: ${osrl} Truncated: \n ${objectStreamResolution.substring(0, 5000
      )} ... ${objectStreamResolution.substring(osrl - 5000, osrl)}`

      S3DB_Logging("warn", "920", `Final outcome (truncated to first and last 5,000 characters) of all processing for ${ProcessS3ObjectStreamOutcome.Key}: \n ${JSON.stringify(objectStreamResolution)}`)
    }


    const k = ProcessS3ObjectStreamOutcome.Key
    const p = ProcessS3ObjectStreamOutcome.Processed

    S3DB_Logging("info", "505", `Completing S3DropBucket Processing of Request Id ${event.Records[0].responseElements["x-amz-request-id"]} for ${k} \n${p}`)
    S3DB_Logging("info", "920", `Final outcome of all processing for ${ProcessS3ObjectStreamOutcome.Key}: \n${JSON.stringify(objectStreamResolution)}`)

    //Done with logging the results of this pass, empty the object for the next processing cycle
    ProcessS3ObjectStreamOutcome = {} as ProcessS3ObjectStreamResult

    return objectStreamResolution   //Return the results logging object
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `S3DropBucket Handler - Exception thrown in Handler: ${e}`)
  }


  //end of handler
}

//
//

export default s3DropBucketHandler


export async function S3DB_Logging (level: string, index: string, msg: string) {

  let selectiveDebug = process.env.S3DropBucketSelectiveLogging ?? S3DBConfig.s3dropbucket_selectivelogging ?? "_97,_98,_99,_504,_511,_901,_910,_924,"

  if (typeof customersConfig.selectivelogging !== "undefined" &&
    customersConfig.selectivelogging.length > 0 &&
    customersConfig.selectivelogging !== "") selectiveDebug = customersConfig.selectivelogging

  if (localTesting) process.env.S3DropBucketLogLevel = "ALL"

  const r = await s3.config.region() ?? "Unknown"   //Authoritative
  //const r = `Region: ${process.env.AWS_REGION ?? "Unknown"}`   //
  //const r = `Region: ${process.env.S#DropBucketRegion ?? "Unknown"}`  // 

  const li = `_${index},`

  if (
    (selectiveDebug.indexOf(li) > -1 || index === "" || process.env.S3DropBucketLogLevel?.toLowerCase() === "all") &&
    process.env.S3DropBucketLogLevel?.toLowerCase() !== "none"
  )
  {
    if (process.env.S3DropBucketLogLevel?.toLowerCase() === "all") index = `(LOG ALL-${index})`

    if (level.toLowerCase() === "info") console.info(`S3DBLog-Info ${index}: ${msg} \nRegion: ${r} Version: ${version}`)
    if (level.toLowerCase() === "warn") console.warn(`S3DBLog-Warning ${index}: ${msg} \nRegion: ${r} Version: ${version}`)
    if (level.toLowerCase() === "error") console.error(`S3DBLog-Error ${index}: ${msg} \nRegion: ${r} Version: ${version}`)
    if (level.toLowerCase() === "debug") console.debug(`S3DBLog-Debug ${index}: ${msg} \nRegion: ${r} Version: ${version}`)
  }

  if (level.toLowerCase() === "exception") console.error(`S3DBLog-Exception ${index}: ${msg}  \nRegion: ${r} Version: ${version}`)

  //ToDo: Send Logging to Firehose Aggregator 
  // Send All debug messaging regardless of S3DropBucket Config??
  // Send All info messaging regardless of S3DropBucket Config??
  //  
  //ToDo: Add Firehose Logging
  //ToDo: Add DataDog Logging

  //
  //Possible Logging Option - Using Firehose
  //
  /*
  if (S3DBConfig.S3DropBucketLog) {
     const logMsg: object[] = [{osr}]      //need to check this one, 
     debugger ///
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

}

async function processS3ObjectContentStream (
  key: string,
  bucket: string,
  custConfig: CustomerConfig
) {
  S3DB_Logging("info", "514", `Processing S3 Content Stream for ${key}`)

  const s3C: GetObjectCommandInput = {
    Key: key,
    Bucket: bucket,
  }

  let preserveArraySize = 0

  let streamResult: ProcessS3ObjectStreamResult  // = ProcessS3ObjectStreamResolution

  ProcessS3ObjectStreamOutcome = await s3
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

      //Needed for Stream processing, although is an opportunity for Transform processing.
      const t = transform(function (data) {
        //"The \"chunk\" argument must be of type string or an instance of Buffer or Uint8Array. Received an instance of Object"
        //ToDo: Future - Opportunity for Transforms
        let r
        if (Buffer.isBuffer(data)) r = data.toString("utf8")
        else r = JSON.stringify(data) + "\n"
        return r
      })

      if (
        custConfig.format.toLowerCase() === "csv" &&
        key.indexOf("S3DropBucket_Aggregator") < 0
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
        //selectiveLogging("info", "", `Future Logging Opportunity - JSON Flag`)
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

      let jsonSep = process.env.S3DropBucketJsonSeparator
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
      //  as JSON, as a series of Objects (One at a time in the Stream) with a line break after each Object.
      //   {data:data, data:data, .....}
      //   {data:data, data:data, .....}
      //   {data:data, data:data, .....}
      //
      // Later, Stream.OnData processing populates an Array with each line/Object, so the
      // final format will be an Array of Objects:
      // [ {data:data, data:data, .....},
      //  { data: data, data: data, .....},
      //  { data: data, data: data, .....}, ... ]
      //

      // s3ContentReadableStream = s3ContentReadableStream.pipe(t).pipe(jsonParser)
      s3ContentReadableStream = s3ContentReadableStream.pipe(jsonParser)
      //s3ContentReadableStream = s3ContentReadableStream.pipe(tJsonParser)

      s3ContentReadableStream.setMaxListeners(
        Number(S3DBConfig.s3dropbucket_eventemittermaxlisteners)
      )

      chunksGlobal = []
      batchCount = 0
      recs = 0
      let iter = 0
      let apiLimit = 100
      if(custConfig.targetupdate.toLowerCase() === "connect") apiLimit = 25

      let packageResult = {} as StoreAndQueueWorkResults

      await new Promise((resolve, reject) => {
        s3ContentReadableStream
          .on("error", async function (err: string) {
            const errMessage = `An error has stopped Content Parsing at record ${recs} for s3 object ${key}.\n${err} \n${chunksGlobal}`
            S3DB_Logging("error", "909", errMessage)

            streamResult = {
              ...streamResult,
              ReadStreamException: `s3ContentReadableStreamErrorMessage ${JSON.stringify(errMessage)}`
            }

            S3DB_Logging("error", "909", `Error on Readable Stream for s3DropBucket Object ${key}.\nError Message: ${errMessage} `)

            //??? 
            //ToDo: need to check we're exiting as expected here 
            reject(errMessage)
            throw new Error(
              `Error on Readable Stream for s3DropBucket Object ${key}.\nError Message: ${errMessage} `
            )

          })

          .on("data", async function (s3Chunk: {
            key: string
            parent: object
            stack: object
            value: object
          }) {

            if (typeof s3Chunk.value === "undefined") throw new Error(`S3ContentStream OnData (File Stream Iter: ${iter}) - s3Chunk.value is undefined for ${key}`)

            if (
              key.toLowerCase().indexOf("aggregat") < 0 && recs > custConfig.updatemaxrows
            )
            {
              S3DB_Logging("warn", "515", `The number of Updates in this batch (${recs}) from ${key} Exceeds Max Row Updates allowed in the Customers Config (${custConfig.updatemaxrows}). Review data file for unexpected data or update Customer Config. `)

              //throw new Error(
              //  `The number of Updates in this batch (${recs}) Exceeds Max Row Updates allowed in the Customers Config (${custConfig.updatemaxrows}).  ${key} will not be deleted from ${S3DBConfig.s3dropbucket} to allow for review and possible restaging.`
              //)
            }

            iter++

            S3DB_Logging("info", "913", `S3ContentStream OnData (File Stream Iter: ${iter}) - A Chunk or Line from ${key} has been read. Records previously processed: ${recs}`)

            try
            {
              const oa = s3Chunk.value

              //What JSON is possible to come through here (at this point it's always JSON)
              //  {} a single Object - Pura
              //  [{},{},{}] An Array of Objects - Alerus
              //  An Array of Line Delimited Objects - (Firehose Aggregated files)
              //      [{}
              //      {}
              //       ...
              //      {}]
              // An array of Strings (when CSV Parsing creates the JSON)
              //  [{"key1":"value1","key2":"value2"},{"key1":"value1","key2":"value2"},...]
              //

              //
              //Next, Build a consistent Object of an Array of Objects
              // [{},{},{},...]
              

              //Debug a buried Undefined Exception in Handler
              if (typeof oa === "undefined") S3DB_Logging("exception", "", `OnData Array is Undefined`)
              //
              
              if (Array.isArray(oa))
              {
                preserveArraySize = oa.length

                for (const a in oa)
                {
                  let e = oa[a]
                  if (typeof e === "string")
                  {
                    e = JSON.parse(e)
                  }
                  chunksGlobal.push(e)
                }
              } else
              {
                //chunksGlobal.push( JSON.stringify( oa ) )
                chunksGlobal.push(oa)
              }
            } catch (e)
            {
              debugger //catch

              streamResult = {
                ...streamResult,
                OnDataReadStreamException: `Exception - First Catch - ReadStream-OnData Processing for ${key} \nBatch ${batchCount} of ${recs} Updates. \n${e} `
              }

              S3DB_Logging("exception", "", `Exception - ReadStream-OnData - Chunk aggregation for ${key} \nBatch ${batchCount} of ${recs} Updates. \n${e}`)

              throw new Error(`Exception - ReadStream (OnData) Chunk aggregation for ${key} \nBatch ${batchCount} of ${recs} Updates.\n${e}`)

            }
            //OnEnd gets the bulk of the data when files are moderate in size, for Large files OnData processes through multiple Read Chunks 
            //At each 99 updates, package them up, if there are fewer than 99 then "OnEnd" will pick up the remainder. 
            try 
            {
              if (chunksGlobal.length > apiLimit)  //ToDo: refactor for Connect Limits, for now, can use 100 and let arrive at Connect POST where it will be carved up as 25x4
              {
                const updates = []
                let limit = apiLimit

                while (chunksGlobal.length > 0)
                {
                  const chunk = chunksGlobal.splice(0, limit)
                  updates.push(...chunk)
                  recs += chunk.length
                  batchCount++

                  S3DB_Logging("info", "938", `S3ContentStream OnData - A Batch (${batchCount}) of ${chunk.length} Updates from ${key} is now being sent to Packaging. \nPreviously processed ${recs} records of the size of the data read of ${preserveArraySize} records. \n${chunksGlobal.length} records are waiting to be processed.`)

                  
                  packageResult = await packageUpdates(updates, key, custConfig, iter) as StoreAndQueueWorkResults

                  //ToDo: Refactor this status reporting approach, need a way to preserve every Update status without storing volumes
                  streamResult = {
                    ...streamResult,
                    OnDataResult: {StoreAndQueueWorkResult: packageResult}
                  }
                }
              }
            } catch (e)
            {
              debugger //catch

              S3DB_Logging("exception", "", `Exception - ReadStream-OnData - Batch Packaging for ${key} \nBatch ${batchCount} of ${recs} Updates. \n${e} `)

              streamResult = {
                ...streamResult,
                OnDataReadStreamException: `Exception - Second Catch - ReadStream-OnData - Batch Packaging for ${key} \nBatch ${batchCount} of ${recs} Updates. \n${e} `,
              }
            }
          }
          )

          //File completed streaming, chunkcGlobal holds any updates left to be processed -> Packaged 
          .on("end", async function () {

            if (recs < 1 && chunksGlobal.length < 1)      //Ooops, We got here without finding/processing any data 
            {
              S3DB_Logging("exception", "", `Stream Exception - No records returned from parsing file. Check the contents of the file and that the file extension and file format matches the configured file type(${custConfig.format}).`)

              streamResult = {
                ...streamResult,
                OnEndNoRecordsException: `Exception - No records returned from parsing file.Check the content as well as the configured file format(${custConfig.format}) matches the content of the file.`,
              }
              S3DB_Logging("error", "", `Error - No records returned from parsing file. Check the content as well as the configured file format(${custConfig.format}) matches the content of the file.`)
              //throw new Error( `Exception - onEnd ${ JSON.stringify( streamResult ) } ` )

              return streamResult
            }

            try     //Overall Try/Catch for On-End processing
            {
              iter++
              // If there are ChunksGlobal to process (and there likely will be), send to Packaging
              // Create a Work file and Queue entry for Processing to Campaign / Connect

              //ChunksGlobal has been populated in OnData, so when OnEnd hits it contains all the data leftover after OnData processing
              //S3DB_Logging("info", "", `Debug (OnEnd): ${chunksGlobal.length} ${batchCount}`) 

              //Need to keep an eye on arriving here with an array with more than 100 entries, more than 100 should be processed in OnData above.
              if (chunksGlobal.length > 0)  //Should be the case but may not be
              {

                //const updates = chunksGlobal
                const updates = []

                while (chunksGlobal.length > 0)
                {
                  let chunk = []
                  let limit = apiLimit
                  
                  //if (custConfig.targetupdate.toLowerCase() === "campaign") limit = 100 
                  //if (custConfig.targetupdate.toLowerCase() === "connect") limit = 25 
                  
                  chunk = chunksGlobal.splice(0, limit)
      
                  updates.push(...chunk)
                  recs += chunk.length
                  batchCount++

                  S3DB_Logging("info", "938", `S3ContentStream OnEnd - A Batch (${batchCount}) of ${chunk.length} Updates from ${key} is now being sent to Packaging. \nPreviously processed ${recs} records of the size of the data read of ${preserveArraySize} records. \n${chunksGlobal.length} records are waiting to be processed.`)

                  //ToDo: Refactor this status approach, only getting the last, need a way to preserve every Update status without storing volumes
                  packageResult = await packageUpdates(updates, key, custConfig, iter)
                    .then((res) => {
                      S3DB_Logging("info", "948", `S3ContentStream OnEnd - PackageUpdates Outcome - Raw Results: ${JSON.stringify(res)} \nFor (${batchCount}) of ${chunk.length} Updates \nfrom ${key} \nPreviously processed ${recs} records of the size of the data read of ${preserveArraySize} records. \n${chunksGlobal.length} records are waiting to be processed.`)
                      return res as StoreAndQueueWorkResults
                    })
                }
        
                //Best we can do is reflect the last packaging outcome, as we
                // should Exception out if there are any issues, the last reflects success mostly
                streamResult = {
                  ...streamResult,
                  OnEndStreamEndResult: {
                    StoreAndQueueWorkResult: packageResult
                  }
                }
              
              }
              else
              {

                S3DB_Logging("info", "938", `S3ContentStream OnEnd - With No Remaining Updates to Process from ${key}. Current Batch ${batchCount}. \nPreviously processed ${recs} records of the size of the data read of ${preserveArraySize} records.`)
                            
                streamResult = {
                  ...streamResult,
                  OnEndStreamEndResult: {
                    StoreAndQueueWorkResult: {
                      AddWorkToS3WorkBucketResults: {
                        versionId: "",
                        S3ProcessBucketResultStatus: "200",       //Fake Status to drive cleanup/deletion logic, but true enough here
                        AddWorkToS3ProcessBucket: "All Work Packaged in OnData, No Work Left to Package"
                      },
                      AddWorkToSQSWorkQueueResults: {
                        SQSWriteResultStatus: "200",              //Fake Status to drive cleanup/deletion logic, but true enough here
                        AddToSQSQueue: "All Work Packaged in OnData, No Work Left to Package"
                      },
                      PutToFireHoseAggregatorResult: "",
                      PutToFireHoseAggregatorResultDetails: "",
                      PutToFireHoseException: ""
                    }
                  }
                }
              }

            } catch (e)    //Overall Try/Catch for On-End processing
            {
              debugger //catch

              const sErr = `Exception - ReadStream OnEnd Processing - \n${e} `
              S3DB_Logging("exception", "", sErr)
              return {...streamResult, OnEndStreamResult: sErr}
            }

            //
            //wrap up all OnEnd processing 
            //
            const streamEndResult = `S3 Content Stream Ended for ${key}.Processed ${recs} records as ${batchCount} batches.`

            streamResult = {
              ...streamResult,
                ReadStreamEndResult: streamEndResult,
                OnEndRecordStatus: `Processed ${recs} records as ${batchCount} batches.`
              }
            

            S3DB_Logging("info", "902", `Content Stream OnEnd for (${key}) - Store and Queue Work of ${batchCount} Batches of ${recs} records - Stream Result: \n${JSON.stringify(streamResult)} `)

            resolve({...streamResult})

            // return streamResult
          })

          .on("close", async function () {
            streamResult = {
              ...streamResult,
              OnClose_Result: `S3 Content Stream Closed for ${key}`
            }
          })
        S3DB_Logging("info", "502", `S3 Content Stream Opened for ${key}`)
      })
        .then((r) => {
          return {
            ...streamResult,
            ReturnLocation: `Returning from ReadStream Then Clause.\n${r} `
          }
        })
        .catch((e) => {       //Catch for Processing Promise 
          debugger //catch

          const err = `Exception - ReadStream(catch) - Process S3 Object Content Stream for ${key}. \n${e} `
          //throw new Error( err )
          S3DB_Logging("exception", "", err)
          return {...streamResult, OnCloseReadStreamException: `${err}`}
        })

      // return { ...readStream, ReturnLocation: `...End of ReadStream Promise` }
      return {
        ...streamResult,
        ReturnLocation: `Returning from End of ReadStream Promise`
      }
    })

    .catch((e) => {
      debugger //catch

      //throw new Error( `Exception(throw) - ReadStream - For ${ key }.\nResults: ${ JSON.stringify( streamResult ) }.\n${ e } ` )

      streamResult = {
        ...streamResult,
        ReadStreamException: `Exception (ProcessS3ObjectStreamResult catch) - ReadStream - For ${key}.\n ${JSON.stringify(streamResult)}.\n${e} `
      }

      S3DB_Logging("exception", "", `Exception (ProcessS3ObjectStreamResult catch) - ReadStream - For ${key}.\nResults: ${JSON.stringify(streamResult)}.\n${e} `)

      //return {
      //  ...streamResult,
      //  ReadStreamException: `Exception (ProcessS3ObjectStreamResult catch) - ReadStream - For ${key}.\nResults: ${JSON.stringify(streamResult)}.\n${e} `
      //}
      return streamResult
    })

  // return processS3ObjectResults
  return ProcessS3ObjectStreamOutcome
  //return streamResult
}


/**
 * A Lambda function to process the Event payload received from SQS - AWS Queues.
 * 
 * Notes: Be sure that Lambda Reserved Concurrency is set greater than 1, ref: 
 * https://data.solita.fi/lessons-learned-from-combining-sqs-and-lambda-in-a-data-project/
 *
 */
export const S3DropBucketQueueProcessorHandler: Handler = async (
  event: SQSEvent,
  context: Context
) => {

  let s3dbQM: S3DBQueueMessage = {
    workKey: "",
    versionId: "",
    marker: "",
    attempts: 0,
    batchCount: "",
    updateCount: "",
    lastQueued: "",
    custconfig: {
      customer: "",
      format: "",
      separator: "",
      updates: "",
      targetupdate: "",
      listid: "",
      listname: "",
      updatetype: "",
      dbkey: "",
      lookupkeys: [],
      updateKey: "",
      pod: "",
      region: "",
      updatemaxrows: 0,
      refreshtoken: "",
      clientid: "",
      clientsecret: "",
      datasetid: "",
      subscriptionid: "",
      x_api_key: "",
      x_acoustic_region: "",
      selectivelogging: "",
      sftp: {
        user: "",
        password: "",
        filepattern: "",
        schedule: "",
      },
      transforms: {
        contactid: "",
        contactkey: "",
        addressablefields: {"": ""},
        consent: {"": ""},
        audienceupdate: {"": ""},
        methods: {
          daydate: "",
          date_iso1806_type: {"": ""},
          phone_number_type: {"": ""},
          string_to_number_type: {"": ""},
          method2: {"": ""}
        },
        jsonmap: {},
        csvmap: {},
        ignore: []   //always last
      }
    }
  }

  try
  {

    if (typeof process.env.AWS_REGION !== "undefined") 
    {
      s3 = new S3Client({
        region: process.env.AWS_REGION,         //{region: 'us-east-1'})
        requestHandler: smithyReqHandler
      })

    }
    else
    {
      if (typeof process.env.S3DropBucketRegion !== "undefined" && process.env.S3DropBucketRegion?.length > 6)
      {
        
        s3 = new S3Client({
          region: process.env.S3DropBucketRegion,         //{region: 'us-east-1'})
          requestHandler: smithyReqHandler
        })
      }
      else 
      {
        S3DB_Logging("warn", "96", `AWS Region can not be determined. Region returns as: ${process.env.AWS_REGION}`)
        throw new Error(`AWS Region can not be determined. Region returns as:${process.env.AWS_REGION}`)
      }
    }

    const s3ClientRegion = await s3.config.region()

    //If an obscure config does not exist in process.env then we need to refresh S3DropBucket Config 
    if (
      process.env["EventEmitterMaxListeners"] === undefined ||
      process.env["EventEmitterMaxListeners"] === "" ||
      process.env["EventEmitterMaxListeners"] === null
    )
    {
      S3DBConfig = await getValidateS3DropBucketConfig()
      S3DB_Logging("info", "901", `Parsed S3DropBucket Config:  process.env.S3DropBucketConfigFile: \n${JSON.stringify(S3DBConfig)} `)
    }

    S3DB_Logging("info", "97", `Environment Vars: ${JSON.stringify(process.env)} `)
    S3DB_Logging("info", "98", `S3DropBucket Configuration: ${JSON.stringify(S3DBConfig)} `)
    S3DB_Logging("info", "99", `S3DropBucket Logging Options: ${process.env.S3DropBucketSelectiveLogging} `)

    const ra: Array<Record<string, string>> = []
    event.Records.forEach((r) => {
      ra.push({"messageId": r.messageId})
    })
      
    S3DB_Logging("info", "506", `Received a Batch of SQS Work Queue Events (${event.Records.length}. \nWork Queue Record MessageIds: \n${JSON.stringify(ra)} \nContext: ${JSON.stringify(context)}`)
    S3DB_Logging("info", "907", `Received a Batch of SQS Work Queue Events (${event.Records.length} Work Queue Records): \n${JSON.stringify(event)} \nContext: ${JSON.stringify(context)}`)

    if (S3DBConfig.s3dropbucket_workqueuequiesce)
    {
      S3DB_Logging("warn", "923", `WorkQueue Quiesce is in effect, no New Work will be Queued up in the SQS Process Queue.`)
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

    let custconfig: CustomerConfig = customersConfig

    let postResult: string = "false"

    //Empty the BatchFail array
    sqsBatchFail.batchItemFailures.forEach(() => {
      sqsBatchFail.batchItemFailures.pop()
    })

    //Process this Inbound Batch
    for (const q of event.Records)
    {

      s3dbQM = JSON.parse(q.body)

      if (typeof s3dbQM.workKey === "undefined")
      {
        S3DB_Logging("error", "941", `Error Parsing Incoming Queue Message for WorkKey:  \n${JSON.stringify(s3dbQM)}`)
      }
      if (typeof s3dbQM.custconfig === "undefined")
      {
        S3DB_Logging("error", "941", `Error Parsing Incoming Queue Message for CustConfig:  \n${JSON.stringify(s3dbQM)}`)
      }
      if (typeof s3dbQM.custconfig.customer === "undefined")
      {
        S3DB_Logging("error", "941", `Error Parsing Incoming Queue Message for Customer:  \n${JSON.stringify(s3dbQM)}`)
      }
      if (typeof s3dbQM.updateCount === "undefined")
      {
        S3DB_Logging("error", "941", `Error Parsing Incoming Queue Message for UpdateCount:  \n${JSON.stringify(s3dbQM)}`)
        s3dbQM.updateCount = "'Not Provided'"
      }



      ////When Testing locally  (Launch config has pre-stored queue message payload) - get some actual work queued
      //if (s3dbQM.workKey === "")
      //{
      //  s3dbQM.workKey = (await getAnS3ObjectforTesting(S3DBConfig.s3dropbucket_workbucket)) ?? ""
      //}

      if (s3dbQM.workKey === "devtest.xml")
      {
        //tqm.workKey = await getAnS3ObjectforTesting( tcc.s3DropBucketWorkBucket! ) ?? ""
        s3dbQM.workKey = testS3Key
        s3dbQM.custconfig.customer = testS3Key
        S3DBConfig.s3dropbucket_workbucket = testS3Bucket
        localTesting = true
      } else
      {
        testS3Key = ""
        testS3Bucket = ""
        localTesting = false
      }



      S3DB_Logging("info", "507", `Start Processing ${s3dbQM.workKey} off Work Queue. `)

      S3DB_Logging("info", "911", `Start Processing Work Item: SQS Event: \n${JSON.stringify(q)}`)

      //try
      //{

      //} catch (e)
      //{
      //  S3DB_Logging("exception", "", `Exception - Retrieving Customer Config for Work Queue Processing (work file: ${s3dbQM.workKey} \n${e}} \n${JSON.stringify(s3dbQM)}`)
      //}

      try
      {

        custconfig = await getFormatCustomerConfig(s3dbQM.custconfig.customer) as CustomerConfig

        const work = await getS3Work(s3dbQM.workKey, S3DBConfig.s3dropbucket_workbucket)

        if (work.length > 0)
        {
          //Retrieve Contents of the Work File
          S3DB_Logging("info", "512", `S3 Retrieve results for Work file ${s3dbQM.workKey}: ${JSON.stringify(work)}`)
          
          if ((custconfig.updatetype.toLowerCase() === "createupdatecontacts") ||
            (custconfig.updatetype.toLowerCase() === "createattributes")
          )
          {
            postResult = await postToConnect(
              work,
              custconfig as CustomerConfig,
              s3dbQM.updateCount,
              s3dbQM.workKey
            )
          }

          

          if (custconfig.updatetype.toLowerCase() === "relational" ||
            custconfig.updatetype.toLowerCase() === "dbkeyed" ||
            custconfig.updatetype.toLowerCase() === "dbnonkeyed")
          {
            postResult = await postToCampaign(
              work,
              custconfig as CustomerConfig,
              s3dbQM.updateCount,
              s3dbQM.workKey
            )
          }


          //  postResult can contain:
          //      retry
          //      unsuccessful post
          //      partially successful
          //      successfully posted

        
          S3DB_Logging("info", "508", `POST Result for ${s3dbQM.workKey}: ${postResult} `)

          let deleteWork = false

          if (postResult.toLowerCase().indexOf("retry ") > -1)
          {
            //Add to SQS BatchFail array to Retry processing the work
            sqsBatchFail.batchItemFailures.push({itemIdentifier: q.messageId})

            S3DB_Logging("warn", "516", `Retry Marked for ${s3dbQM.workKey}, added back to Queue for Retry (Queue MessageId: ${q.messageId}) \nRetry Queue Items:\n ${JSON.stringify(sqsBatchFail)} (Total Retry Count: ${sqsBatchFail.batchItemFailures.length + 1}) \nPOST Result requiring retry: ${postResult}`)
          }
          else if (postResult.toLowerCase().indexOf("unsuccessful post ") > -1)
          {
            S3DB_Logging("error", "520", `Error - Unsuccessful POST (Hard Failure) for ${s3dbQM.workKey}: \n${postResult} \nQueue MessageId: ${q.messageId} \nCustomer: ${custconfig.customer} `)
            deleteWork = true
          }
          else if (postResult.toLowerCase().indexOf("partially successful") > -1)
          {
            S3DB_Logging("warn", "518", `Updates (Partially Successful) POSTed (work file (${s3dbQM.workKey}) \nQueue MessageId: ${q.messageId}, \nUpdated ${s3dbQM.custconfig.listname} from ${s3dbQM.workKey}, however there were some exceptions: \n${postResult} `)
            deleteWork = true
          }
          else if (postResult.toLowerCase().indexOf("successfully posted") > -1)
          {
            S3DB_Logging("info", "519", `Updates (Successfully) POSTed (work file (${s3dbQM.workKey}). \nQueue MessageId: ${q.messageId} \nUpdated ${s3dbQM.custconfig.listname} from ${s3dbQM.workKey}, \n${postResult} \nThe Work will now be deleted from the S3 Process Queue`)
            deleteWork = true
          }
          else
          {
            //we should not come across a result we did not expect.
            S3DB_Logging("exception", "", `Results of Posting Work is not determined: ${JSON.stringify(postResult)} \n(work file (${s3dbQM.workKey}). \nQueue MessageId: ${q.messageId} \nUpdated ${s3dbQM.custconfig.listname} from ${s3dbQM.workKey}, \n${postResult}`)
          }

          if (!localTesting && deleteWork)
          {
            //Delete the Work file
            const fd = await deleteS3Object(
              s3dbQM.workKey,
              S3DBConfig.s3dropbucket_workbucket
            )
            if (fd === "204")
            {
              S3DB_Logging("info", "924", `Processing Complete - Deletion of Queued Work file: ${s3dbQM.workKey}  \nQueue MessageId: ${q.messageId}`)
            } else S3DB_Logging("error", "924", `Processing Complete but Failed to Delete Queued Work File ${s3dbQM.workKey}. Expected '204' but received ${fd} \nQueue MessageId: ${q.messageId}`)

            const qd = await sqsClient.send(
              new DeleteMessageCommand({
                QueueUrl: S3DBConfig.s3dropbucket_workqueue,
                ReceiptHandle: q.receiptHandle
              })
            )
            if (qd.$metadata.httpStatusCode === 200)
            {
              S3DB_Logging("info", "925", `Processing Complete - Deletion of SQS Queue Message (Queue MessageId: ${q.messageId}) \nWorkfile ${s3dbQM.workKey}. \nDeletion Response: ${JSON.stringify(qd)}`)
            } else S3DB_Logging("error", "925", `Processing Complete but Failed to Delete SQS Queue Message (Queue MessageId: ${q.messageId}) \nWorkfile ${s3dbQM.workKey}. \nExpected '200' but received ${qd.$metadata.httpStatusCode} \nDeletion Response: ${JSON.stringify(qd)}`)

          }

          S3DB_Logging("info", "510", `POSTed ${event.Records.length} Work Queue updates. Result: ${postResult}. 
            \nUpdates to be Retried Count: ${sqsBatchFail.batchItemFailures.length} 
            \nUpdates to Retried List: \n${JSON.stringify(sqsBatchFail)} `
          )
          
          S3DB_Logging("info", "511", `POSTed ${s3dbQM.updateCount} Updates from ${s3dbQM.workKey}`)


          //ToDo: need to add similar status object like S3ObjectStreamResolution to QueueProcessor reporting
          // S3DropBucketQueueProcessorResolution.postStatus = ....
          // S3DropBucketQueueProcessorResolution.deleteStatus = ....
          // S3DropBucketQueueProcessorResolution.returnedToQueueStatus = ....


        } else throw new Error(`Failed to retrieve work file(${s3dbQM.workKey}) `)
      } catch (e: any)
      {
        debugger //catch

        S3DB_Logging("exception", "", `Exception - Processing Work File (${s3dbQM.workKey} off the Work Queue \nFor Queue MessageId: ${q.messageId} \n${e}} `)

        if (String(e).indexOf("Work Not Found") > -1)
        {
          //Work File not found - so, let's make sure to delete the Queued Event Message else it can possibly come back in the Queue
          const qd = await sqsClient.send(
            new DeleteMessageCommand({
              QueueUrl: S3DBConfig.s3dropbucket_workqueue,
              ReceiptHandle: q.receiptHandle
            })
          )
          if (qd.$metadata.httpStatusCode === 200)
          {
            S3DB_Logging("info", "945", `Deletion of Queue Message (Queue MessageId: ${q.messageId}) due to Work file not found (${s3dbQM.workKey}) Exception. \nDeletion Response: ${JSON.stringify(qd)}`)
          } else S3DB_Logging("error", "945", `Deletion of Queue Message (Queue MessageId: ${q.messageId}) due to Work file not found (${s3dbQM.workKey}) Exception Failed: Expected '200' but received ${qd.$metadata.httpStatusCode} \nDeletion Response: ${JSON.stringify(qd)}`)
        }
      }
    }




    //Deprecated but left for future use in a new treatment for maintenance
    //
    //let maintenance: (number | string[])[] = []
    //if (S3DBConfig.S3DropBucketWorkQueueMaintHours > 0) {
    //  try {
    //    maintenance = (await maintainS3DropBucketQueueBucket()) ?? [0, ""]

    //    if (S3DBConfig.SelectiveLogging.indexOf("_927,") > -1) {
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
    //    debugger //catch
    //  }
    //}


    S3DB_Logging("info", "507", `Completed Processing ${s3dbQM.workKey}. Return to Retry Queue List:\n${JSON.stringify(sqsBatchFail)} `)

    return sqsBatchFail

    //For debugging - report no fails
    // return {
    //     batchItemFailures: [
    //         {
    //             itemIdentifier: ''
    //         }
    //     ]
    // }

  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `QueueProcessorHandler Exception processing Work Queue items ${JSON.stringify(s3dbQM)} \n${e}`)
  }

}

export const s3DropBucketSFTPHandler: Handler = async (
  event: SQSEvent,
  context: Context
) => {
  // const SFTPClient = new sftpClient()

  S3DB_Logging("info", "97", `S3 Dropbucket SFTP Processor - Environment Vars: ${JSON.stringify(process.env)} `)
  S3DB_Logging("info", "99", `S3 Dropbucket SFTP Processor - S3DropBucket Logging Options(process.env): ${process.env.S3DropBucketSelectiveLogging} `)

  //ToDo: change out the env var checked to determine config 
  if (
    process.env["WorkQueueVisibilityTimeout"] === undefined ||
    process.env["WorkQueueVisibilityTimeout"] === "" ||
    process.env["WorkQueueVisibilityTimeout"] === null
  )
  {
    S3DBConfig = await getValidateS3DropBucketConfig()

    S3DB_Logging("info", "901", `Parsed S3DropBucket Config:  process.env.S3DropBucketConfigFile: \n${JSON.stringify(S3DBConfig)} `)

  }

  S3DB_Logging("info", "98", `S3 Dropbucket SFTP Processor - S3DropBucket Configuration: ${JSON.stringify(S3DBConfig)} `)

  S3DB_Logging("info", "700", `S3 Dropbucket SFTP Processor - Received Event: ${JSON.stringify(event)}.\nContext: ${context} `)


  if (!S3DBConfig.s3dropbucket_sftp) return



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
      S3DB_Logging("info", "700", JSON.stringify(res))

      return response
    })
    .catch((err) => {
      debugger //catch

      S3DB_Logging("exception", "", `Error - Failed to retrieve SFTP Scheduler2 Events: ${err} `)

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


  //console.info(
  //  `Received SFTP SQS Events Batch of ${event.Records.length} records.`
  //)

  //ToDo: Assign debug number for these messages
  S3DB_Logging("info", "700", `Received SFTP SQS Events Batch of ${event.Records.length} records.`)
  S3DB_Logging("info", "700", `Received ${event.Records.length} SFTP Queue Records.Records are: \n${JSON.stringify(event)} `)

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
  for (const q of event.Records)
  {
    //General Plan:
    // Process SQS Events being emitted for Check FTP Directory for Customer X
    // Process SQS Events being emitted for
    // Check  configs to emit another round of SQS Events for the next round of FTP Work.

    // event.Records.forEach(async (i: SQSRecord) => {
    const tqm: S3DBQueueMessage = JSON.parse(q.body)

    tqm.workKey = JSON.parse(q.body).workKey

    //When Testing - get some actual work queued
    if (tqm.workKey === "devtest.csv")
    {
      tqm.workKey = (await getAnS3ObjectforTesting(S3DBConfig.s3dropbucket!)) ?? ""
    }

    S3DB_Logging("info", "513", `SQS Events - Processing Batch Item ${JSON.stringify(q)} \nProcessing Work Queue for ${tqm.workKey}`)

    // try
    // {
    //     const work = await getS3Work(tqm.workKey, "s3dropbucket-process")
    //     if (work.length > 0)        //Retreive Contents of the Work File
    //     {
    //         postResult = await postToCampaign(work, tqm.custconfig, tqm.updateCount)
    //         if (tcc.SelectiveLogging.indexOf("_8,") > -1) console.info(`Selective Debug 8 - POST Result for ${ tqm.workKey }: ${ postResult } `)

    //         if (postResult.indexOf('retry') > -1)
    //         {
    //             console.warn(`Retry Marked for ${ tqm.workKey }(Retry Report: ${ sqsBatchFail.batchItemFailures.length + 1 }) Returning Work Item ${ q.messageId } to Process Queue.`)
    //             //Add to BatchFail array to Retry processing the work
    //             sqsBatchFail.batchItemFailures.push({ itemIdentifier: q.messageId })
    //             if (tcc.SelectiveLogging.indexOf("_12,") > -1) console.info(`Selective Debug 12 - Added ${ tqm.workKey } to SQS Events Retry \n${ JSON.stringify(sqsBatchFail) } `)
    //         }

    //         if (postResult.toLowerCase().indexOf('unsuccessful post') > -1)
    //             console.error(`Error - Unsuccesful POST(Hard Failure) for ${ tqm.workKey }: \n${ postResult } \n Customer: ${ tqm.custconfig.Customer }, Pod: ${ tqm.custconfig.pod }, ListId: ${ tqm.custconfig.listId } \n${ work } `)

    //         if (postResult.toLowerCase().indexOf('successfully posted') > -1)
    //         {
    //             console.info(`Work Successfully Posted to Campaign(${ tqm.workKey }), Deleting Work from S3 Process Queue`)

    //             const d: string = await deleteS3Object(tqm.workKey, 's3dropbucket-process')
    //             if (d === "204") console.info(`Successful Deletion of Work: ${ tqm.workKey } `)
    //             else console.error(`Failed to Delete ${ tqm.workKey }. Expected '204' but received ${ d } `)
    //         }

    //     }
    //     else throw new Error(`Failed to retrieve work file(${ tqm.workKey }) `)

    // } catch (e)
    // {
    //     console.error(`Exception - Processing a Work File(${ tqm.workKey } - \n${ e } \n${ JSON.stringify(tqm) }`)
    // }
  }

  S3DB_Logging("info", "700", `Processed ${event.Records.length} SFTP Requests.Items Fail Count: ${sqsBatchFail.batchItemFailures.length}\nItems Failed List: ${JSON.stringify(sqsBatchFail)}`)

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

async function sftpConnect (options: {
  host: string
  port: string
  username?: string
  password?: string
}) {
  S3DB_Logging("info", "700", `Connecting to ${options.host}: ${options.port}`)

  try
  {
    // await SFTPClient.connect(options)
  } catch (err)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Failed to connect: ${err}`)
  }
}

async function sftpDisconnect () {
  // await SFTPClient.end()
}

async function sftpListFiles (remoteDir: string, fileGlob: ListFilterFunction) {
  S3DB_Logging("info", "700", `Listing ${remoteDir} ...`)

  let fileObjects: sftpClient.FileInfo[] = []
  try
  {
    fileObjects = await SFTPClient.list(remoteDir, fileGlob)
  } catch (err)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Listing failed: ${err}`)
  }

  const fileNames = []

  for (const file of fileObjects)
  {
    if (file.type === "d")
    {
      S3DB_Logging("info", "700", `${new Date(file.modifyTime).toISOString()} PRE ${file.name}`)
    } else
    {
      S3DB_Logging("info", "700", `${new Date(file.modifyTime).toISOString()} ${file.size} ${file.name}`)
    }

    fileNames.push(file.name)
  }

  return fileNames
}

async function sftpUploadFile (localFile: string, remoteFile: string) {
  S3DB_Logging("info", "700", `Uploading ${localFile} to ${remoteFile} ...`)
  try
  {
    // await SFTPClient.put(localFile, remoteFile)
  } catch (err)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Uploading failed: ${err}`)
  }
}

async function sftpDownloadFile (remoteFile: string, localFile: string) {
  S3DB_Logging("info", "700", `Downloading ${remoteFile} to ${localFile} ...`)
  try
  {
    // await SFTPClient.get(remoteFile, localFile)
  } catch (err)
  {

    debugger //catch

    S3DB_Logging("exception", "", `Downloading failed: ${err}`)
  }
}

async function sftpDeleteFile (remoteFile: string) {
  S3DB_Logging("info", "700", `Deleting ${remoteFile}`)
  try
  {
    // await SFTPClient.delete(remoteFile)
  } catch (err)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Deleting failed: ${err}`)
  }
}



async function getValidateS3DropBucketConfig () {
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

  if (!process.env.S3DropBucketConfigBucket) process.env.S3DropBucketConfigBucket = "s3dropbucket-configs"

  if (!process.env.S3DropBucketConfigFile) process.env.S3DropBucketConfigFile = "s3dropbucket_config.jsonc"

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
  let s3dbc = {} as S3DBConfig

  try
  {
    s3dbc = await s3.send(new GetObjectCommand(getObjectCmd))
      .then(async (getConfigS3Result: GetObjectCommandOutput) => {
        s3dbcr = (await getConfigS3Result.Body?.transformToString(
          "utf8"
        )) as string

        S3DB_Logging("info", "912", `Pulling S3DropBucket Config File (bucket:${getObjectCmd.Bucket}  key:${getObjectCmd.Key}) \nResult: ${s3dbcr}`)

        //Fix extra space/invlaid formatting of "https:  //  ...." errors before parsing 
        s3dbcr = s3dbcr.replaceAll(new RegExp(/(?:(?:https)|(?:HTTPS)): \/\//gm), "https://")

        //Parse comments out of the json before returning parsed config json
        s3dbcr = s3dbcr.replaceAll(new RegExp(/[^:](\/\/.*(,|$|")?)/g), "")
        s3dbcr = s3dbcr.replaceAll(" ", "")
        s3dbcr = s3dbcr.replaceAll("\n", "")

        return JSON.parse(s3dbcr)
      })
  } catch (e)
  {

    debugger //catch

    S3DB_Logging("exception", "", `Exception - Pulling S3DropBucket Config File (bucket:${getObjectCmd.Bucket}  key:${getObjectCmd.Key}) \nResult: ${s3dbcr} \nException: \n${e} `)
    //return {} as s3DBConfig

    throw new Error(
      `Exception - Pulling S3DropBucket Config File(bucket: ${getObjectCmd.Bucket}  key: ${getObjectCmd.Key}) \nResult: ${s3dbcr} \nException: \n${e} `
    )


  }

  try
  {

    const validateBySchema = new Ajv()
    const su = "https://raw.githubusercontent.com/KWLandry-acoustic/s3dropbucket_Schemas/main/json-schema-s3dropbucket_config.json"
    const fetchSchema = async () => {
      return await fetch(su).then(async response => {
        if (response.status >= 400)
        {
          //throw new Error(`Fetch of schema error: ${JSON.stringify(response)}`)
          S3DB_Logging("error", "", `Fetch of schema error: ${JSON.stringify(response)}`)
        }
        const r = await response.json()
        return JSON.stringify(r)
      })
    }

    const sf = await fetchSchema() as string
    sf


    //ToDo Validate Configs through Schema definition
    //const vs = validateBySchema.validate(sf, s3dbc)

    //S3DB_Logging("info", "",`Schema Validation returns: ${vs}`)


    //  *Must* set EventEmitterMaxListeners in environment vars as this flags whether config is already parsed.
    if (!isNaN(s3dbc.s3dropbucket_eventemittermaxlisteners) && typeof s3dbc.s3dropbucket_eventemittermaxlisteners === "number") process.env["EventEmitterMaxListeners"] = s3dbc.s3dropbucket_eventemittermaxlisteners.toString()
    else
    {
      throw new Error(
        `S3DropBucket Config invalid or missing definition: EventEmitterMaxListeners.`
      )
    }


    //ToDo: refactor to a foreach validation instead of the Long List approach. 
    if (!s3dbc.s3dropbucket_loglevel || s3dbc.s3dropbucket_loglevel === "")
    {
      throw new Error(
        `S3DropBucket Config invalid definition: missing LogLevel.`
      )
    } else process.env.s3dropbucketLogLevel = s3dbc.s3dropbucket_loglevel


    if (!s3dbc.s3dropbucket_selectivelogging || s3dbc.s3dropbucket_selectivelogging === "")
    {
      throw new Error(
        `S3DropBucket Config invalid definition: missing SelectiveLogging.`
      )

    } else process.env["S3DropBucketSelectiveLogging"] = s3dbc.s3dropbucket_selectivelogging

    if (!s3dbc.s3dropbucket || s3dbc.s3dropbucket === "")
    {
      throw new Error(
        `S3DropBucket Config invalid definition: missing S3DropBucket (${s3dbc.s3dropbucket}).`
      )
    } else process.env["S3DropBucket"] = s3dbc.s3dropbucket

    if (!s3dbc.s3dropbucket_configs || s3dbc.s3dropbucket_configs === "")
    {
      throw new Error(
        `S3DropBucket Config invalid definition: missing S3DropBucketConfigs (${s3dbc.s3dropbucket_configs}).`
      )
    } else process.env["S3DropBucketConfigs"] = s3dbc.s3dropbucket_configs

    if (!s3dbc.s3dropbucket_workbucket || s3dbc.s3dropbucket_workbucket === "")
    {
      throw new Error(
        `S3DropBucket Config invalid definition: missing S3DropBucketWorkBucket (${s3dbc.s3dropbucket_workbucket}) `
      )
    } else process.env["S3DropBucketWorkBucket"] = s3dbc.s3dropbucket_workbucket

    if (!s3dbc.s3dropbucket_workqueue || s3dbc.s3dropbucket_workqueue === "")
    {
      throw new Error(
        `S3DropBucket Config invalid definition: missing S3DropBucketWorkQueue (${s3dbc.s3dropbucket_workqueue}) `
      )
    } else process.env["S3DropBucketWorkQueue"] = s3dbc.s3dropbucket_workqueue


    if (!s3dbc.connectapiurl || s3dbc.connectapiurl === "")
    {
      throw new Error(
        `S3DropBucket Config invalid definition: missing connectapiurl - ${s3dbc.connectapiurl} `)
    } else process.env["connectapiurl"] = s3dbc.connectapiurl

    if (!s3dbc.xmlapiurl || s3dbc.xmlapiurl === "")
    {
      throw new Error(
        `S3DropBucket Config invalid definition: missing xmlapiurl - ${s3dbc.xmlapiurl} `
      )
    } else process.env["xmlapiurl"] = s3dbc.xmlapiurl

    if (!s3dbc.restapiurl || s3dbc.restapiurl === "") 
    {
      throw new Error(
        `S3DropBucket Config invalid definition: missing restapiurl - ${s3dbc.restapiurl} `
      )
    } else process.env["restapiurl"] = s3dbc.restapiurl


    if (!s3dbc.authapiurl || s3dbc.authapiurl === "")
    {
      throw new Error(
        `S3DropBucket Config invalid definition: missing authapiurl - ${S3DBConfig.authapiurl} `
      )
    } else
      process.env["authapiurl"] = s3dbc.authapiurl


    //Default Separator
    if (!s3dbc.s3dropbucket_jsonseparator || s3dbc.s3dropbucket_jsonseparator === "")
    {
      throw new Error(
        `S3DropBucket Config invalid definition: missing jsonSeperator - ${s3dbc.s3dropbucket_workqueuequiesce} `
      )
    } else
    {
      if (s3dbc.s3dropbucket_jsonseparator.toLowerCase() === "null") s3dbc.s3dropbucket_jsonseparator = `''`
      if (s3dbc.s3dropbucket_jsonseparator.toLowerCase() === "empty") s3dbc.s3dropbucket_jsonseparator = `""`
      if (s3dbc.s3dropbucket_jsonseparator.toLowerCase() === "\n") s3dbc.s3dropbucket_jsonseparator = "\n"
      process.env["S3DropBucketJsonSeparator"] = s3dbc.s3dropbucket_jsonseparator
    }

    if (s3dbc.s3dropbucket_workqueuequiesce === true || s3dbc.s3dropbucket_workqueuequiesce === false) process.env["WorkQueueQuiesce"] = s3dbc.s3dropbucket_workqueuequiesce.toString()
    else
      throw new Error(
        `S3DropBucket Config invalid definition: WorkQueueQuiesce - ${s3dbc.s3dropbucket_workqueuequiesce} `
      )

    //process.env["RetryQueueInitialWaitTimeSeconds"]
    if (!isNaN(s3dbc.s3dropbucket_maxbatcheswarning) && typeof s3dbc.s3dropbucket_maxbatcheswarning === "number") process.env["MaxBatchesWarning"] = s3dbc.s3dropbucket_maxbatcheswarning.toFixed()
    else
      throw new Error(
        `S3DropBucket Config invalid definition: missing MaxBatchesWarning - ${s3dbc.s3dropbucket_maxbatcheswarning} `
      )

    if (s3dbc.s3dropbucket_quiesce === true || s3dbc.s3dropbucket_quiesce === false) process.env["S3DropBucketQuiesce"] = s3dbc.s3dropbucket_quiesce.toString()
    else
      throw new Error(
        `S3DropBucket Config invalid or missing definition: DropBucketQuiesce - ${s3dbc.s3dropbucket_quiesce} `
      )

    if (!isNaN(s3dbc.s3dropbucket_mainthours) && typeof s3dbc.s3dropbucket_mainthours === "number") process.env["S3DropBucketMaintHours"] = s3dbc.s3dropbucket_mainthours.toString()
    else
    {
      s3dbc.s3dropbucket_mainthours = -1
      process.env["S3DropBucketMaintHours"] = s3dbc.s3dropbucket_mainthours.toString()
    }

    if (!isNaN(s3dbc.s3dropbucket_maintlimit) && typeof s3dbc.s3dropbucket_maintlimit === "number") process.env["S3DropBucketMaintLimit"] = s3dbc.s3dropbucket_maintlimit.toString()
    else
    {
      s3dbc.s3dropbucket_maintlimit = 0
      process.env["S3DropBucketMaintLimit"] = s3dbc.s3dropbucket_maintlimit.toString()
    }

    if (!isNaN(s3dbc.s3dropbucket_maintconcurrency) && typeof s3dbc.s3dropbucket_maintconcurrency === "number") process.env["S3DropBucketMaintConcurrency"] = s3dbc.s3dropbucket_maintconcurrency.toString()
    else
    {
      s3dbc.s3dropbucket_maintlimit = 1
      process.env["S3DropBucketMaintConcurrency"] = s3dbc.s3dropbucket_maintconcurrency.toString()
    }

    if (!isNaN(s3dbc.s3dropbucket_workqueuemainthours) && typeof s3dbc.s3dropbucket_workqueuemaintlimit === "number") process.env["S3DropBucketWorkQueueMaintHours"] = s3dbc.s3dropbucket_workqueuemainthours.toString()
    else
    {
      s3dbc.s3dropbucket_workqueuemainthours = -1
      process.env["S3DropBucketWorkQueueMaintHours"] = s3dbc.s3dropbucket_workqueuemainthours.toString()
    }

    if (!isNaN(s3dbc.s3dropbucket_workqueuemaintlimit) && typeof s3dbc.s3dropbucket_workqueuemaintlimit === "number") process.env["S3DropBucketWorkQueueMaintLimit"] = s3dbc.s3dropbucket_workqueuemaintlimit.toString()
    else
    {
      s3dbc.s3dropbucket_workqueuemaintlimit = 0
      process.env["S3DropBucketWorkQueueMaintLimit"] = s3dbc.s3dropbucket_workqueuemaintlimit.toString()
    }

    if (!isNaN(s3dbc.s3dropbucket_workqueuemaintconcurrency) && typeof s3dbc.s3dropbucket_workqueuemaintconcurrency === "number") process.env["S3DropBucketWorkQueueMaintConcurrency"] = s3dbc.s3dropbucket_workqueuemaintconcurrency.toString()
    else
    {
      s3dbc.s3dropbucket_workqueuemaintconcurrency = 1
      process.env["S3DropBucketWorkQueueMaintConcurrency"] = s3dbc.s3dropbucket_workqueuemaintconcurrency.toString()
    }

    if (s3dbc.s3dropbucket_log === true || s3dbc.s3dropbucket_log === false)
    {
      process.env["S3DropBucketLog"] = s3dbc.s3dropbucket_log.toString()
    } else
    {
      s3dbc.s3dropbucket_log = false
      process.env["S3DropBucketLog"] = s3dbc.s3dropbucket_log.toString()
    }

    if (!s3dbc.s3dropbucket_logbucket || s3dbc.s3dropbucket_logbucket === "") s3dbc.s3dropbucket_logbucket = ""
    process.env["S3DropBucketLogBucket"] = s3dbc.s3dropbucket_logbucket.toString()

    if (!s3dbc.s3dropbucket_purge || s3dbc.s3dropbucket_purge === "")
    {
      throw new Error(
        `S3DropBucket Config invalid or missing definition: DropBucketPurge - ${s3dbc.s3dropbucket_purge} `
      )
    } else process.env["S3DropBucketPurge"] = s3dbc.s3dropbucket_purge

    if (!isNaN(s3dbc.s3dropbucket_purgecount) && typeof s3dbc.s3dropbucket_purgecount === "number") process.env["S3DropBucketPurgeCount"] = s3dbc.s3dropbucket_purgecount.toFixed()
    else
    {
      throw new Error(
        `S3DropBucket Config invalid or missing definition: S3DropBucketPurgeCount - ${s3dbc.s3dropbucket_purgecount} `
      )
    }

    if (s3dbc.s3dropbucket_queuebucketquiesce === true || s3dbc.s3dropbucket_queuebucketquiesce === false)
    {
      process.env["S3DropBucketQueueBucketQuiesce"] = s3dbc.s3dropbucket_queuebucketquiesce.toString()
    } else
      throw new Error(
        `S3DropBucket Config invalid or missing definition: QueueBucketQuiesce - ${s3dbc.s3dropbucket_queuebucketquiesce} `
      )

    if (!s3dbc.s3dropbucket_workqueuebucketpurge || s3dbc.s3dropbucket_workqueuebucketpurge === "")
    {
      throw new Error(
        `S3DropBucket Config invalid or missing definition: WorkQueueBucketPurge - ${s3dbc.s3dropbucket_workqueuebucketpurge} `
      )
    } else process.env["S3DropBucketWorkQueueBucketPurge"] = s3dbc.s3dropbucket_workqueuebucketpurge

    if (!isNaN(s3dbc.s3dropbucket_workqueuebucketpurgecount) && typeof s3dbc.s3dropbucket_workqueuebucketpurgecount === "number") process.env["S3DropBucketWorkQueueBucketPurgeCount"] = s3dbc.s3dropbucket_workqueuebucketpurgecount.toFixed()
    else
    {
      throw new Error(
        `S3DropBucket Config invalid or missing definition: WorkQueueBucketPurgeCount - ${s3dbc.s3dropbucket_workqueuebucketpurgecount} `
      )
    }

    if (s3dbc.s3dropbucket_prefixfocus && s3dbc.s3dropbucket_prefixfocus !== "" && s3dbc.s3dropbucket_prefixfocus.length > 3)
    {
      process.env["S3DropBucketPrefixFocus"] = s3dbc.s3dropbucket_prefixfocus
      S3DB_Logging("warn", "937", `A Prefix Focus has been configured. Only S3DropBucket Objects with the prefix "${s3dbc.s3dropbucket_prefixfocus}" will be processed.`)
    } else process.env["S3DropBucketPrefixFocus"] = s3dbc.s3dropbucket_prefixfocus



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



  } catch (e)
  {

    debugger //catch

    S3DB_Logging("exception", "", `Exception - Parsing S3DropBucket Config File ${e} `)
    throw new Error(`Exception - Parsing S3DropBucket Config File ${e} `)
  }
  return s3dbc
}

async function getFormatCustomerConfig (filekey: string) {

  //Populate/Refresh Customer Config List 
  if (process.env.S3DropBucketConfigBucket === "") process.env.S3DropBucketConfigBucket = "s3dropbucket-configs"
  const ccl = await getAllCustomerConfigsList(process.env.S3DropBucketConfigBucket ?? "s3dropbucket-configs")
  process.env.S3DropBucketCustomerConfigsList = JSON.stringify(ccl)
  
  // Retrieve file's prefix as the Customer Name
  if (!filekey)
    throw new Error(
      `Exception - Cannot resolve Customer Config without a valid filename (filename is ${filekey})`
    )

  let customer = filekey


  //Need to 'normalize' filename by removing Path details
  while (customer.indexOf("/") > -1)
  {
    //remove any folders from name
    customer = customer.split("/").at(-1) ?? customer
  }

  //Normalize if timestamp - normalize timestamp out 
  const r = new RegExp(/\d{4}_\d{2}_\d{2}T.*Z.*/, "gm")
  //remove timestamps from name as can confuse customer name parsing
  if (customer.match(r))
  {
    customer = customer.replace(r, "") //remove timestamp from name
  }

  //Normalize if Aggregator File
  //Check for Aggregator File Name - normalize Aggregator file string out 
  const r2 = new RegExp(/(S3DropBucket_Aggregator.*)/, "gm")
  //remove Aggregator File String from name as can confuse dataflow name parsing
  if (customer.match(r2))
  {
    customer = customer.replace(r2, "") //remove S3DropBucket_Aggregator and rest of string from name
  }


  //Now, need to 'normalize' (deconstruct) all other strings that are possible to arrive at (reconstruct) a valid dataflow name with a trailing underscore 
  if (customer.lastIndexOf('_') > 3)   //needs to have at least 4 chars for dataflow name 
  {
    let i = customer.lastIndexOf('_')
    customer = customer.substring(0, i)
    let ca = customer.split('_')
    let c = ""
    for (const n in ca)
    {
      c += ca[n] + '_'
    }
    customer = c
  } else
  {
    S3DB_Logging("exception", "", `Exception - Parsing File Name for Dataflow Config Name returns: ${customer}}. Cannot continue.`)
    throw new Error(`Exception - Parsing File Name for Dataflow Config Name returns: ${customer}}. Cannot continue.`)
  }

  //Should be left with valid Dataflow Name, data flow and trailing underscore
  if (!customer.endsWith('_'))
  {
    throw new Error(
      `Exception - Cannot resolve Customer Config without a valid Customer Prefix (filename is ${filekey})`
    )
  }


  //ToDo: Need to change this up to getting a listing of all configs and matching up against filename,
  //  allowing Configs to match on filename regardless of case
  //  populate 'process.env.S3DropBucketConfigsList' and walk over it to match config to filename

  let ccKey = `${customer}config.jsonc`

  //Match File to Customer Config file
  //ToDo: test and confirm 
  try
  {
    const cclist = JSON.parse(process.env.S3DropBucketCustomerConfigsList ?? "")
    for (const i in cclist)
    {
      if (cclist[i].toLowerCase() === `${customer}config.jsonc`.toLowerCase()) ccKey = `${cclist[i]}`
      break
    }
  } catch (e)
  {

    debugger //catch

    S3DB_Logging("exception", "", `Matching filename to Customer config : \n${e}`)
  }

  const getObjectCommand = {
    Key: ccKey,
    //Bucket: 's3dropbucket-configs'
    Bucket: process.env.S3DropBucketConfigBucket,
  }

  let ccr
  let configJSON = customersConfig as CustomerConfig

  try
  {
    ccr = await s3
      .send(new GetObjectCommand(getObjectCommand))
      .then(async (getConfigS3Result: GetObjectCommandOutput) => {

        let cc = (await getConfigS3Result.Body?.transformToString(
          "utf8"
        )) as string

        S3DB_Logging("info", "910", `Customer (${customer}) Config: \n ${cc.trim()} `)

        //S3DB_Logging("info", "910", `Customer (${customer}) Config: \n ${cc.replace(/\s/g, '')} `)

        //Remove Schema line to avoid parsing error
        cc = cc.replaceAll(new RegExp(/^.*?"\$schema.*?$/gm), "")

        //Parse comments out of the json before parse for config
        cc = cc.replaceAll(new RegExp(/[^:](\/\/.*(,|$|")?)/g), "")
        const cc1 = cc.replaceAll("\n", "")

        //Parse comments out of the json before parse
        //const ccr1 = ccr.replaceAll(new RegExp(/(".*":)/g), (match) => match.toLowerCase())
        const cc2 = cc1.replaceAll(new RegExp(/(\/\/.*(?:$|\W|\w|\S|\s|\r).*)/gm), "")
        const cc3 = cc2.replaceAll(new RegExp(/\n/gm), "")
        cc = cc3.replaceAll("   ", "")

        return cc
      })
      .catch((e) => {

        debugger //catch

        const err: string = JSON.stringify(e)

        if (err.indexOf("specified key does not exist") > -1)
          throw new Error(
            `Exception - Customer Config - ${customer}config.jsonc does not exist on ${S3DBConfig.s3dropbucket_configs} bucket (while processing ${filekey}) \nException ${e} `
          )

        if (err.indexOf("NoSuchKey") > -1)
          throw new Error(
            `Exception - Customer Config Not Found(${customer}config.jsonc) on ${S3DBConfig.s3dropbucket_configs}. \nException ${e} `
          )

        throw new Error(
          `Exception - Retrieving Config (${customer}config.jsonc) from ${S3DBConfig.s3dropbucket_configs}. \nException ${e} `
        )
      })

  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - On Try when Pulling Customer Config \n${ccr}. \n${e} `)
    throw new Error(`Exception - (Try) Pulling Customer Config \n${ccr} \n${e} `)
  }

  configJSON = JSON.parse(ccr) as CustomerConfig

  //Potential Transform opportunity (values only)
  //configJSON = JSON.parse(ccr, function (key, value) {
  //  return value
  //});

  const setPropsLowCase = (object: object, container: string) => {
    type okt = keyof typeof object
    for (const key in object)
    {
      const k = key as okt

      const lk = (key as string).toLowerCase()

      if (container.match(new RegExp(/contactid|contactkey|consent|audienceupdate|addressablefields|methods|jsonmap|csvmap|ignore/))) continue

      //if (typeof object[k] === "object") setPropsLowCase(object[k])
      if (Object.prototype.toString.call(object[k]) === "[object Object]") setPropsLowCase(object[k], lk)

      if (k === lk) continue
      object[lk as okt] = object[k]
      delete object[k]
    }
    return object
  }

  configJSON = setPropsLowCase(configJSON, '') as CustomerConfig

  configJSON = await validateCustomerConfig(configJSON) as CustomerConfig

  return configJSON as CustomerConfig
}

async function validateCustomerConfig (config: CustomerConfig) {

  try
  {
    if (!config || config === null)
    {
      throw new Error("Invalid  CustomerConfig - empty or null config")
    }

    if (!config.targetupdate)
    {
      {
        throw new Error(
          "Invalid Customer Config - TargetUpdate is required and must be either 'Connect' or 'Campaign'  "
        )
      }
    }

    if (!config.targetupdate.toLowerCase().match(/^(?:connect|campaign)$/gim))
    {
      throw new Error(
        "Invalid Customer Config - TargetUpdate is required and must be either 'Connect' or 'Campaign'  "
      )
    }


    if (!config.updatetype)
    {
      throw new Error("Invalid Customer Config - updatetype is not defined")
    }


    ////Confirm updatetype has a valid value
    //  if ( !config.updatetype.toLowerCase().match(/^(?:relational|dbkeyed|dbnonkeyed|referenceset|createupdatecontacts|createattributes)$/gim)
    //    //DBKeyed, DBNonKeyed, Relational, ReferenceSet, CreateUpdateContacts, CreateAttributes
    //  )
    //  {
    //    throw new Error(
    //      "Invalid Customer Config - updatetype is required and must be either 'Relational', 'DBKeyed' or 'DBNonKeyed', ReferenceSet, CreateUpdateContacts, CreateAttributes. "
    //    )
    //  }

    if (config.targetupdate.toLowerCase() == "campaign")
    {
      //updatetype has valid Campaign values and Campaign dependent values
      if (!config.updatetype.toLowerCase().match(/^(?:relational|dbkeyed|dbnonkeyed)$/gim))
      //DBKeyed, DBNonKeyed, Relational
      {
        throw new Error(
          "Invalid Customer Config - Update set to be Campaign, however updatetype is not Relational, DBKeyed, or DBNonKeyed. "
        )
      }

      if (config.updatetype.toLowerCase() == "dbkeyed" && !config.dbkey)
      {
        throw new Error(
          "Invalid Customer Config - Update set as Database Keyed but DBKey is not defined. "
        )
      }

      if (config.updatetype.toLowerCase() == "dbnonkeyed" && config.lookupkeys.length <= 0)
      {
        throw new Error(
          "Invalid Customer Config - Update set as Database NonKeyed but LookupKeys is not defined. "
        )
      }

      if (!config.clientid)
      {
        throw new Error("Invalid Customer Config - Target Update is Campaign but ClientId is not defined")
      }
      if (!config.clientsecret)
      {
        throw new Error("Invalid Customer Config - Target Update is Campaign but ClientSecret is not defined")
      }
      if (!config.refreshtoken)
      {
        throw new Error("Invalid Customer Config - Target Update is Campaign but RefreshToken is not defined")
      }


      if (!config.listid)
      {
        throw new Error("Invalid Customer Config - ListId is not defined")
      }
      if (!config.listname)
      {
        throw new Error("Invalid Customer Config - Target Update is Campaign but ListName is not defined")
      }
      if (!config.pod)
      {
        throw new Error("Invalid Customer Config - Target Update is Campaign but Pod is not defined")
      }
      if (!config.region)
      {
        //Campaign POD Region
        throw new Error("Invalid Customer Config - Target Update is Campaign but Region is not defined")
      }

      if (!config.region.toLowerCase().match(/^(?:us|eu|ap|ca)$/gim))
      {
        throw new Error(
          "Invalid Customer Config - Region is not 'US', 'EU', CA' or 'AP'. "
        )
      }

    }

    if (config.targetupdate.toLowerCase() === "connect")
    {
      //updatetype has valid Campaign values and Campaign dependent values
      if (config.updatetype.toLowerCase().match(/^(?:referenceset|createupdatecontacts|createattributes)$/gim))
      {
        if (!config.datasetid)
        {
          throw new Error("Invalid Customer Config - Target Update is Connect but DataSetId is not defined")
        } if (!config.subscriptionid)
        {
          throw new Error("Invalid Customer Config - Target Update is Connect but SubscriptionId is not defined")
        }
        if (!config.x_api_key)
        {
          throw new Error("Invalid Customer Config - Target Update is Connect but X-Api-Key is not defined")
        }
        if (!config.x_acoustic_region)
        {
          throw new Error("Invalid Customer Config - Target Update is Connect but X-Acoustic-Region is not defined")
        }

        if (config.x_acoustic_region.toLowerCase().match(/^(?:"us-east-1"| "us-east-2"| "us-west-1"| "us-west-2"| "af-south-1"| "ap-east-1"| "ap-south-1"| "ap-south-2"| "ap-southeast-1"| "ap-southeast-2"| "ap-southeast-3"| "ap-southeast-4"| "ap-northeast-1"| "ap-northeast-2"| "ap-northeast-3"| "ca-central-1"| "eu-central-1"| "eu-central-2"| "eu-north-1"| "eu-south-1"| "eu-south-2"| "eu-west-1"| "eu-west-2"| "eu-west-3"| "il-central-1"| "me-central-1"| "me-south-1"| "sa-east-1" )$/gim))
        {
          throw new Error("Invalid Customer Config - Target Update is Connect but Region is incorrect or undefined")
        }

      }
    }


    if (!config.updates)
    {
      throw new Error("Invalid Customer Config - Updates is not defined")
    }

    if (!config.updates.toLowerCase().match(/^(?:singular|multiple|bulk)$/gim))
    {
      throw new Error(
        "Invalid Customer Config - Updates is not 'Singular' or 'Multiple' "
      )
    }

    //Remove legacy config value
    if (config.updates.toLowerCase() === "bulk") config.updates = "Multiple"


    if (!config.format)
    {
      throw new Error("Invalid Customer Config - Format is not defined")
    }
    if (!config.format.toLowerCase().match(/^(?:csv|json)$/gim))
    {
      throw new Error("Invalid Customer Config - Format is not 'CSV' or 'JSON' ")
    }

    if (!config.separator)
    {
      //see: https://www.npmjs.com/package/@streamparser/json-node
      //JSONParser / Node separator option: null = `''` empty = '', otherwise a separator eg. '\n'
      config.separator = "\n"
    }

    //Customer specific separator
    if (config.separator.toLowerCase() === "null") config.separator = `''`
    if (config.separator.toLowerCase() === "empty") config.separator = `""`
    if (config.separator.toLowerCase() === "\n") config.separator = "\n"


    if (!config.pod.match(/^(?:0|1|2|3|4|5|6|7|8|9|a|b)$/gim))
    {
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

    switch (config.pod.toLowerCase())
    {
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




    if (!config.sftp)
    {
      config.sftp = {user: "", password: "", filepattern: "", schedule: ""}
    }

    if (config.sftp.user && config.sftp.user !== "")
    {
      S3DB_Logging("info", "700", `SFTP User: ${config.sftp.user}`)
    }
    if (config.sftp.password && config.sftp.password !== "")
    {
      S3DB_Logging("info", "700", `SFTP Pswd: ${config.sftp.password}`)
    }
    if (config.sftp.filepattern && config.sftp.filepattern !== "")
    {
      S3DB_Logging("info", "700", `SFTP File Pattern: ${config.sftp.filepattern}`)
    }
    if (config.sftp.schedule && config.sftp.schedule !== "")
    {
      S3DB_Logging("info", "700", `SFTP Schedule: ${config.sftp.schedule}`)
    }


    if (!config.transforms.consent)
    {
      Object.assign(config.transforms, {consent: {}})
    }

    if (!config.transforms.audienceupdate)
    {
      Object.assign(config.transforms, {audienceupdate: {}})
    }

    if (!config.transforms)
    {
      Object.assign(config, {transforms: {}})
    }

    if (!config.transforms.contactid)
    {
      Object.assign(config.transforms, {contactid: {}})
    }

    if (!config.transforms.contactkey)
    {
      Object.assign(config.transforms, {contactkey: {}})
    }

    if (!config.transforms.addressablefields)
    {
      Object.assign(config.transforms, {addressablefields: {}})
    }

    if (!config.transforms.methods)
    {
      Object.assign(config.transforms, {methods: {}})
    }

    if (!config.transforms.methods.daydate)
    {
      Object.assign(config.transforms.methods, {daydate: {}})
    }

    if (!config.transforms.methods.date_iso1806_type)
    {
      Object.assign(config.transforms.methods, {date_iso1806_type: {}})
    }

    if (!config.transforms.methods.phone_number_type)
    {
      Object.assign(config.transforms.methods, {phone_number_type: {}})
    }

    if (!config.transforms.methods.string_to_number_type)
    {
      Object.assign(config.transforms.methods, {string_to_number_type: {}})
    }


    if (!config.transforms.jsonmap)
    {
      Object.assign(config.transforms, {jsonmap: {}})
    }
    if (!config.transforms.csvmap)
    {
      Object.assign(config.transforms, {csvmap: {}})
    }
    if (!config.transforms.ignore)
    {
      Object.assign(config.transforms, {ignore: []})
    }



    S3DB_Logging("info", "919", `Transforms configured in Customer Config: \n${JSON.stringify(config.transforms)}`)

    if (config.transforms.jsonmap)
    {
      const tmpMap: {[key: string]: string} = {}
      const jm = config.transforms.jsonmap as {[key: string]: string}
      for (const m in jm)
      {
        const p = jm[m]
        const p2 = p.substring(2, p.length)

        if (["contactid", "contactkey", "addressablefields", "consent", "audience"].includes(m.toLowerCase()) ||
          ["contactid", "contactkey", "addressablefields", "consent", "audience"].includes(p2.toLowerCase())
        )
        {
          S3DB_Logging("error", "999", `JSONMap config: Either part of the JSONMap statement, the Column to create or the JSONPath statement, cannot use a reserved word ("contactid", "contactkey", "addressablefields", "consent", or "audience") ${m}: ${p}`)

          throw new Error(`JSONMap config: Either part of the JSONMap statement, the Column to create or the JSONPath statement, cannot reference a reserved word ("contactid", "contactkey", "addressablefields", "consent", or "audience") ${m}: ${p}`)
        }
        else
        {
          try
          {
            const v = jsonpath.parse(p)  //checking for parse exception highlighting invalid jsonpath
            tmpMap[m] = jm[m]
          } catch (e)
          {
            debugger //catch

            S3DB_Logging("exception", "", `Invalid JSONPath defined in Customer config: ${m}: "${m}", \nInvalid JSONPath - ${e} `)
          }
        }
      }
      config.transforms.jsonmap = tmpMap
    }

  } catch (e)
  {
    debugger //catch

    S3DB_Logging("error", "", `Exception - Validate Customer Config: \n${e}`)
  }


  return config as CustomerConfig
}

async function packageUpdates (workSet: object[], key: string, custConfig: CustomerConfig, iter: number
) {

  let sqwResult: object = {}

  S3DB_Logging("info", "918", `Packaging ${workSet.length} updates from ${key} (File Stream Iter: ${iter}). \nBatch count so far ${batchCount}. `)

  //Check if these updates are to be Aggregated or this is an Aggregated file coming through. 

  // If there are Chunks to Process and Singular Updates is set, 
  //    send to Aggregator (unless these are updates coming through FROM an Aggregated file).
  if (
    key.toLowerCase().indexOf("s3dropbucket_aggregator") < 0 &&     //This is Not an Aggregator file
    custConfig.updates.toLowerCase() === "singular" &&              //Cust Config marks these updates to be Aggregated when coming through
    workSet.length > 0)                                             //There are Updates to be processed 
  {

    let firehoseResults = {}

    try   //Interior try/catch for firehose processing
    {
      //A Check for when long processing flow does not provide periodic updates
      //fhi++      
      //if (fhi % 100 === 0)
      //{
      //  S3DB_Logging("info", "918", `Processing update ${fhi} of ${chunks.length} updates for ${custConfig.customer}`)
      // 
      //}

      firehoseResults = await putToFirehose(
        workSet,
        key,
        custConfig.customer,
        iter
      ).then((res) => {
        const fRes = res as {
          //OnEnd_PutToFireHoseAggregator: string
          PutToFireHoseAggregatorResult: string
          PutToFireHoseException: string
        }

        sqwResult = {
          ...sqwResult,
          ...fRes,
        }

        return sqwResult

      })
    } catch (e)    //Interior try/catch for firehose processing
    {
      debugger //catch

      S3DB_Logging("exception", "", `Exception - PutToFirehose (File Stream Iter: ${iter}) - \n${e} `)

      sqwResult = {
        ...sqwResult,
        PutToFireHoseException: `Exception - PutToFirehose \n${e} `,
      }
    }

    S3DB_Logging("info", "943", `Completed FireHose Aggregator processing ${key} (File Stream Iter: ${iter}) \n${JSON.stringify(sqwResult)}`)

    return sqwResult = {
      ...sqwResult,
      PutToFireHoseResults: `PutToFirehose Results: \n${JSON.stringify(firehoseResults)} `,
    }

  } else        //Based on outcome of If - Not Firehose Aggregator work so need to package to Connect or Campaign
  {
    //Ok, the work to be Packaged is either from a "Multiple" updates Customer file or from an "Aggregated" file. 
    let updates: object[] = []

    try
    {

      //Process everything passed, especially if there are more than 100 passed at one time, or fewer than 100. 
      while (workSet.length > 0)
      {
        updates = [] as object[]
        while (workSet.length > 0 && updates.length < 100)
        {
          const c = workSet.pop() ?? {}

          updates.push(c)
        }

        //Ok, now send to appropriate staging for actually updating endpoint (Campaign or Connect)
        if (custConfig.targetupdate.toLowerCase() === "connect")
          sqwResult = await storeAndQueueConnectWork(updates, key, custConfig, iter)
            .then((res) => {
              //console.info( `Debug Await StoreAndQueueWork (Connect) Result: ${ JSON.stringify( res ) }` )
              return res
            })
        else if (custConfig.targetupdate.toLowerCase() === "campaign")
        {
          sqwResult = await storeAndQueueCampaignWork(updates, key, custConfig, iter)
            .then((res) => {
              //console.info( `Debug Await StoreAndQueueWork (Campaign) Result: ${ JSON.stringify( res ) }` )
              return res
            })
        }
        else
        {
          throw new Error(`Target for Update does not match any Target: ${custConfig.targetupdate}`)
        }

        S3DB_Logging("info", "921", `PackageUpdates StoreAndQueueWork for ${key} (File Stream Iter: ${iter}). \nFor a total of ${recs} Updates in ${batchCount} Batches.  Result: \n${JSON.stringify(sqwResult)} `)
      }
    } catch (e)
    {
      debugger //catch

      S3DB_Logging("exception", "", `Exception - packageUpdates for ${key} (File Stream Iter: ${iter}) \n${e} `)

      sqwResult = {
        ...sqwResult,
        StoreQueueWorkException: `Exception - PackageUpdates StoreAndQueueWork for ${key} (File Stream Iter: ${iter}) \nBatch ${batchCount} of ${recs} Updates. \n${e} `,
      }
    }
  }

  return sqwResult
}



async function storeAndQueueConnectWork (
  updates: object[],
  s3Key: string,
  custConfig: CustomerConfig,
  iter: number
) {

  if (batchCount > S3DBConfig.s3dropbucket_maxbatcheswarning && batchCount % 100 === 0)
    S3DB_Logging("info", "", `Warning: Updates from the S3 Object(${s3Key}) (File Stream Iter: ${iter}) are exceeding (${batchCount}) the Warning Limit of ${S3DBConfig.s3dropbucket_maxbatcheswarning} Batches per Object.`)

  const updateCount = updates.length

  //Customers marked as "Singular" updates files are not transformed, but sent to Firehose prior to getting here.
  //  therefore if Aggregate file, or files config'd as "Multiple" updates, then need to perform Transforms before queuing up the work
  try
  {

    //Apply Transforms, if any, 
    updates = transforms(updates, custConfig)
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - Transforms - ${e}`)
    throw new Error(`Exception - Transforms - ${e}`)
  }



  S3DB_Logging("info", "800", `After Transform (Updates: ${updateCount}. File Stream Iter: ${iter}): \n${JSON.stringify(updates)}`)

  let mutations
  ////DBKeyed, DBNonKeyed, Relational, ReferenceSet, CreateUpdateContacts, CreateAttributes
  //if (customersConfig.updatetype.toLowerCase() === "createupdatecontacts") res = ConnectCreateMultipleContacts()
  //if (customersConfig.updatetype.toLowerCase() === "createattributes") res = ConnectCreateAttributes()
  ////if (true) res = ConnectReferenceSet().then((m) => {return m})
  //const mutationCall = JSON.stringify(res)
  //const m = buildConnectMutation(JSON.parse(updates))


  // ReferenceSet   -    Need to establish SFTP and Job Creation for this
  // CreateContacts   - Done - CreateUpdateContacts call as Create will also Update
  // UpdateContacts    - Done
  // Audience - Done - Transform
  // Consent - Done - Transform
  // ContactKey - Done - Transform 
  // ContactId - Done - Transform
  // AddressableFields - Done - Transform


  //For now will need to treat Reference Sets completely differently until an API shows up, 
  // hopefully similar to Contacts API

  if (customersConfig.updatetype.toLowerCase() === "createupdatecontacts" ||
    customersConfig.updatetype.toLowerCase() === "referenceset")
  {
    mutations = await buildMutationsConnect(updates, custConfig)
      .then((r) => {
        return r
      })
  }

  const mutationWithUpdates = JSON.stringify(mutations)

  ////  Testing - Call POST to Connect immediately
  //if (localTesting)
  //{
  //  S3DB_Logging("info", "855", `Testing - GraphQL Call (${S3DBConfig.connectapiurl}) Updates: \n${mutationUpdates}`)
  //  const c = await postToConnect(mutationUpdates, customersConfig, "6", s3Key)
  //  debugger ///
  //}
  

  //Derive Key Name for Update File
  if (s3Key.indexOf("TestData") > -1)
  {
    //strip /testdata folder from key
    s3Key = s3Key.split("/").at(-1) ?? s3Key
  }

  let key = s3Key

  while (key.indexOf("/") > -1)
  {
    key = key.split("/").at(-1) ?? key
  }

  key = key.replace(".", "-")

  key = `${key}-update-${batchCount}-${updateCount}-${uuidv4()}.json`

  //if ( Object.values( updates ).length !== recs )
  //{
  //     selectiveLogging("error", "", `Recs Count ${recs} does not reflect Updates Count ${Object.values(updates).length} `)
  //}

  S3DB_Logging("info", "811", `Queuing Work File ${key} for ${s3Key}. Batch ${batchCount} of ${updateCount} records)`)

  let addWorkToS3WorkBucketResult
  let addWorkToSQSWorkQueueResult

  try
  {
    addWorkToS3WorkBucketResult = await addWorkToS3WorkBucket(mutationWithUpdates, key)
      .then((res) => {
        return {"workfile": key, ...res} //{"AddWorktoS3Results": res}
      })
      .catch((err) => {
        debugger //catch

        S3DB_Logging("exception", "", `Exception - AddWorkToS3WorkBucket (file: ${key}) ${err}`)
      })
  } catch (e)
  {
    debugger //catch

    const s3StoreError = `Exception - StoreAndQueueWork Add work (file: ${key}) to S3 Work Bucket exception \n${e} `
    S3DB_Logging("exception", "", s3StoreError)

    return {
      StoreS3WorkException: s3StoreError,
      StoreQueueWorkException: "",
      AddWorkToS3WorkBucketResults: JSON.stringify(addWorkToS3WorkBucketResult),
    }
  }

  const marker = "Initially Queued on " + new Date()

  try
  {
    addWorkToSQSWorkQueueResult = await addWorkToSQSWorkQueue(
      custConfig,
      key,
      //v,
      batchCount,
      updates.length.toString(),
      marker
    ).then((res) => {
      
      //S3DB_Logging(rawresult logging)
      
      return res
    })
  } catch (e)
  {
    debugger //catch

    const sqwError = `Exception - StoreAndQueueWork Add work to SQS Queue exception \n${e} `
    S3DB_Logging("exception", "", sqwError)

    return {StoreQueueWorkException: sqwError}
  }

  S3DB_Logging("info", "915", `Results of Storing and Queuing (Connect) Work ${key} to Work Queue: ${JSON.stringify(addWorkToSQSWorkQueueResult)} 
  \n${JSON.stringify(addWorkToS3WorkBucketResult)}`)

  return {
    AddWorkToS3WorkBucketResults: addWorkToS3WorkBucketResult,
    AddWorkToSQSWorkQueueResults: addWorkToSQSWorkQueueResult,
  }
}


async function storeAndQueueCampaignWork (
  updates: object[],
  s3Key: string,
  config: CustomerConfig,
  iter: number
) {

  if (batchCount > S3DBConfig.s3dropbucket_maxbatcheswarning)
    S3DB_Logging("info", "", `Warning: Updates from the S3 Object(${s3Key}) (File Stream Iter: ${iter}) are exceeding(${batchCount}) the Warning Limit of ${S3DBConfig.s3dropbucket_maxbatcheswarning} Batches per Object.`)

  // throw new Error(`Updates from the S3 Object(${ s3Key }) Exceed(${ batch }) Safety Limit of 20 Batches of 99 Updates each.Exiting...`)

  const updateCount = updates.length

  //Customers marked as "Singular" updates files are not transformed, but sent to Firehose prior to getting here.
  // therefore if this is an Aggregate file, or is a file config'd to be "Multiple" updates, then need to perform Transforms now
  try
  {
    //Apply Transforms, if any, 
    updates = transforms(updates, config)
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - Transforms - ${e}`)
    throw new Error(`Exception - Transforms - ${e}`)
  }

  if (
    customersConfig.updatetype.toLowerCase() === "dbkeyed" ||
    customersConfig.updatetype.toLowerCase() === "dbnonkeyed"
  )
  {
    xmlRows = convertJSONToXML_DBUpdates(updates, config)
  }

  if (customersConfig.updatetype.toLowerCase() === "relational")
  {
    xmlRows = convertJSONToXML_RTUpdates(updates, config)
  }

  //ToDo: refactor this above this function
  if (s3Key.indexOf("TestData") > -1)
  {
    //strip /testdata folder from key
    s3Key = s3Key.split("/").at(-1) ?? s3Key
  }

  let key = s3Key

  while (key.indexOf("/") > -1)
  {
    key = key.split("/").at(-1) ?? key
  }

  key = key.replace(".", "_")

  key = `${key}-update-${batchCount}-${updateCount}-${uuidv4()}.xml`

  //if ( Object.values( updates ).length !== recs )
  //{
  //     selectiveLogging("error", "", `Recs Count ${recs} does not reflect Updates Count ${Object.values(updates).length} `)
  //}


  let addWorkToS3WorkBucketResult
  let addWorkToSQSWorkQueueResult

  try
  {
    addWorkToS3WorkBucketResult = await addWorkToS3WorkBucket(xmlRows, key)
      .then((res) => {
        return {"workfile": key, ...res}  //return res 
      })
      .catch((err) => {
        debugger //catch

        S3DB_Logging("exception", "", `Exception - AddWorkToS3WorkBucket ${err} (File Stream Iter: ${iter} file: ${key})`)
      })
  } catch (e)
  {
    debugger //catch

    const s3StoreError = `Exception - StoreAndQueueWork Add work (File Stream Iter: ${iter} (file: ${key})) to S3 Bucket exception \n${e} `

    S3DB_Logging("exception", "", s3StoreError)

    return {
      StoreS3WorkException: s3StoreError,
      StoreQueueWorkException: "",
      AddWorkToS3WorkBucketResults: JSON.stringify(addWorkToS3WorkBucketResult),
    }
  }

  //S3DB_Logging(oppty to message s3 store results)

  const marker = "Initially Queued on " + new Date()

  try
  {
    addWorkToSQSWorkQueueResult = await addWorkToSQSWorkQueue(
      config,
      key,
      batchCount,
      updates.length.toString(),
      marker
    ).then((res) => {

      //S3DB_Logging(oppty for rawresult logging)
      
      return res
    })
  } catch (e)
  {
    debugger //catch

    const sqwError = `Exception - StoreAndQueueWork Add work to SQS Queue exception \n${e} `
    S3DB_Logging("exception", "", sqwError)

    return {StoreQueueWorkException: sqwError}
  }

  
  //If we made it this Far, all's good 
  S3DB_Logging("info", "915", `Results of Storing and Queuing (Campaign) Work ${key} to Work Queue: ${JSON.stringify(addWorkToSQSWorkQueueResult)} \n${JSON.stringify(
    addWorkToS3WorkBucketResult)}`)

  return {
    AddWorkToS3WorkBucketResults: addWorkToS3WorkBucketResult,
    AddWorkToSQSWorkQueueResults: addWorkToSQSWorkQueueResult,
  }
}


async function buildMutationsConnect (updates: object[], config: CustomerConfig) {

  let mutation
  let variables
  /*
    try
    {
      if (config.updatetype.toLowerCase() === "referenceset")
      {
        
    mutation = `mutation CreateImportJob {
      createImportJob(
          importInput: {
              fileFormat: DELIMITED
              delimiter: "\\n"
              importType: ADD_UPDATE
              notifications: null
              mappings: { columnIndex: null, attributeName: null }
              createSegment: true
              segmentName: "S3DBSegment"
              consent: {
                  enableOverrideExistingOptOut: null
                  channels: null
                  consentGroups: null
              }
              dateFormat: MONTH_DAY_YEAR_SLASH_SEPARATED
              dataSetId: "1234"
              dataSetName: "xyz"
              dataSetType: REFERENCE_SET
              jobName: "S3DBRefSet"
              fileLocation: { type: SFTP, folder: "S3DBFolder", filename: "S3DBFile" }
              attributes: { create: null }
              # When columnIndex is used for mapping, skipFirstRow determines whether the first row contains headers that should not be ingested as data When columnHeader is used for mapping, skipFirstRow must either not be supplied, or set to true.
              skipFirstRow: null
          }
      ) {
          id
      }
  }`
  
      mutation = {
          mutation: {
            createImportJob: {
              importInput: {
                fileFormat: "DELIMITED",
                delimiter: "\n",
                importType: "ADD_UPDATE",
                mappings: {
                  columnIndex: 0,
                  attributeName: "id"
                },
                createSegment: true,
                segmentName: "S3DBSegment",
                consent: {
                  enableOverrideExistingOptOut: false,
                  channels: [],
                  consentGroups: []
                },
                dateFormat: "MONTH_DAY_YEAR_SLASH_SEPARATED",
                dataSetId: "1234",
                dataSetName: "xyz",
                dataSetType: "REFERENCE_SET",
                jobName: "S3DBRefSet",
                fileLocation: {
                  type: "SFTP",
                  folder: "S3DBFolder",
                  filename: "S3DBFile"
                },
                attributes: {
                  create: []
                },
                skipFirstRow: false
              }
            },
            fields: {
              id: true,
              status: true,
              message: true
            }
          }
        }
  
        mutation.mutation.createImportJob.importInput.
  
        
  
        //let createVariables = {} as CreateContactsVariables
        variables = {dataSetId: config.datasetid, contactsInput: []} as CreateContactsVariables
  
        for (const upd in updates)
        {
          //Audience is just a placeholder for now, so removing until it is valid in graphQL type
          //let ccr: CreateContactsInput = {"to": {"audience":[], "consent":[], "attributes": []}}
  
          let ca: ContactAttribute = {name: "", value: ""}
          let ccr: CreateContactRecord = {attributes: []}
          let cci: CreateContactInput = {attributes: []}
  
          variables.contactsInput.push(cci)
  
          //Build ContactId, ContactKey, AddressableFields, Consent, Audience properties
          const u = updates[upd] as Record<string, any>
  
          //ToDo: Add logic CreateContact vs UpdateContact Key/Id/Addressable fields
          //Create:
          //No Key, No Addressable but UniqueId must be in the data
          //if (typeof u.contactId !== "undefined") Object.assign(variables.contactsInput[upd], {contactId: u.contactId})
          //else if (typeof u.contactKey !== "undefined") Object.assign(variables.contactsInput[upd], {key: u.contactKey})
          //else if (typeof u.addressable !== "undefined") Object.assign(variables.contactsInput[upd], {addressable: u.addressable})
  
          //if (typeof u.consent !== "undefined") Object.assign(variables.contactsInput, {consent: u.consent})
          if (typeof u.consent !== "undefined") Object.assign(cci, {consent: u.consent})
  
  
          //Audience is just a placeholder for now, so removing until it is valid in graphQL type
          //if (typeof u.audience !== "undefined") Object.assign(variables.contactsInput, {audience: u.audience})
  
          //Add S3DBConfirmation value as a means to quickly confirm Testing outcomes
          const now: Date = new Date()
          const date: string = now.toLocaleDateString()
          const time: string = now.toLocaleTimeString()
          cci.attributes.push({name: "S3DBConfirmation", value: date + " - " + time})
          //variables.contactsInput[upd].attributes[upd] = {name: "S3DBConfirmation", value: date + " - " + time}
  
  
          //Build Attribute Array from each row of inbound data
          for (const [key, value] of Object.entries(u))
          {
            //let v
            //if (typeof value === "string") v = value
            //else v = String(value)
  
            //Skip Payload Vars, Vars injected to carry Transformed Values and are already processed above,
            // and are not valid for the actual Update
            if (!["contactid", "contactkey", "addressable", "consent", "audience"].includes(key.toLowerCase()))
            {
              ca = {name: key, value: value}
              cci.attributes.push(ca)
            }
  
          }
          Object.assign(variables.contactsInput[upd], cci)
          //variables.contactsInput.push(cci)
        }
  
        S3DB_Logging("info", "817", `Create Multiple Contacts Mutation: \n${mutation} and Vars: \n${JSON.stringify(variables)}`)
        return {mutation, variables}
  
      }
    } catch (e)
    {
      S3DB_Logging("exception", "", `Exception - Build Mutations - CreateUpdateContacts - ${e}`)
      debugger //catch
    }
  
  */


  interface CreateContactInput {
    attributes: ContactAttribute[]
    //audience?: {id: string}
    consent?: {id: string}
  }

  interface ContactAttribute {
    name: string
    value: any
  }

  interface CreateContactRecord {
    consent?: {}
    attributes: Array<{
      name: string
      value: any
    }>
  }

  interface CreateUpdateContactsVariables {
    dataSetId: string
    contactsData: CreateContactRecord[]
  }


  mutation = `mutation S3DropBucketCreateUpdateMutation (
        $contactsData: [ContactCreateInput!]!
        ) 
        {
            createContacts(
            contactsInput: $contactsData
            ) {
                  items {
                        contactKey
                        message
                        identifyingField {
                              value
                              attributeData {
                                    type
                                    decimalPrecision
                                    name
                                    category
                                    mapAs
                                    validateAs
                                    identifyAs {
                                          channels
                                          key
                                          index
                                    }
                                    tracking {
                                          createdBy
                                          createdAt
                                          lastModifiedBy
                                          lastModifiedAt
                                    }
                              }
                        }
                  }
            }
        }`
  

  try
  {
    if (config.updatetype.toLowerCase() === "createupdatecontacts")
    {
     

      //let createVariables = {} as CreateUpdateContactsVariables
      variables = {dataSetId: config.datasetid, contactsData: []} as CreateUpdateContactsVariables

      for (const upd in updates)
      {
        //Audience is just a placeholder for now, so removing until it is valid in graphQL type
        //let ccr: CreateContactsInput = {"to": {"audience":[], "consent":[], "attributes": []}}

        let ca: ContactAttribute = {name: "", value: ""}
        let ccr: CreateContactRecord = {attributes: []}
        let cci: CreateContactInput = {attributes: []}

        variables.contactsData.push(cci)

        //Build ContactId, AddressableFields, Consent, Audience properties
        const u = updates[upd] as Record<string, any>

        //if (typeof u.consent !== "undefined") Object.assign(variables.contactsInput, {consent: u.consent})
        if (typeof u.consent !== "undefined") Object.assign(cci, {consent: u.consent})


        //Audience is just a placeholder for now, so removing until it is valid in graphQL type
        //if (typeof u.audience !== "undefined") Object.assign(variables.contactsInput, {audience: u.audience})

        //Add S3DBConfirmation value as a means to quickly confirm Testing outcomes
        const now: Date = new Date()
        const date: string = now.toLocaleDateString()
        const time: string = now.toLocaleTimeString()
        cci.attributes.push({name: "S3DBConfirmation", value: date + " - " + time})
        //variables.contactsInput[upd].attributes[upd] = {name: "S3DBConfirmation", value: date + " - " + time}


        //Build Attribute Array from each row of inbound data
        for (const [key, value] of Object.entries(u))
        {
          //let v
          //if (typeof value === "string") v = value
          //else v = String(value)

          //Skip Payload Vars, Vars injected to carry Transformed Values and are already processed above,
          // and are not valid for the actual Update
          if (!["contactid", "contactkey", "addressable", "consent", "audience"].includes(key.toLowerCase()))
          {
            ca = {name: key, value: value}
            cci.attributes.push(ca)
          }

        }
        
        // ToDo: add validation logic for variables structure

        Object.assign(variables.contactsData[upd], cci)
        //variables.contactsInput.push(cci)
      }

      S3DB_Logging("info", "817", `CreateUpdate Multiple Contacts Mutation: ${JSON.stringify({query: mutation, variables: variables})}`)
      return {query: mutation, variables: variables}

    }
  } catch (e)
  {

    debugger //catch

    S3DB_Logging("exception", "", `Exception - Build Mutations - CreateUpdateContacts - ${e}`)
  }


  try
  {
    if (config.updatetype.toLowerCase() === "updatecontacts") //Deprecated for now, until its known this is required aside from CreateUpdate
    {

      interface ContactAttribute {
        name: string
        value: any
      }
      interface UpdateContactRecord {
        attributes: Array<{
          name: string
          value: string
        }>
        audience?: Array<{}>              //Audience is just a placeholder for now, so ignoring until it is valid in graphQL type
        consent?: Array<{}>
      }
      interface UpdateContactsVariables {
        contactsInput: UpdateContactsInput[]
      }

      interface UpdateContactsInput {
        contactId?: string
        key?: string
        addressable?: Array<{
          field: string
          eq: string
        }>
        to: {
          attributes: Array<{
            name: string
            value: string
          }>
        }
        //to: UpdateContactRecord[]
      }

      mutation = `mutation updateContacts (
        $contactsInput: [UpdateContactInput!]!
        ) {
        updateContacts(
            updateContactInputs: $contactsInput
            ) 
        {
        modifiedCount 
        }
    }`



      //mutation {
      //  updateContacts(
      //    updateContactInputs: [
      //      {
      //        key: "contact_key_value"
      //        to: {
      //          attributes: [
      //            { name: "firstName", value: "John" }
      //            { name: "lastName", value: "Doe" }
      //            { name: "email", value: "john.doe@acoustic.co" }
      //          ]
      //          consent: { channels: [{ channel: EMAIL, status: OPT_IN }] }
      //        }
      //      }
      //      { key: "another_contact_key_value", to: { attributes: [{ name: "email", value: null }] } }
      //    ]
      //  ) {
      //    modifiedCount
      //  }
      //}

      variables = {contactsInput: []} as UpdateContactsVariables

      for (const upd in updates)
      {
        //Audience is just a placeholder for now, so removing until it is valid in graphQL type
        //let uci: UpdateContactsInput = {"to": {"audience":[], "consent":[], "attributes": []}}

        let ca: ContactAttribute = {"name": "", "value": ""}
        let ucr: UpdateContactRecord = {attributes: []}
        let uci: UpdateContactsInput = {to: {attributes: []}}

        variables.contactsInput.push(uci)

        //Build ContactId, AddressableFields, Consent, Audience properties
        const u = updates[upd] as Record<string, any>

        //ToDo: Add logic CreateContact vs UpdateContact Key/Id/Addressable fields
        if (typeof u.contactId !== "undefined") Object.assign(variables.contactsInput[upd], {contactId: u.contactId})
        else if (typeof u.addressable !== "undefined") Object.assign(variables.contactsInput[upd], {addressable: u.addressable})

        if (typeof u.consent !== "undefined") Object.assign(uci.to, {consent: u.consent})

        //Audience is just a placeholder for now, so removing until it is valid in graphQL type
        //if (typeof u.audience !== "undefined") Object.assign(variables.contactsInput, {audience: u.audience})

        //Add S3DBConfirmation value as a means to quickly confirm Testing outcomes
        const now: Date = new Date()
        const date: string = now.toLocaleDateString()
        const time: string = now.toLocaleTimeString()
        uci.to.attributes.push({name: "S3DBConfirmation", value: date + " - " + time})


        //Build Attribute Array from each row of inbound data
        for (const [key, value] of Object.entries(u))
        {
          //let v
          //if (typeof value === "string") v = value as string
          //else v = String(value)

          //Skip Payload Vars, Vars injected to carry Transformed Values and are already processed above,
          // and are not valid for the actual Update
          if (!["contactid", "contactkey", "addressable", "consent", "audience"].includes(key.toLowerCase()))
          {
            ca = {name: key, value: value}
            uci.to.attributes.push(ca)
          }

        }

        Object.assign(variables.contactsInput[upd], uci)
        //variables.contactsInput.push(uci)

      }

      S3DB_Logging("info", "817", `Update Multiple Contacts Mutation: \n${mutation} and Vars: \n${JSON.stringify(variables)}`)
      return {query: mutation, variables}

    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - Build Mutations - UpdateContacts - ${e}`)
  }


} //Function Close


function convertJSONToXML_RTUpdates (updates: object[], config: CustomerConfig) {
  if (updates.length < 1)
  {
    throw new Error(
      `Exception - Convert JSON to XML for RT - No Updates(${updates.length}) were passed to process into XML. Customer ${config.customer} `
    )
  }

  xmlRows = `<Envelope> <Body> <InsertUpdateRelationalTable> <TABLE_ID> ${config.listid} </TABLE_ID><ROWS>`

  let r = 0

  for (const upd in updates)
  {

    //const updAtts = JSON.parse( updates[ upd ] )
    const updAtts = updates[upd]

    r++
    xmlRows += `<ROW>`
    // Object.entries(jo).forEach(([key, value]) => {

    type ua = keyof typeof updAtts

    for (const uv in updAtts)
    {
      const uVal: ua = uv as ua
      xmlRows += `<COLUMN name="${uVal}"> <![CDATA[${updAtts[uVal]}]]> </COLUMN>`
    }

    xmlRows += `</ROW>`
  }

  //Tidy up the XML
  xmlRows += `</ROWS></InsertUpdateRelationalTable></Body></Envelope>`

  S3DB_Logging("info", "906", `Converting ${r} updates to XML RT Updates. Packaged ${Object.values(updates).length} rows as updates to ${config.customer}'s ${config.listname} \nJSON to be converted to XML RT Updates: ${JSON.stringify(updates)}`)

  S3DB_Logging("info", "917", `Converting ${r} updates to XML RT Updates. Packaged ${Object.values(updates).length} rows as updates to ${config.customer}'s ${config.listname} \nXML from JSON for RT Updates: ${xmlRows}`)

  return xmlRows
}

function convertJSONToXML_DBUpdates (updates: object[], config: CustomerConfig) {
  if (updates.length < 1)
  {
    throw new Error(
      `Exception - Convert JSON to XML for DB - No Updates (${updates.length}) were passed to process. Customer ${config.customer} `
    )
  }

  xmlRows = `<Envelope><Body>`
  let r = 0

  try
  {
    for (const upd in updates)
    {

      r++

      const updAtts = updates[upd]
      //const s = JSON.stringify(updAttr )

      xmlRows += `<AddRecipient><LIST_ID>${config.listid}</LIST_ID><CREATED_FROM>0</CREATED_FROM><UPDATE_IF_FOUND>true</UPDATE_IF_FOUND>`

      // If Keyed, then Column that is the key must be present in Column Set
      // If Not Keyed must add Lookup Fields
      // Use SyncFields as 'Lookup" values, Columns hold the Updates while SyncFields hold the 'lookup' values.

      //Only needed on non-keyed (In Campaign use DB -> Settings -> LookupKeys to find what fields are Lookup Keys)
      if (config.updatetype.toLowerCase() === "dbnonkeyed")
      {

        //const lk = config.lookupkeys.split(",")
        const lk = config.lookupkeys as typeof config.lookupkeys

        xmlRows += `<SYNC_FIELDS>`
        try
        {
          for (let k in lk)
          {
            k = k.trim()
            const lu = lk[k] as keyof typeof updAtts
            if (updAtts[lu] === undefined)
              throw new Error(
                `No value for LookupKey found in the update. LookupKey: \n${k}`
              )
            const sf = `<SYNC_FIELD><NAME>${lu}</NAME><VALUE><![CDATA[${updAtts[lu]}]]></VALUE></SYNC_FIELD>`
            xmlRows += sf
          }
        } catch (e)
        {
          debugger //catch

          S3DB_Logging("exception", "", `Building XML for DB Updates - ${e}`)
        }

        xmlRows += `</SYNC_FIELDS>`
      }

      //<SYNC_FIELDS>
      //  <SYNC_FIELD>
      //  <NAME>EMAIL </NAME>
      //  < VALUE > somebody@domain.com</VALUE>
      //    </SYNC_FIELD>
      //    < SYNC_FIELD >
      //    <NAME>Customer Id </NAME>
      //      < VALUE > 123 - 45 - 6789 </VALUE>
      //      </SYNC_FIELD>
      //      </SYNC_FIELDS>



      //
      if (config.updatetype.toLowerCase() === "dbkeyed")
      {
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
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - ConvertJSONtoXML_DBUpdates - \n${e}`)
  }

  xmlRows += `</Body></Envelope>`

  S3DB_Logging("info", "916", `Converted ${r} updates - JSON for DB Updates: ${JSON.stringify(updates)}\nPackaging ${Object.values(updates).length} rows as updates to ${config.customer}'s ${config.listname}`)
  S3DB_Logging("info", "917", `Converted ${r} updates - XML for DB Updates: ${xmlRows}\nPackaging ${Object.values(updates).length} rows as updates to ${config.customer}'s ${config.listname}`)

  return xmlRows
}

async function putToFirehose (chunks: object[], key: string, cust: string, iter: number) {

  let putFirehoseResp: object = {}

  const tu = chunks.length
  let ut = 0

  let x = 0


  try
  {

    for (const j in chunks)
    {

      let jo = chunks[j]

      jo = Object.assign(jo, {Customer: cust})
      const fd = Buffer.from(JSON.stringify(jo), "utf-8")

      const fp = {
        DeliveryStreamName: S3DBConfig.s3dropbucket_firehosestream,
        Record: {
          Data: fd,
        },
      } as PutRecordCommandInput

      const firehoseCommand = new PutRecordCommand(fp)

      interface FireHosePutResult {
        PutToFireHoseAggregatorResult: string
        PutToFireHoseAggregatorResultDetails: string
        PutToFireHoseException: string
      }

      let firehosePutResult: FireHosePutResult =
      {
        PutToFireHoseAggregatorResult: "",
        PutToFireHoseAggregatorResultDetails: "",
        PutToFireHoseException: "",
      }

      let fhRetry = true
      while (fhRetry)
      {
        try
        {
          putFirehoseResp = await fh_Client.send(firehoseCommand)
            .then((res: PutRecordCommandOutput) => {

              S3DB_Logging("info", "922", `Inbound Update from ${key} - Put to Firehose Aggregator (File Stream Iter: ${iter}) Detailed Result: \n${fd.toString()} \n\nFirehose Result: ${JSON.stringify(res)} `)

              if (res.$metadata.httpStatusCode === 200)
              {
                ut++

                firehosePutResult.PutToFireHoseAggregatorResult = `${res.$metadata.httpStatusCode}`
                firehosePutResult.PutToFireHoseAggregatorResultDetails = `Successful Put to Firehose Aggregator for ${key} (File Stream Iter: ${iter}).\n${JSON.stringify(res)}. \n${res.RecordId} `
                firehosePutResult.PutToFireHoseException = ""

              } else
              {
                firehosePutResult.PutToFireHoseAggregatorResult = `${res.$metadata.httpStatusCode}`
                firehosePutResult.PutToFireHoseAggregatorResultDetails = `UnSuccessful Put to Firehose Aggregator for ${key} (File Stream Iter: ${iter}). \n ${JSON.stringify(res)} `
                firehosePutResult.PutToFireHoseException = ""
              }

              return firehosePutResult

            })
            .catch(async (e) => {

              debugger //catch

              const fr = await fh_Client.config.region()

              S3DB_Logging("exception", "", `Exception - Put to Firehose Aggregator (promise catch) (File Stream Iter: ${iter}) for ${key} \n( FH Data Length: ${fp.Record?.Data?.length}. FH Delivery Stream: ${JSON.stringify(fp.DeliveryStreamName)} FH Client Region: ${fr})  \n${e} `)

              firehosePutResult.PutToFireHoseAggregatorResult = "Exception"
              firehosePutResult.PutToFireHoseAggregatorResultDetails = `UnSuccessful Put to Firehose Aggregator for ${key} (File Stream Iter: ${iter}) \n ${JSON.stringify(e)} `
              firehosePutResult.PutToFireHoseException = `Exception - Put to Firehose Aggregator for ${key} (File Stream Iter: ${iter}) \n${e} `

              return firehosePutResult
            })

          // Testing - sample firehose put 
          x++
          if (x % 20 === 0)
          {
            debugger //testing
          }

          if (firehosePutResult.PutToFireHoseAggregatorResultDetails.indexOf("ServiceUnavailableException: Slow down") > -1)
          {
            fhRetry = true
            setTimeout(() => {
              //Don't like this approach but it appears to be the best way to get a promise safe retry
              S3DB_Logging("warn", "944", `Retrying Put to Firehose Aggregator (Slow Down requested) for ${key} (File Stream Iter: ${iter}) `)
            }, 100)
          }
          else fhRetry = false

        } catch (e)
        {
          debugger //catch

          S3DB_Logging("exception", "", `Exception - PutToFirehose (catch) (File Stream Iter: ${iter}) \n${e} `)
        }

      }

      S3DB_Logging("info", "922", `Completed Put to Firehose Aggregator (from ${key} - File Stream Iter: ${iter}) Detailed Result: \n${fd.toString()} \n\nFirehose Result: ${JSON.stringify(firehosePutResult)} `)

    }

    if (tu === ut) S3DB_Logging("info", "942", `Firehose Aggregator PUT results for inbound Updates (File Stream Iter: ${iter}) from ${key}. Successfully sent ${ut} of ${tu} updates to Aggregator.`)
    else S3DB_Logging("info", "942", `Firehose Aggregator PUT results for inbound Updates (File Stream Iter: ${iter}) from ${key}.  Partially successful sending ${ut} of ${tu} updates to Aggregator.`)

    return putFirehoseResp
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - Put to Firehose Aggregator (try-catch) for ${key} (File Stream Iter: ${iter}) \n${e} `)
  }

  //end of function
}


async function addWorkToS3WorkBucket (queueUpdates: string, key: string) {
  if (S3DBConfig.s3dropbucket_queuebucketquiesce)
  {
    S3DB_Logging("warn", "923", `Work/Process Bucket Quiesce is in effect, no New Work Files are being written to the S3 Queue Bucket. This work file is for ${key}`)

    return {
      versionId: "",
      S3ProcessBucketResultStatus: "",
      AddWorkToS3ProcessBucket: "In Quiesce",
    }
  }

  const s3WorkPutInput = {
    Body: queueUpdates,
    Bucket: S3DBConfig.s3dropbucket_workbucket,
    Key: key,
  }

  let S3ProcessBucketResultStatus = ""
  let addWorkToS3ProcessBucket

  try
  {
    addWorkToS3ProcessBucket = await s3
      .send(new PutObjectCommand(s3WorkPutInput))
      .then(async (s3PutResult: PutObjectCommandOutput) => {
        if (s3PutResult.$metadata.httpStatusCode !== 200)
        {
          throw new Error(
            `Failed to write Work File to S3 Process Store (Result ${s3PutResult}) for ${key} of ${queueUpdates.length} characters.`
          )
        }
        return s3PutResult
      })
      .catch((err) => {
        debugger //catch

        S3DB_Logging("exception", "", `Exception - Put Object Command for writing work(${key} to S3 Processing bucket(${S3DBConfig.s3dropbucket_workbucket}): \n${err}`)
        throw new Error(
          `PutObjectCommand Results Failed for (${key} of ${queueUpdates.length} characters) to S3 Processing bucket (${S3DBConfig.s3dropbucket_workbucket}): \n${err}`
        )
        //return {StoreS3WorkException: err}
      })
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - Put Object Command for writing work(${key} to S3 Processing bucket(${S3DBConfig.s3dropbucket_workbucket}): \n${e}`)
    throw new Error(
      `Exception - Put Object Command for writing work(${key} to S3 Processing bucket(${S3DBConfig.s3dropbucket_workbucket}): \n${e}`
    )
    // return { StoreS3WorkException: e }
  }

  S3ProcessBucketResultStatus = JSON.stringify(addWorkToS3ProcessBucket.$metadata.httpStatusCode, null, 2)

  const vidString = addWorkToS3ProcessBucket.VersionId ?? ""

  S3DB_Logging("info", "914", `Added Work File ${key} to Work Bucket (${S3DBConfig.s3dropbucket_workbucket}). \n${JSON.stringify(addWorkToS3ProcessBucket)}`)

  const aw3pbr = {
    versionId: vidString,
    AddWorkToS3ProcessBucket: JSON.stringify(addWorkToS3ProcessBucket),
    S3ProcessBucketResultStatus: S3ProcessBucketResultStatus,
  }

  return aw3pbr
}

async function addWorkToSQSWorkQueue (
  config: CustomerConfig,
  key: string,
  //versionId: string,
  batch: number,
  recCount: string,
  marker: string
) {
  if (S3DBConfig.s3dropbucket_queuebucketquiesce)
  {
    S3DB_Logging("warn", "923", `Work/Process Bucket Quiesce is in effect, no New Work Files are being written to the SQS Queue of S3 Work Bucket. This work file is for ${key}`)

    return {
      //versionId: "",
      S3ProcessBucketResultStatus: "",
      AddWorkToS3ProcessBucket: "In Quiesce",
    }
  }

  const sqsQMsgBody = {} as S3DBQueueMessage
  sqsQMsgBody.workKey = key
  //sqsQMsgBody.versionId = versionId
  sqsQMsgBody.marker = marker
  sqsQMsgBody.attempts = 1
  sqsQMsgBody.batchCount = batch.toString()
  sqsQMsgBody.updateCount = recCount
  sqsQMsgBody.custconfig = config
  sqsQMsgBody.lastQueued = Date.now().toString()

  const sqsParams = {
    MaxNumberOfMessages: 1,
    QueueUrl: S3DBConfig.s3dropbucket_workqueue,
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
  let sqsWriteResultStatus

  try
  {
    await sqsClient
      .send(new SendMessageCommand(sqsParams))
      .then((sqsSendMessageResult: SendMessageCommandOutput) => {

        sqsWriteResultStatus = JSON.stringify(sqsSendMessageResult.$metadata.httpStatusCode, null, 2)

        if (sqsWriteResultStatus !== "200")
        {
          const storeQueueWorkException = `Failed writing to SQS Process Queue (queue URL: ${sqsParams.QueueUrl}), ${sqsQMsgBody.workKey}, SQS Params${JSON.stringify(sqsParams)})`
          return {StoreQueueWorkException: storeQueueWorkException}
        }
        //sqsSendResult = sqsSendMessageResult
        //S3DB_Logging("info", "946", `Queued Work to SQS Process Queue (${sqsQMsgBody.workKey}) \nResult: ${sqsWriteResultStatus} \n${JSON.stringify(sqsSendMessageResult)} `)

        S3DB_Logging("info", "946", `Queued Work (${sqsQMsgBody.workKey}} to SQS Process Queue (for ${recCount} updates). \nWork Queue (${S3DBConfig.s3dropbucket_workqueue}) \nSQS Params: ${JSON.stringify(sqsParams)}. \nresults: ${JSON.stringify(sqsSendMessageResult)} \nStatus: ${JSON.stringify({SQSWriteResultStatus: sqsWriteResultStatus, AddToSQSQueue: JSON.stringify(sqsSendResult)})}`)

        //return sqsSendMessageResult
        return sqsSendResult
      })
      .catch((err) => {
        debugger //catch

        const storeQueueWorkException = `Failed writing to SQS Process Queue (${err}). \nQueue URL: ${sqsParams.QueueUrl})\nWork to be Queued: ${sqsQMsgBody.workKey}\nSQS Params: ${JSON.stringify(sqsParams)}`

        S3DB_Logging("exception", "", `Failed to Write to SQS Process Queue. \n${storeQueueWorkException}`)

        return {StoreQueueWorkException: storeQueueWorkException}
      })
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - Writing to SQS Process Queue - (queue URL${sqsParams.QueueUrl
      }), ${sqsQMsgBody.workKey}, SQS Params${JSON.stringify(sqsParams)}) - Error: ${e}`)
  }

  S3DB_Logging("info", "940", `Work Queued (${key} for ${recCount} updates) to the SQS Work Queue (${S3DBConfig.s3dropbucket_workqueue}) \nresults: \n${JSON.stringify({SQSWriteResultStatus: sqsWriteResultStatus, AddToSQSQueue: JSON.stringify(sqsSendResult)})}`)

  return {
    SQSWriteResultStatus: sqsWriteResultStatus,
    AddToSQSQueue: JSON.stringify(sqsSendResult),
  }
}

function transforms (updates: object[], config: CustomerConfig) {
  //Apply Transforms
  //Approach: => Process Entire Set of Updates in Each Transform step, NOT each Transform against each update.
  //Sequence:  //Have to run transforms in the following sequence
  // Daydate
  // Date-to-ISO-8601
  // String-To-Number
  // jsonMap  --- Essentially Add Columns to Data (future: refactor as 'addcolumns')
  // csvMap  --- Essentially Add Columns to Data (future: refactor as 'addcolumns')
  //
  // --- Have to run "Reference" transforms (transforms that reference values) After transforms that modify data
  // ContactId
  // Addressable Fields
  // Channel Consent
  // Audience
  //
  // --- Have to Run Ignore (Remove Columns in data) last,
  // Ignore



  //Transform: DayDate
  //
  //ToDo: Provide a transform to break timestamps out into
  //          Day - Done
  //    Hour - tbd
  //    Minute - tbd
  //
  //

  try
  {

    if (typeof config.transforms.methods.daydate !== undefined &&
      Object.keys(config.transforms.methods.daydate).length > 0)
    {
      const t: typeof updates = []
      let toDay: string = ""

      const days = [
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
      ]

      for (const update of updates)
      {
        Object.entries(config.transforms.methods.daydate).forEach(([key, val]) => {
          //Element implicitly has an 'any' type because expression of type 'string' can't be used to index type '{} '.
          //No index signature with a parameter of type 'string' was found on type '{}'.ts(7053)
          const updateObj: {[key: string]: string} = update as {[key: string]: string}

          toDay = updateObj[val] as string

          if (typeof toDay !== "undefined" && toDay !== "")
          {
            const dt = new Date(toDay)
            const day = {dateday: days[dt.getDay()]}
            Object.assign(update, day)
          }
        })

        t.push(update)
      }

      if (t.length === updates.length)
      {
        updates = t
      } else
      {
        S3DB_Logging("error", "933", `Error - Transform - Applying DayDate Transform returns fewer records (${t.length}) than initial set ${updates.length}`)
        throw new Error(
          `Error - Transform - Applying DayDate Transform returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }

      S3DB_Logging("info", "919", `Transforms (DayDate) applied: \n${JSON.stringify({"pending": "tbd"})}`)

    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "934", `Exception - Applying DayDate Transform \n${e}`)
  }



  //Transform - Date-to-ISO-8601
  //

  debugger ///

  try
  {
    if (typeof config.transforms.methods?.date_iso1806_type !== undefined &&
      Object.keys(config.transforms.methods.date_iso1806_type).length > 0) 
    {
      //const iso1806Col = config.transforms.methods.date_iso1806 ?? 'iso1806Date' 

      const t: typeof updates = []
      let toISO1806: string = ""

      for (const update of updates)
      {
        Object.entries(config.transforms.methods.date_iso1806_type).forEach(([key, val]) => {
          //Element implicitly has an 'any' type because expression of type 'string' can't be used to index type '{} '.
          //No index signature with a parameter of type 'string' was found on type '{}'.ts(7053)
          const updateObj: {[key: string]: string} = update as {[key: string]: string}

          toISO1806 = updateObj[val] as string

          if (typeof toISO1806 !== "undefined" && toISO1806 !== "")
          {
            const dt = new Date(toISO1806)
            const isoString: string = dt.toISOString()
            const tDate = {[key]: isoString}
            Object.assign(update, tDate)
          }

        })

        t.push(update)
      }

      if (t.length === updates.length)
      {
        updates = t
      } else
      {
        S3DB_Logging("error", "933", `Error - Transform - Applying date_iso1806 Transform returns fewer records (${t.length}) than initial set ${updates.length}`)
        throw new Error(
          `Error - Transform - Applying date_iso1806 Transform returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }

      S3DB_Logging("info", "919", `Transforms (Date_ISO1806) applied: \n${JSON.stringify({"pending": "tbd"})}`)

    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "934", `Exception - Applying date_iso1806 Transform \n${e}`)
  }


  //Transform - Phone Number 
  //

  debugger ///

  try
  {

    if (typeof config.transforms.methods.phone_number_type !== undefined &&
      Object.keys(config.transforms.methods.phone_number_type).length > 0)
    {
      //const iso1806Col = config.transforms.methods.date_iso1806 ?? 'iso1806Date' 

      const t: typeof updates = []
      let pn: string = ""

      for (const update of updates)
      {
        Object.entries(config.transforms.methods.phone_number_type).forEach(([key, val]) => {
          //Element implicitly has an 'any' type because expression of type 'string' can't be used to index type '{} '.
          //No index signature with a parameter of type 'string' was found on type '{}'.ts(7053)
          const updateObj: {[key: string]: string} = update as {[key: string]: string}

          pn = updateObj[val] as string

          if (typeof pn !== "undefined" && pn !== "")
          {

            const npn = pn.replaceAll(new RegExp(/(\D)/gm), "")

            if (!/\d{7,}/.test(npn))
            {
              S3DB_Logging("error", "933", `Error - Transform - Applying Phone_Number Transform returns non-numeric value.`)
            }

            const pnu = {[key]: npn}
            Object.assign(update, pnu)

          }
          else
          {
            S3DB_Logging("error", "933", `Error - Transform - Applying Phone_Number Transform ${key}: ${val} returns empty value.`)
          }
        })

        t.push(update)

      }
      if (t.length === updates.length)
      {
        updates = t
      } else
      {
        S3DB_Logging("error", "933", `Error - Transform - Applying Phone_Number Transform returns fewer records (${t.length}) than initial set ${updates.length}`)
        throw new Error(
          `Error - Transform - Applying Phone_Number Transform returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }

      S3DB_Logging("info", "919", `Transforms (Phone Number) applied: \n${JSON.stringify({"pending": "tbd"})}`)


    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "934", `Exception - Applying PhoneNumber Transform \n${e}`)
  }




  //Transform - String-To-Number
  //

  try
  {
    if (typeof config.transforms.methods.string_to_number_type !== undefined &&
      Object.keys(config.transforms.methods.string_to_number_type).length > 0)
    {
      const t: typeof updates = []
      let strToNumber: string = ""

      for (const update of updates)
      {
        Object.entries(config.transforms.methods.string_to_number_type).forEach(([key, val]) => {
          //Element implicitly has an 'any' type because expression of type 'string' can't be used to index type '{} '.
          //No index signature with a parameter of type 'string' was found on type '{}'.ts(7053)
          const updateObj: {[key: string]: string} = update as {[key: string]: string}

          strToNumber = updateObj[val] as string

          if (typeof strToNumber !== "undefined" && strToNumber !== "")
          {
            const n = Number(strToNumber)
            if (String(n) === 'NaN')
            {
              S3DB_Logging("error", "933", `Error - Transform - String-To-Number transform failed as the string value for ${key} cannot be converted to a number: ${val}.`)
            }
            else
            {
              const num = {[key]: n}
              Object.assign(update, num)
            }
          }
        })

        t.push(update)
      }

      if (t.length === updates.length)
      {
        updates = t
      } else
      {
        S3DB_Logging("error", "933", `Error - Transform - Applying String-To-Number Transform returns fewer records (${t.length}) than initial set ${updates.length}`)
        throw new Error(
          `Error - Transform - Applying String-To-Number Transform returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }

      S3DB_Logging("info", "919", `Transforms (StringToNumber) applied: \n${JSON.stringify({"pending": "tbd"})}`)

    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "934", `Exception - Applying String-To-Number Transform \n${e}`)
  }




  //Transform: JSONMap

  //Apply the JSONMap -
  //  JSONPath statements
  //      "jsonMap": {
  //          "email": "$.uniqueRecipient",
  //              "zipcode": "$.context.traits.address.postalCode"
  //      },

  //Need failsafe test of empty object jsonMap has no transforms. 
  try
  {
    if (typeof config.transforms.jsonmap !== undefined &&
      Object.keys(config.transforms.jsonmap).length > 0)
    {
      const t: typeof updates = []
      try
      {
        for (const update of updates)
        {

          Object.entries(config.transforms.jsonmap).forEach(([key, val]) => {

            //const jo = JSON.parse( l )
            let j = applyJSONMap(update, {[key]: val})
            if (typeof j === "undefined" || j === "") j = "Not Found"
            Object.assign(update, {[key]: j})
          })

          t.push(update)
        }


      } catch (e)
      {
        debugger //catch

        S3DB_Logging("exception", "934", `Exception - Transform - Applying JSONMap \n${e}`)

      }

      if (t.length === updates.length)
      {
        updates = t
      } else
      {
        debugger //catch

        S3DB_Logging("error", "933", `Error - Transform - Applying JSONMap returns fewer records(${t.length}) than initial set ${updates.length}`)

        throw new Error(
          `Error - Transform - Applying JSONMap returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }

      
      //if no throw/exception yet then we are successful
      S3DB_Logging("info", "919", `Transforms (JSONMap) applied: \n${JSON.stringify(t)}`)

    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "934", `Exception - Applying JSONMap Transform \n${e}`)
  }




  //Transform: CSVMap

  //Apply CSVMap
  // "csvMap": { //Mapping when processing CSV files
  //       "Col_AA": "COL_XYZ", //Write Col_AA with data from Col_XYZ in the CSV file
  //       "Col_BB": "COL_MNO",
  //       "Col_CC": "COL_GHI",

  //       "Col_DD": 1, //Write Col_DD with data from the 1st column of data in the CSV file.
  //       "Col_DE": 2,
  //       "Col_DF": 3
  // },

  try
  {
    if (typeof config.transforms.csvmap !== undefined &&
      Object.keys(config.transforms.csvmap).length > 0)
    {
      const t: typeof updates = []
      try
      {
        for (const jo of updates)
        {
          //const jo = JSON.parse( l )

          const map = config.transforms.csvmap as {[key: string]: string}
          Object.entries(map).forEach(([key, val]) => {

            //type dk = keyof typeof jo
            //const k: dk = ig as dk
            //delete jo[k]
            type kk = keyof typeof jo
            const k: kk = key as kk
            type vv = keyof typeof jo
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

          t.push(jo)

        }
      } catch (e)
      {
        debugger //catch

        S3DB_Logging("exception", "934", `Exception - Transforms - Applying CSVMap \n${e}`)

      }

      if (t.length === updates.length)
      {
        updates = t
      } else
      {
        S3DB_Logging("error", "933", `Error - Transform - Applying CSVMap returns fewer records(${t.length}) than initial set ${updates.length}`)
        throw new Error(
          `Error - Transform - Applying CSVMap returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }

      //if no throw/exception yet then we are successful
      S3DB_Logging("info", "919", `Transforms (CSVMap) applied: \n${JSON.stringify(t)}`)
    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "934", `Exception - Applying CSVMap Transform \n${e}`)
  }



  //Transform: ContactId
  //
  if (typeof config.transforms.contactid !== undefined &&
    config.transforms.contactid.length > 3) 
  {
    let s: string = ""
    try
    {
      const t: typeof updates = []

      if (config.transforms.contactid.startsWith('$')) s = 'jsonpath'
      else if (config.transforms.contactid.startsWith('@')) s = 'csvcolumn'
      else if (s === "" && config.transforms.contactid.length > 3) s = 'static'
      else S3DB_Logging("error", "999", `Error - Transform - ContactId invalid configuration.`)


      //Process All Updates for this Transform
      for (const update of updates)
      {
        Object.assign(update, {"contactId": ""})

        switch (s)
        {
          case 'static': {
            Object.assign(update, {"contactId": config.transforms.contactid})
            break
          }
          case 'jsonpath': {
            let j = applyJSONMap(update, {contactId: config.transforms.contactid})
            if (typeof j === "undefined" || j === "") j = "Not Found"
            Object.assign(update, {"contactId": j})
            break
          }
          case 'csvcolumn':
            {
              //strip preceding '@.'
              let csvmapvalue = config.transforms.contactid.substring(2, config.transforms.contactid.length)
              let colRef = csvmapvalue as keyof typeof update

              let v = update[colRef] as string
              if (typeof v === "undefined" || v === "") v = "Not Found"
              Object.assign(update, {"contactId": v})
              break
            }
        }

        t.push(update)
      }

      //All Updates transformed from ContactId transform
      if (t.length === updates.length)
      {
        updates = t
      } else
      {
        S3DB_Logging("error", "933", `Error - Transform - Applying ContactId Transform returns fewer records (${t.length}) than initial set ${updates.length}`)
        throw new Error(
          `Error - Transform - Applying ContactId Transform returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }

      S3DB_Logging("info", "919", `Transforms (ContactId) applied: \n${JSON.stringify({"pending": "tbd"})}`)


    } catch (e)
    {
      debugger //catch

      S3DB_Logging("exception", "934", `Exception - Applying ContactId Transform \n${e}`)
    }
  }

 /*
 //ContactKey should not need to be manipulated - Use as Template for Processing  
 else if (typeof config.transforms.contactkey !== undefined &&
    config.transforms.contactkey.length > 3)
  {
    //Transform: ContactKey

    let s: string = ""
    try
    {

      const t: typeof updates = []

      if (typeof config.transforms.contactkey !== undefined &&
        config.transforms.contactkey.length > 3) 
      {
        if (config.transforms.contactkey.startsWith('$')) s = 'jsonpath'
        else if (config.transforms.contactkey.startsWith('@')) s = 'csvcolumn'
        else if (s === "" && config.transforms.contactkey.length > 3) s = 'static'
        else S3DB_Logging("error", "999", `Error - Transform - ContactKey invalid configuration.`)

        //Process All Updates for this Transform
        for (const update of updates)
        {
          Object.assign(update, {"contactKey": ""})

          switch (s)
          {
            case 'static': {
              if (config.transforms.contactkey === '....') Object.assign(update, {"contactKey": ""})
              else Object.assign(update, {"contactKey": config.transforms.contactkey})
              break
            }
            case 'jsonpath': {
              let j = applyJSONMap(update, {contactKey: config.transforms.contactkey})
              if (typeof j === "undefined" || j === "") j = "Not Found"
              Object.assign(update, {"contactKey": j})
              //Object.assign(u, {"contactKey": applyJSONMap(update, {contactkey: config.transforms.contactkey})})
              break
            }
            case 'csvcolumn':
              {

                //strip preceding '@.'
                let csvmapvalue = config.transforms.contactkey.substring(2, config.transforms.contactkey.length)
                let colRef = csvmapvalue as keyof typeof update

                let v = update[colRef] as string
                if (typeof v === "undefined" || v === "") v = "Not Found"
                Object.assign(update, {"contactKey": v})
                break
              }
          }

          t.push(update)
        }
      }
      //All Updates now transformed from ContactKey transform
      if (t.length === updates.length)
      {
        updates = t
      } else
      {
        S3DB_Logging("error", "933", `Error - Transform - Applying ContactKey Transform returns fewer records (${t.length}) than initial set ${updates.length}`)
        throw new Error(
          `Error - Transform - Applying ContactKey Transform returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }

          S3DB_Logging("info", "919", `Transforms (ContactKey) applied: \n${JSON.stringify({"pending": "tbd"})}`)


    } catch (e)
    {
      debugger //catch

      S3DB_Logging("exception", "934", `Exception - Applying ContactKey Transform \n${e}`)
    }
  }
  */
  
  else if (typeof config.transforms.addressablefields !== undefined &&
    Object.entries(config.transforms.addressablefields).length > 0)
  {

    //Customer COnfig: 
    //Transform: Addressable Fields
    //"addressablefields": {  // Only 3 allowed. 
    //  // Can use either mapping or a "statement" definition, if defined, a 'statement' definition overrides any other config. 
    //  //"Email": "$.email",
    //  //"Mobile Number": "$['Mobile Number']",
    //  //"Example3": "999445599",
    //  //"Example4": "$.jsonfileValue", //example
    //  //"Example5": "@.csvfileColumn" //example
    //  "statement": {
    //    "addressable": [
    //      {"field": "Email", "eq": "$.email"} //, 
    //      //{ "field": "SMS", "eq": "$['Mobile Number']" }     
    //    ]
    //  }
    //},


    //S3DB_Logging("debug", "999", `Debug - Addressable Fields Transform Entered - ${config.targetupdate}`)

    //if (typeof config.transforms.addressablefields !== "undefined" &&
    //  Object.keys(config.transforms.addressablefields).length > 0)
    //{

    interface AddressableItem {
      field: string
      eq: string
    }
    interface AddressableStatement {
      addressable: AddressableItem[]
    }
    interface AddressableFields {
      [key: string]: string | AddressableStatement
      statement: AddressableStatement
    }

    const addressableFields = config.transforms.addressablefields as AddressableFields
    
    const addressableStatement = addressableFields.statement as AddressableStatement
    
    const fieldsArray = addressableStatement.addressable

    try
    {
      const t: typeof updates = []

      //Process All Updates for this Transform
      for (const update of updates)
      {

        const addressableArray = [] as object[]

        //If 'Statement' configured, takes precedence over all others
        if (typeof config.transforms.addressablefields["statement"] !== undefined &&
          config.transforms.addressablefields["statement"] !== "")
        {
          for (const fieldItem of fieldsArray)
          {

            //  "statement": {
            //    "addressable": [
            //      {"field": "Email", "eq": "$.email"} //,
            //      //{ "field": "SMS", "eq": "$['Mobile Number']" }
            //    ]

            let s: string = ""

            const fieldVar = fieldItem.eq

            if (fieldVar.startsWith('$')) s = 'jsonpath'
            else if (fieldVar.startsWith('@')) s = 'csvcolumn'
            else if (s === "" && fieldVar.length > 3) s = 'static'  //Should never see Static but there may be a use-case
            else S3DB_Logging("error", "933", `Error - Transform - AddressableFields.Statement invalid configuration.`)

            switch (s)
            {
              case 'static': {
                addressableArray.push({fieldItem})
                break
              }
              case 'jsonpath': {

                let j = applyJSONMap(update, {"AddressableFields": fieldVar})
                if (typeof j === 'undefined' || j === 'undefined' || j === "") j = "Not Found"

                const field = {
                  field: fieldItem.field,
                  eq: j
                }

                //addressable: [   //valid mutation example
                // { field: "email", eq: "john.doe1@acoustic.co" },
                // { field: "sms", eq: "+48555555555" }
                //]

                addressableArray.push(field)
                break
              }
              case 'csvcolumn':
                {
                  //strip preceding '@.'
                  let csvmapvalue = fieldVar.substring(2, fieldVar.length)
                  let colRef = csvmapvalue as keyof typeof update

                  let v = update[colRef] as string
                  if (typeof v === "undefined" || v === "") v = "Not Found"

                  const field = {
                    field: fieldItem.field,
                    eq: v
                  }

                  addressableArray.push({field})
                  break
                }
            }
          }

          Object.assign(update, {addressable: addressableArray})

        }
        else      //No 'Statement', so process individual Field definition(s)
        {
          for (const [key, fieldItem] of Object.entries(addressableFields))
          {

            const addressableValue = fieldItem as string

            //"Email": "Email",
            //"Mobile Number": "Mobile Number",
            //"Example3": "999445599",
            //"Example4": "$.jsonfileValue", //example
            //"Example5": "@.csvfileColumn" //example

            let s: string = ""

            if (addressableValue.startsWith('$')) s = 'jsonpath'
            else if (addressableValue.startsWith('@')) s = 'csvcolumn'
            else if (s === "" && addressableValue.length > 3) s = 'static'
            else S3DB_Logging("error", "933", `Error - Transform - AddressableFields invalid configuration.`)

            switch (s)
            {
              case 'static': {
                addressableArray.push({addressable: addressableValue})
                break
              }
              case 'jsonpath': {
                let j = applyJSONMap(update, {addressable: addressableValue})
                if (typeof j === "undefined" || j === "") j = "Not Found"

                const field = {
                  field: fieldItem,
                  eq: j
                }

                addressableArray.push({addressable: field})
                break
              }
              case 'csvcolumn':
                {
                  //strip preceding '@.'
                  let csvmapvalue = addressableValue.substring(2, addressableValue.length)
                  let colRef = csvmapvalue as keyof typeof update

                  let v = update[colRef] as string
                  if (typeof v === "undefined" || v === "") v = "Not Found"

                  const field = {
                    field: fieldItem,
                    eq: v
                  }

                  addressableArray.push(field)

                  break
                }
            }
          }
          //})

          Object.assign(update, {addressable: {addressable: addressableArray}})

        }

        t.push(update)

      }

      //All Updates now transformed from consent transform
      if (t.length === updates.length)
      {
        updates = t
      } else
      {
        S3DB_Logging("error", "933", `Error - Transform - Applying AddressableFields Transform returns fewer records (${t.length}) than initial set ${updates.length}`)
        throw new Error(
          `Error - Transform - Applying AddressableFields Transform returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }

      S3DB_Logging("info", "919", `Transforms (Addressable Fields) applied: \n${JSON.stringify({"pending": "tbd"})}`)


    } catch (e)
    {
      debugger //catch

      S3DB_Logging("exception", "934", `Exception - Applying AddressableFields Transform \n${e}`)
    }

  }
  else
  {
    if (config.targetupdate.toLowerCase() === "connect")
    {
      S3DB_Logging("warn", "933", `Warning - No ContactKey or Addressable Field provided for a Connect Create or Update ${config.targetupdate} Contact.`)
    //ToDo: Possible throw exception here for missing addressable, contactID or Addressable Field 

    }
  }


  //Transform: Channel Consent

  //need loop through Config consent and build consent object.

  //"consent": { //Can use either mapping or a "statement" definition, if defined, a 'statement' definition overrides any other Consent config. 
  //  "Email": "OPT_IN_UNVERIFIED", //Static Values: Use When Consent status is not in the Data
  //  "SMS": "OPT_OUT", //Static Values: Use When Consent status is not in the Data
  //  "WhatsApp": "$.whatsappconsent", //Dynamic Values: Use when consent data can be mapped from the data (JSONPath in this case) .
  //  "Email2": "@.csvfileColumn", //Dynamic Values: Use when consent data can be mapped from the data (CSV Column reference in this case).
  //  "statement": { //Mutually Exclusive: Use to define a Consent update to be used with this dataflow, useful to define a compound Group Consent statement as in the example in the description. 
  //    "channels": [
  //      {"channel": "EMAIL", "status": "OPT_IN"},
  //      {"channel": "SMS", "status": "OPT_IN"}
  //    ]

  // Mutation: 
  // consent: {
  //  channels: [
  //    { channel: EMAIL, status: OPT_IN_UNVERIFIED },
  //    { channel: SMS, status: OPT_IN }
  //    ]
  //  }

  if (typeof config.transforms.consent !== undefined &&
    Object.keys(config.transforms.consent).length > 0)
  {
    try
    {

      let s: string = ""

      const t: typeof updates = []
      const channelconsents: typeof config.transforms.consent = config.transforms.consent

      //Process All Updates for this Transform
      for (const update of updates)
      {

        const channelConsentsArray = [] as object[]

        //Consent: Either Channels or consentGroups
        //"consent": {   
        //  "channels": [
        //     { "channel": "EMAIL", "status": "OPT_IN" }
        //     ]

        //"consentGroups": [
        //  {
        //  "consentGroupId": "3a7134b8-dcb5-509a-b7ff-946b48333cc9",
        //  "status": "OPT_IN"
        //  }
        //] 


        //If 'Statement' configured, takes precedence over all others
        if (typeof config.transforms.consent["statement"] !== undefined &&
          config.transforms.consent["statement"] !== "")
        {
          //Consent Statement is not the same as AddressableFields, for Consent 'Statement" simply copy 
          // what was provided in the Customer Config as the Consent Statement to provide for the operation. 
          Object.assign(update, {consent: config.transforms.consent["statement"]})

        }
        else    //No 'Statement', so process individual Field definition(s)
        {
          //for (const [key, consentChannel] of Object.entries(consentChannel))
          //{
          Object.keys(channelconsents).forEach((consentChannel) => {

            //"Email": "OPT_IN_UNVERIFIED",
            //"SMS": "OPT_OUT",
            //"WhatsApp": "$.jsonfileValue",
            //"Email2": "@.csvfileColumn"
            //"statement": {}

            //forUpdateOnly????
            //consent: {channels: {channel: EMAIL, status: OPT_IN} } 

            //From Create mutation doc.  
            //consent: {
            //  channels: [
            //    {channel: EMAIL, status: OPT_IN_UNVERIFIED}
            //    {channel: SMS, status: OPT_IN}
            //  ]
            //}

            let s: string = ""
            const consentValue = channelconsents[consentChannel]

            if (consentValue.startsWith('$')) s = 'jsonpath'
            else if (consentValue.startsWith('@')) s = 'csvcolumn'
            else if (s === "" && consentValue.length > 3) s = 'static'
            else S3DB_Logging("error", "933", `Error - Transform - ContactKey transform has an invalid configuration.`)

            switch (s)
            {
              case 'static': {
                channelConsentsArray.push({channel: consentChannel, status: consentValue})
                break
              }
              case 'jsonpath': {
                let j = applyJSONMap(update, {consentChannel: consentValue})
                if (typeof j === "undefined" || j === "") j = "Not Found"


                channelConsentsArray.push({channel: consentChannel, status: j})
                //Object.assign(u, {"consent": consentfromJPath})
                break
              }
              case 'csvcolumn':
                {
                  //strip preceding '@.'
                  let csvmapvalue = consentValue.substring(2, consentValue.length)
                  let colRef = csvmapvalue as keyof typeof update

                  let v = update[colRef] as string
                  if (typeof v === "undefined" || v === "") v = "Not Found"

                  channelConsentsArray.push({channel: consentChannel, status: v})
                  break
                }
            }

          })

          //consent: {
          //  channels: [
          //    {channel: EMAIL, status: OPT_IN_UNVERIFIED}
          //    {channel: SMS, status: OPT_IN}
          //  ]
          //}

          Object.assign(update, {consent: {channels: channelConsentsArray}})

        }

        t.push(update)

      }

      //All Updates now transformed from consent transform
      if (t.length === updates.length)
      {
        updates = t
      } else
      {
        S3DB_Logging("error", "933", `Error - Transform - Applying Consent Transform returns fewer records (${t.length}) than initial set ${updates.length}`)

        throw new Error(
          `Error - Transform - Applying Consent Transform returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }

      S3DB_Logging("info", "919", `Transforms (Channel Consent) applied: \n${JSON.stringify({"pending": "tbd"})}`)

    } catch (e)
    {
      debugger //catch

      S3DB_Logging("exception", "934", `Exception - Applying Consent Transform \n${e}`)
    }

  }

  //Transform: Audience Update
  //need loop through Config audienceupdate and build audience object.

  try
  {
    if (typeof config.transforms.audienceupdate !== undefined &&
      Object.keys(config.transforms.audienceupdate).length > 0)
    {
      let s: string = ""

      const t: typeof updates = []
      const audienceUpdates: typeof config.transforms.audienceupdate = config.transforms.audienceupdate

      //"audienceupdate": { //Static Values: Use When Audience status is not in the Data (use JSONMap/CSVMap when Consent is in data)
      //"AudienceXYZ": "Exited",
      //"AudienceABC": "Entered",
      //"AudiencePQR": "$.jsonfileValue",
      //"AudienceMNO": "@.csvfileColumn"

      //Object.assign(update, {"Audience": []})

      //Process All Updates for this Transform
      for (const update of updates)
      {
        const audienceUpdatesArray = [] as object[]

        Object.keys(audienceUpdates).forEach((audUpdate) => {

          let s: string = ""
          const audUpdateValue = audienceUpdates[audUpdate]

          if (audUpdateValue.startsWith('$')) s = 'jsonpath'
          else if (audUpdateValue.startsWith('@')) s = 'csvcolumn'
          else if (s === "" && audUpdateValue.length > 3) s = 'static'
          else S3DB_Logging("error", "933", `Error - Transform - AudienceUpdate transform has an invalid configuration.`)

          switch (s)
          {
            case 'static': {
              audienceUpdatesArray.push({"audience": audUpdate, "status": audUpdateValue})
              //Object.assign(update, {"Audience": audienceValue})
              break
            }
            case 'jsonpath': {

              let j = applyJSONMap(update, {audUpdate: audUpdateValue})
              if (typeof j === "undefined" || j === "") j = "Not Found"
              audienceUpdatesArray.push({"audience": audUpdate, "status": j})
              // const ajp = applyJSONMap(update, audienceValue)
              // Object.assign(u, {"Audience": ajp})
              break
            }
            case 'csvcolumn':
              {
                //strip preceding '@.'
                let csvmapvalue = audUpdateValue.substring(2, audUpdateValue.length)
                let colRef = csvmapvalue as keyof typeof update

                let v = update[colRef] as string
                if (typeof v === "undefined" || v === "") v = "Not Found"
                audienceUpdatesArray.push({"audience": audUpdate, "status": v})
                break
              }
          }

          Object.assign(update, {audience: {audience: audienceUpdatesArray}})

        })

        t.push(update)

      }

      //All Updates now transformed from AudienceUpdates transform
      if (t.length === updates.length)
      {
        updates = t
      } else
      {
        S3DB_Logging("error", "933", `Error - Transform - Applying AudienceUpdates Transform returns fewer records (${t.length}) than initial set ${updates.length}`)

        throw new Error(
          `Error - Transform - Applying AudienceUpdates Transform returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }


    }

    S3DB_Logging("info", "919", `Transforms (Audience Update) applied: \n${JSON.stringify({"pending": "tbd"})}`)

  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "934", `Exception - Applying AudienceUpdates Transform \n${e}`)
  }

  

  //When processing an Aggregator file we need to remove "Customer" column that is added by the 
  // Aggregator. Which, we will not reach here if not an Aggregator, as transforms are not applied to
  // incoming data before sending to Aggregator.
  //Delete "Customer" column from data

  debugger ///  Need to check that Customer column is getting deleted correctly - Apr 2025

try {
  if (config.updates.toLowerCase() === "singular")  //Denotes Aggregator is used, so remove Customer
  {

    const t: typeof updates = []  //start a "Processed for Aggregator" update set
    try
    {
      for (const jo of updates)
      {
        type dk = keyof typeof jo
        const k: dk = "Customer" as dk
        delete jo[k]

        t.push(jo)
      }
    } catch (e)
    {
      debugger //catch

      S3DB_Logging("exception", "933", `Exception - Transform - Removing Aggregator Surplus "Customer" Field \n${e}`)

    }

    if (t.length === updates.length)
    {
      updates = t
    } else
    {
      debugger //catch

      S3DB_Logging("error", "933", `Error - Transform for Aggregator Files - Removing Surplus Customer Field returns fewer records ${t.length} than initial set ${updates.length}`)

      throw new Error(
        `Error - Transform - Transform for Aggregator Files: Removing Surplus Customer Field returns fewer records ${t.length} than initial set ${updates.length}`
      )
    }

    S3DB_Logging("info", "919", `Transform for Aggregator Files - Removing Surplus Customer Field applied: \n${JSON.stringify(t)}`)
  }

} catch (e)
{
  debugger //catch

  S3DB_Logging("exception", "934", `Exception - Applying Transform for Aggregator Files - Removing Surplus Customer Field \n${e}`)
}    

  



  //Transform: Ignore
  // Ignore must be last to take advantage of cleaning up any extraneous columns after previous transforms

  try
  {
    const igno = config.transforms.ignore as typeof config.transforms.ignore ?? []

    if (typeof config.transforms.ignore !== "undefined" &&
      config.transforms.ignore.length > 0)
    {

      const t: typeof updates = []  //start an 'ignore processed' update set
      try
      {
        for (const jo of updates)
        {

          for (const ig of igno)
          {
            type dk = keyof typeof jo
            const k: dk = ig as dk
            delete jo[k]
          }
          t.push(jo)
        }
      } catch (e)
      {
        debugger //catch

        S3DB_Logging("exception", "934", `Exception - Transform - Applying Ignore - \n${e}`)

      }

      if (t.length === updates.length)
      {
        updates = t
      } else
      {
        debugger //catch

        S3DB_Logging("error", "933", `Error - Transform - Applying Ignore returns fewer records ${t.length} than initial set ${updates.length}`)

        throw new Error(
          `Error - Transform - Applying Ignore returns fewer records ${t.length} than initial set ${updates.length}`
        )
      }

      S3DB_Logging("info", "919", `Transforms (Ignore) applied: \n${JSON.stringify(t)}`)
    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "934", `Exception - Applying Ignore Transform \n${e}`)
  }

  return updates
}

function applyJSONMap (jsonObj: object, map: object) //map: {[key: string]: string}) {
{
  let j
  Object.entries(map).forEach(([key, jpath]) => {

    //const p = jm[key]  
    //const p2 = jpath.substring(2, jpath.length)
    //if (["contactid", "contactkey", "addressablefields", "consent", "audience"].includes(key.toLowerCase()) ||
    //  ["contactid", "contactkey", "addressablefields", "consent", "audience"].includes(p2.toLowerCase())
    //)
    //{
    //  S3DB_Logging("error", "999", `JSONMap config: Either part of the JSONMap statement, the Column to create or the JSONPath statement, cannot use a reserved word ("contactid", "contactkey", "addressablefields", "consent", or "audience") ${key}: ${jpath}`)
    //  throw new Error(`JSONMap config: Either part of the JSONMap statement, the Column to create or the JSONPath statement, cannot reference a reserved word ("contactid", "contactkey", "addressablefields", "consent", or "audience") ${key}: ${jpath}`)
    //}


    try
    {
      j = jsonpath.value(jsonObj, jpath)
      //Object.assign(jsonObj, {[k]: j})

      S3DB_Logging("info", "930", `JSONPath statement ${jpath} returns ${j} from: \nTarget Data: \n${JSON.stringify(jsonObj)} `)
    } catch (e)
    {
      debugger //catch

      //if (e instanceof jsonpath.JsonPathError) { S3DB_Logging("error","",`JSONPath error: e.message`) }

      S3DB_Logging("warning", "930", `Error parsing data for JSONPath statement ${key} ${jpath}, ${e} \nTarget Data: \n${JSON.stringify(jsonObj)} `)
    }
  })
  // const a1 = jsonpath.parse(value)
  // const a2 = jsonpath.parent(s3Chunk, value)
  // const a3 = jsonpath.paths(s3Chunk, value)
  // const a4 = jsonpath.query(s3Chunk, value)
  // const a6 = jsonpath.value(s3Chunk, value)

  //Confirms Update was accomplished
  // const j = jsonpath.query(s3Chunk, v)
  // console.info(`${ j } `)
  //})
  //return jsonObj
  if (typeof j === "string") return j
  return String(j)
}

async function getS3Work (s3Key: string, bucket: string) {
  S3DB_Logging("info", "517", `GetS3Work for Key: ${s3Key}`)

  const getObjectCmd = {
    Bucket: bucket,
    Key: s3Key,
  } as GetObjectCommandInput


  let work: string = ""

  try
  {
    await s3
      .send(new GetObjectCommand(getObjectCmd))
      .then(async (getS3Result: GetObjectCommandOutput) => {
        work = (await getS3Result.Body?.transformToString("utf8")) as string
        S3DB_Logging("info", "517", `Work Pulled (${work.length} chars): ${s3Key}`)

      })
  } catch (e)
  {
    debugger //catch

    const err: string = JSON.stringify(e)
    if (err.toLowerCase().indexOf("nosuchkey") > -1)
    {
      //S3DB_Logging("exception", "", `Work Not Found on S3 Process Queue (${s3Key}. Work will not be marked for Retry.`)
      throw new Error(
        `Exception - Work Not Found on S3 Process Queue (${s3Key}. Work will not be marked for Retry. \n${e}`
      )
    }
    else
    {
      S3DB_Logging("exception", "", `Exception - Retrieving Work from S3 Process Queue for ${s3Key}.  \n ${e}`)
      throw new Error(
        `Exception - Retrieving Work from S3 Process Queue for ${s3Key}.  \n ${e}`
      )
    }
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

async function deleteS3Object (s3ObjKey: string, bucket: string) {
  let delRes = ""

  const s3D = {
    Key: s3ObjKey,
    Bucket: bucket,
  }

  const d = new DeleteObjectCommand(s3D)

  try
  {
    await s3
      .send(d)
      .then(async (s3DelResult: DeleteObjectCommandOutput) => {
        delRes = JSON.stringify(s3DelResult.$metadata.httpStatusCode, null, 2)
      })
      .catch((e) => {
        debugger //catch

        S3DB_Logging("exception", "", `Exception - Attempting S3 Delete Command for ${s3ObjKey}: \n ${e} `)

        return delRes
      })
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - Attempting S3 Delete Command for ${s3ObjKey}: \n ${e} `)
  }
  return delRes
}

export async function getAccessToken (config: CustomerConfig) {
  try
  {
    interface RatResp {
      access_token: string,
      status: number,
      error: string,
      error_description: string
    }

    const rat = await fetch(
      `https://api-campaign-${config.region}-${config.pod}.goacoustic.com/oauth/token`,
      {
        method: "POST",
        body: new URLSearchParams({
          refresh_token: config.refreshtoken,
          client_id: config.clientid,
          client_secret: config.clientsecret,
          grant_type: "refresh_token",
        }),
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          "User-Agent": "S3DropBucket GetAccessToken",
        },
      }
    ).then(async (r) => {

      if (r.status != 200)
      {
        S3DB_Logging("error", "900", `Problem retrieving Access Token (${r.status} - ${r.statusText}) Error: ${JSON.stringify(r)}`)
        throw new Error(
          `Problem retrieving Access Token(${r.status} - ${r.statusText}) Error: ${JSON.stringify(r)}`
        )
      }

      return await r.json() as RatResp
    })

    const accessToken = rat.access_token
    return {accessToken}.accessToken
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "900", `Exception - On GetAccessToken: \n ${e}`)

    throw new Error(`Exception - On GetAccessToken: \n ${e}`)
  }
}



async function postToConnect (mutations: string, custconfig: CustomerConfig, count: string, workFile: string) {
  //ToDo: 
  //Transform Add Contacts to Mutation Create Contact
  //Transform Updates to Mutation Update Contact
  //    Need Attributes - Add Attributes to update as seen in inbound data
  //get access token
  //post to connect
  //return result


  //[
  //  {"key": "subscriptionId", "value": "bb6758fc16bbfffbe4248214486c06cc3a924edf","description": "", "enabled": true},
  //  {"key": "x-api-key", "value": "b1fa7ef5024e4da3b0a40aed8331761c", "description": "", "enabled": true},
  //  {"key": "x-acoustic-region", "value": "us-east-1", "description": "", "enabled": true}
  //]

  const myHeaders = new Headers()
  //myHeaders.append("Content-Type", "text/xml")
  //myHeaders.append("Authorization", "Bearer " + process.env[`${c}_accessToken`])
  myHeaders.append("subscriptionId", custconfig.subscriptionid)
  myHeaders.append("x-api-key", custconfig.x_api_key)
  myHeaders.append("x-acoustic-region", custconfig.x_acoustic_region)
  myHeaders.append("Content-Type", "application/json")
  myHeaders.append("Connection", "keep-alive")
  myHeaders.append("Accept", "*/*")
  myHeaders.append("Accept-Encoding", "gzip, deflate, br")

  const requestOptions: RequestInit = {
    method: "POST",
    headers: myHeaders,
    body: mutations,
    redirect: "follow"
  }


  const host = S3DBConfig.connectapiurl

  S3DB_Logging("info", "805", `Connect Mutation Updates about to be POSTed (${workFile}) are: ${mutations}`)

  let connectMutationResult: string = ""


  //{                 //graphQL Spec Doc
  //  "data": { ...},
  //  "errors": [... ],
  //    "extensions": { ...}
  //}

  interface ConnectSuccessResult {
    "data": {
      "createContacts": {
        "items": [
          {
            "contactId": string
          }
        ]
      }
    }
  }

  interface ConnectErrorResult {
    "data": null,
    "errors": [
      {
        "message": string
        "locations": [{"line": number, "column": number}],
        "path": [string],
        "extensions": {"code": string}
      }
    ]
  }

  try
  {

    //{         
    // //graphQL Spec Doc
    //  "query": "...",
    //    "operationName": "...",
    //      "variables": {"myVariable": "someValue", ...},
    //  "extensions": {"myExtension": "someValue", ...}
    //}

    
    connectMutationResult = await fetch(host, requestOptions)
      //.then((response) => response.text())
      .then(async (response) => {
        return await response.json()  // .text())
        //const rj = await response.json()
        //return rj
  })
      .then(async (result) => {
        S3DB_Logging("info", "808", `POST to Connect - Raw Response: \n${JSON.stringify(result)}`)

        //ToDo: Create specific Messaging to line out this error as the Target DB does not have the attribute Defined
        //{"errors": [{"message": "No defined Attribute with name: 'email'", "locations": [{"line": 1, "column": 38}], "path": ["updateContacts"], "extensions": {"code": "ATTRIBUTE_NOT_DEFINED"}}], "data": null}

        //const r3 = {          // as ConnectSuccessResult
        //      "data" : {
        //        "createContacts" : {
        //          "items" : [
        //            {
        //              "contactId" : "529dbe72-11c6-515a-a6f4-0069988668f5"
        //            },
        //            {
        //              "contactId" : "5510fe0f-27b1-56ea-90a9-67a3bad9f364"
        //            }
        //          ]
        //        }
        //      }
        //    }

        //{                   //  as  ConnectErrorResult
        //  "errors": [
        //    {
        //      "message": "Field Mobile Number has invalid value: 211-411-5555. Category: PHONE_FORMAT. Message: Invalid phone number, only 0-9 allowed as phone number",
        //      "locations": [
        //        {
        //          "line": 1,
        //          "column": 47
        //        }
        //      ],
        //      "path": [
        //        "createContacts"
        //      ],
        //      "extensions": {
        //        "code": "INVALID_ATTRIBUTE_VALUE",
        //        "category": "PHONE_FORMAT"
        //      }
        //    }
        //  ],
        //    "data": null
        //}


        //POST Result: {"message": "Endpoint request timed out"}

        debugger ///

        if (JSON.stringify(result).indexOf("Endpoint request timed out") > 0)
        {
          S3DB_Logging("warn", "809", `Connect Mutation POST - Temporary Error: Request Timed Out (${JSON.stringify(result)}). Work will be Sent back to Retry Queue. `)
          return `retry ${JSON.stringify(result)}`
        }


        let errors = []
        const cer = result as ConnectErrorResult
        
        if (typeof cer.errors !== "undefined")
        {
          for (const e in cer.errors)
          {
            S3DB_Logging("error", "827", `Connect Mutation POST - Error: ${cer.errors[e].message}`)
            //throw new Error(`Connect Mutation POST - Error: ${cr.errors[e].message}`)
            errors.push(cer.errors[e].message)
          }

          return `Unsuccessful POST of the Updates \n ${JSON.stringify(errors)}`
        }


        //S3DB_Logging("warn", "829", `Temporary Failure - POST Updates - Marked for Retry. \n${result}`)
        //S3DB_Logging("error", "827", `Unsuccessful POST of the Updates (${m.length} of ${count}) - \nFailure Msg: ${JSON.stringify(msg)}`)


        const csr = result as ConnectSuccessResult

        //If we haven't returned before now then it's a successful post 
        const spr = JSON.stringify(csr).replace("\n", " ")

        S3DB_Logging("info", "826", `Successful POST Result (${workFile}): ${spr}`)

        //return `Successfully POSTed (${count}) Updates - Result: ${result}`
        return `Successfully POSTed (${count}) Updates - Result: ${spr}`
      })
      .catch((e) => {

        debugger //catch

        const h = host
        const r = requestOptions
        const m = mutations

        if (e.message.indexOf("econnreset") > -1)
        {
          S3DB_Logging("exception", "829", `PostToConnect Error (then.catch) - Temporary failure to POST the Updates - Marked for Retry. ${JSON.stringify(e)}`)
          return `retry ${JSON.stringify(e)}`
        } else
        {
          S3DB_Logging("exception", "", `Error - Unsuccessful POST of the Updates: ${JSON.stringify(e)}`)
          return `Unsuccessful POST of the Updates \n${JSON.stringify(e)}`
        }
      })
  } catch (e)
  {
    debugger //catch
    
    const h = host
    const r = requestOptions
    const m = mutations

    S3DB_Logging("error", "829", `PostToConnect - Error (try-catch): ${JSON.stringify(e)}`)
    return `unsuccessful post \n ${JSON.stringify(e)}`
  }

  //retry
  //unsuccessful post
  //partially successful
  //successfully posted

  return connectMutationResult
}


export async function postToCampaign (
  xmlCalls: string,
  config: CustomerConfig,
  count: string,
  workFile: string
) {
  const c = config.customer

  //Store AccessToken in process.env vars for reference across invocations, save requesting it repeatedly
  if (
    process.env[`${c}_accessToken`] === undefined ||
    process.env[`${c}_accessToken`] === null ||
    process.env[`${c}_accessToken`] === ""
  )
  {
    process.env[`${c}_accessToken`] = (await getAccessToken(config)) as string
    const at = process.env[`${c}_accessToken"`] ?? ""
    const l = at.length
    const redactAT = "......." + at.substring(l - 10, l)
    S3DB_Logging("info", "900", `Generated a new AccessToken: ${redactAT}`)  //ToDo: Add Debug Number 
  } else
  {
    const at = process.env["accessToken"] ?? ""
    const l = at.length
    const redactAT = "......." + at.substring(l - 8, l)
    S3DB_Logging("info", "900", `Access Token already stored: ${redactAT}`) //ToDo: Add Debug Number 
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

  S3DB_Logging("info", "905", `Updates to be POSTed (${workFile}) are: ${xmlCalls}`)

  let postRes: string = ""

  // try
  // {
  postRes = await fetch(host, requestOptions)
    .then((response) => response.text())
    .then(async (result) => {
      S3DB_Logging("info", "908", `Campaign Raw POST Response (${workFile}) : ${result}`)

      const faults: string[] = []

      //const f = result.split( /<FaultString><!\[CDATA\[(.*)\]\]/g )

      if (
        result.toLowerCase().indexOf("max number of concurrent") > -1 ||
        result.toLowerCase().indexOf("access token has expired") > -1
      )
      {
        S3DB_Logging("warn", "929", `Temporary Failure - POST Updates - Marked for Retry. \n${result}`)

        return `retry ${JSON.stringify(result)}`

      } else if (result.indexOf("<FaultString><![CDATA[") > -1)
      {

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

        const f = result.split(/<FaultString><!\[CDATA\[(.*)\]\]/g)
        if (f && f?.length > 0)
        {
          for (const fl in f)
          {
            faults.push(f[fl])
          }
        }
        

        S3DB_Logging("warn", "928", `Partially Successful POST of the Updates (${f.length} FaultStrings on ${count} updates from ${workFile}) - \nResults\n ${JSON.stringify(faults)}`)

        return `Partially Successful - (${f.length} FaultStrings on ${count} updates) \n${JSON.stringify(faults)}`
      }
      //<FAILURE failure_type="transient" description = "Error saving row" >
      else if (result.indexOf("<FAILURE failure_type") > -1)
      {
        let msg = ""

        //Add this Fail
          //<SUCCESS> true < /SUCCESS>
          //    < FAILURES >
          //    <FAILURE failure_type="permanent" description = "There is no column registeredAdvisorTitle" >
      
        const m = result.match(/<FAILURE (.*)>$/g)

        if (m && m?.length > 0)
        {
          for (const l in m)
          {
            // "<FAILURE failure_type=\"permanent\" description=\"There is no column name\">"
            //Actual message is ambiguous, changing it to read less confusingly:
            l.replace("There is no column name", "There is no column named")
            msg += l
          }

          S3DB_Logging("error", "927", `Unsuccessful POST of the Updates (${m.length} of ${count}) - \nFailure Msg: ${JSON.stringify(msg)}`)

          return `Error - Unsuccessful POST of the Updates (${m.length} of ${count}) - \nFailure Msg: ${JSON.stringify(msg)}`
        }
      }


      //If we haven't returned before now then it's a successful post 
      const spr = JSON.stringify(result).replace("\n", " ")
      S3DB_Logging("info", "926", `Successful POST Result: ${spr}`)
      return `Successfully POSTed (${count}) Updates - Result: ${spr}`
    })
    .catch((e) => {
      debugger //catch

      if (typeof e === "string" && e.toLowerCase().indexOf("econnreset") > -1)
      {
        S3DB_Logging("exception", "929", `Error - Temporary failure to POST the Updates - Marked for Retry. ${e}`)

        return "retry"
      } else
      {
        S3DB_Logging("exception", "927", `Error - Unsuccessful POST of the Updates: ${JSON.stringify(e)}`)
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


//function checkMetadata() {
//  //Pull metadata for table/db defined in config
//  // confirm updates match Columns
//  //ToDo:  Log where Columns are not matching
//}

async function getAnS3ObjectforTesting (bucket: string) {
  let s3Key: string = ""

  if (testS3Key !== null) return

  const listReq = {
    Bucket: bucket,
    MaxKeys: 101,
    Prefix: S3DBConfig.s3dropbucket_prefixfocus,
  } as ListObjectsCommandInput

  await s3
    .send(new ListObjectsCommand(listReq))
    .then(async (s3ListResult: ListObjectsCommandOutput) => {
      let i: number = 0

      if (s3ListResult.Contents)
      {
        const kc: number = (s3ListResult.Contents.length as number) - 1
        if ((kc == 0))
          throw new Error("No S3 Objects to retrieve as Test Data, exiting")

        if (kc > 10)
        {
          i = Math.floor(Math.random() * (10 - 1 + 1) + 1)
        }
        if ((kc == 1)) i = 0
        s3Key = s3ListResult.Contents?.at(i)?.Key as string

        while (s3Key.toLowerCase().indexOf("aggregat") > -1)
        {
          i++
          s3Key = s3ListResult.Contents?.at(i)?.Key as string
        }
        //Log all Test files found
        S3DB_Logging("info", "", `S3 List: \n${JSON.stringify(s3ListResult.Contents)} `)
        //Log the file used for this test run 
        S3DB_Logging("info", "", `This is a Test Run(${i}) Retrieved ${s3Key} for this Test Run`)

      } else
      {
        S3DB_Logging("exception", "", `No S3 Object available for Testing: ${bucket} `)
        throw new Error(`No S3 Object available for Testing: ${bucket} `)
      }
      return s3Key
    })
    .catch((e) => {
      debugger //catch

      S3DB_Logging("exception", "", `Exception - On S3 List Command for Testing Objects from ${bucket}: ${e} `)
    })

  return s3Key
}

async function getAllCustomerConfigsList (bucket: string) {

  const listReq = {
    Bucket: bucket,        //`${bucket}.s3.amazonaws.com`,
    MaxKeys: 500
    //Prefix: S3DBConfig.PrefixFocus,
  } as ListObjectsCommandInput

  const l = [] as string[]

  //"Exception - Retrieving Customer Configs List from s3dropbucket-configs: PermanentRedirect: The bucket you are 
  //attempting to access must be addressed using the specified endpoint.Please send all future requests to this 
  //endpoint. "

  await s3
    .send(new ListObjectsCommand(listReq))
    .then(async (s3ListResult: ListObjectsCommandOutput) => {

      if (s3ListResult.Contents?.length == 0)
      {
        throw new Error("No Customer Configs Found, unable to establish Customer Configs List. ")
      }

      const cc = (s3ListResult.Contents) as object[]

      //Scrub all but '.jsonc' out 
      for (const m of cc)
      {
        type cck = keyof typeof m
        const i: cck = "Key" as cck
        const km = m[i] as string
        if (km.indexOf('.jsonc') > -1) l.push(km)
      }

    })
    .catch((e) => {
      debugger //catch
      S3DB_Logging("exception", "", `Exception - Retrieving Customer Configs List from ${bucket}: ${e} `)
    })

  return l
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
//              debugger //catch
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
//  //                     let versionId = ""
//  //                     let batch = ""
//  //                     let updates = ""

//  //                     const r1 = new RegExp( /json_update_(.*)_/g )
//  //                     let rm = r1.exec( key ) ?? ""
//  //                     batch = rm[ 1 ]

//  //                     const r2 = new RegExp( /json_update_.*?_(.*)\./g )
//  //                     rm = r2.exec( key ) ?? ""
//  //                     updates = rm[ 1 ]

//  //                     const marker = "ReQueued on: " + new Date()

//  //

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

//  const l = reQueue.length
//  return [l, reQueue]
//}




