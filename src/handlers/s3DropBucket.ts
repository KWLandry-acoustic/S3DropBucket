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
  GetObjectCommand,
  GetObjectCommandOutput,
  GetObjectCommandInput,
  InventoryConfigurationFilterSensitiveLog
} from "@aws-sdk/client-s3"

import {NodeHttpHandler} from "@smithy/node-http-handler"
import https from "https";  


import {
  DeleteMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs"

import {
  FirehoseClient,
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


import {parse} from "csv-parse"



import sftpClient, {ListFilterFunction} from "ssh2-sftp-client"
import {config} from 'process'
import {sftpConnect} from './sftpConnect'
import {sftpDisconnect} from './sftpDisconnect'
import {sftpListFiles} from './sftpListFiles'
import {sftpUploadFile} from './sftpUploadFile'
import {sftpDownloadFile} from './sftpDownloadFile'
import {sftpDeleteFile} from './sftpDeleteFile'
import {getValidateS3DropBucketConfig} from './getValidateS3DropBucketConfig'
import {getFormatCustomerConfig} from './getFormatCustomerConfig'
import {packageUpdates} from './packageUpdates'
import {getS3Work} from './getS3Work'
import {deleteS3Object} from './deleteS3Object'
import {postToConnect} from './postToConnect'
import {postToCampaign} from './postToCampaign'
import {getAnS3ObjectforTesting} from './getAnS3ObjectforTesting'
import type {S3DBQueueMessage} from './addWorkToSQSWorkQueue'



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



export let s3 = {} as S3Client

export const fh_Client = new FirehoseClient({region: process.env.S3DropBucketRegion})

export const SFTPClient = new sftpClient()


//For when needed to reference Lambda execution environment /tmp folder
// import { ReadStream, close } from 'fs'

export const sqsClient = new SQSClient({region: process.env.S3DropBucketRegion})

export type sqsObject = {
  bucketName: string
  objectKey: string
}



let localTesting = false

let chunksGlobal: object[]

export let xmlRows = ""
let mutationRows = ""
export let batchCount = 0
export let recs = 0

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
      dateday: string
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
      dateday: string       //ToDo: refactor as multiple properties config
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

export let customersConfig: CustomerConfig = {
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
      "dateday": "",
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

export interface S3DBConfig {
  aws_region: string
  connectapiurl: string
  xmlapiurl: string
  restapiurl: string
  authapiurl: string
  s3dropbucket: string
  s3dropbucket_workbucket: string
  s3dropbucket_bulkimport: string
  s3dropbucket_bulkimportquiesce: boolean
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
  s3dropbucket_bulkimport: string

  // Queue configuration
  s3dropbucket_workqueue: string

  // Firehose configuration
  s3dropbucket_firehosestream: string

  // Quiesce settings
  s3dropbucket_quiesce: boolean
  s3dropbucket_workqueuequiesce: boolean
  s3dropbucket_queuebucketquiesce: boolean
  s3dropbucket_bulkimportquiesce: boolean

  //// Maintenance settings for incoming files
  //s3dropbucket_mainthours: number
  //s3dropbucket_maintlimit: number
  //s3dropbucket_maintconcurrency: number

  //// Work queue maintenance settings
  //s3dropbucket_workqueuemainthours: number
  //s3dropbucket_workqueuemaintlimit: number
  //s3dropbucket_workqueuemaintconcurrency: number

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



export let s3dbConfig = {} as S3DBConfig

export interface SQSBatchItemFails {
  batchItemFailures: [
    {
      itemIdentifier: string
    }
  ]
}

export interface AddWorkToS3WorkBucketResults {
  versionId: string
  S3ProcessBucketResultStatus: string
  AddWorkToS3WorkBucketResult: string
}

export interface AddWorkToSQSWorkQueueResults {
  SQSWriteResultStatus: string
  AddWorkToSQSQueueResult: string
}

export interface AddWorkToBulkImportResults {
  BulkImportWriteResultStatus: string
  AddWorkToBulkImportResult: string
}

export interface StoreAndQueueWorkResults {
  AddWorkToS3WorkBucketResults: AddWorkToS3WorkBucketResults
  AddWorkToSQSWorkQueueResults: AddWorkToSQSWorkQueueResults
  AddWorkToBulkImportResults: AddWorkToBulkImportResults
  StoreQueueWorkException: string
  PutToFireHoseAggregatorResults: string
  PutToFireHoseAggregatorResultDetails: string
  PutToFireHoseException: string
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
    StoreAndQueueWorkResult: StoreAndQueueWorkResults
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
        AddWorkToS3WorkBucketResult: ''
      },
      AddWorkToSQSWorkQueueResults: {
        SQSWriteResultStatus: '',
        AddWorkToSQSQueueResult: ''
      },
      AddWorkToBulkImportResults: {
        BulkImportWriteResultStatus: '',
        AddWorkToBulkImportResult: ''
      },
      PutToFireHoseAggregatorResults: '',
      PutToFireHoseAggregatorResultDetails: '',
      PutToFireHoseException: '',
      StoreQueueWorkException: ''
    }
  },
  OnEndStreamEndResult: {
    StoreAndQueueWorkResult: {
      AddWorkToS3WorkBucketResults: {
        versionId: '',
        S3ProcessBucketResultStatus: '',
        AddWorkToS3WorkBucketResult: ''
      },
      AddWorkToSQSWorkQueueResults: {
        SQSWriteResultStatus: '',
        AddWorkToSQSQueueResult: ''
      },
      AddWorkToBulkImportResults: {
        BulkImportWriteResultStatus: '',
        AddWorkToBulkImportResult: ''
      },
      PutToFireHoseAggregatorResults: '',
      PutToFireHoseAggregatorResultDetails: '',
      PutToFireHoseException: '',
      StoreQueueWorkException: ''
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
export let testS3Key: string
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
//testS3Key = "TestData/MasterCustomer_Sample-mar232025.json"
//testS3Key = "TestData/KingsfordWeather_S3DropBucket_Aggregator-10-2025-03-26-09-10-22-dab1ccdf-adbc-339d-8993-b41d27696a3d.json"
//testS3Key = "TestData/MasterCustomer_Sample-mar232025-json-update-10-25-000d4919-2865-49fa-a5ac-32999a583f0a.json"
//testS3Key = "TestData/MasterCustomer_Sample-Queued-json-update-10-25-000d4919-2865-49fa-a5ac-32999a583f0a.json"
//testS3Key = "TestData/SugarCRM_Leads_Leads.data.json.1746103736.13047"
//testS3Key = "TestData/Clorox_UpdateMaster_SUR-WEB-CLX-FTR1.csv"
testS3Key = "TestData/SugarCRM_Contacts_Contacts.data.json.1747640024.16675"
//testS3Key = "TestData/SugarCRM_Contacts_Contacts-data.json.1747640024-updatesTesting.json"
//testS3Key = "TestData/Jai-Shopify-Products_juy092025.json"






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
    
    
    if (typeof process.env.AWS_REGION !== "undefined" && process.env.AWS_REGION !== "undefined" && process.env.AWS_REGION.length > 0) 
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
      s3dbConfig = await getValidateS3DropBucketConfig()

      S3DB_Logging("info", "901", `Parsed S3DropBucket Config:  process.env.S3DropBucketConfigFile: \n${JSON.stringify(s3dbConfig)} `)

    }

    S3DB_Logging("info", "97", `Environment Vars: ${JSON.stringify(process.env)} `)
    S3DB_Logging("info", "98", `S3DropBucket Configuration: ${JSON.stringify(s3dbConfig)} `)
    S3DB_Logging("info", "99", `S3DropBucket Logging Options(process.env): ${process.env.S3DropBucketSelectiveLogging} `)


    if (event.Records[0].s3.object.key.indexOf("S3DropBucket_Aggregator") > -1)
    {
      S3DB_Logging("info", "947", `Processing an Aggregated File ${event.Records[0].s3.object.key}`)
      
    }

    if (s3dbConfig.s3dropbucket_quiesce)
    {
      if (!localTesting)
      {
        S3DB_Logging("warn", "923", `S3DropBucket Quiesce is in effect, new Files from ${s3dbConfig.s3dropbucket} will be ignored and not processed. \nTo Process files that have arrived during a Quiesce of the Cache, reference the S3DropBucket Guide appendix for AWS cli commands.`)
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

      vid = r.s3.object.versionId ?? ""
      et = r.s3.object.eTag ?? ""

      try
      { 
        customersConfig = await getFormatCustomerConfig(key) as CustomerConfig

      } catch (e)
      {
        debugger //catch

        S3DB_Logging("exception", "", `Exception - Awaiting Customer Config (${key}) \n${e} `)
        break
      }

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
            Put To Firehose: ${streamRes.OnEndStreamEndResult.StoreAndQueueWorkResult?.PutToFireHoseAggregatorResults ?? "Not Found"}. 
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
              (typeof streamRes.OnEndStreamEndResult.StoreAndQueueWorkResult.PutToFireHoseAggregatorResults !== "undefined" &&
                streamRes.OnEndStreamEndResult.StoreAndQueueWorkResult.PutToFireHoseAggregatorResults === "200") ||

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
        s3dbConfig = await getValidateS3DropBucketConfig()
        S3DB_Logging("info", "901", `Checked and Refreshed S3DropBucket Config \n ${JSON.stringify(s3dbConfig)} `)

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

  let slConfig = "S3DropBucketConfig"
  let selectiveDebug = process.env.S3DropBucketSelectiveLogging ?? s3dbConfig.s3dropbucket_selectivelogging ?? "_97,_98,_99,_504,_511,_901,_910,_924,"

  if (typeof customersConfig.selectivelogging !== "undefined" &&
    customersConfig.selectivelogging !== "undefined" &&
    customersConfig.selectivelogging.length > 0 &&
    customersConfig.selectivelogging !== "")
  {
    selectiveDebug = customersConfig.selectivelogging
    slConfig = `CustomersConfig:${customersConfig.customer}`
  }
    
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

    if (level.toLowerCase() === "info") console.info(`S3DBLog-Info ${index}: ${msg}    \nRegion: ${r} Version: ${version} \nRef: ${slConfig} - Selective Logging: ${selectiveDebug}`)
    if (level.toLowerCase() === "warn") console.warn(`S3DBLog-Warning ${index}: ${msg} \nRegion: ${r} Version: ${version} \nRef: ${slConfig} - Selective Logging: ${selectiveDebug}`)
    if (level.toLowerCase() === "error") console.error(`S3DBLog-Error ${index}: ${msg} \nRegion: ${r} Version: ${version} \nRef: ${slConfig} - Selective Logging: ${selectiveDebug}`)
    if (level.toLowerCase() === "debug") console.debug(`S3DBLog-Debug ${index}: ${msg} \nRegion: ${r} Version: ${version} \nRef: ${slConfig} - Selective Logging: ${selectiveDebug}`)
  }

  if (level.toLowerCase() === "exception") console.error(`S3DBLog-Exception ${index}: ${msg}  \nRegion: ${r} Version: ${version} \nRef: ${slConfig} - Selective Logging: ${selectiveDebug}`)

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
        Number(s3dbConfig.s3dropbucket_eventemittermaxlisteners)
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

              //What JSON is possible to come through here? (at this point it's always JSON)
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

                  
                  packageResult = await packageUpdates(updates, key, custConfig, s3dbConfig, iter) as StoreAndQueueWorkResults

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
                  packageResult = await packageUpdates(updates, key, custConfig, s3dbConfig, iter)
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
                        S3ProcessBucketResultStatus: "200", //Fake Status to drive cleanup/deletion logic, but true enough here
                        AddWorkToS3WorkBucketResult: "All Work Packaged in OnData, No Work Left to Package"
                      },
                      AddWorkToSQSWorkQueueResults: {
                        SQSWriteResultStatus: "200", //Fake Status to drive cleanup/deletion logic, but true enough here
                        AddWorkToSQSQueueResult: "All Work Packaged in OnData, No Work Left to Package"
                      },
                      AddWorkToBulkImportResults: {
                        BulkImportWriteResultStatus: '',
                        AddWorkToBulkImportResult: ''
                      },
                      PutToFireHoseAggregatorResults: "",
                      PutToFireHoseAggregatorResultDetails: "",
                      PutToFireHoseException: "",
                      StoreQueueWorkException: ''
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
          dateday: "",
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

    if (typeof process.env.AWS_REGION !== "undefined" && process.env.AWS_REGION !== "undefined" && process.env.AWS_REGION.length > 0) 
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
      s3dbConfig = await getValidateS3DropBucketConfig()
      S3DB_Logging("info", "901", `Parsed S3DropBucket Config:  process.env.S3DropBucketConfigFile: \n${JSON.stringify(s3dbConfig)} `)
    }

    S3DB_Logging("info", "97", `Environment Vars: ${JSON.stringify(process.env)} `)
    S3DB_Logging("info", "98", `S3DropBucket Configuration: ${JSON.stringify(s3dbConfig)} `)
    S3DB_Logging("info", "99", `S3DropBucket Logging Options: ${process.env.S3DropBucketSelectiveLogging} `)

    const ra: Array<Record<string, string>> = []
    event.Records.forEach((r) => {
      ra.push({"messageId": r.messageId})
    })
      
    S3DB_Logging("info", "506", `Received a Batch (${event.Records.length}) of SQS Work Queue Events. \nWork Queue Record MessageIds: \n${JSON.stringify(ra)} \nContext: ${JSON.stringify(context)}`)
    S3DB_Logging("info", "907", `Received a Batch (${event.Records.length}) of SQS Work Queue Events: \nWork Queue Records: \n${JSON.stringify(event)} \nContext: ${JSON.stringify(context)}`)

    if (s3dbConfig.s3dropbucket_workqueuequiesce)
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

    //let custconfig: CustomerConfig = customersConfig


    //try
    //{
    //  //if (key.indexOf("S3DropBucket_Aggregator") > -1)
    //  //{
    //  //  key = key.replace("S3DropBucket_Aggregator-", "S3DropBucketAggregator-")
    //  //  S3DB_Logging("info", "", `Aggregator File key reformed: ${key}`)
    //  //}      

    //  customersConfig = await getFormatCustomerConfig(key) as CustomerConfig

    //} catch (e)
    //{
    //  debugger //catch

    //  S3DB_Logging("exception", "", `Exception - Awaiting Customer Config (${key}) \n${e} `)
    //  break
    //}

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
        s3dbConfig.s3dropbucket_workbucket = testS3Bucket
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

        customersConfig = await getFormatCustomerConfig(s3dbQM.custconfig.customer) as CustomerConfig

        const work = await getS3Work(s3dbQM.workKey, s3dbConfig.s3dropbucket_workbucket)

        if (work.length > 0)
        {
          //Retrieve Contents of the Work File
          S3DB_Logging("info", "512", `Work file ${s3dbQM.workKey} retrieved: Result:\n${JSON.stringify(work)}`)
          
          if ((customersConfig.updatetype.toLowerCase() === "createupdatecontacts") ||
            (customersConfig.updatetype.toLowerCase() === "updatecontacts")
            //(customersConfig.updatetype.toLowerCase() === "referenceset") //Should never see this at this point, Bulk Import is finished in the first lambda
          )
          {
            postResult = await postToConnect(
              work,
              customersConfig as CustomerConfig,
              s3dbQM.updateCount,
              s3dbQM.workKey
            )
          }

          

          if (customersConfig.updatetype.toLowerCase() === "relational" ||
            customersConfig.updatetype.toLowerCase() === "dbkeyed" ||
            customersConfig.updatetype.toLowerCase() === "dbnonkeyed")
          {
            postResult = await postToCampaign(
              work,
              customersConfig as CustomerConfig,
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
            S3DB_Logging("error", "520", `Error - Unsuccessful POST (Hard Failure) for ${s3dbQM.workKey}: \n${postResult} \nQueue MessageId: ${q.messageId} \nCustomer: ${s3dbQM.custconfig.customer} `)
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
              s3dbConfig.s3dropbucket_workbucket
            )
            if (fd === "204")
            {
              S3DB_Logging("info", "924", `Processing Complete - Deletion of Queued Work file: ${s3dbQM.workKey}  \nQueue MessageId: ${q.messageId}`)
            } else S3DB_Logging("error", "924", `Processing Complete but Failed to Delete Queued Work File ${s3dbQM.workKey}. Expected '204' but received ${fd} \nQueue MessageId: ${q.messageId}`)

            const qd = await sqsClient.send(
              new DeleteMessageCommand({
                QueueUrl: s3dbConfig.s3dropbucket_workqueue,
                ReceiptHandle: q.receiptHandle
              })
            )
            if (qd.$metadata.httpStatusCode === 200)
            {
              S3DB_Logging("info", "925", `Processing Complete - Deletion of SQS Queue Message (Queue MessageId: ${q.messageId}) \nWorkfile ${s3dbQM.workKey}. \nDeletion Response: ${JSON.stringify(qd)}`)
            } else S3DB_Logging("error", "925", `Processing Complete but Failed to Delete SQS Queue Message (Queue MessageId: ${q.messageId}) \nWorkfile ${s3dbQM.workKey}. \nExpected '200' but received ${qd.$metadata.httpStatusCode} \nDeletion Response: ${JSON.stringify(qd)}`)

          }

          S3DB_Logging("info", "510", `Finished ${event.Records.length} Work Queue Events.
            \nUpdates to be Retried Count: ${sqsBatchFail.batchItemFailures.length} 
            \nUpdates to Retried List: \n${JSON.stringify(sqsBatchFail)} `
            //Last Result: \n${postResult}. `
          )
          
          //ToDo: verify count is correct for the work files being processed, 
          S3DB_Logging("info", "511", `Finished ${s3dbQM.updateCount} Updates from ${s3dbQM.workKey}`)


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
              QueueUrl: s3dbConfig.s3dropbucket_workqueue,
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
    s3dbConfig = await getValidateS3DropBucketConfig()

    S3DB_Logging("info", "901", `Parsed S3DropBucket Config:  process.env.S3DropBucketConfigFile: \n${JSON.stringify(s3dbConfig)} `)

  }

  S3DB_Logging("info", "98", `S3 Dropbucket SFTP Processor - S3DropBucket Configuration: ${JSON.stringify(s3dbConfig)} `)

  S3DB_Logging("info", "700", `S3 Dropbucket SFTP Processor - Received Event: ${JSON.stringify(event)}.\nContext: ${context} `)


  if (!s3dbConfig.s3dropbucket_sftp) return



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
      tqm.workKey = (await getAnS3ObjectforTesting(s3dbConfig.s3dropbucket!)) ?? ""
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


