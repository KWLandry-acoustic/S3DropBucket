"use strict";

import { PutObjectCommand, PutObjectCommandOutput, S3, S3Client, S3ClientConfig } from "@aws-sdk/client-s3"
import { GetObjectCommand, GetObjectCommandOutput, GetObjectCommandInput } from "@aws-sdk/client-s3"
import { DeleteObjectCommand, DeleteObjectCommandInput, DeleteObjectCommandOutput, DeleteObjectOutput, DeleteObjectRequest } from "@aws-sdk/client-s3"
import { ListObjectsV2Command, ListObjectsV2CommandInput, ListObjectsV2CommandOutput } from "@aws-sdk/client-s3"
import { Handler, S3Event, Context, SQSEvent } from "aws-lambda"
import fetch, { Headers, RequestInit, Response } from "node-fetch"
// import { Writable, Readable, Stream, Duplex } from "node:stream";
import Papa from 'papaparse';

import { SQSClient, ReceiveMessageCommand, DeleteMessageBatchCommand, ReceiveMessageCommandOutput, Message, paginateListQueues, SendMessageCommand, SendMessageCommandOutput } from "@aws-sdk/client-sqs";

// import 'source-map-support/register'

// import { packageJson } from '@aws-sdk/client3/package.json'
// const version = packageJson.version


// process.env.AWS_REGION = "us-east-1"
// process.env.accessToken = ''


//SQS 
//ToDo: pickup config from tricklercache config
// process.env.SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/777957353822/tricklercacheQueue";

const sqsClient = new SQSClient({});

const sqsParams = {
    MaxNumberOfMessages: 1,
    QueueUrl: process.env.SQS_QUEUE_URL,
    VisibilityTimeout: 30,
    WaitTimeSeconds: 10,
     MessageAttributes: {
            FirstQueued: {
                DataType: "String",
                StringValue: ''
        },
            Retry: {
                DataType: "Number",
                StringValue: '0',
            },
        },
    MessageBody: ''
};

export type sqsObject = {
    bucketName: string;
    objectKey: string;
};

//-----------SQS


/**
  * A Lambda function to process the Event payload received from S3.
  */

const s3 = new S3Client({ region: "us-east-1" })
 

let workQueuedSuccess = false
let POSTSuccess = false;


let xmlRows: string = ''

interface S3Object {
    Bucket: string
    Key: string
}

interface customerConfig {
    customer: string
    format: string          // csv (w/ Headers), json (as idx'd), jsonFixed (fixed Columns), csvFixed (fixed Columns)
    listId: string
    listName: string
    colVals: { [idx: string]: string }
    columns: string[]
    pod: string                // 1,2,3,4,5,6,7
    region: string             // US, EU, AP
    refreshToken: string       // API Access
    clientId: string           // API Access
    clientSecret: string       // API Access
}


let config = {} as customerConfig


export interface accessResp {
    access_token: string
    token_type: string
    refresh_token: string
    expires_in: number
}

export interface tcQueueMessage {
    workKey: string,
    updates: string, 
    custconfig: customerConfig
}


export interface tcConfig {
    "AWS_REGION": string,
    "SQS_QUEUE_URL": string,
    "xmlapiurl": string,
    "restapiurl": string,
    "authapiurl": string,
    "ProcessQueueVisibilityTimeout": number,
    "ProcessQueueWaitTimeSeconds": number,
    "RetryQueueVisibilityTimeout": number,
    "RetryQueueInitialWaitTimeSeconds": number
}

let tc = {} as tcConfig

// description="There is no column __parsed_extra">
// Pipes the S3 Read Stream, converting csv to json. 
const csvParseStream = Papa.parse(Papa.NODE_STREAM_INPUT, {
    header: true,
    comments: '#',
    // fastMode: true,  //ToDo: can use as long as no "Quoted" strings as values, commas skew results, need to escape commas in values. 
    skipEmptyLines: true,
    // step: function (results, parser) {               //Cannot use Step when using Streams
    //     console.log("Row data: ", results.data);
    //     console.log("Character Index: ", parser.getCharIndex)
    //     console.log("Row errors: ", results.errors);
    //     console.log("Results Metadata: ", results.meta)
    //     return results
    // },
    transform: function (value, field) {
        // console.log(`ParseCSVStream - Row Field: ${field}, Row Value: ${value}`)
        return value.trim()
    },
    transformHeader: function (header, index) {
        index
        return header.trim()
    }
})


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
export const tricklerQueueProcessorHandler: Handler = async (event: SQSEvent, context: Context) => {


    //ToDo: Confirm SQS Queue deletes queued work
    //Interface to View and Edit Customer Configs
    //Interface to view Logs/Errors (echo cloudwatch logs?)
    //
    
    if (process.env.ProcessQueueVisibilityTimeout === undefined || process.env.ProcessQueueVisibilityTimeout === '' || process.env.ProcessQueueVisibilityTimeout === null)
    {
        console.log(`Debug-Process Env not populated: ${JSON.stringify(process.env)}`)
        tc = await getTricklerConfig()
    }
    else console.log(`Debug-Process Env already populated: ${JSON.stringify(process.env)}`)


    const tqm: tcQueueMessage = JSON.parse(event.Records[0].body)
    debugger;
    
    console.log(`Processing Work Queue for ${JSON.stringify(tqm.workKey)}`)
    console.log(`Debug-Processing Work Queue - Work File: \n ${JSON.stringify(tqm)}`)

    let postResult

    try
    {
        const work = await getS3Work(tqm.workKey)
        if(!work) throw new Error(`Failed to retrieve work (${tqm.workKey}) from Queue: `)

        postResult = await postToCampaign(work, tqm.custconfig, tqm.updates)
        if (postResult.postRes === 'retry')
        {
            await reQueue(event, tqm)
            return postResult?.POSTSuccess
        }

        if (!postResult.POSTSuccess) throw new Error(`Failed to process work ${tqm.workKey} - ${postResult.postRes} `)
            
        if (postResult.POSTSuccess)
        {
            deleteS3Object(tqm.workKey, 'trickler-process')
        }
            
    } catch (e)
    {
        console.log(`Processing Work Queue Exception: ${e}`)
    }
    debugger;

    console.log(`Debug-Processing Work Queue - Work (${tqm.workKey})\n ${postResult?.postRes}`)
    console.log(`Processing Work Queue - Work (${tqm.workKey}), Success(${postResult?.POSTSuccess})`)

 return postResult?.POSTSuccess

}

export const s3JsonLoggerHandler: Handler = async (event: S3Event, context: Context) => {


    // console.log(`AWS-SDK Version: ${version}`)
    // console.log('ENVIRONMENT VARIABLES\n' + JSON.stringify(process.env, null, 2))

    //When Local Testing - pull an S3 Object and so avoid the not-found error
    if (!event.Records[0].s3.object.key || event.Records[0].s3.object.key === "devtest.csv") {
        event.Records[0].s3.object.key = await getAnS3ObjectforTesting(event) as string
    }
    
    if (process.env.ProcessQueueVisibilityTimeout === undefined || process.env.ProcessQueueVisibilityTimeout === '' || process.env.ProcessQueueVisibilityTimeout === null)
    {
        console.log(`Debug-Process Env not populated: ${JSON.stringify(process.env)}`)
        tc = await getTricklerConfig()
    }
    else console.log(`Debug-Process Env already populated: ${JSON.stringify(process.env)}`)

    

    const customer = (event.Records[0].s3.object.key.split("_")[0] + "_")
    // console.log("GetCustomerConfig: Customer string is ", customer)

    console.log(`Processing Object from S3 Trigger, Event RequestId: ", ${event.Records[0].responseElements["x-amz-request-id"]}. Customer is ${customer}, Num of Events to be processed: ${event.Records.length}`)

    //Just in case we start getting multiple file triggers for whatever reason
    if (event.Records.length > 1) throw new Error(`Expecting only a single S3 Object from a Triggered S3 write of a new Object, received ${event.Records.length} Objects`)

    try {
        config = await getCustomerConfig(customer) as customerConfig
    } catch (e) {
        throw new Error(`Exception Retrieving Config: \n ${e}`)

    }
    try {
        config = await validateConfig(config)
    } catch (e) {
        throw new Error(`Exception Validating Config ${e}`)

    }

    const s3Result = await processS3ObjectContentStream(event)

    console.log(`Processing of ${ event.Records[0].s3.object.key } Completed (${ s3Result.workQueuedSuccess }), \n${ s3Result.s3ContentResults }`)

    //Once successful delete the original S3 Object
    if (s3Result.workQueuedSuccess) {
        const delResultCode = await deleteS3Object(event.Records[0].s3.object.key, event.Records[0].s3.bucket.name);
        console.log(`Result from Delete of ${event.Records[0].s3.object.key}: ${delResultCode} `);
    }
    else
        throw new Error(`Deletion of Object ${event.Records[0].s3.object.key} skipped as previous processing failed`)

    return `TricklerCache Processing of ${event.Records[0].s3.object.key} Successfully Completed.  ${s3Result.workQueuedSuccess}, ${s3Result.s3ContentResults}`

};

export default s3JsonLoggerHandler


async function getTricklerConfig () {
    
    //populate env vars with tricklercache config 
    const getObjectCmd = {
        Bucket: "tricklercache-configs",
        Key: 'tricklercache_config.json'
    }

    let tc = {} as tcConfig
    try
    {
        tc = await s3.send(new GetObjectCommand(getObjectCmd))
            .then(async (s3Result: GetObjectCommandOutput) => {
                const cr = await s3Result.Body?.transformToString('utf8') as string
                debugger;
                tc = JSON.parse(cr)
                console.log(`Tricklercache Config: \n ${cr}`)
                return tc
            })
    } catch (e)
    {
        console.log(`Pulling TricklerConfig Exception \n ${e}`)
    }

    try
    {
        //ToDo: Need validation of Config 
        process.env.SQS_QUEUE_URL = tc.xmlapiurl
        process.env.xmlapiurl = tc.xmlapiurl
        process.env.restapiurl = tc.restapiurl
        process.env.authapiurl = tc.authapiurl
        process.env.ProcessQueueVisibilityTimeout = tc.ProcessQueueVisibilityTimeout.toFixed()
        process.env.ProcessQueueWaitTimeSeconds = tc.ProcessQueueWaitTimeSeconds.toFixed()
        process.env.RetryQueueVisibilityTimeout = tc.ProcessQueueWaitTimeSeconds.toFixed()
        process.env.RetryQueueInitialWaitTimeSeconds = tc.RetryQueueInitialWaitTimeSeconds.toFixed()
    } catch (e)
    {
        throw new Error(`Exception parsing TricklerCache Config File ${e}`)
    }
    return tc
}



async function getCustomerConfig(customer: string) {

    // ToDo: Once retrieved need to validate all fields 

    let configJSON = {}
    const configObjs = [new Uint8Array()];

    try {
        await s3.send(
            new GetObjectCommand({
                Key: `${customer}config.json`,
                Bucket: `tricklercache-configs`
            })
        ).then(async (s3Result: GetObjectCommandOutput) => {
            if (s3Result.$metadata.httpStatusCode != 200) {
                let errMsg = JSON.stringify(s3Result.$metadata);
                throw new Error(`Get S3 Object Command failed for ${customer}config.json \n${errMsg}`);
            }
            const s3Body = s3Result.Body as NodeJS.ReadableStream

            configJSON = new Promise<customerConfig>(async (resolve, reject) => {
                if (s3Body !== undefined) {

                    s3Body.on("data", (chunk: Uint8Array) => {
                        configObjs.push(chunk)
                    })
                    s3Body.on("error", () => {
                        reject;
                    })
                    s3Body.on("end", () => {
                        let cj = {} as customerConfig
                        let cf = Buffer.concat(configObjs).toString("utf8")
                        try {
                            cj = JSON.parse(cf)
                            console.log(`Parsing Config File: ${cj.customer}, Format: ${cj.format}, Region: ${cj.region}, Pod: ${cj.pod}, List Name: ${cj.listName},List  Id: ${cj.listId}, `)
                        } catch (e) {
                            throw new Error(`Exception Parsing Config ${customer}config.json: ${e}`)
                        }

                        resolve(cj)
                    })
                }
            }).catch((e) => {
                throw new Error(`Exception retrieving Customer Config ${customer}config.json \n${e}`)
            })
        })
    } catch (e) {
        throw new Error(`Exception retrieving Customer Config ${customer}config.json: \n ${e}`);
    }

    return configJSON
}

async function validateConfig(config: customerConfig) {
    if (!config || config === null) { throw new Error("Invalid Config Content - is not defined (empty or null config)") }
    if (!config.customer) { throw new Error("Invalid Config Content - Customer is not defined") }
    if (!config.clientId) { throw new Error("Invalid Config Content - ClientId is not defined") }
    if (!config.clientSecret) { throw new Error("Invalid Config Content - ClientSecret is not defined") }
    if (!config.format) { throw new Error("Invalid Config Content - Format is not defined") }
    if (!config.listId) { throw new Error("Invalid Config Content - ListName is not defined") }
    if (!config.listName) { throw new Error("Invalid Config Content - ListName is not defined") }
    if (!config.pod) { throw new Error("Invalid Config Content - Pod is not defined") }
    if (!config.region) { throw new Error("Invalid Config Content - Region is not defined") }
    if (!config.refreshToken) { throw new Error("Invalid Config Content - RefreshToken is not defined") }

    if (!config.format.toLowerCase().match(/^(?:csv|json)$/igm)) {
        throw new Error("Invalid Config - Format is not 'CSV' or 'JSON' ")
    }

    if (!config.pod.match(/^(?:0|1|2|3|4|5|6|7|8|9|a|b)$/igm)) {
        throw new Error("Invalid Config - Pod is not 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, A, or B. ")
    }

    if (!config.region.toLowerCase().match(/^(?:us|eu|ap|ca)$/igm)) {
        throw new Error("Invalid Config - Region is not 'US', 'EU', CA' or 'AP'. ")
    }

    return config as customerConfig
}

async function processS3ObjectContentStream(event: S3Event) {

    let chunks: string[] = new Array();
    let s3ContentResults = ''
    let batchCount = 0

    console.log(`Pulling S3 Content for ${event.Records[0].s3.object.key}`)
                                

    try {
        await s3.send(
            new GetObjectCommand({
                Key: event.Records[0].s3.object.key,
                Bucket: event.Records[0].s3.bucket.name
            })
        )
            .then(async (s3Result: GetObjectCommandOutput) => {
                if (s3Result.$metadata.httpStatusCode != 200) {
                    let errMsg = JSON.stringify(s3Result.$metadata);
                    throw new Error(`Get S3 Object Command failed for ${event.Records[0].s3.object.key}. Result is ${errMsg}`);
                }

                let recs = 0

                s3ContentResults = await new Promise<string>(async (resolve, reject) => {
                    console.log(`1.Keep an eye on Batch count:   ${batchCount}`)
                    try {
                        let s3ContentStream = s3Result.Body as NodeJS.ReadableStream

                        if (config.format.toLowerCase() === 'csv') {
                            s3ContentStream = s3ContentStream.pipe(csvParseStream)
                        }

                        s3ContentStream
                            .on('data', async function (jsonChunk: string) {
                                recs++
                                // console.log(`Another chunk (${recs}): ${jsonChunk}, chunks length is ${chunks.length}`)
                                chunks.push(jsonChunk)

                                if (chunks.length > 98)
                                {
                                    batchCount++
                                    console.log(`2.Keep an eye on Batch count:   ${batchCount}`)
                                    console.log(`Parsing S3 Content Stream (batch ${batchCount}) processed ${recs} chunks: `, jsonChunk)
                                    xmlRows = convertToXML(chunks, config)
                                    await queueWork(xmlRows, event, batchCount.toString(), chunks.length.toString())
                                    chunks.length = 0
                                }
                            })

                            .on('end', async function (msg: string) {
                                batchCount++
                                console.log(`3.Keep an eye on Batch count:   ${batchCount}`)
                                console.log(`S3 Content Stream Ended for ${event.Records[0].s3.object.key}  (${batchCount} Batches - Processed ${recs} records)`)
                                xmlRows = convertToXML(chunks, config)
                                await queueWork(xmlRows, event, batchCount.toString(), chunks.length.toString())
                                chunks.length = 0
                                batchCount = 0
                                workQueuedSuccess = true
                                return('S3 Content parsing Successful End')
                            })

                            .on('error', async function (err: string) {
                                chunks.length = 0
                                batchCount = 0
                                console.log("Failed Parsing S3 Content - Error: ", err)
                                workQueuedSuccess = false
                                throw new Error(`An error has stopped Content Parsing at record ${recs} for s3 object ${event.Records[0].s3.object.key}. ${err}`)
                            })

                            .on('close', function (msg: string) {
                                chunks.length = 0
                                batchCount = 0
                                console.log(`Completed Parsing S3 Content, processed ${recs} records from ${event.Records[0].s3.object.key}`)
                            })

                    } catch (e)
                    {
                        workQueuedSuccess = false
                        chunks.length = 0
                        batchCount = 0
                        throw new Error(`Exception parsing S3 Content for ${event.Records[0].s3.object.key}, \n${e}`)
                    }

                })
                // .catch((e) => {
                //         throw new Error(`Exception Processing (Promise) S3 Get Object Content for ${event.Records[0].s3.object.key}: \n ${e}`);
                //     })

            })
    } catch (e)
    {
        chunks.length = 0
        batchCount = 0
        throw new Error(`Exception during Processing of S3 Object for ${event.Records[0].s3.object.key}: \n ${e}`);
    }

    return { s3ContentResults, workQueuedSuccess }
}


function convertToXML(rows: string[], config: customerConfig) {

    console.log(`Packaged ${rows.length} rows as updates to ${config.customer}'s ${config.listName}`)
    
    xmlRows = `<Envelope><Body><InsertUpdateRelationalTable><TABLE_ID>${config.listId}</TABLE_ID><ROWS>`
    let r = 0

    rows.forEach((jo) => {
        r++
        xmlRows += `<ROW>`
        Object.entries(jo)
            .forEach(([key, value]) => {
                // console.log(`Record ${r} as ${key}: ${value}`)
                xmlRows += `<COLUMN name="${key}"> <![CDATA[${value}]]> </COLUMN>`
            })
        xmlRows += `</ROW>`
    })

    //Tidy up the XML 
    xmlRows += `</ROWS></InsertUpdateRelationalTable></Body></Envelope>`

    return xmlRows
}

async function queueWork(queueContent: string, event: S3Event, batchNum: string, count: string) {

    console.log(`Debug-QueueWork:  Batch - ${batchNum}  Count - ${count} `)

    const s3Key = event.Records[0].s3.object.key
    await storeWorkToS3(queueContent, s3Key, batchNum)
    await addWorkToProcessQueue(config, s3Key, batchNum, count) 

}

async function storeWorkToS3(queueContent: string, s3Key: string, batch: string) {

    //write to the S3 Process Bucket
    const s3PutInput = {
        "Body": queueContent,
        "Bucket": "tricklercache-process",
        "Key": `process_${batch}_${s3Key}`
    };

    console.log(`Queuing Work (process_${batch}_${s3Key}) to Process Queue`);

    let qs3

    try {
        qs3 = await s3.send(new PutObjectCommand(s3PutInput))
            .then(async (s3PutResult: PutObjectCommandOutput) => {
                qs3 = JSON.stringify(s3PutResult.$metadata.httpStatusCode, null, 2)
                console.log(`Queue Work - Write work (process_${batch}_${s3Key} to Processing Queue - ${qs3}`);
            })
            .catch((err) => {
                throw new Error(`Failed writing work(process_${ batch }_${ s3Key } to S3 Processing bucket: ${ err }`)
        })
    } catch (e) {
        throw new Error(`Exception writing work(process_${batch}_${s3Key} to S3 Processing bucket: ${e}`)
    }
return qs3

}

async function addWorkToProcessQueue (config: customerConfig, s3Key: string, batch: string, count: string) {
    const qb = {} as tcQueueMessage
    qb.workKey = `process_${batch}_${s3Key}`
    qb.updates = count
    qb.custconfig = config
    const qc = JSON.stringify(qb) 
    
    const writeSQSCommand = new SendMessageCommand({
        QueueUrl: sqsParams.QueueUrl,
        DelaySeconds: 1,
        MessageAttributes: {
            FirstQueued: {
                DataType: "String",
                StringValue: `${Date.now().toString()}`
        },
            Retry: {
                DataType: "Number",
                StringValue: '0',
            },
        },
        MessageBody: qc
    });

let qAdd

    try {
        qAdd = await sqsClient.send(writeSQSCommand)
            .then(async (sqsWriteResult: SendMessageCommandOutput) => {
                const wr = JSON.stringify(sqsWriteResult.$metadata.httpStatusCode, null, 2)
                console.log(`Wrote Work to Process Queue (process_${s3Key}) - Result: ${wr} `);
                if (wr !== '200') throw new Error(`Failed writing to Process Queue(${sqsParams}, ${sqsParams.QueueUrl}) - (process_${ qb.workKey }`)
                qAdd = JSON.stringify(sqsWriteResult)
                workQueuedSuccess = true
                //Now Delete S3 File
            })
            .catch((err) => {
            console.log(`Failed writing to Process Queue -(process_${s3Key}) - Error: ${err} `)
        })
    } catch (e) {
        console.log(`Exception writing to Process Queue -(process_${s3Key}) - Error: ${e}`)
    }

    return { qAdd, workQueuedSuccess }

}

async function reQueue (sqsevent: SQSEvent , queued: tcQueueMessage) {

    let mar = sqsevent.Records[0].messageAttributes.Retry.stringValue as string
    let n = parseInt(mar)
    n++
    const r: string = n.toString()
    
    debugger;

    const w = JSON.parse(sqsevent.Records[0].body).work
    
    const sp = sqsParams
    sp.VisibilityTimeout = 10

    //ToDo: create random waittime, based on retry count. 
    sp.WaitTimeSeconds = 10 
    
    sp.MessageAttributes.Retry = {
                DataType: "Number",
                StringValue: r,
    }
    sp.MessageBody = sqsevent.Records[0].body

    const writeSQSCommand = new SendMessageCommand(sp)

    let qAdd

    try {
        qAdd = await sqsClient.send(writeSQSCommand)
            .then(async (sqsWriteResult: SendMessageCommandOutput) => {
                const rr = JSON.stringify(sqsWriteResult.$metadata.httpStatusCode, null, 2)
                console.log(`Process Queue - Wrote Retry Work to SQS Queue (process_${w} - Result: ${rr} `);
                return JSON.stringify(sqsWriteResult)
            });
    } catch (e) {
        throw new Error(`ReQueue Work Exception - Writing Retry Work to SQS Queue: ${e}`)
    }
    debugger;

    return qAdd

}



async function updateDatabase() {
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

async function getS3Work(s3Key: string){
    
    // console.log(`GetS3Work Key: ${s3Key}`)

    const getObjectCmd = {
        Bucket: "tricklercache-process",
        Key: s3Key
    } as GetObjectCommandInput

    let work: string = ''
    try
    {
        await s3.send(new GetObjectCommand(getObjectCmd))
            .then(async (s3Result: GetObjectCommandOutput) => {
                // work = JSON.stringify(b, null, 2)
                work = await s3Result.Body?.transformToString('utf8') as string
                console.log(`Work Pulled (${work.length} chars): ${s3Key}`)
            });
    } catch (e)
    {
        throw new Error(`ProcessQueue - Get Work S3 Object (${s3Key}) Exception ${e}`)
    }
return work
}

export async function getAccessToken(config: customerConfig) {

    try {
        const rat = await fetch(`https://api-campaign-${config.region}-${config.pod}.goacoustic.com/oauth/token`, {
            method: 'POST',
            body: new URLSearchParams({
                refresh_token: config.refreshToken,
                client_id: config.clientId,
                client_secret: config.clientSecret,
                grant_type: 'refresh_token'
            }),
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'User-Agent': 'S3 TricklerCache GetAccessToken'
            }
        })

        const ratResp = (await rat.json()) as accessResp
        if (rat.status != 200) {
            throw new Error(`Exception retrieving Access Token:   ${rat.status} - ${rat.statusText}`)
        }
        const accessToken = ratResp.access_token
        return { accessToken }.accessToken

    } catch (e) {
        console.log("Exception in getAccessToken: \n", e)
    }
}

export async function postToCampaign(xmlCalls: string, config: customerConfig, count: string) {
    
    //
    //For Testing only
    //
            config.pod = '2'
            config.listId = '12663209'
            xmlCalls = xmlCalls.replace('3055683', '12663209')
    //
    //For Testing only
    //

    debugger;

    if ((process.env.accessToken === null) || process.env.accessToken == '')
    {
        console.log(`Need AccessToken...`)
        process.env.accessToken = await getAccessToken(config) as string

        const l = process.env.accessToken.length
        const at = "......." + process.env.accessToken.substring(l - 10, l)
        console.log(`Generated a new AccessToken: ${at}`)
    }
    else
    {
        const l = process.env.accessToken?.length ?? 0
        const at = "......." + process.env.accessToken?.substring(l - 8, l)
        console.log(`Access Token already present: ${at}`)
    }
    debugger;
    const myHeaders = new Headers();
    myHeaders.append("Content-Type", "text/xml");
    myHeaders.append("Authorization", "Bearer " + process.env.accessToken)
    myHeaders.append("Content-Type", "text/xml")
    myHeaders.append("Connection", "keep-alive")
    myHeaders.append("Accept", "*/*")
    myHeaders.append("Accept-Encoding", "gzip, deflate, br")

    let requestOptions: RequestInit = {
        method: 'POST',
        headers: myHeaders,
        body: xmlCalls,
        redirect: 'follow'
    };

    const host = `https://api-campaign-${config.region}-${config.pod}.goacoustic.com/XMLAPI`
    
    let postRes

    try {
        postRes = await fetch(host, requestOptions)
        .then((response) => response.text())
        .then((result) => {
            // console.log("POST Update Result: ", result)
            debugger; 

            if (result.indexOf('max number of concurrent') > -1)
                return "retry" 
   
            if (result.toLowerCase().indexOf('false</success>') > -1)
            {
                // "<Envelope><Body><RESULT><SUCCESS>false</SUCCESS></RESULT><Fault><Request/>
                //   <FaultCode/><FaultString>Invalid XML Request</FaultString><detail><error>
                //   <errorid>51</errorid><module/><class>SP.API</class><method/></error></detail>
                //    </Fault></Body></Envelope>\r\n"
                
                throw new Error(`Unsuccessful POST of Update - ${result}`)
            }

            POSTSuccess = true
            result = result.replace('\n', ' ')
            return `Processed ${count} Updates - Result: ${result}`
        })
        .catch((e) => {
            console.log(`Exception on POST to Campaign: ${e}`)
        })

    } catch(e) {
        console.log(`Exception during POST to Campaign (AccessToken ${process.env.accessToken}) Result: ${e}`)
    }
    debugger;

    return { postRes, POSTSuccess } 
}


async function deleteS3Object(s3ObjKey: string, bucket: string) {
    try {
        console.log(`DeleteS3Object : \n'  ${s3ObjKey}`)
        debugger;
        await s3.send(
            new DeleteObjectCommand({
                Key: s3ObjKey,        
                Bucket: bucket
            })
        ).then(async (s3DelResult: DeleteObjectCommandOutput) => {
            // console.log("Received the following Object: \n", data.Body?.toString());

            const delRes = JSON.stringify(s3DelResult.$metadata.httpStatusCode, null, 2);

            // console.log("Received the following Object: \n", JSON.stringify(data.Body, null, 2));
            console.log(`Result from Delete of ${s3ObjKey}: ${delRes} `);

            return delRes
        });
    } catch (e) {
        console.log(`Exception Processing S3 Delete Command for ${s3ObjKey}: \n ${e}`);
    }
}

function checkMetadata () {
    
    //Pull metadata for table/db defined in config
    // confirm updates match Columns
    // Log where Columns are not matching 
}


async function getAnS3ObjectforTesting(event: S3Event) {
    const listReq = {
        Bucket: event.Records[0].s3.bucket.name,
        MaxKeys: 10
    } as ListObjectsV2CommandInput;

    let s3Key: string = "";

    try
    {
        await s3.send(new ListObjectsV2Command(listReq))
            .then(async (s3ListResult: ListObjectsV2CommandOutput) => {
                // const d = JSON.stringify(s3ListResult.Body, null, 2)

                s3Key = s3ListResult.Contents?.at(1)?.Key as string;

                // event.Records[0].s3.object.key  = s3ListResult.Contents?.at(0)?.Key as string
                // console.log("Received the following Object: \n", JSON.stringify(data.Body, null, 2));
                console.log("TestRun: Retrieved ", s3Key, " for this Test Run \n..\n..\n..");
            });
    } catch (e)
    {
        console.log("Exception Processing S3 List Command: \n..", e, '\n..\n..\n..');
    }
    return s3Key;
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
//             debugger
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
//     debugger;
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


