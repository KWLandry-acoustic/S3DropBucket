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


process.env.AWS_REGION = "us-east-1"
process.env.accessToken = ''


//SQS
process.env.SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/777957353822/tricklercacheQueue";
const sqsClient = new SQSClient({});
const sqsParams = {
    MaxNumberOfMessages: 1,
    QueueUrl: process.env.SQS_QUEUE_URL,
    VisibilityTimeout: 30,
    WaitTimeSeconds: 10
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

let configSuccess = false
let getContentSuccess = false
let updateSuccess = false
let processSuccess = false
let packageAndQueuedSuccess = false


let xmlRows: string = ''

interface S3Object {
    Bucket: string
    Key: string
}

interface tricklerConfig {
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


let custConfig = {} as tricklerConfig


export interface accessResp {
    access_token: string
    token_type: string
    refresh_token: string
    expires_in: number
}

export interface tcQueueMessage {
    work: string,
    config: tricklerConfig
}

//Simply pipe the Readable Stream to the stream returned from
const csvParseStream = Papa.parse(Papa.NODE_STREAM_INPUT, {
    header: true,
    comments: '#',
    fastMode: true,
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
    

    const a = JSON.stringify(event.Records[0].messageAttributes)
    const b = JSON.stringify(event.Records[0].body)
    const c = JSON.stringify(event.Records[0].attributes)
    
    console.log(`TricklerQueueProcessor - MessageAttributes: ${a}`)
    console.log(`TricklerQueueProcessor - Body: ${b}`)
    console.log(`TricklerQueueProcessor - Attributes: ${c}`)

    const qc:tcQueueMessage = JSON.parse(event.Records[0].body)
    console.log("tcQueueMessage: ", qc)
    console.log(`tcQueueMessage work: : ${qc.work}`)

    let postSuccess

    try
    {
        const work = await getS3Work(qc.work, qc.config)
        if(!work) throw new Error(`Work was not retrieved from Queue: ${qc.work}`)

        postSuccess = await postToCampaign(work, qc.config)
        
        if (!postSuccess)
        {
            queueForRetry(work) 
        }
    } catch (e)
    {
        console.log(`${e}`)
    }
    console.log(`POST Success: ${postSuccess}`)

 return true

}

export const s3JsonLoggerHandler: Handler = async (event: S3Event, context: Context) => {


    // console.log(`AWS-SDK Version: ${version}`)
    // console.log('ENVIRONMENT VARIABLES\n' + JSON.stringify(process.env, null, 2))

    console.log("Processing Object from S3 Trigger, Event RequestId: ", event.Records[0].responseElements["x-amz-request-id"])
    console.log("Num of Events to be processed: ", event.Records.length)

    //Just in case we start getting multiple file triggers for whatever reason
    if (event.Records.length > 1) throw new Error(`Expecting only a single S3 Object from a Triggered S3 write of a new Object, received ${event.Records.length} Objects`)

    //When Local Testing - pull an S3 Object and so avoid the not-found error
    if (!event.Records[0].s3.object.key || event.Records[0].s3.object.key === "devtest.csv") {
        event.Records[0].s3.object.key = await getAnS3ObjectforTesting(event) as string
    }

    try {
        custConfig = await getCustomerConfig(event) as tricklerConfig
    } catch (e) {
        throw new Error(`Exception Retrieving Config: \n ${e}`)

    }
    try {
        custConfig = await validateConfig(custConfig)
    } catch (e) {
        throw new Error(`Exception Validating Config ${e}`)

    }

    const s3ObjectContentResult = await processS3ObjectContentStream(event)

    // if (!s3ObjectContent || s3ObjectContent.length < 1) {
    //     throw new Error(`Exception retrieving S3 Object (Get content returns null or empty) for ${event.Records[0].s3.object.key}`)
    // }


    const finalStatus = ` Pulled Config Success - ${configSuccess}    /
                          Valid Config - ${configSuccess}   /
                          Pulled S3 Object Content: - ${s3ObjectContentResult}  /
                          Packaged and Queued S3 Object Content - ${packageAndQueuedSuccess}
                          Event Process Result: ${updateSuccess} /

        for ${event.Records[0].s3.object.key} /
    } `

    console.log(`Final Status is : ${finalStatus}`)

    //Once successful delete the original S3 Object
    if (updateSuccess) {
        const delResultCode = await deleteS3Object(event);
        console.log(`Result from Delete of ${event.Records[0].s3.object.key}: ${delResultCode} `);
    }
    else
        throw new Error(`Deletion of Object ${event.Records[0].s3.object.key} skipped as previous processing failed`)

    return "TricklerCache Complete. Final Status is " + finalStatus

};

export default s3JsonLoggerHandler

async function getCustomerConfig(event: S3Event) {

    // ToDo: Once retrieved need to validate all fields 
    const customer = (event.Records[0].s3.object.key.split("_")[0] + "_")

    console.log("GetCustomerConfig: Customer string is ", customer)

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

            configJSON = new Promise<tricklerConfig>(async (resolve, reject) => {
                if (s3Body !== undefined) {

                    s3Body.on("data", (chunk: Uint8Array) => {
                        configObjs.push(chunk)
                    })
                    s3Body.on("error", () => {
                        reject;
                    })
                    s3Body.on("end", () => {
                        let cj = {} as tricklerConfig
                        let cf = Buffer.concat(configObjs).toString("utf8")
                        console.log(`Parsing Config File: ${customer}config.json \n ${cf}`)
                        try {
                            cj = JSON.parse(cf)
                        } catch (e) {
                            throw new Error(`Exception Parsing Config ${customer}config.json: ${e}`)
                        }

                        resolve(cj)
                    })
                }
            }).catch((e) => {
                configSuccess = false
                throw new Error(`Promise Exception when retrieving Customer Config ${customer}config.json \n${e}`)
            })
        })
    } catch (e) {
        throw new Error(`S3 Command Exception retrieving Customer Config ${customer}config.json: \n ${e}`);
    }

    return configJSON
}

async function validateConfig(config: tricklerConfig) {
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

    console.log("ValidateCutomerConfig succeeded: ", config)

    configSuccess = true

    return config as tricklerConfig
}

async function processS3ObjectContentStream(event: S3Event) {

    let chunks: string[] = new Array();
    let s3ContentResults = ''
    let batchCount = 0
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
                let queueToProcess

                s3ContentResults = await new Promise<string>(async (resolve, reject) => {

                    try {
                        let s3ContentStream = s3Result.Body as NodeJS.ReadableStream

                        if (custConfig.format.toLowerCase() === 'csv') {
                            s3ContentStream = s3ContentStream.pipe(csvParseStream)
                        }

                        s3ContentStream
                            .on('data', async function (jsonChunk: string) {
                                recs++
                                // console.log(`Another chunk (${recs}): ${jsonChunk}, chunks length is ${chunks.length}`)
                                chunks.push(jsonChunk)
                                if (chunks.length == 25)
                                {
                                    batchCount++
                                    console.log(`s3ContentStream onData has processed ${recs} chunks: `, jsonChunk)
                                    xmlRows = convertToXML(chunks, custConfig)
                                    let queueUp = await queueWork(xmlRows, event, batchCount.toString())
                                    chunks.length = 0
                                }
                            })

                            .on('end', async function (msg: string) {
                                batchCount++
                                console.log(`S3ContentStream OnEnd (${msg}), processed  (${recs}) records from ${event.Records[0].s3.object.key}.`)
                                xmlRows = convertToXML(chunks, custConfig)
                                let queueToProcess = await queueWork(xmlRows, event, batchCount.toString())
                                chunks.length = 0
                                batchCount = 0
                                resolve('s3Content End')
                            })

                            .on('error', async function (err: string) {
                                console.log("csvParseStream Error", err)
                                reject(`An error has stopped Content Parsing at record ${recs} for s3 object ${event.Records[0].s3.object.key}. ${err}`)
                            })

                            .on('close', function (msg: string) {
                                console.log(`csvParseStream Closed (${msg}), processed ${recs} records from ${event.Records[0].s3.object.key}`)
                            })

                    } catch (e) {
                        throw new Error(`Exception processing S3 Object Content for ${event.Records[0].s3.object.key}, \n${e}`)
                    }

                })
                // .catch((e) => {
                //         throw new Error(`Exception Processing (Promise) S3 Get Object Content for ${event.Records[0].s3.object.key}: \n ${e}`);
                //     })

            })
    } catch (e) {
        throw new Error(`Exception Processing S3 Get Object for ${event.Records[0].s3.object.key}: \n ${e}`);
    }

    processSuccess = true
    return s3ContentResults
}


function convertToXML(rows: string[], config: tricklerConfig) {

    console.log(`Package99: packaging ${rows.length} rows`)

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

async function queueWork(queueContent: string, event: S3Event, batch: string) {

    console.log(`Process Queue:  ${queueContent} rows `)

    const s3Key = event.Records[0].s3.object.key
    const qs3 = await storeS3Work(queueContent, s3Key, batch)
    const qAdd = await addToProcessQueue(custConfig, s3Key, batch) 

}

async function storeS3Work(queueContent: string, s3Key: string, batch: string) {

    //write to the S3 Process Bucket
    const s3PutInput = {
        "Body": queueContent,
        "Bucket": "tricklercache-process",
        "Key": `process_${batch}_${s3Key}`
    };
    
    let qs3

    try {
        console.log(`ProcessQueue - Write to S3 Process Bucket: ", process_${s3Key}`);

        qs3 = await s3.send(new PutObjectCommand(s3PutInput))
            .then(async (s3PutResult: PutObjectCommandOutput) => {
                const d = JSON.stringify(s3PutResult, null, 2)
                // const r = s3PutResult.VersionId
                console.log(`ProcessQueue - Write to Process Bucket Result: ${d}`);
            });
    } catch (e) {
        console.log(`Exception - ProcessingQueue - S3 Put Command (process_${s3Key}): ${e}`)
    }
return qs3

}

async function addToProcessQueue (custConfig: tricklerConfig, s3Key: string, batch: string) {

    const qc = JSON.stringify({
        "work": `process_${batch}_${s3Key}`,
        "custconfig": custConfig
    })
    
    const writeSQSCommand = new SendMessageCommand({
        QueueUrl: sqsParams.QueueUrl,
        DelaySeconds: 1,
        MessageAttributes: {
            tricklerProcessQueue: {
                DataType: "String",
                StringValue: `process_${batch}_${s3Key}`,
            },
        },
        MessageBody: qc
    });

let qAdd

    try {
        qAdd = await sqsClient.send(writeSQSCommand)
            .then(async (sqsWriteResult: SendMessageCommandOutput) => {
                const wr = JSON.stringify(sqsWriteResult, null, 2)
                console.log(`Process Queue - Writing SQS Queue for process_${s3Key} - Result: ${wr} `);
                qAdd = JSON.stringify(sqsWriteResult)
            });
    } catch (e) {
        console.log(`Exception - Processing Queue - SQS Write: ${e}`)
    }

    return qAdd

}


async function queueForRetry(update: string) {
    console.log(`Queuing For Retry: ${update.length}`)
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

export async function getAccessToken(config: tricklerConfig) {

    console.log(`GetAccessToken - Region: ${config.region}`)
    console.log(`GetAccessToken - Pod: ${config.pod}`)
    console.log(`GetAccessToken - AccessToken: ${process.env.accessToken}`)
    // console.log(`${config.region}`)
    // console.log(`${config.region}`)
    // console.log(`${config.region}`)
    // console.log(`${config.region}`)
    
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

export async function postToCampaign(xmlCalls: string, config: tricklerConfig) {

    console.log(`Region: ${config.region}`)
    console.log(`Pod: ${config.pod}`)
    console.log(`AccessToken: ${process.env.accessToken}`)
    // console.log(`${config.region}`)
    // console.log(`${config.region}`)
    // console.log(`${config.region}`)
    // console.log(`${config.region}`)
    
    if (process.env.accessToken != null) {
        process.env.accessToken = await getAccessToken(config) as string
    }
    else console.log(`Access Token already present: ${process.env.accessToken} ...`)


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

    // try {
    const postRes = await fetch(host, requestOptions
    )
        .then((response) => response.text())
        .then((result) => {
            // console.log("POST Update Result: ", result)
            if (result.toLowerCase().indexOf('false</success>') > 0) {
                // "<Envelope><Body><RESULT><SUCCESS>false</SUCCESS></RESULT><Fault><Request/>
                //   <FaultCode/><FaultString>Invalid XML Request</FaultString><detail><error>
                //   <errorid>51</errorid><module/><class>SP.API</class><method/></error></detail>
                //    </Fault></Body></Envelope>\r\n"
                queueForRetry(xmlRows)
                throw new Error(`Unsuccessful POST of Update (AccessToken ${process.env.accessToken}) result: ${result}`)
            }

            updateSuccess = true
            return result
        })
        .catch((e) => {
            console.log(`Exception during POST to Campaign (AccessToken ${process.env.accessToken}) Result: ${e}`)
        })

    // } catch(e) {
    //     debugger;
    //     console.log(`Exception during POST to Campaign (AccessToken ${accessToken}) Result: ${e}`)
    // })
    return postRes
}


async function deleteS3Object(event: S3Event) {
    try {
        console.log(`DeleteS3Object : \n'  ${event.Records[0].s3.object.key}`);

        await s3.send(
            new DeleteObjectCommand({
                Key: event.Records[0].s3.object.key,
                Bucket: event.Records[0].s3.bucket.name
            })
        ).then(async (s3DelResult: DeleteObjectCommandOutput) => {
            // console.log("Received the following Object: \n", data.Body?.toString());

            const delRes = JSON.stringify(s3DelResult.$metadata.httpStatusCode, null, 2);

            // console.log("Received the following Object: \n", JSON.stringify(data.Body, null, 2));
            console.log(`Result from Delete of ${event.Records[0].s3.object.key}: ${delRes} `);

            return delRes

            // console.log(`Response from deleteing Object ${event.Records[0].responseElements["x-amz-request-id"]} \n ${del.$metadata.toString()}`);

        });
    } catch (e) {
        console.log(`Exception Processing S3 Delete Command for ${event.Records[0].s3.object.key}: \n ${e}`);
    }
}


async function getS3Work(s3Key: string, config: tricklerConfig){
    
    console.log(`GetS3Work key provided: ${s3Key}`)

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
                console.log(`Work Pulled: ${s3Key} - ${work}`)
            });
    } catch (e)
    {
        console.log(`ProcessQueue - Get Work - Exception ${e}`)
    }
return work
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


