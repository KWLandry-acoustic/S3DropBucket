"use strict";
import { S3, S3Client, S3ClientConfig, GetObjectCommand, GetObjectCommandOutput, DeleteObjectCommand, DeleteObjectCommandInput, DeleteObjectCommandOutput, DeleteObjectOutput, DeleteObjectRequest, ListObjectsV2Command, ListObjectsV2CommandInput, ListObjectsV2CommandOutput } from "@aws-sdk/client-s3"
import { Handler, S3Event, Context } from "aws-lambda"
import fetch from "node-fetch"
import { Body } from "node-fetch";
import { Readable } from "stream";
import csv from 'csvtojson';



// import 'source-map-support/register'


// import { packageJson } from '@aws-sdk/client3/package.json'
// const version = packageJson.version

// const path = require('path');
// const util = require('util');

// import { HttpRequest } from '@aws-sdk/protocol-http';
// import type { HttpHandlerOptions } from '@aws-sdk/types';
// import { FetchHttpHandler, FetchHttpHandlerOptions } from '@aws-sdk/fetch-http-handler'
// import { default as fetch } from '@aws-sdk/fetch-http-handler/node_modules/@smithy/fetch-http-handler'
// import { FetchHttpHandler, FetchHttpHandlerOptions} from '@aws-sdk/client-s3/dist-types/'



// import { GetObjectCommand } from "@aws-sdk/client-s3"

// const g: FetchHttpHandlerOptions = {}
// const f = new FetchHttpHandler(g)




export interface S3Object {
    Bucket: string
    Key: string
    Region: string
}


/**
  * A Lambda function to process the Event payload received from S3.
  */


// Create a client to read objects from S3
const s3 = new S3Client({ region: "us-east-1" });
let s3Data = ""


export interface accessResp {
    access_token: string
    token_type: string
    refresh_token: string
    expires_in: number
}

export interface authCreds {
    accessToken: string
    clientId: string
    clientSecret: string
    refreshToken: string
    refreshTokenUrl: string
}



export const s3JsonLoggerHandler: Handler = async (event: S3Event, context: Context) => {

    // console.log(`AWS-SDK Version: ${version}`)
    // console.log('ENVIRONMENT VARIABLES\n' + JSON.stringify(process.env, null, 2))

    console.log("Processing Object from S3 Trigger, Event RequestId: ", event.Records[0].responseElements["x-amz-request-id"], "\n\nNum of Events to be processed: ", event.Records.length)

    if (event.Records.length > 1) throw new Error(`Expecting only a single S3 Object from a Triggered S3 write
     of a new Object, received ${event.Records.length} Objects`)


    //Local Testing - pull an S3 Object and so avoid the not-found error
    let s3Key: string = await getAnS3ObjectforTesting(event);
    //Local Testing - pull an S3 Object and so avoid the not-found error


    const s3Data = await getS3Object(s3Key, event) as unknown as string[]

    console.log("Returned from getS3Object: ", s3Data)

    debugger; 
    const xmlRows = parseCSVtoXML(s3Data)

    const updateResult = postCampaign(xmlRows)



    //Once successful delete the original S3 Object
    const delResultCode = await deleteS3Object(event);
    console.log(`Result from Delete: ${s3Key}: ${delResultCode} ` + "\\n..\\n..\\n..");

    return `Event Process Result:     ${updateResult}      for Request Id:     ${event.Records[0].responseElements["x-amz-request-id"]}  \\n..\\n..\\n..`

};

export default s3JsonLoggerHandler

async function getS3Object(s3Key: string, event: S3Event) {
    let s3D: string[] = []

    try {
        await s3.send(
            new GetObjectCommand({
                // Key: event.Records[0].s3.object.key,
                Key: s3Key,
                Bucket: event.Records[0].s3.bucket.name
            })
        ).then(async (s3Result: GetObjectCommandOutput) => {
            // console.log("Received the following Object: \n", data.Body?.toString());

            // const d = JSON.stringify(s3Result.Body, null, 2)
            const stream = s3Result.Body as Readable;
            debugger;
            csv({
                noheader: false,
                trim: true,
                flatKeys: true,
                needEmitAll: true,
                downstreamFormat: "line"
            })
                .fromStream(stream)
                .then((rows) => {
                    console.log("Output from csvtojson: ", rows)
                    s3D = rows
                })

            // for await (const cc of stream) {
            //     s3D.push(cc);
            // }

            // // s3Data = Buffer.concat(s);

            // // const s3Data = JSON.parse(s3d.toString())
            // // s3Data = s3d.map.toString();

            // // console.log("Received the following Object: \n", JSON.stringify(data.Body, null, 2));
            // console.log(`Result from Get: ${s3Data} \n..\n..\n..`);

        });
        
        return s3D

    } catch (e) {
        console.log("Exception Processing S3 Get Command: \n..", e, '\\n..\\n..\\n..');
    }

}

export function parseCSVtoXML(csvIn: string[]) {
    let xmlRows = '' as string
    debugger;

    // csv({
    //     noheader: false,
    //     trim: true,
    // })
    //     .fromString(csvIn.forEach())
    //     .then((csvRow) => {

    //         for (const c in csvJson) {

    //             xmlRows += `  
    //                 <ROW>
    //                 <COLUMN name="EMAIL">           <![CDATA[${csvRow.email}]]></COLUMN>
    //                 <COLUMN name="EventSource">     <![CDATA[${csvRow.eventSource}]]></COLUMN>  
    //                 <COLUMN name="EventName">       <![CDATA[${csvRow.eventName}]]></COLUMN>
    //                 <COLUMN name="EventValue">      <![CDATA[${csvRow.eventValue}]]></COLUMN>
    //                 <COLUMN name="Event Timestamp"> <![CDATA[${csvRow.timestamp}]]></COLUMN>
    //                 </ROW>`
    //         }


    //     })
    return xmlRows
}

export async function postCampaign(xmlCalls: string) {

    const accessToken = getAccessToken()
    console.log("Access Token: ", accessToken)


    try {
        const r = await fetch('https://api-campaign-us-6.goacoustic.com/XMLAPI', {
            method: 'POST',
            // body: JSON.stringify(xmlCalls),
            body: xmlCalls,
            headers: {
                'Authorization': `Bearer: ${accessToken}`,
                'Content-Type': 'application/json'
            },
        })
            .then(res => res.json())
            .then(json => {
                console.log("Return from POST Campaign: \n", json)
                return JSON
            })

    } catch (e) {
        console.log("Exception during POST to Campaign: \n", e)
    }
}

export async function getAccessToken() {

    const ac: authCreds = {
        accessToken: "",
        clientId: "",
        clientSecret: "",
        refreshToken: "",
        refreshTokenUrl: ""
    }

    ac.accessToken = ''
    ac.clientId = '1853dc2f-1a79-4219-b538-edb018be9d52'
    ac.clientSecret = '329f1765-0731-4c9e-a5da-0e8f48559f45'
    ac.refreshToken = 'r7nyDaWJ6GYdH5l6mlR9uqFqqrWZvwKD9RSq-hFgTMdMS1'
    ac.refreshTokenUrl = `https://api-campaign-us-6.goacoustic.com/oauth/token`
    //https://api-campaign-us-6.goacoustic.com/XMLAPI

    try {
        const rat = await fetch(ac.refreshTokenUrl, {
            method: 'POST',
            body: new URLSearchParams({
                refresh_token: ac.refreshToken,
                client_id: ac.clientId,
                client_secret: ac.clientSecret,
                grant_type: 'refresh_token'
            }),
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'User-Agent': 'S3 TricklerCache GetAccessToken'
            }
        })
        const ratResp = (await rat.json()) as accessResp
        ac.accessToken = ratResp.access_token

        return { accessToken: ac.accessToken, refreshToken: ac.refreshToken }
    } catch (e) {
        console.log("Exception in getAccessToken: \n", e)
    }
}

async function deleteS3Object(event: S3Event) {
    try {
        console.log('Processed Event: \n' + JSON.stringify(event, null, 2) + "\n..\n..\n..");

        await s3.send(
            new DeleteObjectCommand({
                Key: event.Records[0].s3.object.key,
                Bucket: event.Records[0].s3.bucket.name
            })
        ).then(async (s3Result: DeleteObjectCommandOutput) => {
            // console.log("Received the following Object: \n", data.Body?.toString());

            const d = JSON.stringify(s3Result.$metadata.httpStatusCode, null, 2);

            // console.log("Received the following Object: \n", JSON.stringify(data.Body, null, 2));
            console.log(`Result from Delete of ${event.Records[0].s3.object.key}: ${d} ` + "\n..\n..\n..");

            return d

            // console.log(`Response from deleteing Object ${event.Records[0].responseElements["x-amz-request-id"]} \n ${del.$metadata.toString()}`);

        });
    } catch (e) {
        console.log(`Exception Processing S3 Delete Command for ${event.Records[0].s3.object.key}: \n ${e}` + "\n..\n..\n..");
    }
}






async function getAnS3ObjectforTesting(event: S3Event) {
    const listReq = {
        Bucket: event.Records[0].s3.bucket.name,
        MaxKeys: 10
    } as ListObjectsV2CommandInput;

    let s3Key: string = "";

    try {
        await s3.send(new ListObjectsV2Command(listReq))
            .then(async (s3Result: ListObjectsV2CommandOutput) => {
                // const d = JSON.stringify(s3Result.Body, null, 2)

                s3Key = s3Result.Contents?.at(1)?.Key as string;

                // event.Records[0].s3.object.key  = s3Result.Contents?.at(0)?.Key as string
                // console.log("Received the following Object: \n", JSON.stringify(data.Body, null, 2));
                console.log("Result from List: ", s3Key, "\n..\n..\n..");
            });
    } catch (e) {
        console.log("Exception Processing S3 List Command: \n..", e, '\n..\n..\n..');
    }
    return s3Key;
}




