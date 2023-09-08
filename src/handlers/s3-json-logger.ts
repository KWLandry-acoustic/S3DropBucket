"use strict";
import { S3, S3Client, S3ClientConfig, GetObjectCommand, GetObjectCommandOutput, DeleteObjectCommand, DeleteObjectCommandInput, DeleteObjectCommandOutput, DeleteObjectOutput, DeleteObjectRequest } from "@aws-sdk/client-s3"
import { Handler, S3Event, Context } from "aws-lambda"
import fetch from "node-fetch"


// import { packageJson } from '@aws-sdk/client3/package.json'
// const version = packageJson.version

// const path = require('path');
// const util = require('util');

// import { HttpRequest } from '@aws-sdk/protocol-http';
// import type { HttpHandlerOptions } from '@aws-sdk/types';
// import { FetchHttpHandler, FetchHttpHandlerOptions } from '@aws-sdk/fetch-http-handler'
// import { default as fetch } from '@aws-sdk/fetch-http-handler/node_modules/@smithy/fetch-http-handler'
// import { FetchHttpHandler, FetchHttpHandlerOptions} from '@aws-sdk/client-s3/dist-types/'


// import { Readable } from "stream";



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
const s3 = new S3Client({ region: "us-east-1", });



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


    if  ( event.Records.length > 1)    throw new Error(`Expecting only a single S3 Object from a Triggered S3 write of a new Object, received ${event.Records.length} Objects`)
    
    
    const getS3Obj = async () => {

        const data = await s3.send(
            new GetObjectCommand({
                Key: event.Records[0].s3.object.key,
                Bucket: event.Records[0].s3.bucket.name
            })
        )

        // console.log("Received the following Object: \n", data.Body?.toString());
        console.log("Received the following Object: \n", JSON.stringify(data.Body,null,2));

        console.log('Processed Event: \n' + JSON.stringify(event, null, 2));


        const del = await s3.send(
            new DeleteObjectCommand({
                Key: event.Records[0].s3.object.key,
                Bucket: event.Records[0].s3.bucket.name
            })
        )

        console.log(`Response from deleteing Object ${event.Records[0].responseElements["x-amz-request-id"]} \n ${del.$metadata.toString()}`);

    };

    getS3Obj();




    // // //usage
    // const s3Config: S3ClientConfig = "" 
    // const bareBonesS3 = new S3Client(s3Config.endpoint);
    // await bareBonesS3.send(new GetObjectCommand({...}));




    // try {
    //     const a = await pullS3Object(params)
    //     const b = await postCampaign(a as string)
    //     console.log("Return from Post to Campaign: \n", b)
    // } catch (e) {
    //     console.log("Exception during Pull or Post: /n", e)
    // }

    // return context.logStreamName;
};

export default s3JsonLoggerHandler




async function pullS3Object(params: S3Object) {
    try {
        console.log("Pulling S3 Object\n", params.Bucket, '\n', params.Key);

        const command = new GetObjectCommand({
            Key: params.Key,
            Bucket: params.Bucket,
        });

        const s3Item: GetObjectCommandOutput = await s3.send(command);
        return s3Item.Body?.transformToString();

    } catch (e) {
        console.log("Pull S3Object Exception:", e);
    }
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