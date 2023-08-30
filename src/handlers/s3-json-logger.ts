"use strict";
import { S3, S3Client, S3ClientConfig, GetObjectCommand, GetObjectCommandOutput } from "@aws-sdk/client-s3"
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

/**
  * A Lambda function to process the Event payload received from S3.
  */


// Create a client to read objects from S3
const s3 = new S3Client({ region: "us-east-1", });


export interface S3Object {
    Bucket: string
    Key: string
    Region: string
}

export interface evRec {
    s3: {
        object: { key: any; };
        bucket: { name: any; };
    };
}

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

// commonJS
// module.exports.Handler = async (event: S3Event, context: Context) => {

//ESM  - es6 ecmascript
 // export const handler = async (event) => {
export const s3JsonLoggerHandler: Handler = async (event: S3Event, context: Context) => {

    // console.log(`AWS-SDK Version: ${version}`)
    console.log('ENVIRONMENT VARIABLES\n' + JSON.stringify(process.env, null, 2))
    console.log("Num of Events to be processed: ", event.Records.length)
    console.log('EVENT: \n' + JSON.stringify(event, null, 2));

    if (event.Records.length < 1) {
        console.log(await lambdaWait(2000));
    } else {
        let processFilePromises: {}[] = []


        event.Records.forEach(async (r: evRec) => {

            const command = new GetObjectCommand({
                Key: r.s3.object.key,
                Bucket: r.s3.bucket.name
            })

            const s3O = {
                Bucket: r.s3.bucket.name,
                Key: r.s3.object.key,
                Region: 'us-east-1'
            }

            const data = await s3.send(
                // new GetObjectCommand({
                //     Bucket: params.Bucket,
                //     Key: params.Key,
                //     Body: params.Body,
                // })
                new GetObjectCommand(s3O)
            )

            processFilePromises.push(s3.send(command))
        })

        await Promise.all(processFilePromises);
    }

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


function lambdaWait(n: number) {
    return new Promise((resolve, reject) => {
        setTimeout(() => resolve("hello"), n)
    });
}


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