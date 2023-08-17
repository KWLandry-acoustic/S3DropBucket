// // this imports a bare-bones version of S3 that exposes the .send operation
import { S3Client, S3ClientConfig, GetObjectCommand, GetObjectCommandOutput } from "@aws-sdk/client-s3"
import { Handler, S3Event, Context, S3ObjectACLUpdatedNotificationEvent } from 'aws-lambda';
// import { HttpRequest } from '@aws-sdk/protocol-http';
// import type { HttpHandlerOptions } from '@aws-sdk/types';
import { FetchHttpHandler, FetchHttpHandlerOptions } from '@aws-sdk/fetch-http-handler'
import { default as fetch, Request, Response } from 'node-fetch';
import { HttpRequest } from "@aws-sdk/protocol-http";

// import { Readable } from "stream";

// // this imports just the getObject operation from S3
// import { GetObjectCommand } from "@aws-sdk/client-s3"


/**
  * A Lambda function to process the Event payload received from S3.
  */


// Create a client to read objects from S3
const s3Client = new S3Client({ region: "us-east-1", });
export type S3Object = {
    Bucket: string
    Key: string
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


export const s3JsonLoggerHandler: Handler = async (event: S3Event, context: Context) => {

    console.log("ENVIRONMENT VARIABLES\n" + JSON.stringify(process.env, null, 2))
    console.log('EVENT: \n' + JSON.stringify(event, null, 2));
    console.warn("What does 'Warn' look like ?")

    // // //usage
    // const s3Config: S3ClientConfig = "" 
    // const bareBonesS3 = new S3Client(s3Config.endpoint);
    // await bareBonesS3.send(new GetObjectCommand({...}));


    const params: S3Object = {
        Bucket: event.Records[0].s3.bucket.name,
        Key: event.Records[0].s3.object.key,
    }

    // s3Client.getObject(params,)

    const a = await pullS3Object(params)

    // const b = postCampaign(a as string)    
    const b = await postCampaign(a as string)

    console.log("Return from Post to Campaign: \n", b)

    return context.logStreamName;
};

export default s3JsonLoggerHandler


async function pullS3Object(params: S3Object) {
    try {
        console.log("Pulling S3 Object\n", params.Bucket, '\n', params.Key);

        const command = new GetObjectCommand({
            Key: params.Key,
            Bucket: params.Bucket,
        });

        const s3Item: GetObjectCommandOutput = await s3Client.send(command);
        return s3Item.Body?.transformToString();

    } catch (e) {
        console.log("Pull S3Object Exception:", e);
    }
}

export async function postCampaign(xmlCalls: string) {

    const accessToken = getAccessToken()

    try {
        const r = await fetch(`https://api-campaign-us-6.goacoustic.com/XMLAP`, {
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
                console.log(json)
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
    ac.refreshTokenUrl = `https://api-campaign-${region}-${pod}.goacoustic.com/oauth/token`

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
}