"use strict";
import { S3, S3Client, S3ClientConfig, GetObjectCommand, GetObjectCommandOutput } from "@aws-sdk/client-s3"
import {S3Event } from "aws-lambda"


let processFilePromises: {}[] = []


export interface S3Object {
    Bucket: string
    Key: string
    Region: string
}

export interface s3Obj {
    s3: {
        object: { key: any; };
        bucket: { name: any; };
    };
}

// Create a client to read objects from S3
const s3 = new S3Client({ region: "us-east-1", });

export async function getAllS3Obj(event: S3Event, r: s3Obj) {



    //Wait for 99 or 1 minute more than last record received. 

    // if (event.Records.length = 1) {
    //     // console.log(await lambdaWait(2000));
    // } else {

    

    event.Records.forEach(async (r: s3Obj) => {


        const command = new GetObjectCommand({
            Key: r.s3.object.key,
            Bucket: r.s3.bucket.name
        })
        processFilePromises.push(s3.send(command))

        console.log('Processed Event: \n' + JSON.stringify(event, null, 2));
    })


    await Promise.all(processFilePromises);

}


function lambdaWait(n: number) {
    return new Promise((resolve, reject) => {
        setTimeout(() => resolve("hello"), n)
    });
}