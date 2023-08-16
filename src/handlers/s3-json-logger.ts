// // this imports a bare-bones version of S3 that exposes the .send operation
import { S3Client, S3ClientConfig, GetObjectCommand, GetObjectCommandOutput } from "@aws-sdk/client-s3"
import { Handler, S3Event, Context, S3ObjectACLUpdatedNotificationEvent } from 'aws-lambda';
import fetch from "node-fetch"
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

    const b = postCampaign(a as string)

    console.log("Return from Post to Campaign: \n", b)

    return context.logStreamName;
};

export default s3JsonLoggerHandler


async function pullS3Object(params: S3Object) {
    try {
        console.log("Pulling S3 Object");

        const command = new GetObjectCommand({
            Key: params.Key,
            Bucket: params.Bucket,
        });

        const s3Item: GetObjectCommandOutput = await s3Client.send(command);
        return s3Item.Body?.transformToString();

    } catch (e) {
        console.log("PullS3Object Exception:", e);
    }
}



// async function getObjectS3(params: S3.GetObjectRequest) {
//     try {

//         // const s3Object = {
//         //     Bucket: params.Bucket,
//         //     Key: params.Key
//         // }

//         // return s3Client.getObject(s3Object).Body

//         const data = await s3Client..getObject(params).promise()
//             .catch(function (err) {
//                 console.log("Promise Exception-Message: \n", err.message);
//                 console.log("Promise Exception-Stack: \n", err.stack);
//             })
//             .then(function (d) {
//                 console.log("Then: getObjectS3: \n", d)
//                 return d        //.toString('utf-8');
//             })

//     } catch (e) {
//         console.log(" GetObjectS3 Exception: \n", e);
//     }

// }

export async function postCampaign(s3Object: string) {

    // const req = new HttpRequest(ep, "us-east-1")
    // req.host = "https://api-campaign-us-6.goacoustic.com/XMLAPI"
    // req.method = "POST"
    // req.port = 443
    // req.headers = {
    //     'Content-Type': 'application/xml',
    // }
    // req.body = JSON.stringify(s3Object)

    // const post = (path: string, payload: string) => new Promise((resolve, reject) => {
    //     const options = { ...postOptions, path, method: 'POST' };
    //     const req = HttpRequest.Request(options, res => {
    //         let buffer = "";
    //         res.on('data', chunk => buffer += chunk)
    //         res.on('end', () => resolve(JSON.parse(buffer)))
    //     });
    //     req.on('error', e => reject(e.message));
    //     req.write(JSON.stringify(payload));
    //     req.end();
    // })


    const res = await fetch('https://nodejs.org/api/documentation.json');
    if (res.ok) {
        const data = await res.json()
        console.log(data);
        return data
    }

    // const response = {
    //     statusCode: 200,
    //     body: JSON.stringify('Hello from Lambda!'),
    // };





}



// exports.handler = async function s3JsonLoggerHandler(event: S3Event, context: Context) {

//     console.log("Handler w/ event:\n", event.Records.map, "\n\n", context)

//     console.log("ENVIRONMENT VARIABLES\n" + JSON.stringify(process.env, null, 2))
//     console.info("EVENT\n" + JSON.stringify(event, null, 2))
//     console.warn("Event not processed.")
//     return context.logStreamName
// }







//     exports.handler = async (event, context) => new Promise(async (resolve, reject) => {

//         const token = await post("/auth/login", { username: "test@test.com", password: "password" });

// }











// const getObjectRequest = event.Records.map(async (record) => {
//     // event.Records.map(async (record) => {

//     const params = {
//         Bucket: record.s3.bucket.name,
//         Key: record.s3.object.key,
//     }

//     const r = await s3.getObject(params).promise()
//         .catch(function (err) {
//             console.log("Promise Exception-Message: \n", err.message);
//             console.log("Promise Exception-Stack: \n", err.stack);
//         })
//         .then(function (d) {
//             console.log("await getObject:\n", d)
//         })

//     // console.log("This is line 47 r: \n\n", r)

//     return {
//         statusCode: 200,
//         body: JSON.stringify(r)
//     }

// })

// await Promise.all(getObjectRequest)
//     .catch(function (err) {
//         console.log("await Promise.all GetObjectRequests Promise Exception-Message: \n", err.message);
//         console.log("await Promise.all GetObjectRequests Promise Exception-Stack: \n", err.stack);
//         // tracer.addErrorAsMetadata(err as Error);
//         logger.error(`Error response from API endpoint: ${err}`)
//     })
//     .then(function (res) {
//         console.log("This is await Promise.all GetObjectRequests - res: \n", res)
//         logger.info(`Successful response from API endpoint: ${event.Records}`, JSON.stringify(res));

//         return {
//             statusCode: 200,
//             body: JSON.stringify(res)
//         }
//     })


// }




// try {

// const interval = 2000; // Blocks the main thread in 100 millisecond interval
// //const blockingInterval = setInterval(() => undefined, 100)
// const blockingInterval = setInterval(function () {

//     // await getObjectS3("simple-app-bucket", "test/key")
//     //     .then(value => {
//     //         clearInterval(blockingInterval)
//     //         // ... Rest of code here
//     //     })
//     //     .catch(err => {
//     //         clearInterval(blockingInterval)
//     //         console.log("Exception in Wait: \n", err)
//     //     })

//     console.log("This should be fun: \n")
// }, interval)

// const params = {
//     Bucket: "simple-app-bucket",
//     Key: "test/key",
// }

// async function getObjectS3(bucket, objectKey) {
//     try {
//         const params = {
//             Bucket: bucket,
//             Key: objectKey
//         }

//         return s3.getObject(params).Body
//         //const data = await s3.getObject(params).promise()
//         // .catch(function (err) {
//         //     console.log("Promise Exception-Message: \n", err.message);
//         //     console.log("Promise Exception-Stack: \n", err.stack);
//         // })
//         // .then(function (d) {
//         //     console.log("This is getObjectS3: \n", d)
//         // })

//         return data.Body.toString('utf-8');

//     } catch (e) {
//         console.log("function GetObjectS3 Exception-Message: \n", e.message);
//         console.log("function GetObjectS3 Exception-Stack: \n", e.stack);
//         // console.log(`Could not retrieve file from S3\n: ${e.message}`)
//     }
// }




// try {

// await getObjectS3(params)
//     .catch(function (err) {
//         console.log("Await GetObjectS3 Promise Exception-Message: \n", err.message);
//         console.log("Await GetObjectS3 Promise Exception-Stack: \n", err.stack);
//     })
//     .then(function (res) {
//         console.log("This is Await GetObjectS3 - res: \n", res)
//     })
// //console.log("This is Resp: \n", s3r)


// await Promise.all(getObjectS3(params))
//     .catch(function (err) {
//         console.log("Await Promise.all GetObjectS3 Promise Exception-Message: \n", err.message);
//         console.log("Await Promise.all GetObjectS3 Promise Exception-Stack: \n", err.stack);
//     })
//     .then(function (res) {
//         console.log("This is Await Promise.all GetObjectS3 - res: \n", res)
//     })
// //console.log("This is Resp: \n", s3r)


// await Promise.all(getObjectRequest)
//     .catch(function (err) {
//         console.log("await Promise.all GetObjectRequests Promise Exception-Message: \n", err.message);
//         console.log("await Promise.all GetObjectRequests Promise Exception-Stack: \n", err.stack);
//     })
//     .then(function (res) {
//         console.log("This is await Promise.all GetObjectRequests - res: \n", res)
//     })
// //console.log("This is Resp: \n", gor)


// const s = await getObject(params)
// console.log("This is S: \n", s)

// const t = await getObjectRequests().catch(function (err) {
//     console.log("Await Promise Exception-Message: \n", err.message);
//     console.log("Await Promise Exception-Stack: \n", err.stack);
// })



//     // console.log("This is r: \n\n", r)

//     //const response = await getObjectRequests().then()
//     //const r = await response.Records

//     //console.log("This is the response: \n\n", response)


// }
// catch (e) {
//     console.log("Exception : ", e)
// }

// }
// catch (e) {

// }
