import { config, S3, Lambda, HttpRequest, HttpResponse, HTTPOptions, Endpoint } from  'aws-sdk' 
// import * as metrics_1 from '@aws-lambda-powertools/metrics';
import * as logger_1 from '@aws-lambda-powertools/logger';
// import * as tracer_1 from '@aws-lambda-powertools/tracer';
import { S3Event, Context } from 'aws-lambda';
// import { UnknownFile } from 'list-open-files';


// import "source-map-support/register";

//run it again and again and 9

// Create a client to read objects from S3

// import pkg from 'aws-sdk';
// const { S3 } = pkg
const s3 = new S3({ httpOptions: { timeout: 900 } });


// const metrics = new metrics_1.Metrics();
const logger = new logger_1.Logger();
// const tracer = new tracer_1.Tracer();


const path: string = "https://api-campaign-us-6.goacoustic.com/XMLAPI"

const postOptions = {
    host: path,
    port: 443,
    headers: {
        'Content-Type': 'application/json',
    }
}

const payload: string = "" 
const ep = new Endpoint("https://api-campaign-us-6.goacoustic.com/XMLAPI")
const httpRequest = new HttpRequest(ep, "us-east-1")
httpRequest.method = "POST"
httpRequest.body = JSON.stringify({a: " "})

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


/**
  * A Lambda function that logs the payload received from S3.
  */
exports.handler = async function s3JsonLoggerHandler(event: S3Event, context: Context) {

    let r: {}


//     exports.handler = async (event, context) => new Promise(async (resolve, reject) => {

//         const token = await post("/auth/login", { username: "test@test.com", password: "password" });

// }




    console.log("Handler w/ event:\n", event.Records.map, "\n\n", context)

    // // Log the incoming event
    // logger.info('Lambda invocation event', { event });

    // // Append awsRequestId to each log statement
    // logger.appendKeys({ awsRequestId: context.awsRequestId });

    // Get facade segment created by AWS Lambda
    // const segment = tracer.getSegment();

    // if (!segment) {
    //     r = {
    //         statusCode: 500,
    //         body: "Failed to get segment"
    //     }
    //     return r;
    // }

    // // Create subsegment for the function & set it as active
    // const handlerSegment = segment.addNewSubsegment(`## ${process.env._HANDLER}`);
    // tracer.setSegment(handlerSegment);

    // // Annotate the subsegment with the cold start & serviceName
    // tracer.annotateColdStart();
    // tracer.addServiceNameAnnotation();

    // // Add annotation for the awsRequestId
    // tracer.putAnnotation('awsRequestId', context.awsRequestId);
    // // Capture cold start metrics
    // metrics.captureColdStartMetric();
    // // Create another subsegment & set it as active
    // const subsegment = handlerSegment.addNewSubsegment('### MySubSegment');
    // tracer.setSegment(subsegment);



    const getObjectRequest = event.Records.map(async (record) => {
        // event.Records.map(async (record) => {

        const params = {
            Bucket: record.s3.bucket.name,
            Key: record.s3.object.key,
        }

        const r = await s3.getObject(params).promise()
            .catch(function (err) {
                console.log("Promise Exception-Message: \n", err.message);
                console.log("Promise Exception-Stack: \n", err.stack);
            })
            .then(function (d) {
                console.log("await getObject:\n", d)
            })

        // console.log("This is line 47 r: \n\n", r)

        return {
            statusCode: 200,
            body: JSON.stringify(r)
        }

    })

    await Promise.all(getObjectRequest)
        .catch(function (err) {
            console.log("await Promise.all GetObjectRequests Promise Exception-Message: \n", err.message);
            console.log("await Promise.all GetObjectRequests Promise Exception-Stack: \n", err.stack);
            // tracer.addErrorAsMetadata(err as Error);
            logger.error(`Error response from API endpoint: ${err}`)
        })
        .then(function (res) {
            console.log("This is await Promise.all GetObjectRequests - res: \n", res)
            logger.info(`Successful response from API endpoint: ${event.Records}`, JSON.stringify(res));

            return {
                statusCode: 200,
                body: JSON.stringify(res)
            }
        })





    // Set the facade segment as active again (the one created by AWS Lambda)
    // tracer.setSegment(segment)

    // Publish all stored metrics
    // metrics.publishStoredMetrics();

    //console.log("This is Resp: \n", gor)

    // return response

}




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
