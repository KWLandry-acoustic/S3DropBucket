"use strict";
// Create clients outside of the handler
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.s3JsonLoggerHandler = void 0;
const aws_sdk_1 = __importDefault(require("aws-sdk"));
const { S3 } = aws_sdk_1.default;
const s3 = new S3({ httpOptions: { timeout: 900 } });
//run it again and again
/**
  * A Lambda function that logs the payload received from S3.
  */
function s3JsonLoggerHandler(event, context) {
    return __awaiter(this, void 0, void 0, function* () {
        console.log("Handler w/ event:\n", event.Records.map, "\n\n", context);
        const getObjectRequest = event.Records.map((record) => __awaiter(this, void 0, void 0, function* () {
            // event.Records.map(async (record) => {
            const params = {
                Bucket: record.s3.bucket.name,
                Key: record.s3.object.key,
            };
            const r = yield s3.getObject(params).promise()
                .catch(function (err) {
                console.log("Promise Exception-Message: \n", err.message);
                console.log("Promise Exception-Stack: \n", err.stack);
            })
                .then(function (d) {
                console.log("await getObject:\n", d);
            });
            // console.log("This is line 47 r: \n\n", r)
        }));
        yield Promise.all(getObjectRequest)
            .catch(function (err) {
            console.log("await Promise.all GetObjectRequests Promise Exception-Message: \n", err.message);
            console.log("await Promise.all GetObjectRequests Promise Exception-Stack: \n", err.stack);
        })
            .then(function (res) {
            console.log("This is await Promise.all GetObjectRequests - res: \n", res);
        });
        //console.log("This is Resp: \n", gor)
    });
}
exports.s3JsonLoggerHandler = s3JsonLoggerHandler;
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
