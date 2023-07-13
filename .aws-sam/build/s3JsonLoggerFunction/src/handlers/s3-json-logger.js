// Create clients outside of the handler

// Create a client to read objects from S3
import pkg from 'aws-sdk';
const { S3 } = pkg
const s3 = new S3({ httpOptions: { timeout: 30000 }, timeout: 900 });


/**
  * A Lambda function that logs the payload received from S3.
  */
export async function s3JsonLoggerHandler(event, context) {

    console.log("This is handled: \n\n") //, event.Records.Body, "\n\n", context) 

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
                console.log("This is await getObject - d : \n", d)
            })

        console.log("This is line 47 r: \n\n", r)
        })



        // const getObjectPromise = () => new Promise((resolve, reject) => {
        //     try {
        //         const data = s3.getObject(params);
        //         resolve(data);
        //     } catch (error) {
        //         reject(error);
        //     }
        // });

        // const response = await getObjectPromise();



        // const s = await getObjectS3(params.Bucket, params.Key)
        //     .catch(function (err) {
        //         console.log("Promise Exception-Message: \n", err.message);
        //         console.log("Promise Exception-Stack: \n", err.stack);
        //     })
        //     .then(function (d) {
        //         console.log("This is getObjectS3: \n", d)
        //     })
        // console.log("This is S: \n\n", s)


        // try {

        //     console.log("Trying to fetch " + record.s3.object.key + " from bucket " + record.s3.bucket.name)
        //     const data = await s3.getObject(params).promise()

        // const data = (await (s3.getObject(params).promise())).Body.toString('utf-8')
        //const data = s3.getObject(params)

        // console.log("Done loading image from S3:\n\n", data.Body)
        //console.log( data.Body.toString('utf-8'))

        // console.log("This is here: \n\n", params, "\n\n")

        // const r = (await (s3.getObject(params).promise())).Body.toString('utf-8')
        // //const data: string[] = [];     
        // const data = [];
        // for await (const chunk of data) {
        //     data.push(chunk.toString());
        // }

        // return data.join('');

        // console.log("Waiting on Get Object Requests \n\n\n", d)


        // const { Body } = s3.getObject(params, async (resp) => {

        // })


        // const Body = await s3.getObject(params);


        //const {Body} = await s3.getObject(params).promise();

        //s3.getObject(params);

        // return await s3.getObject(params).promise().then(async (res) => {
        //     return res
        // })            

        //return s3.getObject(params).promise()

        //const data1 = (await (s3.getObject(params).promise())).Body.toString('utf-8')
        //console.log("This is data1: ", { data1 });


        // All log statements are written to CloudWatch by default. For more information, see
        // https://docs.aws.amazon.com/lambda/latest/dg/nodejs-prog-model-logging.html
        //console.log("This is the Body: \n\n", Body.toString());


        // } catch (e) {
        //     console.error('\n\n\nError calling S3 getObject:\n\n', e);
        //     //throw e;
        // }
        // finally {
        //     console.log("Finally here: \n\n") // , Body.Body, "\n\n")
        // }
        // });


    // })


}


    await Promise.all(getObjectRequest)
        .catch(function (err) {
            console.log("await Promise.all GetObjectRequests Promise Exception-Message: \n", err.message);
            console.log("await Promise.all GetObjectRequests Promise Exception-Stack: \n", err.stack);
        })
        .then(function (res) {
            console.log("This is await Promise.all GetObjectRequests - res: \n", res)
        })
    //console.log("This is Resp: \n", gor)






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

