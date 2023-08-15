import { config, S3, Lambda, HttpRequest, HttpResponse, HTTPOptions, Endpoint } from  'aws-sdk' 
import { S3Event, Context } from 'aws-lambda';
// import * as logger_1 from '@aws-lambda-powertools/logger';
// import * as metrics_1 from '@aws-lambda-powertools/metrics';
// import * as tracer_1 from '@aws-lambda-powertools/tracer';
// import "source-map-support/register";

//run it again and again and 9

/**
  * A Lambda function that logs the payload received from S3.
  */


import { Handler } from 'aws-lambda';

export const s3JsonLoggerHandler: Handler = async (event, context) => {
    console.log('EVENT: \n' + JSON.stringify(event, null, 2));
    return context.logStreamName;
};

export default s3JsonLoggerHandler

