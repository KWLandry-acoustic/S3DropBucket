import { S3Event, Context} from 'aws-lambda';
import { s3JsonLoggerHandler } from './src/handlers/s3-json-logger.js';

exports.Handler = async (event: S3Event, context: Context) => {
    console.log(`Event: ${JSON.stringify(event, null, 2)}`);
    console.log(`Context: ${JSON.stringify(context, null, 2)}`);
    await s3JsonLoggerHandler(event, context)
};