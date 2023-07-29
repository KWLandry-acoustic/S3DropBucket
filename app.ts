import { Handler, S3Event, Context, S3BatchEvent} from 'aws-lambda';
import { s3JsonLoggerHandler } from './src/handlers/s3-json-logger.js';

//app.js not needed as the solution is a standalone lambda Handler defined in template.yml and registered on invocation

exports.Handler = async (event: S3Event, context: Context) => {
    console.log(`Event: ${JSON.stringify(event, null, 2)}`);
    console.log(`Context: ${JSON.stringify(context, null, 2)}`);
    await s3JsonLoggerHandler(event, context)
    return {
        statusCode: 200,
        body: `Queries: ${JSON.stringify(event, null, 2)}`
      }

};



