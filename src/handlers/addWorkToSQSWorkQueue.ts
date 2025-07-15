/* eslint-disable no-debugger */
"use strict"
import {SendMessageCommand, type SendMessageCommandOutput} from '@aws-sdk/client-sqs'
import {type CustomerConfig, s3dbConfig, S3DB_Logging, type S3DBQueueMessage, type AddWorkToSQSWorkQueueResults, sqsClient} from './s3DropBucket'


let sqwResult: AddWorkToSQSWorkQueueResults = {
  SQSWriteResultStatus: '',
  AddWorkToSQSQueueResult: ''
}

export async function addWorkToSQSWorkQueue (
  config: CustomerConfig,
  key: string,
  //versionId: string,
  batch: number,
  recCount: string,
  marker: string
) {
  if (s3dbConfig.s3dropbucket_queuebucketquiesce)
  {
    S3DB_Logging("warn", "923", `Work/Process Bucket Quiesce is in effect, no New Work Files are being written to the SQS Queue of S3 Work Bucket. This work file is for ${key}`)
    sqwResult = {
      SQSWriteResultStatus: "in Quiesce",
      AddWorkToSQSQueueResult: ''
    }

    return sqwResult
  }

  const sqsQMsgBody = {} as S3DBQueueMessage
  sqsQMsgBody.workKey = key
  //sqsQMsgBody.versionId = versionId
  sqsQMsgBody.marker = marker
  sqsQMsgBody.attempts = 1
  sqsQMsgBody.batchCount = batch.toString()
  sqsQMsgBody.updateCount = recCount
  sqsQMsgBody.custconfig = config
  sqsQMsgBody.lastQueued = Date.now().toString()

  const sqsParams = {
    MaxNumberOfMessages: 1,
    QueueUrl: s3dbConfig.s3dropbucket_workqueue,
    //Defer to setting these on the Queue in AWS SQS Interface
    // VisibilityTimeout: parseInt(tcc.WorkQueueVisibilityTimeout),
    // WaitTimeSeconds: parseInt(tcc.WorkQueueWaitTimeSeconds),
    MessageAttributes: {
      FirstQueued: {
        DataType: "String",
        StringValue: Date.now().toString(),
      },
      Retry: {
        DataType: "Number",
        StringValue: "0",
      },
    },
    MessageBody: JSON.stringify(sqsQMsgBody),
  }

  let sqsSendResult: {}

  try
  {
    sqsSendResult = await sqsClient
      .send(new SendMessageCommand(sqsParams))
      .then((sqsSendMessageResult: SendMessageCommandOutput) => {

        if (JSON.stringify(sqsSendMessageResult.$metadata.httpStatusCode, null, 2) !== "200")
        {
          const storeQueueWorkException = `Failed writing to SQS Process Queue (queue URL: ${sqsParams.QueueUrl}), ${sqsQMsgBody.workKey}, SQS Params${JSON.stringify(sqsParams)})`

          let sqwResult: AddWorkToSQSWorkQueueResults = {
            SQSWriteResultStatus: 'error',
            AddWorkToSQSQueueResult: storeQueueWorkException
          }
          
          return sqwResult
        }

        
        let sqwResult: AddWorkToSQSWorkQueueResults = {
          SQSWriteResultStatus: JSON.stringify(sqsSendMessageResult.$metadata.httpStatusCode, null, 2),
          AddWorkToSQSQueueResult: JSON.stringify(sqsSendMessageResult)
        }


        //S3DB_Logging("info", "946", `Queued Work to SQS Process Queue (${sqsQMsgBody.workKey}) \nResult: ${sqsWriteResultStatus} \n${JSON.stringify(sqsSendMessageResult)} `)
        S3DB_Logging("info", "946", `Queued Work (${sqsQMsgBody.workKey}} to SQS Process Queue (for ${recCount} updates). \nWork Queue (${s3dbConfig.s3dropbucket_workqueue}) \nSQS Params: ${JSON.stringify(sqsParams)}. \nresults: ${JSON.stringify(sqsSendMessageResult)} \nStatus: ${JSON.stringify({SQSWriteResultStatus: sqwResult.SQSWriteResultStatus, AddToSQSQueue: JSON.stringify(sqsSendResult)})}`)
      
        return sqwResult
      })
      .catch((err) => {
        debugger //catch

        const storeQueueWorkException = `Failed writing to SQS Process Queue (${err}). \nQueue URL: ${sqsParams.QueueUrl})\nWork to be Queued: ${sqsQMsgBody.workKey}\nSQS Params: ${JSON.stringify(sqsParams)}`

        S3DB_Logging("exception", "", `Failed to Write to SQS Process Queue. \n${storeQueueWorkException}`)
        
        sqwResult = {
          SQSWriteResultStatus: "Exception",
          AddWorkToSQSQueueResult: storeQueueWorkException
        }
        
        return sqwResult
      })
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - Writing to SQS Process Queue - (queue URL${sqsParams.QueueUrl}), ${sqsQMsgBody.workKey}, SQS Params${JSON.stringify(sqsParams)}) - Error: ${e}`)
  }

  S3DB_Logging("info", "940", `Work Queued (${key} for ${recCount} updates) to the SQS Work Queue (${s3dbConfig.s3dropbucket_workqueue}) \nresults: \n${JSON.stringify(sqwResult)}`)

  return sqwResult
}
