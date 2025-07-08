/* eslint-disable no-debugger */
"use strict"
import {type GetObjectCommandInput, GetObjectCommand, type GetObjectCommandOutput} from '@aws-sdk/client-s3'
import {S3DB_Logging, s3dbConfig, s3} from './s3DropBucket'

export async function getS3Work (s3Key: string, bucket: string) {
  S3DB_Logging("info", "517", `GetS3Work for Key: ${s3Key}`)

  const getObjectCmd = {
    Bucket: bucket,
    Key: s3Key,
  } as GetObjectCommandInput


  let work: string = ""

  try
  {
    await s3
      .send(new GetObjectCommand(getObjectCmd))
      .then(async (getS3Result: GetObjectCommandOutput) => {
        work = (await getS3Result.Body?.transformToString("utf8")) as string
        S3DB_Logging("info", "517", `Work Pulled (${work.length} chars): ${s3Key}`)

      })
  } catch (e)
  {
    debugger //catch

    const err: string = JSON.stringify(e)
    if (err.toLowerCase().indexOf("nosuchkey") > -1)
    {
      //S3DB_Logging("exception", "", `Work Not Found on S3 Process Queue (${s3Key}. Work will not be marked for Retry.`)
      throw new Error(
        `Exception - Work Not Found on S3 Process Queue (${s3Key}. Work will not be marked for Retry. \n${e}`
      )
    }

    else
    {
      S3DB_Logging("exception", "", `Exception - Retrieving Work from S3 Process Queue for ${s3Key}.  \n ${e}`)
      throw new Error(
        `Exception - Retrieving Work from S3 Process Queue for ${s3Key}.  \n ${e}`
      )
    }
  }
  return work
}
