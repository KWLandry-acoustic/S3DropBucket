/* eslint-disable no-debugger */
"use strict"
import {PutObjectCommand, type PutObjectCommandInput, type PutObjectCommandOutput} from '@aws-sdk/client-s3'
import {s3dbConfig, S3DB_Logging, s3} from './s3DropBucket'


export async function addWorkToS3WorkBucket (queueUpdates: string, key: string) {
  if (s3dbConfig.s3dropbucket_queuebucketquiesce)
  {
    S3DB_Logging("warn", "923", `Work/Process Bucket Quiesce is in effect, no New Work Files are being written to the S3 Queue Bucket. This work file is for ${key}`)

    return {
      versionId: "",
      S3ProcessBucketResultStatus: "",
      AddWorkToS3ProcessBucket: "In Quiesce",
    }
  }

  const s3WorkPutInput: PutObjectCommandInput = {
    Body: queueUpdates,
    Bucket: s3dbConfig.s3dropbucket_bulkimportbucket,
    Key: key
  }

  let S3ProcessBucketResultStatus = ""
  let addWorkToS3ProcessBucket

  try
  {
    addWorkToS3ProcessBucket = await s3
      .send(new PutObjectCommand(s3WorkPutInput))
      .then(async (s3PutResult: PutObjectCommandOutput) => {
        if (s3PutResult.$metadata.httpStatusCode !== 200)
        {
          throw new Error(
            `Failed to write Work File to S3 Process Store (Result ${s3PutResult}) for ${key} of ${queueUpdates.length} characters.`
          )
        }
        return s3PutResult
      })
      .catch((err) => {
        debugger //catch

        S3DB_Logging("exception", "", `Exception - Put Object Command for writing work(${key} to S3 Processing bucket(${s3dbConfig.s3dropbucket_bulkimportbucket}): \n${err}`)
        throw new Error(
          `PutObjectCommand Results Failed for (${key} of ${queueUpdates.length} characters) to S3 Processing bucket (${s3dbConfig.s3dropbucket_bulkimportbucket}): \n${err}`
        )
        //return {StoreS3WorkException: err}
      })
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - Put Object Command for writing work(${key} to S3 Processing bucket(${s3dbConfig.s3dropbucket_bulkimportbucket}): \n${e}`)
    throw new Error(
      `Exception - Put Object Command for writing work(${key} to S3 Processing bucket(${s3dbConfig.s3dropbucket_bulkimportbucket}): \n${e}`
    )
    // return { StoreS3WorkException: e }
  }

  S3ProcessBucketResultStatus = JSON.stringify(addWorkToS3ProcessBucket.$metadata.httpStatusCode, null, 2)

  const vidString = addWorkToS3ProcessBucket.VersionId ?? ""

  S3DB_Logging("info", "914", `Added Work File ${key} to Work Bucket (${s3dbConfig.s3dropbucket_bulkimportbucket}). \n${JSON.stringify(addWorkToS3ProcessBucket)}`)

  const aw3pbr = {
    versionId: vidString,
    AddWorkToS3ProcessBucket: JSON.stringify(addWorkToS3ProcessBucket),
    S3ProcessBucketResultStatus: S3ProcessBucketResultStatus,
  }

  return aw3pbr
}
