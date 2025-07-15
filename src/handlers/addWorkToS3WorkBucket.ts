/* eslint-disable no-debugger */
"use strict"
import {PutObjectCommand, type PutObjectCommandInput, type PutObjectCommandOutput} from '@aws-sdk/client-s3'
import {type AddWorkToS3WorkBucketResults, s3dbConfig, S3DB_Logging, s3} from './s3DropBucket'


let addWorkS3BucketResults: AddWorkToS3WorkBucketResults = {
  versionId: "",
  S3ProcessBucketResultStatus: "",
  AddWorkToS3WorkBucketResult: "In Quiesce",
}

export async function addWorkToS3WorkBucket (queueUpdates: string, key: string) {
  
  if (s3dbConfig.s3dropbucket_queuebucketquiesce)
  {
    S3DB_Logging("warn", "923", `Work/Process Bucket Quiesce is in effect, no New Work Files are being written to the S3 Queue Bucket. This work file is for ${key}`)
    
    addWorkS3BucketResults = {
      versionId: '',
      S3ProcessBucketResultStatus: '',
      AddWorkToS3WorkBucketResult: 'In Quiesce'

    }
    return addWorkS3BucketResults
  }

  const s3WorkPutInput: PutObjectCommandInput = {
    Body: queueUpdates,
    Bucket: s3dbConfig.s3dropbucket_workbucket,
    Key: key
  }

  let addWorkToS3ProcessBucket: PutObjectCommandOutput

  try
  {
    addWorkToS3ProcessBucket = await s3
      .send(new PutObjectCommand(s3WorkPutInput))
      .then(async (s3PutResult: PutObjectCommandOutput) => {
        
        return s3PutResult
      })
      .catch((err) => {
        debugger //catch

        S3DB_Logging("exception", "", `Exception - Put Object Command for writing work(${key} to S3 Processing bucket(${s3dbConfig.s3dropbucket_bulkimport}): \n${err}`)

        addWorkS3BucketResults = {
          versionId: '',
          S3ProcessBucketResultStatus: '',
          AddWorkToS3WorkBucketResult: 'Exception ...'
        }

        //return addWorkS3BucketResults
        
        throw new Error(
          `PutObjectCommand Results Failed for (${key} of ${queueUpdates.length} characters) to S3 Processing bucket (${s3dbConfig.s3dropbucket_bulkimport}): \n${err}`
        )
      })
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - Put Object Command for writing work(${key} to S3 Processing bucket(${s3dbConfig.s3dropbucket_bulkimport}): \n${e}`)
    
    addWorkS3BucketResults = {
      versionId: '',
      S3ProcessBucketResultStatus: '',
      AddWorkToS3WorkBucketResult: 'Exception ...'
    }

    throw new Error(
      `Exception - Put Object Command for writing work(${key} to S3 Processing bucket(${s3dbConfig.s3dropbucket_bulkimport}): \n${e}`
    )
    // return addWorkS3BucketResults
  }


  if (addWorkToS3ProcessBucket.$metadata.httpStatusCode !== 200)
  {
    throw new Error(
      `Failed to write Work File to S3 Process Store (Result ${addWorkToS3ProcessBucket}) for ${key} of ${queueUpdates.length} characters.`
    )
  }


  const sc = JSON.stringify(addWorkToS3ProcessBucket.$metadata.httpStatusCode, null, 2)

  const vidString = addWorkToS3ProcessBucket.VersionId ?? ""

  S3DB_Logging("info", "914", `Added Work File ${key} to Work Bucket (${s3dbConfig.s3dropbucket_workbucket}). \n${JSON.stringify(addWorkToS3ProcessBucket)}`)

  addWorkS3BucketResults = {
    versionId: vidString,
    S3ProcessBucketResultStatus: sc,
    AddWorkToS3WorkBucketResult: JSON.stringify(addWorkToS3ProcessBucket)
  }

  return addWorkS3BucketResults
}
