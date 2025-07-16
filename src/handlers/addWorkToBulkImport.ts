/* eslint-disable no-debugger */
"use strict"
import {PutObjectCommand, type PutObjectCommandInput, type PutObjectCommandOutput} from '@aws-sdk/client-s3'
import {S3DB_Logging, s3, type S3DBConfig, type CustomerConfig, type AddWorkToBulkImportResults} from './s3DropBucket'
import {stringify} from 'csv-stringify'


export async function addWorkToBulkImport (key: string, refsetUpdates: object[], s3dbConfig: S3DBConfig, custConfig: CustomerConfig)

{

  debugger ///

  
  if (s3dbConfig.s3dropbucket_bulkimportquiesce)
  {
    S3DB_Logging("warn", "526", `BulkImport Quiesce is in effect, no New Work will be written to the BulkImport Bucket. Updates are from ${key}`)

    let bulkImportStatus: AddWorkToBulkImportResults = {
      BulkImportWriteResultStatus: '',
      AddWorkToBulkImportResult: 'In Quiesce'
    }

    return bulkImportStatus
  }

debugger ///  

  //convert JSON to CSV 
  const jsonCSV = stringify({
    delimiter: ":",
  })

  jsonCSV.on("readable", function () {
    let row
    while ((row = jsonCSV.read()) !== null)
    {
      refsetUpdates.push(row)  //replace with S3 write stream
    }
  })
  jsonCSV.on("error", function (err) {
    console.error(err.message)
  });



  //Build the Bulk Import File Name
  const biKey = custConfig.subscriptionid + "/" + key 

  const bulkImportPutCommand: PutObjectCommandInput = {
    Body: JSON.stringify(refsetUpdates),
    Bucket: s3dbConfig.s3dropbucket_bulkimport,
    Key: key
  }
  
  let bulkImportResult

  try
  {
    bulkImportResult = await s3
      .send(new PutObjectCommand(bulkImportPutCommand))
      .then(async (s3PutResult: PutObjectCommandOutput) => {
        if (s3PutResult.$metadata.httpStatusCode !== 200)
        {
          throw new Error(
            `Failed to write updates to Bulk Import (Result ${s3PutResult}) for ${key} of ${refsetUpdates.length} characters.`
          )
        }
        return s3PutResult
      })
      .catch((err) => {
        debugger //catch

        S3DB_Logging("exception", "", `Exception - Put Object Command for writing work(${key} to Bulk Import bucket(${s3dbConfig.s3dropbucket_bulkimport}): \n${err}`)
        throw new Error(
          `PutObjectCommand Results Failed for (${key} of ${refsetUpdates.length} characters) to Bulk Import bucket (${s3dbConfig.s3dropbucket_bulkimport}): \n${err}`
        )
      })
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - Put Object Command for writing updates (${key} to Bulk Import bucket(${s3dbConfig.s3dropbucket_bulkimport}): \n${e}`)
    throw new Error(
      `Exception - Put Object Command for writing work(${key} to Bulk Import bucket(${s3dbConfig.s3dropbucket_bulkimport}): \n${e}`
    )
    // return { StoreS3WorkException: e }
  }

  let bulkImportStatus = {
    BulkImportWriteResultStatus: JSON.stringify(bulkImportResult.$metadata.httpStatusCode, null, 2),
    AddWorkToBulkImportResult: JSON.stringify(bulkImportResult)
  } as AddWorkToBulkImportResults

  S3DB_Logging("info", "527", `Added Updates ${key} to Bulk Import Bucket (${s3dbConfig.s3dropbucket_bulkimport}). \n${JSON.stringify(bulkImportStatus)}`)

  return bulkImportStatus

}
