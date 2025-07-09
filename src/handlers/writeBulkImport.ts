/* eslint-disable no-debugger */
"use strict"
import {S3DBConfig, CustomerConfig, S3DB_Logging} from './s3DropBucket'

export function writeBulkImport (key: string, updates: object[], s3dbConfig: S3DBConfig, custConfig: CustomerConfig) {

  //throw new Error('Function not implemented.')

  
  S3DB_Logging("error", "525", `Writing ${key} to BulkImport ${}`,)
  


  try
  {
    addRefSetUpdatesToBulkImportResult = await addWorkToS3WorkBucket(updates, key)
      .then((res) => {
        return {"workfile": key, ...res} //{"AddWorktoS3Results": res}
      })
      .catch((err: ) => {
        debugger //catch

        S3DB_Logging("exception", "", `Exception - AddWorkToS3WorkBucket (file: ${key}) ${err}`)
      })
  } catch (e)
  {
    debugger //catch

    const s3StoreError = `Exception - StoreAndQueueWork Add work (file: ${key}) to S3 Work Bucket exception \n${e} `
    S3DB_Logging("exception", "", s3StoreError)

    return {
      StoreS3WorkException: s3StoreError,
      StoreQueueWorkException: "",
      AddWorkToS3WorkBucketResults: JSON.stringify(addWorkToS3WorkBucketResult),
    }
  }






}
