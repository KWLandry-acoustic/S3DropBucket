/* eslint-disable no-debugger */
"use strict"
import {type ListObjectsCommandInput, ListObjectsCommand, type ListObjectsCommandOutput} from '@aws-sdk/client-s3'
import {testS3Key, s3dbConfig, s3, S3DB_Logging} from './s3DropBucket'

//function checkMetadata() {
//  //Pull metadata for table/db defined in config
//  // confirm updates match Columns
//  //ToDo:  Log where Columns are not matching
//}
export async function getAnS3ObjectforTesting (bucket: string) {
  let s3Key: string = ""

  if (testS3Key !== null) return

  const listReq = {
    Bucket: bucket,
    MaxKeys: 101,
    Prefix: S3DBConfig.s3dropbucket_prefixfocus,
  } as ListObjectsCommandInput

  await s3
    .send(new ListObjectsCommand(listReq))
    .then(async (s3ListResult: ListObjectsCommandOutput) => {
      let i: number = 0

      if (s3ListResult.Contents)
      {
        const kc: number = (s3ListResult.Contents.length as number) - 1
        if ((kc == 0))
          throw new Error("No S3 Objects to retrieve as Test Data, exiting")

        if (kc > 10)
        {
          i = Math.floor(Math.random() * (10 - 1 + 1) + 1)
        }
        if ((kc == 1)) i = 0
        s3Key = s3ListResult.Contents?.at(i)?.Key as string

        while (s3Key.toLowerCase().indexOf("aggregat") > -1)
        {
          i++
          s3Key = s3ListResult.Contents?.at(i)?.Key as string
        }
        //Log all Test files found
        S3DB_Logging("info", "", `S3 List: \n${JSON.stringify(s3ListResult.Contents)} `)
        //Log the file used for this test run 
        S3DB_Logging("info", "", `This is a Test Run(${i}) Retrieved ${s3Key} for this Test Run`)

      }
      else
      {
        S3DB_Logging("exception", "", `No S3 Object available for Testing: ${bucket} `)
        throw new Error(`No S3 Object available for Testing: ${bucket} `)
      }
      return s3Key
    })
    .catch((e) => {
      debugger //catch

      S3DB_Logging("exception", "", `Exception - On S3 List Command for Testing Objects from ${bucket}: ${e} `)
    })

  return s3Key
}
