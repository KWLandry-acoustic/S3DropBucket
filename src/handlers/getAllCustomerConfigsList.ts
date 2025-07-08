/* eslint-disable no-debugger */
"use strict"
import {type ListObjectsCommandInput, ListObjectsCommand, type ListObjectsCommandOutput} from '@aws-sdk/client-s3'
import {s3, s3dbConfig, S3DB_Logging} from './s3DropBucket'

export async function getAllCustomerConfigsList (bucket: string) {

  const listReq = {
    Bucket: bucket, //`${bucket}.s3.amazonaws.com`,
    MaxKeys: 500
    //Prefix: S3DBConfig.PrefixFocus,
  } as ListObjectsCommandInput

  const l = [] as string[]

  //"Exception - Retrieving Customer Configs List from s3dropbucket-configs: PermanentRedirect: The bucket you are 
  //attempting to access must be addressed using the specified endpoint.Please send all future requests to this 
  //endpoint. "
  await s3
    .send(new ListObjectsCommand(listReq))
    .then(async (s3ListResult: ListObjectsCommandOutput) => {

      if (s3ListResult.Contents?.length == 0)
      {
        throw new Error("No Customer Configs Found, unable to establish Customer Configs List. ")
      }

      const cc = (s3ListResult.Contents) as object[]

      //Scrub all but '.jsonc' out 
      for (const m of cc)
      {
        type cck = keyof typeof m
        const i: cck = "Key" as cck
        const km = m[i] as string
        if (km.indexOf('.jsonc') > -1) l.push(km)
      }

    })
    .catch((e) => {
      debugger //catch
      S3DB_Logging("exception", "", `Exception - Retrieving Customer Configs List from ${bucket}: ${e} `)
    })

  return l
}
