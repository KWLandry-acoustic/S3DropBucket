/* eslint-disable no-debugger */
"use strict"
import {DeleteObjectCommand, type DeleteObjectCommandOutput} from '@aws-sdk/client-s3'
import {s3, s3dbConfig, S3DB_Logging} from './s3DropBucket'

//async function saveS3Work(s3Key: string, body: string, bucket: string) {
//  if (s3dbLogDebug) console.info(`Debug - SaveS3Work Key: ${s3Key}`)
//  const putObjectCmd = {
//    Bucket: bucket,
//    Key: s3Key,
//    Body: body,
//    // ContentLength: Number(`${body.length}`),
//  } as GetObjectCommandInput
//  let saveS3: string = ""
//  try {
//    await s3
//      .send(new PutObjectCommand(putObjectCmd))
//      .then(async (getS3Result: GetObjectCommandOutput) => {
//        saveS3 = (await getS3Result.Body?.transformToString("utf8")) as string
//        if (s3dbLogDebug)
//          console.info(`Work Saved (${saveS3.length} chars): ${s3Key}`)
//      })
//  } catch (e) {
//    throw new Error(`Exception - Saving Work for ${s3Key}. \n ${e}`)
//  }
//  return saveS3
//}
export async function deleteS3Object (s3ObjKey: string, bucket: string) {
  let delRes = ""

  const s3D = {
    Key: s3ObjKey,
    Bucket: bucket,
  }

  const d = new DeleteObjectCommand(s3D)

  try
  {
    await s3
      .send(d)
      .then(async (s3DelResult: DeleteObjectCommandOutput) => {
        delRes = JSON.stringify(s3DelResult.$metadata.httpStatusCode, null, 2)
      })
      .catch((e) => {
        debugger //catch

        S3DB_Logging("exception", "", `Exception - Attempting S3 Delete Command for ${s3ObjKey}: \n ${e} `)

        return delRes
      })
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - Attempting S3 Delete Command for ${s3ObjKey}: \n ${e} `)
  }
  return delRes
}
