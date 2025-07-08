/* eslint-disable no-debugger */
"use strict"
import {s3dbConfig, S3DB_Logging} from './s3DropBucket'

export async function sftpUploadFile (localFile: string, remoteFile: string) {
  S3DB_Logging("info", "700", `Uploading ${localFile} to ${remoteFile} ...`)
  try
  {
    // await SFTPClient.put(localFile, remoteFile)
  } catch (err)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Uploading failed: ${err}`)
  }
}
