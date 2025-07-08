/* eslint-disable no-debugger */
"use strict"
import {s3dbConfig, S3DB_Logging} from './s3DropBucket'

export async function sftpDeleteFile (remoteFile: string) {
  S3DB_Logging("info", "700", `Deleting ${remoteFile}`)
  try
  {
    // await SFTPClient.delete(remoteFile)
  } catch (err)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Deleting failed: ${err}`)
  }
}
