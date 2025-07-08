/* eslint-disable no-debugger */
"use strict"
import {s3dbConfig, S3DB_Logging} from './s3DropBucket'

export async function sftpDownloadFile (remoteFile: string, localFile: string) {
  S3DB_Logging("info", "700", `Downloading ${remoteFile} to ${localFile} ...`)
  try
  {
    // await SFTPClient.get(remoteFile, localFile)
  } catch (err)
  {

    debugger //catch

    S3DB_Logging("exception", "", `Downloading failed: ${err}`)
  }
}
