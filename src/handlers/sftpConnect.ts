/* eslint-disable no-debugger */
"use strict"
import {S3DB_Logging} from './s3DropBucket'

export async function sftpConnect (options: {
  host: string
  port: string
  username?: string
  password?: string
}) {
  S3DB_Logging("info", "700", `Connecting to ${options.host}: ${options.port}`)

  try
  {
    // await SFTPClient.connect(options)
  } catch (err)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Failed to connect: ${err}`)
  }
}
