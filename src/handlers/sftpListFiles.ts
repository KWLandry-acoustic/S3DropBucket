/* eslint-disable no-debugger */
"use strict"
//import type sftpClient {ListFilterFunction} from 'ssh2-sftp-client'
import type sftpClient from 'ssh2-sftp-client'
import type {ListFilterFunction} from 'ssh2-sftp-client'
import {S3DB_Logging, SFTPClient} from './s3DropBucket'


export async function sftpListFiles (remoteDir: string, fileGlob: ListFilterFunction) {
  S3DB_Logging("info", "700", `Listing ${remoteDir} ...`)

  let fileObjects: sftpClient.FileInfo[] = []
  try
  {
    fileObjects = await SFTPClient.list(remoteDir, fileGlob)
  } catch (err)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Listing failed: ${err}`)
  }

  const fileNames = []

  for (const file of fileObjects)
  {
    if (file.type === "d")
    {
      S3DB_Logging("info", "700", `${new Date(file.modifyTime).toISOString()} PRE ${file.name}`)
    }
    else
    {
      S3DB_Logging("info", "700", `${new Date(file.modifyTime).toISOString()} ${file.size} ${file.name}`)
    }

    fileNames.push(file.name)
  }

  return fileNames
}
