/* eslint-disable no-debugger */
"use strict"
import {type GetObjectCommandInput, GetObjectCommand, type GetObjectCommandOutput} from '@aws-sdk/client-s3'
import Ajv from 'ajv'
import {type S3DBConfig, s3, S3DB_Logging} from './s3DropBucket'

export async function getValidateS3DropBucketConfig () {
  //Article notes that Lambda runs faster referencing process.env vars, lets see.
  //Did not pan out, with all the issues with conversions needed to actually use as primary reference, can't see it being faster
  //Using process.env as a useful reference store, especially for accessToken, good across invocations
  //Validate then populate env vars with S3DropBucket config
  //const cf = process.env.S3DropBucketConfigFile
  //const cb = process.env.S3DropBucketConfigBucket
  let getObjectCmd: GetObjectCommandInput = {
    Bucket: undefined,
    Key: undefined,
  }

  if (!process.env.S3DropBucketConfigBucket) process.env.S3DropBucketConfigBucket = "s3dropbucket-configs"

  if (!process.env.S3DropBucketConfigFile) process.env.S3DropBucketConfigFile = "s3dropbucket_config.jsonc"

  getObjectCmd = {
    Bucket: process.env.S3DropBucketConfigBucket,
    Key: process.env.S3DropBucketConfigFile,
  }

  //if ( process.env.S3DropBucketConfigFile && process.env.S3DropBucketConfigBucket )
  //{
  //    getObjectCmd = {
  //        Bucket: process.env.S3DropBucketConfigBucket,
  //        Key: process.env.S3DropBucketConfigFile
  //    }
  //}
  //else
  //{
  //    getObjectCmd = {
  //        Bucket: process.env.S3DropBucketConfigBucket,    //'s3dropbucket-configs',
  //        Key: 's3dropbucket_config.jsonc',
  //    }
  //}
  let s3dbcr
  let s3dbc = {} as S3DBConfig

  try
  {
    s3dbc = await s3.send(new GetObjectCommand(getObjectCmd))
      .then(async (getConfigS3Result: GetObjectCommandOutput) => {
        s3dbcr = (await getConfigS3Result.Body?.transformToString(
          "utf8"
        )) as string

        S3DB_Logging("info", "912", `Pulling S3DropBucket Config File (bucket:${getObjectCmd.Bucket}  key:${getObjectCmd.Key}) \nResult: ${s3dbcr}`)

        //Fix extra space/invlaid formatting of "https:  //  ...." errors before parsing 
        s3dbcr = s3dbcr.replaceAll(new RegExp(/(?:(?:https)|(?:HTTPS)): \/\//gm), "https://")

        //Parse comments out of the json before returning parsed config json
        s3dbcr = s3dbcr.replaceAll(new RegExp(/[^:](\/\/.*(,|$|")?)/g), "")
        s3dbcr = s3dbcr.replaceAll(" ", "")
        s3dbcr = s3dbcr.replaceAll("\n", "")

        return JSON.parse(s3dbcr)
      })
  } catch (e)
  {

    debugger //catch

    S3DB_Logging("exception", "", `Exception - Pulling S3DropBucket Config File (bucket:${getObjectCmd.Bucket}  key:${getObjectCmd.Key}) \nResult: ${s3dbcr} \nException: \n${e} `)
    //return {} as s3DBConfig
    throw new Error(
      `Exception - Pulling S3DropBucket Config File(bucket: ${getObjectCmd.Bucket}  key: ${getObjectCmd.Key}) \nResult: ${s3dbcr} \nException: \n${e} `
    )


  }

  try
  {

    const validateBySchema = new Ajv()
    const su = "https://raw.githubusercontent.com/KWLandry-acoustic/s3dropbucket_Schemas/main/json-schema-s3dropbucket_config.json"
    const fetchSchema = async () => {
      return await fetch(su).then(async (response) => {
        if (response.status >= 400)
        {
          //throw new Error(`Fetch of schema error: ${JSON.stringify(response)}`)
          S3DB_Logging("error", "", `Fetch of schema error: ${JSON.stringify(response)}`)
        }
        const r = await response.json()
        return JSON.stringify(r)
      })
    }

    const sf = await fetchSchema() as string
    sf


    //ToDo Validate Configs through Schema definition
    //const vs = validateBySchema.validate(sf, s3dbc)
    //S3DB_Logging("info", "",`Schema Validation returns: ${vs}`)
    //  *Must* set EventEmitterMaxListeners in environment vars as this flags whether config is already parsed.
    if (!isNaN(s3dbc.s3dropbucket_eventemittermaxlisteners) && typeof s3dbc.s3dropbucket_eventemittermaxlisteners === "number") process.env["EventEmitterMaxListeners"] = s3dbc.s3dropbucket_eventemittermaxlisteners.toString()

    else
    {
      throw new Error(
        `S3DropBucket Config - Invalid or missing definition: EventEmitterMaxListeners.`
      )
    }


    //ToDo: refactor to a foreach validation instead of the Long List approach. 
    if (!s3dbc.s3dropbucket_loglevel || s3dbc.s3dropbucket_loglevel === "")
    {
      throw new Error(
        `S3DropBucket Config - Invalid definition: missing LogLevel.`
      )
    } else process.env.s3dropbucketLogLevel = s3dbc.s3dropbucket_loglevel


    if (!s3dbc.s3dropbucket_selectivelogging || s3dbc.s3dropbucket_selectivelogging === "")
    {
      throw new Error(
        `S3DropBucket Config - Invalid definition: missing SelectiveLogging.`
      )

    } else process.env["S3DropBucketSelectiveLogging"] = s3dbc.s3dropbucket_selectivelogging

    if (!s3dbc.s3dropbucket || s3dbc.s3dropbucket === "")
    {
      throw new Error(
        `S3DropBucket Config - Invalid definition: missing S3DropBucket (${s3dbc.s3dropbucket}).`
      )
    } else process.env["S3DropBucket"] = s3dbc.s3dropbucket

    if (!s3dbc.s3dropbucket_configs || s3dbc.s3dropbucket_configs === "")
    {
      throw new Error(
        `S3DropBucket Config - Invalid definition: missing S3DropBucketConfigs (${s3dbc.s3dropbucket_configs}).`
      )
    } else process.env["S3DropBucketConfigs"] = s3dbc.s3dropbucket_configs

    if (!s3dbc.s3dropbucket_bulkimport || s3dbc.s3dropbucket_bulkimport === "")
    {
      throw new Error(
        `S3DropBucket Config - Invalid definition: missing S3DropBucket BulkImport Bucket definition (${s3dbc.s3dropbucket_bulkimport}) `
      )
    } else process.env["S3DropBucketBulkImportBucket"] = s3dbc.s3dropbucket_bulkimport

    if (!s3dbc.s3dropbucket_workqueue || s3dbc.s3dropbucket_workqueue === "")
    {
      throw new Error(
        `S3DropBucket Config - Invalid definition: missing S3DropBucketWorkQueue (${s3dbc.s3dropbucket_workqueue}) `
      )
    } else process.env["S3DropBucketWorkQueue"] = s3dbc.s3dropbucket_workqueue


    if (!s3dbc.connectapiurl || s3dbc.connectapiurl === "")
    {
      throw new Error(
        `S3DropBucket Config - Invalid definition: missing connectapiurl - ${s3dbc.connectapiurl} `)
    } else process.env["connectapiurl"] = s3dbc.connectapiurl

    if (!s3dbc.xmlapiurl || s3dbc.xmlapiurl === "")
    {
      throw new Error(
        `S3DropBucket Config - Invalid definition: missing xmlapiurl - ${s3dbc.xmlapiurl} `
      )
    } else process.env["xmlapiurl"] = s3dbc.xmlapiurl

    if (!s3dbc.restapiurl || s3dbc.restapiurl === "")
    {
      throw new Error(
        `S3DropBucket Config - Invalid definition: missing restapiurl - ${s3dbc.restapiurl} `
      )
    } else process.env["restapiurl"] = s3dbc.restapiurl


    if (!s3dbc.authapiurl || s3dbc.authapiurl === "")
    {
      throw new Error(
        `S3DropBucket Config - Invalid definition: missing authapiurl - ${s3dbc.authapiurl} `
      )
    }
    else
      process.env["authapiurl"] = s3dbc.authapiurl


    //Default Separator
    if (!s3dbc.s3dropbucket_jsonseparator || s3dbc.s3dropbucket_jsonseparator === "")
    {
      throw new Error(
        `S3DropBucket Config - Invalid definition: missing jsonSeperator - ${s3dbc.s3dropbucket_workqueuequiesce} `
      )
    }
    else
    {
      if (s3dbc.s3dropbucket_jsonseparator.toLowerCase() === "null") s3dbc.s3dropbucket_jsonseparator = `''`
      if (s3dbc.s3dropbucket_jsonseparator.toLowerCase() === "empty") s3dbc.s3dropbucket_jsonseparator = `""`
      if (s3dbc.s3dropbucket_jsonseparator.toLowerCase() === "\n") s3dbc.s3dropbucket_jsonseparator = "\n"
      process.env["S3DropBucketJsonSeparator"] = s3dbc.s3dropbucket_jsonseparator
    }

    if (s3dbc.s3dropbucket_workqueuequiesce === true || s3dbc.s3dropbucket_workqueuequiesce === false) process.env["WorkQueueQuiesce"] = s3dbc.s3dropbucket_workqueuequiesce.toString()

    else
      throw new Error(
        `S3DropBucket Config - Invalid definition: WorkQueueQuiesce - ${s3dbc.s3dropbucket_workqueuequiesce} `
      )

    //process.env["RetryQueueInitialWaitTimeSeconds"]
    if (!isNaN(s3dbc.s3dropbucket_maxbatcheswarning) && typeof s3dbc.s3dropbucket_maxbatcheswarning === "number") process.env["MaxBatchesWarning"] = s3dbc.s3dropbucket_maxbatcheswarning.toFixed()

    else
      throw new Error(
        `S3DropBucket Config - Invalid definition: missing MaxBatchesWarning - ${s3dbc.s3dropbucket_maxbatcheswarning} `
      )

    if (s3dbc.s3dropbucket_quiesce === true || s3dbc.s3dropbucket_quiesce === false) process.env["S3DropBucketQuiesce"] = s3dbc.s3dropbucket_quiesce.toString()

    else
      throw new Error(
        `S3DropBucket Config - Invalid or missing definition: DropBucketQuiesce - ${s3dbc.s3dropbucket_quiesce} `
      )

    //if (!isNaN(s3dbc.s3dropbucket_mainthours) && typeof s3dbc.s3dropbucket_mainthours === "number") process.env["S3DropBucketMaintHours"] = s3dbc.s3dropbucket_mainthours.toString()

    //else
    //{
    //  s3dbc.s3dropbucket_mainthours = -1
    //  process.env["S3DropBucketMaintHours"] = s3dbc.s3dropbucket_mainthours.toString()
    //}

    //if (!isNaN(s3dbc.s3dropbucket_maintlimit) && typeof s3dbc.s3dropbucket_maintlimit === "number") process.env["S3DropBucketMaintLimit"] = s3dbc.s3dropbucket_maintlimit.toString()

    //else
    //{
    //  s3dbc.s3dropbucket_maintlimit = 0
    //  process.env["S3DropBucketMaintLimit"] = s3dbc.s3dropbucket_maintlimit.toString()
    //}

    //if (!isNaN(s3dbc.s3dropbucket_maintconcurrency) && typeof s3dbc.s3dropbucket_maintconcurrency === "number") process.env["S3DropBucketMaintConcurrency"] = s3dbc.s3dropbucket_maintconcurrency.toString()

    //else
    //{
    //  s3dbc.s3dropbucket_maintlimit = 1
    //  process.env["S3DropBucketMaintConcurrency"] = s3dbc.s3dropbucket_maintconcurrency.toString()
    //}

    //if (!isNaN(s3dbc.s3dropbucket_workqueuemainthours) && typeof s3dbc.s3dropbucket_workqueuemaintlimit === "number") process.env["S3DropBucketWorkQueueMaintHours"] = s3dbc.s3dropbucket_workqueuemainthours.toString()

    //else
    //{
    //  s3dbc.s3dropbucket_workqueuemainthours = -1
    //  process.env["S3DropBucketWorkQueueMaintHours"] = s3dbc.s3dropbucket_workqueuemainthours.toString()
    //}

    //if (!isNaN(s3dbc.s3dropbucket_workqueuemaintlimit) && typeof s3dbc.s3dropbucket_workqueuemaintlimit === "number") process.env["S3DropBucketWorkQueueMaintLimit"] = s3dbc.s3dropbucket_workqueuemaintlimit.toString()

    //else
    //{
    //  s3dbc.s3dropbucket_workqueuemaintlimit = 0
    //  process.env["S3DropBucketWorkQueueMaintLimit"] = s3dbc.s3dropbucket_workqueuemaintlimit.toString()
    //}

    //if (!isNaN(s3dbc.s3dropbucket_workqueuemaintconcurrency) && typeof s3dbc.s3dropbucket_workqueuemaintconcurrency === "number") process.env["S3DropBucketWorkQueueMaintConcurrency"] = s3dbc.s3dropbucket_workqueuemaintconcurrency.toString()

    //else
    //{
    //  s3dbc.s3dropbucket_workqueuemaintconcurrency = 1
    //  process.env["S3DropBucketWorkQueueMaintConcurrency"] = s3dbc.s3dropbucket_workqueuemaintconcurrency.toString()
    //}

    if (s3dbc.s3dropbucket_log === true || s3dbc.s3dropbucket_log === false)
    {
      process.env["S3DropBucketLog"] = s3dbc.s3dropbucket_log.toString()
    }
    else
    {
      s3dbc.s3dropbucket_log = false
      process.env["S3DropBucketLog"] = s3dbc.s3dropbucket_log.toString()
    }

    if (!s3dbc.s3dropbucket_logbucket || s3dbc.s3dropbucket_logbucket === "") s3dbc.s3dropbucket_logbucket = ""
    process.env["S3DropBucketLogBucket"] = s3dbc.s3dropbucket_logbucket.toString()

    //if (!s3dbc.s3dropbucket_purge || s3dbc.s3dropbucket_purge === "")
    //{
    //  throw new Error(
    //    `S3DropBucket Config - Invalid or missing definition: DropBucketPurge - ${s3dbc.s3dropbucket_purge} `
    //  )
    //} else process.env["S3DropBucketPurge"] = s3dbc.s3dropbucket_purge

    //if (!isNaN(s3dbc.s3dropbucket_purgecount) && typeof s3dbc.s3dropbucket_purgecount === "number") process.env["S3DropBucketPurgeCount"] = s3dbc.s3dropbucket_purgecount.toFixed()

    //else
    //{
    //  throw new Error(
    //    `S3DropBucket Config - Invalid or missing definition: S3DropBucketPurgeCount - ${s3dbc.s3dropbucket_purgecount} `
    //  )
    //}

    if (s3dbc.s3dropbucket_queuebucketquiesce === true || s3dbc.s3dropbucket_queuebucketquiesce === false)
    {
      process.env["S3DropBucketQueueBucketQuiesce"] = s3dbc.s3dropbucket_queuebucketquiesce.toString()
    }
    else
      throw new Error(
        `S3DropBucket Config - Invalid or missing definition: QueueBucketQuiesce - ${s3dbc.s3dropbucket_queuebucketquiesce} `
      )

    //if (!s3dbc.s3dropbucket_workqueuebucketpurge || s3dbc.s3dropbucket_workqueuebucketpurge === "")
    //{
    //  throw new Error(
    //    `S3DropBucket Config - Invalid or missing definition: WorkQueueBucketPurge - ${s3dbc.s3dropbucket_workqueuebucketpurge} `
    //  )
    //} else process.env["S3DropBucketWorkQueueBucketPurge"] = s3dbc.s3dropbucket_workqueuebucketpurge

    //if (!isNaN(s3dbc.s3dropbucket_workqueuebucketpurgecount) && typeof s3dbc.s3dropbucket_workqueuebucketpurgecount === "number") process.env["S3DropBucketWorkQueueBucketPurgeCount"] = s3dbc.s3dropbucket_workqueuebucketpurgecount.toFixed()

    //else
    //{
    //  throw new Error(
    //    `S3DropBucket Config - Invalid or missing definition: WorkQueueBucketPurgeCount - ${s3dbc.s3dropbucket_workqueuebucketpurgecount} `
    //  )
    //}

    if (s3dbc.s3dropbucket_prefixfocus && s3dbc.s3dropbucket_prefixfocus !== "" && s3dbc.s3dropbucket_prefixfocus.length > 3)
    {
      process.env["S3DropBucketPrefixFocus"] = s3dbc.s3dropbucket_prefixfocus
      S3DB_Logging("warn", "937", `A Prefix Focus has been configured. Only S3DropBucket Objects with the prefix "${s3dbc.s3dropbucket_prefixfocus}" will be processed.`)
    } else process.env["S3DropBucketPrefixFocus"] = s3dbc.s3dropbucket_prefixfocus



    //deprecated in favor of using AWS interface to set these on the queue
    // if (tc.WorkQueueVisibilityTimeout !== undefined)
    //     process.env.WorkQueueVisibilityTimeout = tc.WorkQueueVisibilityTimeout.toFixed()
    // else
    //     throw new Error(
    //         `S3DropBucket Config - Invalid definition: WorkQueueVisibilityTimeout - ${ tc.WorkQueueVisibilityTimeout } `,
    //     )
    // if (tc.WorkQueueWaitTimeSeconds !== undefined)
    //     process.env.WorkQueueWaitTimeSeconds = tc.WorkQueueWaitTimeSeconds.toFixed()
    // else
    //     throw new Error(
    //         `S3DropBucket Config - Invalid definition: WorkQueueWaitTimeSeconds - ${ tc.WorkQueueWaitTimeSeconds } `,
    //     )
    // if (tc.RetryQueueVisibilityTimeout !== undefined)
    //     process.env.RetryQueueVisibilityTimeout = tc.WorkQueueWaitTimeSeconds.toFixed()
    // else
    //     throw new Error(
    //         `S3DropBucket Config - Invalid definition: RetryQueueVisibilityTimeout - ${ tc.RetryQueueVisibilityTimeout } `,
    //     )
    // if (tc.RetryQueueInitialWaitTimeSeconds !== undefined)
    //     process.env.RetryQueueInitialWaitTimeSeconds = tc.RetryQueueInitialWaitTimeSeconds.toFixed()
    // else
    //     throw new Error(
    //         `S3DropBucket Config - Invalid definition: RetryQueueInitialWaitTimeSeconds - ${ tc.RetryQueueInitialWaitTimeSeconds } `,
    //     )
  } catch (e)
  {

    debugger //catch

    S3DB_Logging("exception", "", `Exception - Parsing S3DropBucket Config File ${e} `)
    throw new Error(`Exception - Parsing S3DropBucket Config File ${e} `)
  }
  return s3dbc
}
