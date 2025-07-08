/* eslint-disable no-debugger */
"use strict"
import {v4 as uuidv4} from 'uuid'
import {convertJSONToXML_DBUpdates} from './convertJSONToXML_DBUpdates'
import {convertJSONToXML_RTUpdates} from './convertJSONToXML_RTUpdates'
import {type CustomerConfig, type S3DBConfig, batchCount, s3dbConfig, S3DB_Logging, customersConfig} from './s3DropBucket'
import {addWorkToS3WorkBucket} from './addWorkToS3WorkBucket'
import {transforms} from './transforms'
import {addWorkToSQSWorkQueue} from './addWorkToSQSWorkQueue'

let xmlRows = ''

export async function storeAndQueueCampaignWork (
  updates: object[],
  s3Key: string,
  config: CustomerConfig,
  s3dbConfig: S3DBConfig,
  iter: number
) {

  if (batchCount > s3dbConfig.s3dropbucket_maxbatcheswarning)
    S3DB_Logging("info", "", `Warning: Updates from the S3 Object(${s3Key}) (File Stream Iter: ${iter}) are exceeding(${batchCount}) the Warning Limit of ${s3dbConfig.s3dropbucket_maxbatcheswarning} Batches per Object.`)

  // throw new Error(`Updates from the S3 Object(${ s3Key }) Exceed(${ batch }) Safety Limit of 20 Batches of 99 Updates each.Exiting...`)
  const updateCount = updates.length

  //Customers marked as "Singular" updates files are not transformed, but sent to Firehose prior to getting here.
  // therefore if this is an Aggregate file, or is a file config'd to be "Multiple" updates, then need to perform Transforms now
  try
  {
    //Apply Transforms, if any, 
    updates = transforms(updates, config)
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - Transforms - ${e}`)
    throw new Error(`Exception - Transforms - ${e}`)
  }

  if (customersConfig.updatetype.toLowerCase() === "dbkeyed" ||
    customersConfig.updatetype.toLowerCase() === "dbnonkeyed")
  {
    xmlRows = convertJSONToXML_DBUpdates(updates, config)
  }

  if (customersConfig.updatetype.toLowerCase() === "relational")
  {
    xmlRows = convertJSONToXML_RTUpdates(updates, config)
  }

  //ToDo: refactor this above this function
  if (s3Key.indexOf("TestData") > -1)
  {
    //strip /testdata folder from key
    s3Key = s3Key.split("/").at(-1) ?? s3Key
  }

  let key = s3Key

  while (key.indexOf("/") > -1)
  {
    key = key.split("/").at(-1) ?? key
  }

  key = key.replace(".", "_")

  key = `${key}-update-${batchCount}-${updateCount}-${uuidv4()}.xml`

  //if ( Object.values( updates ).length !== recs )
  //{
  //     selectiveLogging("error", "", `Recs Count ${recs} does not reflect Updates Count ${Object.values(updates).length} `)
  //}
  let addWorkToS3WorkBucketResult
  let addWorkToSQSWorkQueueResult

  try
  {
    addWorkToS3WorkBucketResult = await addWorkToS3WorkBucket(xmlRows, key)
      .then((res) => {
        return {"workfile": key, ...res} //return res 
      })
      .catch((err) => {
        debugger //catch

        S3DB_Logging("exception", "", `Exception - AddWorkToS3WorkBucket ${err} (File Stream Iter: ${iter} file: ${key})`)
      })
  } catch (e)
  {
    debugger //catch

    const s3StoreError = `Exception - StoreAndQueueWork Add work (File Stream Iter: ${iter} (file: ${key})) to S3 Bucket exception \n${e} `

    S3DB_Logging("exception", "", s3StoreError)

    return {
      StoreS3WorkException: s3StoreError,
      StoreQueueWorkException: "",
      AddWorkToS3WorkBucketResults: JSON.stringify(addWorkToS3WorkBucketResult),
    }
  }

  //S3DB_Logging(oppty to message s3 store results)
  const marker = "Initially Queued on " + new Date()

  try
  {
    addWorkToSQSWorkQueueResult = await addWorkToSQSWorkQueue(
      config,
      key,
      batchCount,
      updates.length.toString(),
      marker
    ).then((res) => {
      //S3DB_Logging(oppty for rawresult logging)

      return res
    })
  } catch (e)
  {
    debugger //catch

    const sqwError = `Exception - StoreAndQueueWork Add work to SQS Queue exception \n${e} `
    S3DB_Logging("exception", "", sqwError)

    return {StoreQueueWorkException: sqwError}
  }


  //If we made it this Far, all's good 
  S3DB_Logging("info", "915", `Results of Storing and Queuing (Campaign) Work ${key} to Work Queue: ${JSON.stringify(addWorkToSQSWorkQueueResult)} \n${JSON.stringify(
    addWorkToS3WorkBucketResult)}`)

  return {
    AddWorkToS3WorkBucketResults: addWorkToS3WorkBucketResult,
    AddWorkToSQSWorkQueueResults: addWorkToSQSWorkQueueResult,
    xmlRows
  }
}
