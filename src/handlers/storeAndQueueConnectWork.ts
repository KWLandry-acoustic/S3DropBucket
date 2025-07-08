/* eslint-disable no-debugger */
"use strict"
import {v4 as uuidv4} from 'uuid'
import {buildMutationsConnect} from './buildMutationsConnect'
import {type CustomerConfig, batchCount, s3dbConfig, S3DB_Logging, customersConfig} from './s3DropBucket'
import {addWorkToS3WorkBucket} from './addWorkToS3WorkBucket'
import {transforms} from './transforms'
import {addWorkToSQSWorkQueue} from './addWorkToSQSWorkQueue'


export async function storeAndQueueConnectWork (
  updates: object[],
  s3Key: string,
  custConfig: CustomerConfig,
  iter: number
) {

  if (batchCount > s3dbConfig.s3dropbucket_maxbatcheswarning && batchCount % 100 === 0)
    S3DB_Logging("info", "", `Warning: Updates from the S3 Object(${s3Key}) (File Stream Iter: ${iter}) are exceeding (${batchCount}) the Warning Limit of ${s3dbConfig.s3dropbucket_maxbatcheswarning} Batches per Object.`)

  const updateCount = updates.length

  //Customers marked as "Singular" updates files are not transformed, but sent to Firehose prior to getting here.
  //  therefore if Aggregate file, or files config'd as "Multiple" updates, then need to perform Transforms before queuing up the work
  try
  {
    //Apply Transforms, if any, 
    updates = transforms(updates, custConfig)
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - Transforms - ${e}`)
    throw new Error(`Exception - Transforms - ${e}`)
  }



  S3DB_Logging("info", "800", `After Transform (Updates: ${updateCount}. File Stream Iter: ${iter}): \n${JSON.stringify(updates)}`)

  let mutations
  ////DBKeyed, DBNonKeyed, Relational, ReferenceSet, CreateUpdateContacts, CreateAttributes
  //if (customersConfig.updatetype.toLowerCase() === "createupdatecontacts") res = ConnectCreateMultipleContacts()
  //if (customersConfig.updatetype.toLowerCase() === "createattributes") res = ConnectCreateAttributes()
  ////if (true) res = ConnectReferenceSet().then((m) => {return m})
  //const mutationCall = JSON.stringify(res)
  //const m = buildConnectMutation(JSON.parse(updates))
  // ReferenceSet   -    Need to establish SFTP and Job Creation for this
  // CreateContacts   - Done - CreateUpdateContacts call as Create will also Update
  // UpdateContacts    - Done
  // Audience - Done - Transform
  // Consent - Done - Transform
  // ContactKey - Done - Transform 
  // ContactId - Done - Transform
  // AddressableFields - Done - Transform
  //For now will need to treat Reference Sets completely differently until an API shows up, 
  // hopefully similar to Contacts API
  if (customersConfig.updatetype.toLowerCase() === "createupdatecontacts" ||
    customersConfig.updatetype.toLowerCase() === "referenceset")
  {
    mutations = await buildMutationsConnect(updates, custConfig)
      .then((r) => {
        return r
      })
  }

  const mutationWithUpdates = JSON.stringify(mutations)

  ////  Testing - Call POST to Connect immediately
  //if (localTesting)
  //{
  //  S3DB_Logging("info", "855", `Testing - GraphQL Call (${s3dbConfig.connectapiurl}) Updates: \n${mutationUpdates}`)
  //  const c = await postToConnect(mutationUpdates, customersConfig, "6", s3Key)
  //  debugger ///
  //}
  //Derive Key Name for Update File
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

  key = key.replace(".", "-")

  key = `${key}-update-${batchCount}-${updateCount}-${uuidv4()}.json`

  //if ( Object.values( updates ).length !== recs )
  //{
  //     selectiveLogging("error", "", `Recs Count ${recs} does not reflect Updates Count ${Object.values(updates).length} `)
  //}
  S3DB_Logging("info", "811", `Queuing Work File ${key} for ${s3Key}. Batch ${batchCount} of ${updateCount} records)`)

  let addWorkToS3WorkBucketResult
  let addWorkToSQSWorkQueueResult

  try
  {
    addWorkToS3WorkBucketResult = await addWorkToS3WorkBucket(mutationWithUpdates, key)
      .then((res) => {
        return {"workfile": key, ...res} //{"AddWorktoS3Results": res}
      })
      .catch((err) => {
        debugger //catch

        S3DB_Logging("exception", "", `Exception - AddWorkToS3WorkBucket (file: ${key}) ${err}`)
      })
  } catch (e)
  {
    debugger //catch

    const s3StoreError = `Exception - StoreAndQueueWork Add work (file: ${key}) to S3 Work Bucket exception \n${e} `
    S3DB_Logging("exception", "", s3StoreError)

    return {
      StoreS3WorkException: s3StoreError,
      StoreQueueWorkException: "",
      AddWorkToS3WorkBucketResults: JSON.stringify(addWorkToS3WorkBucketResult),
    }
  }

  const marker = "Initially Queued on " + new Date()

  try
  {
    addWorkToSQSWorkQueueResult = await addWorkToSQSWorkQueue(
      custConfig,
      key,
      //v,
      batchCount,
      updates.length.toString(),
      marker
    ).then((res) => {
      //S3DB_Logging(rawresult logging)

      return res
    })
  } catch (e)
  {
    debugger //catch

    const sqwError = `Exception - StoreAndQueueWork Add work to SQS Queue exception \n${e} `
    S3DB_Logging("exception", "", sqwError)

    return {StoreQueueWorkException: sqwError}
  }

  S3DB_Logging("info", "915", `Results of Storing and Queuing (Connect) Work ${key} to Work Queue: ${JSON.stringify(addWorkToSQSWorkQueueResult)} 
  \n${JSON.stringify(addWorkToS3WorkBucketResult)}`)

  return {
    AddWorkToS3WorkBucketResults: addWorkToS3WorkBucketResult,
    AddWorkToSQSWorkQueueResults: addWorkToSQSWorkQueueResult,
  }
}
