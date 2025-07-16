/* eslint-disable no-debugger */
"use strict"
import {type CustomerConfig, type S3DBConfig, type StoreAndQueueWorkResults, type AddWorkToBulkImportResults, S3DB_Logging, s3dbConfig, batchCount, recs} from './s3DropBucket'
import {putToFirehose} from './putToFirehose'
import {storeAndQueueCampaignWork} from './storeAndQueueCampaignWork'
import {storeAndQueueConnectWork} from './storeAndQueueConnectWork'
import {addWorkToBulkImport} from './addWorkToBulkImport'


let sqwResult: StoreAndQueueWorkResults = {
  AddWorkToS3WorkBucketResults: {
    versionId: '',
    S3ProcessBucketResultStatus: '',
    AddWorkToS3WorkBucketResult: ''
  },
  AddWorkToSQSWorkQueueResults: {
    SQSWriteResultStatus: '',
    AddWorkToSQSQueueResult: ''
  },
  AddWorkToBulkImportResults: {
    BulkImportWriteResultStatus: '',
    AddWorkToBulkImportResult: ''
  },
  StoreQueueWorkException: '',
  PutToFireHoseAggregatorResults: '',
  PutToFireHoseAggregatorResultDetails: '',
  PutToFireHoseException: ''
}


export async function packageUpdates (workSet: object[], key: string, custConfig: CustomerConfig, s3dbConfig: S3DBConfig, iter: number) {

  S3DB_Logging("info", "918", `Packaging ${workSet.length} updates from ${key} (File Stream Iter: ${iter}). \nBatch count so far ${batchCount}. `)

  //First, Check if these updates are to be Aggregated (or this is an Aggregated file coming through) 
  // If there are Chunks to Process and Singular Updates is set, send to Aggregator, unless these are 
  //  updates coming through FROM an Aggregated file.
  if (key.toLowerCase().indexOf("s3dropbucket_aggregator") < 0 && //This is Not an Aggregator file
    (custConfig.updates.toLowerCase() === "singular" || custConfig.updatetype.toLowerCase() === 'referenceset') && //Cust Config marks these updates to be Aggregated when coming through (Singular or ReferenceSet)
    workSet.length > 0) //There are Updates to be processed 
  {

    //let firehoseResults 
    let firehoseResults = {
      PutToFireHoseAggregatorResults: "",
      PutToFireHoseAggregatorResultDetails: "",
      PutToFireHoseException: "",
    }


    try //Interior try/catch for firehose processing
    {
      //A Check for when long processing flow does not provide periodic updates
      //fhi++      
      //if (fhi % 100 === 0)
      //{
      //  S3DB_Logging("info", "918", `Processing update ${fhi} of ${chunks.length} updates for ${custConfig.customer}`)
      // 
      //}
      firehoseResults = await putToFirehose(
        workSet,
        key,
        custConfig.customer,
        iter
      ).then((res) => {
        
        return res

      })
    } catch (e) //Interior try/catch for firehose processing
    {
      debugger //catch

      S3DB_Logging("exception", "", `Exception - PutToFirehose (File Stream Iter: ${iter}) - \n${e} `)

      sqwResult = {
        ...sqwResult,
        PutToFireHoseException: `Exception - PutToFirehose \n${e} `,
      }
      return sqwResult
    }

    S3DB_Logging("info", "943", `Completed FireHose Aggregator processing ${key} (File Stream Iter: ${iter}) \n${JSON.stringify(sqwResult)}`)

    sqwResult = {
      ...sqwResult,
      PutToFireHoseAggregatorResults: '',
      PutToFireHoseAggregatorResultDetails: `PutToFirehose Results: \n${JSON.stringify(firehoseResults)} `,
      PutToFireHoseException: ''
    }

    return sqwResult

  }
  else //Package to Connect or Campaign or Bulk Import
  {

    //Ok, the work to be Packaged is from either a "Multiple" updates Customer file or an "Aggregated" file. 

    
    //if update type is RefSet and this is an Aggregated file, write direct to bulk import
    if (key.toLowerCase().indexOf("s3dropbucket_aggregator") > 0 &&
      custConfig.updatetype.toLowerCase() === "referenceset")
    {
      const wbi = await addWorkToBulkImport(key, workSet, s3dbConfig, custConfig)
        .then((r) => {
        return r
      })

      //Schedule the Import Job 
      //ToDo: ....

      S3DB_Logging("info", "526", `Result from writing ${key} to BulkImport: \n${wbi}`)
      
      sqwResult.AddWorkToBulkImportResults = wbi

      return sqwResult

    }

    //Otherwise, 
    let updates: object[] = []

    try
    {
      //Process everything passed, especially if there are more than 100 passed at one time, or fewer than 100. 
      while (workSet.length > 0)
      {
        //updates = [] as object[]
        while (workSet.length > 0 && updates.length < 100)
        {
          const c = workSet.pop() ?? {}
          updates.push(c)
        }

        let sqw

        //Ok, now send to appropriate staging for actually updating endpoint (Campaign or Connect)
        if (custConfig.targetupdate.toLowerCase() === "connect")
          sqwResult = await storeAndQueueConnectWork(updates, key, custConfig, iter)
            .then((res) => {
              //console.info( `Debug Await StoreAndQueueWork (Connect) Result: ${ JSON.stringify( res ) }` )
              return res
            })
        else if (custConfig.targetupdate.toLowerCase() === "campaign")
        {
          sqwResult = await storeAndQueueCampaignWork(updates, key, custConfig, s3dbConfig, iter)
            .then((res) => {
              //console.info( `Debug Await StoreAndQueueWork (Campaign) Result: ${ JSON.stringify( res ) }` )
              return res
            })
        }  

        else
        {
          throw new Error(`Target for Update does not match any Target: ${custConfig.targetupdate}`)
        }

        S3DB_Logging("info", "921", `PackageUpdates StoreAndQueueWork for ${key} (File Stream Iter: ${iter}). \nFor a total of ${recs} Updates in ${batchCount} Batches.  Result: \n${JSON.stringify(sqwResult)} `)
        S3DB_Logging("info", "941", `PackageUpdates StoreAndQueueWork - Updates (${recs}) from ${key} \n(File Stream Iter: ${iter}) for ${batchCount} Batches.  Updates: \n${JSON.stringify(updates)} `)

      }
    } catch (e)
    {
      debugger //catch

      S3DB_Logging("exception", "", `Exception - packageUpdates for ${key} (File Stream Iter: ${iter}) \n${e} `)

      sqwResult = {
        ...sqwResult,
        StoreQueueWorkException: `Exception - PackageUpdates StoreAndQueueWork for ${key} (File Stream Iter: ${iter}) \nBatch ${batchCount} of ${recs} Updates. \n${e} `,
      }
    }
  }

  return sqwResult
}

