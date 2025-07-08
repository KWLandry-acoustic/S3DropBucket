/* eslint-disable no-debugger */
"use strict"
import {type PutRecordCommandInput, PutRecordCommand, type PutRecordCommandOutput} from '@aws-sdk/client-firehose'
import {s3dbConfig, fh_Client, S3DB_Logging} from './s3DropBucket'


export async function putToFirehose (chunks: object[], key: string, cust: string, iter: number) {

  let putFirehoseResp: object = {}

  const tu = chunks.length
  let ut = 0

  let x = 0


  try
  {

    for (const j in chunks)
    {

      let jo = chunks[j]

      jo = Object.assign(jo, {Customer: cust})
      const fd = Buffer.from(JSON.stringify(jo), "utf-8")

      const fp = {
        DeliveryStreamName: s3dbConfig.s3dropbucket_firehosestream,
        Record: {
          Data: fd,
        },
      } as PutRecordCommandInput

      const firehoseCommand = new PutRecordCommand(fp)

      interface FireHosePutResult {
        PutToFireHoseAggregatorResult: string
        PutToFireHoseAggregatorResultDetails: string
        PutToFireHoseException: string
      }

      let firehosePutResult: FireHosePutResult = {
        PutToFireHoseAggregatorResult: "",
        PutToFireHoseAggregatorResultDetails: "",
        PutToFireHoseException: "",
      }

      let fhRetry = true
      while (fhRetry)
      {
        try
        {
          putFirehoseResp = await fh_Client.send(firehoseCommand)
            .then((res: PutRecordCommandOutput) => {

              S3DB_Logging("info", "922", `Inbound Update from ${key} - Put to Firehose Aggregator (File Stream Iter: ${iter}) Detailed Result: \n${fd.toString()} \n\nFirehose Result: ${JSON.stringify(res)} `)

              if (res.$metadata.httpStatusCode === 200)
              {
                ut++

                firehosePutResult.PutToFireHoseAggregatorResult = `${res.$metadata.httpStatusCode}`
                firehosePutResult.PutToFireHoseAggregatorResultDetails = `Successful Put to Firehose Aggregator for ${key} (File Stream Iter: ${iter}).\n${JSON.stringify(res)}. \n${res.RecordId} `
                firehosePutResult.PutToFireHoseException = ""

              }
              else
              {
                firehosePutResult.PutToFireHoseAggregatorResult = `${res.$metadata.httpStatusCode}`
                firehosePutResult.PutToFireHoseAggregatorResultDetails = `UnSuccessful Put to Firehose Aggregator for ${key} (File Stream Iter: ${iter}). \n ${JSON.stringify(res)} `
                firehosePutResult.PutToFireHoseException = ""
              }

              return firehosePutResult

            })
            .catch(async (e) => {

              debugger //catch

              const fr = await fh_Client.config.region()

              S3DB_Logging("exception", "", `Exception - Put to Firehose Aggregator (promise catch) (File Stream Iter: ${iter}) for ${key} \n( FH Data Length: ${fp.Record?.Data?.length}. FH Delivery Stream: ${JSON.stringify(fp.DeliveryStreamName)} FH Client Region: ${fr})  \n${e} `)

              firehosePutResult.PutToFireHoseAggregatorResult = "Exception"
              firehosePutResult.PutToFireHoseAggregatorResultDetails = `UnSuccessful Put to Firehose Aggregator for ${key} (File Stream Iter: ${iter}) \n ${JSON.stringify(e)} `
              firehosePutResult.PutToFireHoseException = `Exception - Put to Firehose Aggregator for ${key} (File Stream Iter: ${iter}) \n${e} `

              return firehosePutResult
            })

          // Testing - sample firehose put 
          x++
          if (x % 20 === 0)
          {
            debugger //testing
          }

          if (firehosePutResult.PutToFireHoseAggregatorResultDetails.indexOf("ServiceUnavailableException: Slow down") > -1)
          {
            fhRetry = true
            setTimeout(() => {
              //Don't like this approach but it appears to be the best way to get a promise safe retry
              S3DB_Logging("warn", "944", `Retrying Put to Firehose Aggregator (Slow Down requested) for ${key} (File Stream Iter: ${iter}) `)
            }, 100)
          }
          else fhRetry = false

        } catch (e)
        {
          debugger //catch

          S3DB_Logging("exception", "", `Exception - PutToFirehose (catch) (File Stream Iter: ${iter}) \n${e} `)
        }

      }

      S3DB_Logging("info", "922", `Completed Put to Firehose Aggregator (from ${key} - File Stream Iter: ${iter}) Detailed Result: \n${fd.toString()} \n\nFirehose Result: ${JSON.stringify(firehosePutResult)} `)

    }

    if (tu === ut) S3DB_Logging("info", "942", `Firehose Aggregator PUT results for inbound Updates (File Stream Iter: ${iter}) from ${key}. Successfully sent ${ut} of ${tu} updates to Aggregator.`)
    else S3DB_Logging("info", "942", `Firehose Aggregator PUT results for inbound Updates (File Stream Iter: ${iter}) from ${key}.  Partially successful sending ${ut} of ${tu} updates to Aggregator.`)

    return putFirehoseResp
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - Put to Firehose Aggregator (try-catch) for ${key} (File Stream Iter: ${iter}) \n${e} `)
  }

  //end of function
}
