/* eslint-disable no-debugger */
"use strict"
import {type PutRecordCommandInput, PutRecordCommand, type PutRecordCommandOutput, type FirehoseClient} from '@aws-sdk/client-firehose'
import {s3dbConfig, fh_Client, S3DB_Logging} from './s3DropBucket'


interface FireHosePutResult {
  PutToFireHoseAggregatorResults: string
  PutToFireHoseAggregatorResultDetails: string
  PutToFireHoseException: string
}

let firehosePutResult: FireHosePutResult = {
  PutToFireHoseAggregatorResults: "",
  PutToFireHoseAggregatorResultDetails: "",
  PutToFireHoseException: "",
}


export async function putToFirehose (chunks: object[], key: string, cust: string, iter: number) {

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
      //let putFirehoseResp: object = {}
      let putFirehoseResp = {} as PutRecordCommandOutput

      let fhRetry = true
      while (fhRetry)
      {

        try
        {

          fh_Client.send(firehoseCommand)
            .then((res: PutRecordCommandOutput) => {

              S3DB_Logging("info", "922", `Inbound Update from ${key} - Put to Firehose Aggregator (File Stream Iter: ${iter}) Detailed Result: \n${fd.toString()} \n\nFirehose Result: ${JSON.stringify(res)} `)

              // Example responses from fh_Client.send(firehoseCommand):
              // Success response:
              // {
              //   RecordId: "abc123xyz789",
              //   $metadata: {
              //     httpStatusCode: 200,
              //     requestId: "req-123",
              //     attempts: 1,
              //     totalRetryDelay: 0
              //   }
              // }
              //
              // Error response:
              // {
              //   $metadata: {
              //     httpStatusCode: 500,
              //     requestId: "req-456",
              //     cfId: "xyz789",
              //     attempts: 1,
              //     totalRetryDelay: 0
              //   },
              //   message: "ServiceUnavailableException: Slow down"
              // }

              return res

            })
            .catch(async (e) => {

              debugger //catch

              const fr = await fh_Client.config.region()

              S3DB_Logging("exception", "", `Exception - Put to Firehose Aggregator (promise catch) (File Stream Iter: ${iter}) for ${key} \n( FH Data Length: ${fp.Record?.Data?.length}. FH Delivery Stream: ${JSON.stringify(fp.DeliveryStreamName)} FH Client Region: ${fr})  \n${e} `)

              firehosePutResult.PutToFireHoseAggregatorResults = "Exception"
              firehosePutResult.PutToFireHoseAggregatorResultDetails = `UnSuccessful Put to Firehose Aggregator for ${key} (File Stream Iter: ${iter}) \n ${JSON.stringify(e)} `
              firehosePutResult.PutToFireHoseException = `Exception - Put to Firehose Aggregator for ${key} (File Stream Iter: ${iter}) \n${e} `

              return firehosePutResult
            })

          //// Testing - sample firehose put 
          //x++
          //if (x % 20 === 0)
          //{
          //  debugger //testing
          //}

          let msg = ''

          if (putFirehoseResp.$metadata &&
            typeof putFirehoseResp === 'object' &&
            'message' in putFirehoseResp)
          {
            msg = putFirehoseResp.message as string
          }

          if (msg.indexOf("ServiceUnavailableException: Slow down") > -1)
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

          firehosePutResult = {
            PutToFireHoseAggregatorResults: 'Exception',
            PutToFireHoseAggregatorResultDetails: '',
            PutToFireHoseException: `Exception - PutToFirehose (catch) (File Stream Iter: ${iter}) \n${e} `

          }

          return firehosePutResult

        }

        //end while
      }


      const sc = putFirehoseResp.$metadata.httpStatusCode?.toString(2) || 'na'

      if (sc === '200') 
      {
        ut++

        firehosePutResult.PutToFireHoseAggregatorResults = sc
        firehosePutResult.PutToFireHoseAggregatorResultDetails = `Successful Put to Firehose Aggregator for ${key} (File Stream Iter: ${iter}).\n${JSON.stringify(putFirehoseResp)}. \n${putFirehoseResp.RecordId} `
        firehosePutResult.PutToFireHoseException = ""

      }
      else
      {
        firehosePutResult.PutToFireHoseAggregatorResults = sc
        firehosePutResult.PutToFireHoseAggregatorResultDetails = `UnSuccessful Put to Firehose Aggregator for ${key} (File Stream Iter: ${iter}). \n ${JSON.stringify(putFirehoseResp)} `
        firehosePutResult.PutToFireHoseException = ""
      }


      S3DB_Logging("info", "922", `Completed Put to Firehose Aggregator (from ${key} - File Stream Iter: ${iter}) Detailed Result: \n${fd.toString()} \n\nFirehose Result: ${JSON.stringify(firehosePutResult)} `)

      //end for()
    }

  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - Put to Firehose Aggregator (try-catch) for ${key} (File Stream Iter: ${iter}) \n${e} `)

    firehosePutResult = {
      PutToFireHoseAggregatorResults: 'Exception',
      PutToFireHoseAggregatorResultDetails: '',
      PutToFireHoseException: `Exception - Put to Firehose Aggregator(try-catch) for ${key}(File Stream Iter: ${iter}) \n${e} `
    }

    return firehosePutResult
  }


  if (tu === ut) S3DB_Logging("info", "942", `Firehose Aggregator PUT results for inbound Updates (File Stream Iter: ${iter}) from ${key}. Successfully sent ${ut} of ${tu} updates to Aggregator.`)
  else S3DB_Logging("info", "942", `Firehose Aggregator PUT results for inbound Updates (File Stream Iter: ${iter}) from ${key}.  Partially successful sending ${ut} of ${tu} updates to Aggregator.`)

  return firehosePutResult

  //end of function
}
