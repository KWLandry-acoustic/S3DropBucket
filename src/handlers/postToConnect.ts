/* eslint-disable no-debugger */
"use strict"
import {type CustomerConfig, s3dbConfig, S3DB_Logging} from './s3DropBucket'

export async function postToConnect (mutations: string, custconfig: CustomerConfig, count: string, workFile: string) {
  //ToDo: 
  //Transform Add Contacts to Mutation Create Contact
  //Transform Updates to Mutation Update Contact
  //    Need Attributes - Add Attributes to update as seen in inbound data
  //get access token
  //post to connect
  //return result
  //[
  //  {"key": "subscriptionId", "value": "bb6758fc16bbfffbe4248214486c06cc3a924edf","description": "", "enabled": true},
  //  {"key": "x-api-key", "value": "b1fa7ef5024e4da3b0a40aed8331761c", "description": "", "enabled": true},
  //  {"key": "x-acoustic-region", "value": "us-east-1", "description": "", "enabled": true}
  //]
  const myHeaders = new Headers()
  //myHeaders.append("Content-Type", "text/xml")
  //myHeaders.append("Authorization", "Bearer " + process.env[`${c}_accessToken`])
  myHeaders.append("subscriptionId", custconfig.subscriptionid)
  myHeaders.append("x-api-key", custconfig.x_api_key)
  myHeaders.append("x-acoustic-region", custconfig.x_acoustic_region)
  myHeaders.append("Content-Type", "application/json")
  myHeaders.append("Connection", "keep-alive")
  myHeaders.append("Accept", "*/*")
  myHeaders.append("Accept-Encoding", "gzip, deflate, br")

  const requestOptions: RequestInit = {
    method: "POST",
    headers: myHeaders,
    body: mutations,
    redirect: "follow"
  }


  const host = s3dbConfig.connectapiurl

  S3DB_Logging("info", "805", `Connect Mutation Updates about to be POSTed (${workFile}) are: ${mutations}`)

  let connectMutationResult: string = ""


  //{                 //graphQL Spec Doc
  //  "data": { ...},
  //  "errors": [... ],
  //    "extensions": { ...}
  //}
  interface ConnectSuccessResult {
    "data": {
      "createContacts": {
        "items": [
          {
            "contactId": string
          }
        ]
      }
    }
  }

  interface ConnectErrorResult {
    "data": null
    "errors": [
      {
        "message": string
        "locations": [{"line": number; "column": number}]
        "path": [string]
        "extensions": {"code": string}
      }
    ]
  }

  try
  {

    //{         
    // //graphQL Spec Doc
    //  "query": "...",
    //    "operationName": "...",
    //      "variables": {"myVariable": "someValue", ...},
    //  "extensions": {"myExtension": "someValue", ...}
    //}
    connectMutationResult = await fetch(host, requestOptions)
      //.then((response) => response.text())
      .then(async (response) => {
        return await response.json() // .text())


        //const rj = await response.json()
        //return rj
      })
      .then(async (result) => {
        S3DB_Logging("info", "808", `POST to Connect - Raw Response: \n${JSON.stringify(result)}`)

        //ToDo: Create specific Messaging to line out this error as the Target DB does not have the attribute Defined
        //{"errors": [{"message": "No defined Attribute with name: 'email'", "locations": [{"line": 1, "column": 38}], "path": ["updateContacts"], "extensions": {"code": "ATTRIBUTE_NOT_DEFINED"}}], "data": null}
        //const r3 = {          // as ConnectSuccessResult
        //      "data" : {
        //        "createContacts" : {
        //          "items" : [
        //            {
        //              "contactId" : "529dbe72-11c6-515a-a6f4-0069988668f5"
        //            },
        //            {
        //              "contactId" : "5510fe0f-27b1-56ea-90a9-67a3bad9f364"
        //            }
        //          ]
        //        }
        //      }
        //    }
        //{                   //  as  ConnectErrorResult
        //  "errors": [
        //    {
        //      "message": "Field Mobile Number has invalid value: 211-411-5555. Category: PHONE_FORMAT. Message: Invalid phone number, only 0-9 allowed as phone number",
        //      "locations": [
        //        {
        //          "line": 1,
        //          "column": 47
        //        }
        //      ],
        //      "path": [
        //        "createContacts"
        //      ],
        //      "extensions": {
        //        "code": "INVALID_ATTRIBUTE_VALUE",
        //        "category": "PHONE_FORMAT"
        //      }
        //    }
        //  ],
        //    "data": null
        //}
        //POST Result: {"message": "Endpoint request timed out"}
        if (JSON.stringify(result).indexOf("Endpoint request timed out") > 0)
        {
          S3DB_Logging("warn", "809", `Connect Mutation POST - Temporary Error: Request Timed Out (${JSON.stringify(result)}). Work will be Sent back to Retry Queue. `)
          return `retry ${JSON.stringify(result)}`
        }


        let errors = []
        const cer = result as ConnectErrorResult

        if (typeof cer.errors !== "undefined" && cer.errors.length > 0)
        {
          for (const e in cer.errors)
          {
            S3DB_Logging("error", "827", `Connect Mutation POST - Error: ${cer.errors[e].message}`)
            //throw new Error(`Connect Mutation POST - Error: ${cr.errors[e].message}`)
            errors.push(cer.errors[e].message)
          }

          return `Unsuccessful POST of the Updates \n ${JSON.stringify(errors)}`
        }


        //S3DB_Logging("warn", "829", `Temporary Failure - POST Updates - Marked for Retry. \n${result}`)
        //S3DB_Logging("error", "827", `Unsuccessful POST of the Updates (${m.length} of ${count}) - \nFailure Msg: ${JSON.stringify(msg)}`)
        const csr = result as ConnectSuccessResult

        //If we haven't returned before now then it's a successful post 
        const spr = JSON.stringify(csr).replace("\n", " ")

        S3DB_Logging("info", "826", `Successful POST Result (${workFile}): ${spr}`)

        //return `Successfully POSTed (${count}) Updates - Result: ${result}`
        return `Successfully POSTed (${count}) Updates - Result: ${spr}`
      })
      .catch((e) => {

        debugger //catch

        const h = host
        const r = requestOptions
        const m = mutations

        if (e.message.indexOf("econnreset") > -1)
        {
          S3DB_Logging("exception", "829", `PostToConnect Error (then.catch) - Temporary failure to POST the Updates - Marked for Retry. ${JSON.stringify(e)}`)
          return `retry ${JSON.stringify(e)}`
        }
        else
        {
          S3DB_Logging("exception", "", `Error - PostToConnect - Unsuccessful POST of the Updates: ${JSON.stringify(e)}`)
          return `Unsuccessful POST of the Updates \n${JSON.stringify(e)}`
        }
      })
  } catch (e)
  {
    debugger //catch

    const h = host
    const r = requestOptions
    const m = mutations

    S3DB_Logging("exception", "", `Error - PostToConnect - Hard Error (try-catch): ${JSON.stringify(e)}`)
    return `unsuccessful post \n ${JSON.stringify(e)}`
  }

  //retry
  //unsuccessful post
  //partially successful
  //successfully posted
  return connectMutationResult
}
