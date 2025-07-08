/* eslint-disable no-debugger */
"use strict"
import {type CustomerConfig, s3dbConfig, S3DB_Logging} from './s3DropBucket'
import {getAccessToken} from './getAccessToken'




export async function postToCampaign (
  xmlCalls: string,
  config: CustomerConfig,
  count: string,
  workFile: string
) {
  const c = config.customer

  //Store AccessToken in process.env vars for reference across invocations, save requesting it repeatedly
  if (process.env[`${c}_accessToken`] === undefined ||
    process.env[`${c}_accessToken`] === null ||
    process.env[`${c}_accessToken`] === "")
  {
    process.env[`${c}_accessToken`] = (await getAccessToken(config)) as string
    const at = process.env[`${c}_accessToken"`] ?? ""
    const l = at.length
    const redactAT = "......." + at.substring(l - 10, l)
    S3DB_Logging("info", "900", `Generated a new AccessToken: ${redactAT}`) //ToDo: Add Debug Number 
  }
  else
  {
    const at = process.env["accessToken"] ?? ""
    const l = at.length
    const redactAT = "......." + at.substring(l - 8, l)
    S3DB_Logging("info", "900", `Access Token already stored: ${redactAT}`) //ToDo: Add Debug Number 
  }

  const myHeaders = new Headers()
  myHeaders.append("Content-Type", "text/xml")
  myHeaders.append("Authorization", "Bearer " + process.env[`${c}_accessToken`])
  myHeaders.append("Content-Type", "text/xml")
  myHeaders.append("Connection", "keep-alive")
  myHeaders.append("Accept", "*/*")
  myHeaders.append("Accept-Encoding", "gzip, deflate, br")

  const requestOptions: RequestInit = {
    method: "POST",
    headers: myHeaders,
    body: xmlCalls,
    redirect: "follow",
  }

  const host = `https://api-campaign-${config.region}-${config.pod}.goacoustic.com/XMLAPI`

  S3DB_Logging("info", "905", `Updates to be POSTed (${workFile}) are: ${xmlCalls}`)

  let postRes: string = ""

  // try
  // {
  postRes = await fetch(host, requestOptions)
    .then((response) => response.text())
    .then(async (result) => {
      S3DB_Logging("info", "908", `Campaign Raw POST Response (${workFile}) : ${result}`)

      const faults: string[] = []

      //const f = result.split( /<FaultString><!\[CDATA\[(.*)\]\]/g )
      if (result.toLowerCase().indexOf("max number of concurrent") > -1 ||
        result.toLowerCase().indexOf("access token has expired") > -1)
      {
        S3DB_Logging("warn", "929", `Temporary Failure - POST Updates - Marked for Retry. \n${result}`)

        return `retry ${JSON.stringify(result)}`

      } else if (result.indexOf("<FaultString><![CDATA[") > -1)
      {

        //Add this fail
        //<RESULT>
        //    <SUCCESS>false</SUCCESS>
        //    < /RESULT>
        //    < Fault >
        //    <Request/>
        //    < FaultCode />
        //    <FaultString> <![ CDATA[ Local part of Email Address is Blocked.]]> </FaultString>
        //        < detail >
        //        <error>
        //        <errorid> 121 < /errorid>
        //        < module />
        //        <class> SP.Recipients < /class>
        //        < method />
        //        </error>
        //        < /detail>
        //        < /Fault>
        const f = result.split(/<FaultString><!\[CDATA\[(.*)\]\]/g)
        if (f && f?.length > 0)
        {
          for (const fl in f)
          {
            faults.push(f[fl])
          }
        }


        S3DB_Logging("warn", "928", `Partially Successful POST of the Updates (${f.length} FaultStrings on ${count} updates from ${workFile}) - \nResults\n ${JSON.stringify(faults)}`)

        return `Partially Successful - (${f.length} FaultStrings on ${count} updates) \n${JSON.stringify(faults)}`
      }

      //<FAILURE failure_type="transient" description = "Error saving row" >
      else if (result.indexOf("<FAILURE failure_type") > -1)
      {
        let msg = ""

        //Add this Fail
        //<SUCCESS> true < /SUCCESS>
        //    < FAILURES >
        //    <FAILURE failure_type="permanent" description = "There is no column registeredAdvisorTitle" >
        const m = result.match(/<FAILURE (.*)>$/g)

        if (m && m?.length > 0)
        {
          for (const l in m)
          {
            // "<FAILURE failure_type=\"permanent\" description=\"There is no column name\">"
            //Actual message is ambiguous, changing it to read less confusingly:
            l.replace("There is no column name", "There is no column named")
            msg += l
          }

          S3DB_Logging("error", "927", `Unsuccessful POST of the Updates (${m.length} of ${count}) - \nFailure Msg: ${JSON.stringify(msg)}`)

          return `Error - Unsuccessful POST of the Updates (${m.length} of ${count}) - \nFailure Msg: ${JSON.stringify(msg)}`
        }
      }


      //If we haven't returned before now then it's a successful post 
      const spr = JSON.stringify(result).replace("\n", " ")
      S3DB_Logging("info", "926", `Successful POST Result: ${spr}`)
      return `Successfully POSTed (${count}) Updates - Result: ${spr}`
    })
    .catch((e) => {
      debugger //catch

      if (typeof e === "string" && e.toLowerCase().indexOf("econnreset") > -1)
      {
        S3DB_Logging("exception", "929", `Error - Temporary failure to POST the Updates - Marked for Retry. ${e}`)

        return "retry"
      }
      else
      {
        S3DB_Logging("exception", "927", `Error - Unsuccessful POST of the Updates: ${JSON.stringify(e)}`)
        //throw new Error( `Exception - Unsuccessful POST of the Updates \n${ e }` )
        return "Unsuccessful POST of the Updates"
      }
    })

  //retry
  //unsuccessful post
  //partially successful
  //successfully posted
  return postRes
}
