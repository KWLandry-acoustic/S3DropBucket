/* eslint-disable no-debugger */
"use strict"
import {type CustomerConfig, s3dbConfig, S3DB_Logging} from './s3DropBucket'


export async function getAccessToken (config: CustomerConfig) {
  try
  {
    interface RatResp {
      access_token: string
      status: number
      error: string
      error_description: string
    }

    const rat = await fetch(
      `https://api-campaign-${config.region}-${config.pod}.goacoustic.com/oauth/token`,
      {
        method: "POST",
        body: new URLSearchParams({
          refresh_token: config.refreshtoken,
          client_id: config.clientid,
          client_secret: config.clientsecret,
          grant_type: "refresh_token",
        }),
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          "User-Agent": "S3DropBucket GetAccessToken",
        },
      }
    ).then(async (r) => {

      if (r.status != 200)
      {
        S3DB_Logging("error", "900", `Problem retrieving Access Token (${r.status} - ${r.statusText}) Error: ${JSON.stringify(r)}`)
        throw new Error(
          `Problem retrieving Access Token(${r.status} - ${r.statusText}) Error: ${JSON.stringify(r)}`
        )
      }

      return await r.json() as RatResp
    })

    const accessToken = rat.access_token
    return {accessToken}.accessToken
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "900", `Exception - On GetAccessToken: \n ${e}`)

    throw new Error(`Exception - On GetAccessToken: \n ${e}`)
  }
}
