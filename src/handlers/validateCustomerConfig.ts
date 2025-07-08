/* eslint-disable no-debugger */
"use strict"
import jsonpath from 'jsonpath'
import {type CustomerConfig, s3dbConfig, S3DB_Logging} from './s3DropBucket'



export async function validateCustomerConfig (config: CustomerConfig) {

  try
  {
    if (!config || config === null)
    {
      throw new Error("Invalid  CustomerConfig - empty or null config")
    }

    const cust = config.customer


    if (!config.targetupdate)
    {
      {
        throw new Error(
          `Invalid Customer Config (${cust}) - TargetUpdate is required and must be either 'Connect' or 'Campaign'  `
        )
      }
    }

    if (!config.targetupdate.toLowerCase().match(/^(?:connect|campaign)$/gim))
    {
      throw new Error(
        `Invalid Customer Config (${cust})  - TargetUpdate is required and must be either 'Connect' or 'Campaign'  `
      )
    }


    if (!config.updatetype)
    {
      throw new Error(`Invalid Customer Config (${cust}) - updatetype is not defined`)
    }


    ////Confirm updatetype has a valid value
    //  if ( !config.updatetype.toLowerCase().match(/^(?:relational|dbkeyed|dbnonkeyed|referenceset|createupdatecontacts|createattributes)$/gim)
    //    //DBKeyed, DBNonKeyed, Relational, ReferenceSet, CreateUpdateContacts, CreateAttributes
    //  )
    //  {
    //    throw new Error(
    //      "Invalid Customer Config - updatetype is required and must be either 'Relational', 'DBKeyed' or 'DBNonKeyed', ReferenceSet, CreateUpdateContacts, CreateAttributes. "
    //    )
    //  }
    if (config.targetupdate.toLowerCase() == "campaign")
    {
      //updatetype has valid Campaign values and Campaign dependent values
      if (!config.updatetype.toLowerCase().match(/^(?:relational|dbkeyed|dbnonkeyed)$/gim))
      //DBKeyed, DBNonKeyed, Relational
      {
        throw new Error(
          `Invalid Customer Config (${cust}) - Update set to be Campaign, however updatetype is not Relational, DBKeyed, or DBNonKeyed. `
        )
      }

      if (config.updatetype.toLowerCase() == "dbkeyed" && !config.dbkey)
      {
        throw new Error(
          `Invalid Customer Config (${cust}) - Update set as Database Keyed but DBKey is not defined. `
        )
      }

      if ( //typeof config.updatetype !== "undefined" &&
        config.updatetype.toLowerCase() == "dbnonkeyed" &&
        (typeof config.lookupkeys === "undefined" || config.lookupkeys.length <= 0))
      {
        throw new Error(
          `Invalid Customer Config (${cust}) - Update set as Database NonKeyed but LookupKeys is not defined. `
        )
      }

      if (!config.clientid)
      {
        throw new Error("Invalid Customer Config - Target Update is Campaign but ClientId is not defined")
      }
      if (!config.clientsecret)
      {
        throw new Error(`Invalid Customer Config (${cust}) - Target Update is Campaign but ClientSecret is not defined`)
      }
      if (!config.refreshtoken)
      {
        throw new Error(`Invalid Customer Config  (${cust}) - Target Update is Campaign but RefreshToken is not defined`)
      }


      if (!config.listid)
      {
        throw new Error(`Invalid Customer Config (${cust}) - ListId is not defined`)
      }
      if (!config.listname)
      {
        throw new Error(`Invalid Customer Config (${cust}) - Target Update is Campaign but ListName is not defined`)
      }
      if (!config.pod)
      {
        throw new Error(`Invalid Customer Config (${cust}) - Target Update is Campaign but Pod is not defined`)
      }
      if (!config.region)
      {
        //Campaign POD Region
        throw new Error(`Invalid Customer Config (${cust}) - Target Update is Campaign but Region is not defined`)
      }

      if (!config.region.toLowerCase().match(/^(?:us|eu|ap|ca)$/gim))
      {
        throw new Error(
          `Invalid Customer Config (${cust}) - Region is not 'US', 'EU', CA' or 'AP'. `
        )
      }

    }

    if (config.targetupdate.toLowerCase() === "connect")
    {
      //updatetype has valid Campaign values and Campaign dependent values
      if (config.updatetype.toLowerCase().match(/^(?:referenceset|createupdatecontacts|createattributes)$/gim))
      {
        //if (!config.datasetid)
        //{
        //  throw new Error("Invalid Customer Config - Target Update is Connect but DataSetId is not defined")
        //}
        if (!config.subscriptionid)
        {
          throw new Error(`Invalid Customer Config (${cust}) - Target Update is Connect but SubscriptionId is not defined`)
        }
        if (!config.x_api_key)
        {
          throw new Error(`Invalid Customer Config (${cust}) - Target Update is Connect but X-Api-Key is not defined`)
        }
        if (!config.x_acoustic_region)
        {
          throw new Error(`Invalid Customer Config (${cust}) - Target Update is Connect but X-Acoustic-Region is not defined`)
        }

        if (config.x_acoustic_region.toLowerCase().match(/^(?:"us-east-1"| "us-east-2"| "us-west-1"| "us-west-2"| "af-south-1"| "ap-east-1"| "ap-south-1"| "ap-south-2"| "ap-southeast-1"| "ap-southeast-2"| "ap-southeast-3"| "ap-southeast-4"| "ap-northeast-1"| "ap-northeast-2"| "ap-northeast-3"| "ca-central-1"| "eu-central-1"| "eu-central-2"| "eu-north-1"| "eu-south-1"| "eu-south-2"| "eu-west-1"| "eu-west-2"| "eu-west-3"| "il-central-1"| "me-central-1"| "me-south-1"| "sa-east-1" )$/gim))
        {
          throw new Error(`Invalid Customer Config (${cust}) - Target Update is Connect but Region is incorrect or undefined`)
        }

      }
    }


    if (!config.updates)
    {
      throw new Error("Invalid Customer Config - Updates is not defined")
    }

    if (!config.updates.toLowerCase().match(/^(?:singular|multiple|bulk)$/gim))
    {
      throw new Error(
        `Invalid Customer Config (${cust}) - Updates is not 'Singular' or 'Multiple' `
      )
    }

    //Remove legacy config value
    if (config.updates.toLowerCase() === "bulk") config.updates = "Multiple"


    if (!config.format)
    {
      throw new Error(`Invalid Customer Config (${cust}) - Format is not defined`)
    }
    if (!config.format.toLowerCase().match(/^(?:csv|json)$/gim))
    {
      throw new Error(`Invalid Customer Config (${cust}) - Format is not 'CSV' or 'JSON' `)
    }

    if (!config.separator)
    {
      //see: https://www.npmjs.com/package/@streamparser/json-node
      //JSONParser / Node separator option: null = `''` empty = '', otherwise a separator eg. '\n'
      config.separator = "\n"
    }

    //Customer specific separator
    if (config.separator.toLowerCase() === "null") config.separator = `''`
    if (config.separator.toLowerCase() === "empty") config.separator = `""`
    if (config.separator.toLowerCase() === "\n") config.separator = "\n"


    if (!config.pod.match(/^(?:0|1|2|3|4|5|6|7|8|9|a|b)$/gim))
    {
      throw new Error(
        `Invalid Customer Config (${cust}) - Pod is not 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, A, or B. `
      )
    }

    //
    //    XMLAPI Endpoints
    //Pod 1 - https://api-campaign-us-1.goacoustic.com/XMLAPI
    //Pod 2 - https://api-campaign-us-2.goacoustic.com/XMLAPI
    //Pod 3 - https://api-campaign-us-3.goacoustic.com/XMLAPI
    //Pod 4 - https://api-campaign-us-4.goacoustic.com/XMLAPI
    //Pod 5 - https://api-campaign-us-5.goacoustic.com/XMLAPI
    //Pod 6 - https://api-campaign-eu-1.goacoustic.com/XMLAPI
    //Pod 7 - https://api-campaign-ap-2.goacoustic.com/XMLAPI
    //Pod 8 - https://api-campaign-ca-1.goacoustic.com/XMLAPI
    //Pod 9 - https://api-campaign-us-6.goacoustic.com/XMLAPI
    //Pod A - https://api-campaign-ap-1.goacoustic.com/XMLAPI
    //pod B - https://api-campaign-ap-3.goacoustic.com/XMLAPI
    switch (config.pod.toLowerCase())
    {
      case "6":
        config.pod = "1"
        break
      case "7":
        config.pod = "2"
        break
      case "8":
        config.pod = "1"
        break
      case "9":
        config.pod = "6"
        break
      case "a":
        config.pod = "1"
        break
      case "b":
        config.pod = "3"
        break

      default:
        break
    }




    if (!config.sftp)
    {
      config.sftp = {user: "", password: "", filepattern: "", schedule: ""}
    }

    if (config.sftp.user && config.sftp.user !== "")
    {
      S3DB_Logging("info", "700", `SFTP User: ${config.sftp.user}`)
    }
    if (config.sftp.password && config.sftp.password !== "")
    {
      S3DB_Logging("info", "700", `SFTP Pswd: ${config.sftp.password}`)
    }
    if (config.sftp.filepattern && config.sftp.filepattern !== "")
    {
      S3DB_Logging("info", "700", `SFTP File Pattern: ${config.sftp.filepattern}`)
    }
    if (config.sftp.schedule && config.sftp.schedule !== "")
    {
      S3DB_Logging("info", "700", `SFTP Schedule: ${config.sftp.schedule}`)
    }


    if (!config.transforms.consent)
    {
      Object.assign(config.transforms, {consent: {}})
    }

    //if (!config.transforms.audienceupdate)
    //{
    //  Object.assign(config.transforms, {audienceupdate: {}})
    //}
    if (!config.transforms)
    {
      Object.assign(config, {transforms: {}})
    }

    //if (!config.transforms.contactid)  //should never use this config, here for future reference 
    //{
    //  Object.assign(config.transforms, {contactid: {}})
    //}
    if (!config.transforms.contactkey)
    {
      Object.assign(config.transforms, {contactkey: {}})
    }

    if (!config.transforms.addressablefields)
    {
      Object.assign(config.transforms, {addressablefields: {}})
    }

    if (!config.transforms.methods)
    {
      Object.assign(config.transforms, {methods: {}})
    }

    if (!config.transforms.methods.dateday)
    {
      Object.assign(config.transforms.methods, {dateday: {}})
    }

    if (!config.transforms.methods.date_iso1806_type)
    {
      Object.assign(config.transforms.methods, {date_iso1806_type: {}})
    }

    if (!config.transforms.methods.phone_number_type)
    {
      Object.assign(config.transforms.methods, {phone_number_type: {}})
    }

    if (!config.transforms.methods.string_to_number_type)
    {
      Object.assign(config.transforms.methods, {string_to_number_type: {}})
    }

    if (!config.transforms.jsonmap)
    {
      Object.assign(config.transforms, {jsonmap: {}})
    }
    if (!config.transforms.csvmap)
    {
      Object.assign(config.transforms, {csvmap: {}})
    }
    if (!config.transforms.ignore)
    {
      Object.assign(config.transforms, {ignore: []})
    }



    S3DB_Logging("info", "930", `Transforms configured in Customer Config: \n${JSON.stringify(config.transforms)}`)

    if (typeof config.transforms.jsonmap !== "undefined" &&
      config.transforms.jsonmap !== null &&
      Object.entries(config.transforms.jsonmap).length > 0)
    {
      const tmpMap: {[key: string]: string} = {}
      const jm = config.transforms.jsonmap as {[key: string]: string}
      for (const m in jm)
      {

        const p = jm[m]
        let p2 = ""
        if (typeof p === "string" && p.startsWith("$"))
        {
          p2 = p.substring(2, p.length)
        }

        if (["contactid", "contactkey", "addressablefields", "consent", "audience"].includes(m.toLowerCase()) ||
          ["contactid", "contactkey", "addressablefields", "consent", "audience"].includes(p2.toLowerCase()))
        {
          S3DB_Logging("error", "", `JSONMap config: The JSONMap statement, either the Column to create or the JSONPath statement reference, cannot use a reserved word ("contactid", "contactkey", "addressablefields", "consent", or "audience") ${m}: ${p}`)

          throw new Error(`JSONMap config (${cust}): The JSONMap statement, either the Column to create or the JSONPath statement reference, cannot use a reserved word ("contactid", "contactkey", "addressablefields", "consent", or "audience") ${m}: ${p}`)
        }

        else
        {
          if (typeof p === "string" && p.startsWith("$"))
          {
            try
            {
              const v = jsonpath.parse(p) //checking for parse exception highlighting invalid jsonpath
              tmpMap[m] = jm[m]
            } catch (e)
            {
              debugger //catch

              S3DB_Logging("exception", "", `Invalid JSONPath defined in Customer config (${cust}): ${m}: "${m}", \nInvalid JSONPath - ${e} `)
            }
          }
        }
      }
      config.transforms.jsonmap = tmpMap
    }

  } catch (e)
  {
    debugger //catch

    S3DB_Logging("error", "", `Exception - Validate Customer Config exception: \n${e}`)
  }


  return config as CustomerConfig
}
