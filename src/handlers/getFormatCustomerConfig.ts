/* eslint-disable no-debugger */
"use strict"
import {GetObjectCommand, type GetObjectCommandOutput} from '@aws-sdk/client-s3'
import {getAllCustomerConfigsList} from './getAllCustomerConfigsList'
import {S3DB_Logging, customersConfig, type CustomerConfig, s3, s3dbConfig} from './s3DropBucket'
import {validateCustomerConfig} from './validateCustomerConfig'

export async function getFormatCustomerConfig (filekey: string) {

  //Populate/Refresh Customer Config List 
  if (process.env.S3DropBucketConfigBucket === "") process.env.S3DropBucketConfigBucket = "s3dropbucket-configs"
  const ccl = await getAllCustomerConfigsList(process.env.S3DropBucketConfigBucket ?? "s3dropbucket-configs")
  process.env.S3DropBucketCustomerConfigsList = JSON.stringify(ccl)

  // Retrieve file's prefix as the Customer Name
  if (!filekey)
    throw new Error(
      `Exception - Cannot resolve Customer Config without a valid filename (filename is ${filekey})`
    )

  let customer = filekey


  //Need to 'normalize' filename by removing Path details
  while (customer.indexOf("/") > -1)
  {
    //remove any folders from name
    customer = customer.split("/").at(-1) ?? customer
  }

  //Normalize if timestamp - normalize timestamp out 
  const r = new RegExp(/\d{4}_\d{2}_\d{2}T.*Z.*/, "gm")
  //remove timestamps from name as can confuse customer name parsing
  if (customer.match(r))
  {
    customer = customer.replace(r, "") //remove timestamp from name
  }

  //Normalize if Aggregator File
  //Check for Aggregator File Name - normalize Aggregator file string out 
  const r2 = new RegExp(/(S3DropBucket_Aggregator.*)/, "gm")
  //remove Aggregator File String from name as can confuse dataflow name parsing
  if (customer.match(r2))
  {
    customer = customer.replace(r2, "") //remove S3DropBucket_Aggregator and rest of string from name
  }


  //Now, need to 'normalize' (deconstruct) all other strings that are possible to arrive at (reconstruct) a valid dataflow name with a trailing underscore 
  if (customer.lastIndexOf('_') > 3) //needs to have at least 4 chars for dataflow name 
  {
    let i = customer.lastIndexOf('_')
    customer = customer.substring(0, i)
    let ca = customer.split('_')
    let c = ""
    for (const n in ca)
    {
      c += ca[n] + '_'
    }
    customer = c
  }
  else
  {
    S3DB_Logging("exception", "", `Exception - Parsing File Name for Dataflow Config Name returns: ${customer}}. Cannot continue.`)
    throw new Error(`Exception - Parsing File Name for Dataflow Config Name returns: ${customer}}. Cannot continue.`)
  }

  //Should be left with valid Dataflow Name, data flow and trailing underscore
  if (!customer.endsWith('_'))
  {
    throw new Error(
      `Exception - Cannot resolve Customer Config without a valid Customer Prefix (filename is ${filekey})`
    )
  }


  //ToDo: Need to change this up to getting a listing of all configs and matching up against filename,
  //  allowing Configs to match on filename regardless of case
  //  populate 'process.env.S3DropBucketConfigsList' and walk over it to match config to filename
  let ccKey = `${customer}config.jsonc`

  //Match File to Customer Config file
  //ToDo: test and confirm 
  try
  {
    const cclist = JSON.parse(process.env.S3DropBucketCustomerConfigsList ?? "")
    for (const i in cclist)
    {
      if (cclist[i].toLowerCase() === `${customer}config.jsonc`.toLowerCase()) ccKey = `${cclist[i]}`
      break
    }
  } catch (e)
  {

    debugger //catch

    S3DB_Logging("exception", "", `Matching filename to Customer config : \n${e}`)
  }

  const getObjectCommand = {
    Key: ccKey,
    //Bucket: 's3dropbucket-configs'
    Bucket: process.env.S3DropBucketConfigBucket,
  }

  let ccr
  let configJSON = customersConfig as CustomerConfig

  try
  {
    ccr = await s3
      .send(new GetObjectCommand(getObjectCommand))
      .then(async (getConfigS3Result: GetObjectCommandOutput) => {

        let cc = (await getConfigS3Result.Body?.transformToString(
          "utf8"
        )) as string

        S3DB_Logging("info", "910", `Customer (${customer}) Config: \n ${cc.trim()} `)

        //S3DB_Logging("info", "910", `Customer (${customer}) Config: \n ${cc.replace(/\s/g, '')} `)
        //Remove Schema line to avoid parsing error
        cc = cc.replaceAll(new RegExp(/^.*?"\$schema.*?$/gm), "")

        //Parse comments out of the json before parse for config
        cc = cc.replaceAll(new RegExp(/[^:](\/\/.*(,|$|")?)/g), "")
        const cc1 = cc.replaceAll("\n", "")

        //Parse comments out of the json before parse
        //const ccr1 = ccr.replaceAll(new RegExp(/(".*":)/g), (match) => match.toLowerCase())
        const cc2 = cc1.replaceAll(new RegExp(/(\/\/.*(?:$|\W|\w|\S|\s|\r).*)/gm), "")
        const cc3 = cc2.replaceAll(new RegExp(/\n/gm), "")
        cc = cc3.replaceAll("   ", "")

        return cc
      })
      .catch((e) => {

        debugger //catch

        const err: string = JSON.stringify(e)

        if (err.indexOf("specified key does not exist") > -1)
          throw new Error(
            `Exception - Customer Config - ${customer}config.jsonc does not exist on ${S3DBConfig.s3dropbucket_configs} bucket (while processing ${filekey}) \nException ${e} `
          )

        if (err.indexOf("NoSuchKey") > -1)
          throw new Error(
            `Exception - Customer Config Not Found(${customer}config.jsonc) on ${S3DBConfig.s3dropbucket_configs}. \nException ${e} `
          )

        throw new Error(
          `Exception - Retrieving Config (${customer}config.jsonc) from ${S3DBConfig.s3dropbucket_configs}. \nException ${e} `
        )
      })

  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - On Try when Pulling Customer Config \n${ccr}. \n${e} `)
    throw new Error(`Exception - (Try) Pulling Customer Config \n${ccr} \n${e} `)
  }

  configJSON = JSON.parse(ccr) as CustomerConfig

  //Potential Transform opportunity (values only)
  //configJSON = JSON.parse(ccr, function (key, value) {
  //  return value
  //});
  const setPropsLowCase = (object: object, container: string) => {
    type okt = keyof typeof object
    for (const key in object)
    {
      const k = key as okt

      const lk = (key as string).toLowerCase()

      if (container.match(new RegExp(/contactid|contactkey|consent|audienceupdate|addressablefields|methods|jsonmap|csvmap|ignore/))) continue

      //if (typeof object[k] === "object") setPropsLowCase(object[k])
      if (Object.prototype.toString.call(object[k]) === "[object Object]") setPropsLowCase(object[k], lk)

      if (k === lk) continue
      object[lk as okt] = object[k]
      delete object[k]
    }
    return object
  }

  configJSON = setPropsLowCase(configJSON, '') as CustomerConfig

  configJSON = await validateCustomerConfig(configJSON) as CustomerConfig

  return configJSON as CustomerConfig
}
