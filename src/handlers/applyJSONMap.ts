/* eslint-disable no-debugger */
"use strict"
import jsonpath from 'jsonpath'
import {S3DB_Logging} from './s3DropBucket'


export function applyJSONMap (jsonObj: object, map: object) {
  let j
  Object.entries(map).forEach(([key, jpath]) => {
    //const p = jm[key]  
    //const p2 = jpath.substring(2, jpath.length)
    //if (["contactid", "contactkey", "addressablefields", "consent", "audience"].includes(key.toLowerCase()) ||
    //  ["contactid", "contactkey", "addressablefields", "consent", "audience"].includes(p2.toLowerCase())
    //)
    //{
    //  S3DB_Logging("error", "999", `JSONMap config: Either part of the JSONMap statement, the Column to create or the JSONPath statement, cannot use a reserved word ("contactid", "contactkey", "addressablefields", "consent", or "audience") ${key}: ${jpath}`)
    //  throw new Error(`JSONMap config: Either part of the JSONMap statement, the Column to create or the JSONPath statement, cannot reference a reserved word ("contactid", "contactkey", "addressablefields", "consent", or "audience") ${key}: ${jpath}`)
    //}

    try
    {
      j = jsonpath.value(jsonObj, jpath)
      //Object.assign(jsonObj, {[k]: j})
      S3DB_Logging("info", "949", `JSONPath statement ${jpath} returns ${j} from: \nTarget Data: \n${JSON.stringify(jsonObj)} `)
    } catch (e)
    {
      debugger //catch



      //if (e instanceof jsonpath.JsonPathError) { S3DB_Logging("error","",`JSONPath error: e.message`) }
      S3DB_Logging("warning", "949", `Error parsing data for JSONPath statement ${key} ${jpath}, ${e} \nTarget Data: \n${JSON.stringify(jsonObj)} `)
    }
  })
  // const a1 = jsonpath.parse(value)
  // const a2 = jsonpath.parent(s3Chunk, value)
  // const a3 = jsonpath.paths(s3Chunk, value)
  // const a4 = jsonpath.query(s3Chunk, value)
  // const a6 = jsonpath.value(s3Chunk, value)
  //Confirms Update was accomplished
  // const j = jsonpath.query(s3Chunk, v)
  // console.info(`${ j } `)
  //})
  //return jsonObj
  if (typeof j === "string") return j
  return String(j)
}
