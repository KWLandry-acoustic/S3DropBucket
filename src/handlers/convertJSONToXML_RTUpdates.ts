/* eslint-disable no-debugger */
"use strict"
import {type CustomerConfig, S3DB_Logging} from './s3DropBucket'

export function convertJSONToXML_RTUpdates (updates: object[], config: CustomerConfig) {
  if (updates.length < 1)
  {
    throw new Error(
      `Exception - Convert JSON to XML for RT - No Updates(${updates.length}) were passed to process into XML. Customer ${config.customer} `
    )
  }

  let xmlRows = `<Envelope> <Body> <InsertUpdateRelationalTable> <TABLE_ID> ${config.listid} </TABLE_ID><ROWS>`

  let r = 0

  for (const upd in updates)
  {

    //const updAtts = JSON.parse( updates[ upd ] )
    const updAtts = updates[upd]

    r++
    xmlRows += `<ROW>`
    // Object.entries(jo).forEach(([key, value]) => {
    type ua = keyof typeof updAtts

    for (const uv in updAtts)
    {
      const uVal: ua = uv as ua
      xmlRows += `<COLUMN name="${uVal}"> <![CDATA[${updAtts[uVal]}]]> </COLUMN>`
    }

    xmlRows += `</ROW>`
  }

  //Tidy up the XML
  xmlRows += `</ROWS></InsertUpdateRelationalTable></Body></Envelope>`

  S3DB_Logging("info", "906", `Converting ${r} updates to XML RT Updates. Packaged ${Object.values(updates).length} rows as updates to ${config.customer}'s ${config.listname} \nJSON to be converted to XML RT Updates: ${JSON.stringify(updates)}`)

  S3DB_Logging("info", "917", `Converting ${r} updates to XML RT Updates. Packaged ${Object.values(updates).length} rows as updates to ${config.customer}'s ${config.listname} \nXML from JSON for RT Updates: ${xmlRows}`)

  return xmlRows
}
