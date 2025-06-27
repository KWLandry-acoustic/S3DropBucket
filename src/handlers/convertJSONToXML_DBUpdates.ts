/* eslint-disable no-debugger */
"use strict"
import {type CustomerConfig, xmlRows, S3DB_Logging} from './s3DropBucket'

export function convertJSONToXML_DBUpdates (updates: object[], config: CustomerConfig) {
  if (updates.length < 1)
  {
    throw new Error(
      `Exception - Convert JSON to XML for DB - No Updates (${updates.length}) were passed to process. Customer ${config.customer} `
    )
  }

  xmlRows = `<Envelope><Body>`
  let r = 0

  try
  {
    for (const upd in updates)
    {

      r++

      const updAtts = updates[upd]
      //const s = JSON.stringify(updAttr )
      xmlRows += `<AddRecipient><LIST_ID>${config.listid}</LIST_ID><CREATED_FROM>0</CREATED_FROM><UPDATE_IF_FOUND>true</UPDATE_IF_FOUND>`

      // If Keyed, then Column that is the key must be present in Column Set
      // If Not Keyed must add Lookup Fields
      // Use SyncFields as 'Lookup" values, Columns hold the Updates while SyncFields hold the 'lookup' values.
      //Only needed on non-keyed (In Campaign use DB -> Settings -> LookupKeys to find what fields are Lookup Keys)
      if (config.updatetype.toLowerCase() === "dbnonkeyed")
      {

        //const lk = config.lookupkeys.split(",")
        const lk = config.lookupkeys as typeof config.lookupkeys

        xmlRows += `<SYNC_FIELDS>`
        try
        {
          for (let k in lk)
          {
            k = k.trim()
            const lu = lk[k] as keyof typeof updAtts
            if (updAtts[lu] === undefined)
              throw new Error(
                `No value for LookupKey found in the update. LookupKey: \n${k}`
              )
            const sf = `<SYNC_FIELD><NAME>${lu}</NAME><VALUE><![CDATA[${updAtts[lu]}]]></VALUE></SYNC_FIELD>`
            xmlRows += sf
          }
        } catch (e)
        {
          debugger //catch

          S3DB_Logging("exception", "", `Building XML for DB Updates - ${e}`)
        }

        xmlRows += `</SYNC_FIELDS>`
      }

      //<SYNC_FIELDS>
      //  <SYNC_FIELD>
      //  <NAME>EMAIL </NAME>
      //  < VALUE > somebody@domain.com</VALUE>
      //    </SYNC_FIELD>
      //    < SYNC_FIELD >
      //    <NAME>Customer Id </NAME>
      //      < VALUE > 123 - 45 - 6789 </VALUE>
      //      </SYNC_FIELD>
      //      </SYNC_FIELDS>
      //
      if (config.updatetype.toLowerCase() === "dbkeyed")
      {
        //Placeholder
        //Don't need to do anything with DBKey, it's superfluous but documents the keys of the keyed DB
      }


      type ua = keyof typeof updAtts

      for (const uv in updAtts)
      {
        const uVal: ua = uv as ua
        xmlRows += `<COLUMN><NAME>${uVal}</NAME><VALUE><![CDATA[${updAtts[uVal]}]]></VALUE></COLUMN>`

      }

      xmlRows += `</AddRecipient>`
    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - ConvertJSONtoXML_DBUpdates - \n${e}`)
  }

  xmlRows += `</Body></Envelope>`

  S3DB_Logging("info", "916", `Converted ${r} updates - JSON for DB Updates: ${JSON.stringify(updates)}\nPackaging ${Object.values(updates).length} rows as updates to ${config.customer}'s ${config.listname}`)
  S3DB_Logging("info", "917", `Converted ${r} updates - XML for DB Updates: ${xmlRows}\nPackaging ${Object.values(updates).length} rows as updates to ${config.customer}'s ${config.listname}`)

  return xmlRows
}
