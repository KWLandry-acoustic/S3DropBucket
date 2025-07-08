/* eslint-disable no-debugger */
"use strict"
import {type CustomerConfig, s3dbConfig, S3DB_Logging} from './s3DropBucket'
import {applyJSONMap} from './applyJSONMap'


export function transforms (updates: object[], config: CustomerConfig) {
  //Apply Transforms
  //Approach: => Process Entire Set of Updates in Each Transform call, NOT each and all Transform against each update at a time.
  //
  // Sequence:
  //  Have to run transforms in the following sequence (creation then transformational methods then reference fields then ignore (always last))
  //
  // Create New Columns
  //  jsonMap  --- Essentially Add Columns to Data (future: refactor as 'addcolumns')
  //  csvMap  --- Essentially Add Columns to Data (future: refactor as 'addcolumns')
  //
  // Apply transformational Methods - Transform the values of the Columns
  //  dateday - add a column of the day of the week (Sun, Mon, Tue, Wed, Thus, Fri, Sat)
  //  Connect Date-to-ISO-8601 format
  //  Connect String-To-Number format
  //  Connect Phone format
  //
  // Next, Have to run "Reference" transforms (transforms that create reference fields) After transforms that modify data
  // ContactId
  // Addressable Fields
  // Channel Consent
  // Audience
  //
  // --- Have to Run Ignore (Remove Columns in data) last,
  // Ignore
  //ToDo: on outcomes messaging return only the affected lines not the entire JSON.stringify(updates)
  //Transform: JSONMap
  //Apply JSONMap -
  //  JSONPath statements
  //      "jsonMap": {
  //          "email": "$.uniqueRecipient",   //create new Column email with the value from uniqueRecipient
  //          "zipcode": "$.context.traits.address.postalCode"
  //      },
  //Need failsafe test of empty object, when jsonMap has no transforms. 
  try
  {
    if (typeof config.transforms.jsonmap !== undefined &&
      Object.keys(config.transforms.jsonmap).length > 0)
    {
      const t: typeof updates = []
      try
      {
        for (const update of updates)
        {

          Object.entries(config.transforms.jsonmap).forEach(([key, val]) => {

            //const jo = JSON.parse( l )
            let j = applyJSONMap(update, {[key]: val})
            if (typeof j === "undefined" || j === "") j = "Not Found"
            Object.assign(update, {[key]: j})
          })

          t.push(update)
        }


      } catch (e)
      {
        debugger //catch

        S3DB_Logging("exception", "934", `Exception - Transform - Applying JSONMap \n${e}`)

      }

      if (t.length === updates.length)
      {
        updates = t
      }
      else
      {
        debugger //catch

        S3DB_Logging("error", "933", `Error - Transform - Applying JSONMap returns fewer records(${t.length}) than initial set ${updates.length}`)

        throw new Error(
          `Error - Transform - Applying JSONMap returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }


      //if no throw/exception yet then we are successful
      S3DB_Logging("info", "931", `Transforms (JSONMap) applied: \n${JSON.stringify(t)}`)

    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "934", `Exception - Applying JSONMap Transform \n${e}`)
  }




  //Transform: CSVMap
  //Apply CSVMap
  // "csvMap": { //Mapping when processing CSV files
  //       "Col_AA": "COL_XYZ", //Write Col_AA with data from Col_XYZ in the CSV file
  //       "Col_BB": "COL_MNO",
  //       "Col_CC": "COL_GHI",
  //       "Col_DD": 1, //Write Col_DD with data from the 1st column of data in the CSV file.
  //       "Col_DE": 2,
  //       "Col_DF": 3
  // },

  try
  {
    if (typeof config.transforms.csvmap !== undefined &&
      Object.keys(config.transforms.csvmap).length > 0)
    {
      const t: typeof updates = []
      try
      {
        for (const jo of updates)
        {
          //const jo = JSON.parse( l )
          const map = config.transforms.csvmap as {[key: string]: string}
          Object.entries(map).forEach(([key, val]) => {

            //type dk = keyof typeof jo
            //const k: dk = ig as dk
            //delete jo[k]
            type kk = keyof typeof jo
            const k: kk = key as kk
            type vv = keyof typeof jo
            const v: vv = val as vv
            jo[k] = jo[v]

            //if (typeof v !== "number") jo[k] = jo[v] ?? ""    //Number because looking to use Column Index (column 1) as reference instead of Column Name.
            //else
            //{
            //  const vk = Object.keys(jo)[v]
            //  // const vkk = vk[v]
            //  jo[k] = jo[vk] ?? ""
            //}
          })

          t.push(jo)

        }
      } catch (e)
      {
        debugger //catch

        S3DB_Logging("exception", "934", `Exception - Transforms - Applying CSVMap \n${e}`)

      }

      if (t.length === updates.length)
      {
        updates = t
      }
      else
      {
        S3DB_Logging("error", "933", `Error - Transform - Applying CSVMap returns fewer records(${t.length}) than initial set ${updates.length}`)
        throw new Error(
          `Error - Transform - Applying CSVMap returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }

      //if no throw/exception yet then we are successful
      S3DB_Logging("info", "932", `Transforms (CSVMap) applied: \n${JSON.stringify(t)}`)
    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "934", `Exception - Applying CSVMap Transform \n${e}`)
  }


  //Now, with all New Columns in place, apply Methods that modify values
  //ToDo: Add JSONPath support, not critical as the object can only be JSON at this point, but helpful as consistency for value references
  //Modify Column Values - to replace the value in the column simply specify the same column name for source and destination.
  //Transform: dateday     - Not strictly a modify existing Column Method, will create a new Column with day of week from a referenced date value.
  //
  //ToDo: Provide a transform to break timestamps out into
  //    Day - Done
  //    Hour - tbd
  //    Minute - tbd
  //
  try
  {

    if (typeof config.transforms.methods.dateday !== undefined &&
      Object.keys(config.transforms.methods.dateday).length > 0)
    {
      const t: typeof updates = []
      let toDay: string = ""

      const days = [
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
      ]


      //"dateday": "date_modified",   - Current config 
      //ToDo: modify to support key:value config as in other Transform configs
      // "dateday": {"key": "value"}
      for (const update of updates)
      {
        Object.entries(config.transforms.methods.dateday).forEach(([key, val]) => {
          //Element implicitly has an 'any' type because expression of type 'string' can't be used to index type '{} '.
          //No index signature with a parameter of type 'string' was found on type '{}'.ts(7053)
          const updateObj: {[key: string]: string} = update as {[key: string]: string}

          toDay = updateObj[val] as string

          if (typeof toDay !== "undefined" && toDay !== "undefined" && toDay !== null && toDay.length > 0)
          {
            const dt = new Date(toDay)
            const day = {dateday: days[dt.getDay()]}
            Object.assign(update, day)
          }

          else
          {
            //S3DB_Logging("error", "933", `Error - Transform - dateday Transform failed for ${val} as the string value '${toDay}' returns invalid date value`)
            const day = {dateday: 'na'}
            Object.assign(update, day)
          }
        })

        t.push(update)
      }

      if (t.length === updates.length)
      {
        updates = t
      }
      else
      {
        S3DB_Logging("error", "933", `Error - Transform - Applying dateday Transform returns fewer records (${t.length}) than initial set ${updates.length}`)
        throw new Error(
          `Error - Transform - Applying dateday Transform returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }

      S3DB_Logging("info", "935", `Transforms (dateday) applied: \n${JSON.stringify(updates)}`)

    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "934", `Exception - Applying dateday Transform \n${e}`)
  }



  //Transform - Date-to-ISO-8601
  //
  //Ensure only the following date formats, including separators, are used in the uploaded file to prevent potential errors:
  //YYYY - MM - DD
  //YYYY - MM - DDThh: mm: ssTZD
  //YYYY / MM / DD
  //MM / DD / YYYY
  //DD / MM / YYYY
  //DD.MM.YYYY
  try
  {
    if (typeof config.transforms.methods?.date_iso1806_type !== undefined &&
      Object.keys(config.transforms.methods.date_iso1806_type).length > 0)
    {
      //const iso1806Col = config.transforms.methods.date_iso1806 ?? 'iso1806Date' 
      const t: typeof updates = []
      let toISO1806: string = ""

      for (const update of updates)
      {
        Object.entries(config.transforms.methods.date_iso1806_type).forEach(([key, val]) => {
          //Element implicitly has an 'any' type because expression of type 'string' can't be used to index type '{} '.
          //No index signature with a parameter of type 'string' was found on type '{}'.ts(7053)
          const updateObj: {[key: string]: string} = update as {[key: string]: string}

          toISO1806 = updateObj[val] as string

          if (typeof toISO1806 !== "undefined" && toISO1806 !== "undefined" && toISO1806 !== null && toISO1806.length > 0)
          {
            const dt = new Date(toISO1806)
            const isoString: string = dt.toISOString()
            const tDate = {[key]: isoString}
            Object.assign(update, tDate)
          }

          else
          {
            //S3DB_Logging("error", "933", `Error - Transform - date_iso1806 Transform failed for ${val} as the string value '${toISO1806}' returns invalid date value`)
            const tDate = {[key]: null}
            Object.assign(update, tDate)
          }

        })

        t.push(update)
      }

      if (t.length === updates.length)
      {
        updates = t
      }
      else
      {
        S3DB_Logging("error", "933", `Error - Transform - Applying date_iso1806 Transform returns fewer records (${t.length}) than initial set ${updates.length}`)
        throw new Error(
          `Error - Transform - Applying date_iso1806 Transform returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }

      S3DB_Logging("info", "935", `Transforms (Date_ISO1806) applied: \n${JSON.stringify(updates)}`)

    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "934", `Exception - Applying date_iso1806 Transform \n${e}`)
  }


  //Transform - Phone Number 
  //
  try
  {

    if (typeof config.transforms.methods.phone_number_type !== undefined &&
      Object.keys(config.transforms.methods.phone_number_type).length > 0)
    {

      const t: typeof updates = []
      let pn: string = ""

      for (const update of updates)
      {
        Object.entries(config.transforms.methods.phone_number_type).forEach(([key, val]) => {
          //Element implicitly has an 'any' type because expression of type 'string' can't be used to index type '{} '.
          //No index signature with a parameter of type 'string' was found on type '{}'.ts(7053)
          const updateObj: {[key: string]: string} = update as {[key: string]: string}

          pn = updateObj[val] as string
          //undefined
          //null
          //value gt 0 in length
          if (typeof pn !== "undefined" && pn !== "undefined" && pn !== null && pn.length > 0)
          {

            const npn = pn.replaceAll(new RegExp(/(\D)/gm), "")

            if (!/\d{7,}/.test(npn)) //Phone number should be all numeric and minimum 7 digits
            {
              S3DB_Logging("error", "933", `Error - Transform - Phone_Number transform failed for ${val} as the string value '${pn}' (${updateObj[val]}) returns invalid phone number value: ${npn}`)
            }

            const pnu = {[key]: npn}
            Object.assign(update, pnu)

          }

          else
          {
            //S3DB_Logging("error", "933", `Error - Transform - Phone_Number transform for ${key}: ${val} returns empty value.`)
            const pnu = {[key]: null}
            Object.assign(update, pnu)
          }
        })

        t.push(update)

      }
      if (t.length === updates.length)
      {
        updates = t
      }
      else
      {
        S3DB_Logging("error", "933", `Error - Transform - Phone_Number Transform returns fewer records (${t.length}) than initial set ${updates.length}`)
        throw new Error(
          `Error - Transform - Applying Phone_Number Transform returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }

      S3DB_Logging("info", "935", `Transforms (Phone Number) applied: \n${JSON.stringify(updates)}`)


    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "934", `Exception - Applying PhoneNumber Transform \n${e}`)
  }




  //Transform - String-To-Number
  //
  try
  {
    if (typeof config.transforms.methods.string_to_number_type !== undefined &&
      Object.keys(config.transforms.methods.string_to_number_type).length > 0)
    {
      const t: typeof updates = []
      let strToNumber: string = ""

      for (const update of updates)
      {
        Object.entries(config.transforms.methods.string_to_number_type).forEach(([key, val]) => {
          //Element implicitly has an 'any' type because expression of type 'string' can't be used to index type '{} '.
          //No index signature with a parameter of type 'string' was found on type '{}'.ts(7053)
          const updateObj: {[key: string]: string} = update as {[key: string]: string}

          strToNumber = updateObj[val] as string

          if (typeof strToNumber !== "undefined" && strToNumber !== "undefined" && strToNumber !== null && strToNumber.length > 0)
          {
            const n = Number(strToNumber)
            if (String(n) === 'NaN')
            {
              S3DB_Logging("error", "933", `Error - Transform - String-To-Number transform failed for ${val} as the string value '${strToNumber}' cannot be converted to a number.`)
            }

            else
            {
              const num = {[key]: n}
              Object.assign(update, num)
            }
          }

          else
          {
            //  //S3DB_Logging("error", "933", `Error - Transform - String To Number transform for ${key}: ${val} returns empty value.`)
            const num = {[key]: null}
            Object.assign(update, num)
          }
        })

        t.push(update)
      }

      if (t.length === updates.length)
      {
        updates = t
      }
      else
      {
        S3DB_Logging("error", "933", `Error - Transform - Applying String-To-Number Transform returns fewer records (${t.length}) than initial set ${updates.length}`)
        throw new Error(
          `Error - Transform - Applying String-To-Number Transform returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }

      S3DB_Logging("info", "935", `Transforms (StringToNumber) applied: \n${JSON.stringify(updates)}`)

    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "934", `Exception - Applying String-To-Number Transform \n${e}`)
  }



  //Now create Reference Fields, data that is used in Connect to augment Updates/Mutations 
  /*
    //Transform: ContactId
    //
  
    try
    {
      if (typeof config.transforms.contactid !== undefined &&
        config.transforms.contactid.length > 3)
      {
        let s: string = ""
  
        const t: typeof updates = []
  
        if (config.transforms.contactid.startsWith('$')) s = 'jsonpath'
        else if (config.transforms.contactid.startsWith('@')) s = 'csvcolumn'
        else if (s === "" && config.transforms.contactid.length > 3) s = 'static'
        else S3DB_Logging("error", "933", `Error - Transform - ContactId invalid configuration.`)
  
  
        //Process All Updates for this Transform
        for (const update of updates)
        {
          Object.assign(update, {"contactId": ""})
  
          switch (s)
          {
            case 'static': {
              Object.assign(update, {"contactId": config.transforms.contactid})
              break
            }
            case 'jsonpath': {
              let j = applyJSONMap(update, {contactId: config.transforms.contactid})
              if (typeof j === "undefined" || j === "") j = "Not Found"
              Object.assign(update, {"contactId": j})
              break
            }
            case 'csvcolumn':
              {
                //strip preceding '@.'
                let csvmapvalue = config.transforms.contactid.substring(2, config.transforms.contactid.length)
                let colRef = csvmapvalue as keyof typeof update
  
                let v = update[colRef] as string
                if (typeof v === "undefined" || v === "") v = "Not Found"
                Object.assign(update, {"contactId": v})
                break
              }
          }
  
          t.push(update)
        }
  
        //All Updates transformed from ContactId transform
        if (t.length === updates.length)
        {
          updates = t
        } else
        {
          S3DB_Logging("error", "933", `Error - Transform - Applying ContactId Transform returns fewer records (${t.length}) than initial set ${updates.length}`)
          throw new Error(
            `Error - Transform - Applying ContactId Transform returns fewer records (${t.length}) than initial set ${updates.length}`
          )
        }
  
        S3DB_Logging("info", "950", `Transforms (ContactId) applied: \n${JSON.stringify(updates)}`)
  
      }
    } catch (e)
    {
      debugger //catch
  
      S3DB_Logging("exception", "934", `Exception - Applying ContactId Transform \n${e}`)
    }*/
  /*
  //ContactKey should not need to be manipulated - Use as Template for Processing
  else if (typeof config.transforms.contactkey !== undefined &&
     config.transforms.contactkey.length > 3)
   {
     //Transform: ContactKey
 
     let s: string = ""
     try
     {
 
       const t: typeof updates = []
 
       if (typeof config.transforms.contactkey !== undefined &&
         config.transforms.contactkey.length > 3)
       {
         if (config.transforms.contactkey.startsWith('$')) s = 'jsonpath'
         else if (config.transforms.contactkey.startsWith('@')) s = 'csvcolumn'
         else if (s === "" && config.transforms.contactkey.length > 3) s = 'static'
         else S3DB_Logging("error", "999", `Error - Transform - ContactKey invalid configuration.`)
 
         //Process All Updates for this Transform
         for (const update of updates)
         {
           Object.assign(update, {"contactKey": ""})
 
           switch (s)
           {
             case 'static': {
               if (config.transforms.contactkey === '....') Object.assign(update, {"contactKey": ""})
               else Object.assign(update, {"contactKey": config.transforms.contactkey})
               break
             }
             case 'jsonpath': {
               let j = applyJSONMap(update, {contactKey: config.transforms.contactkey})
               if (typeof j === "undefined" || j === "") j = "Not Found"
               Object.assign(update, {"contactKey": j})
               //Object.assign(u, {"contactKey": applyJSONMap(update, {contactkey: config.transforms.contactkey})})
               break
             }
             case 'csvcolumn':
               {
 
                 //strip preceding '@.'
                 let csvmapvalue = config.transforms.contactkey.substring(2, config.transforms.contactkey.length)
                 let colRef = csvmapvalue as keyof typeof update
 
                 let v = update[colRef] as string
                 if (typeof v === "undefined" || v === "") v = "Not Found"
                 Object.assign(update, {"contactKey": v})
                 break
               }
           }
 
           t.push(update)
         }
       }
       //All Updates now transformed from ContactKey transform
       if (t.length === updates.length)
       {
         updates = t
       } else
       {
         S3DB_Logging("error", "933", `Error - Transform - Applying ContactKey Transform returns fewer records (${t.length}) than initial set ${updates.length}`)
         throw new Error(
           `Error - Transform - Applying ContactKey Transform returns fewer records (${t.length}) than initial set ${updates.length}`
         )
       }
 
           S3DB_Logging("info", "951", `Transforms (ContactKey) applied: \n${JSON.stringify(updates)}`)
 
 
     } catch (e)
     {
       debugger //catch
 
       S3DB_Logging("exception", "934", `Exception - Applying ContactKey Transform \n${e}`)
     }
   }
   */
  try
  {

    //else if
    if (typeof config.transforms.addressablefields !== undefined &&
      Object.entries(config.transforms.addressablefields).length > 0)
    {

      //Customer Config: 
      //Transform: Addressable Fields
      //"addressablefields": {  // Only 3 allowed. 
      //  // Can use either mapping or a "statement" definition, if defined, a 'statement' definition overrides any other config. 
      //  //"Email": "$.email",
      //  //"Mobile Number": "$['Mobile Number']",
      //  //"Example3": "999445599",
      //  //"Example4": "$.jsonfileValue", //example
      //  //"Example5": "@.csvfileColumn" //example
      //  "statement": {
      //    "addressable": [
      //      {"field": "Email", "eq": "$.email"} //, 
      //      //{ "field": "SMS", "eq": "$['Mobile Number']" }     
      //    ]
      //  }
      //},
      //S3DB_Logging("debug", "999", `Debug - Addressable Fields Transform Entered - ${config.targetupdate}`)
      //if (typeof config.transforms.addressablefields !== "undefined" &&
      //  Object.keys(config.transforms.addressablefields).length > 0)
      //{
      interface AddressableItem {
        field: string
        eq: string
      }
      interface AddressableStatement {
        addressable: AddressableItem[]
      }
      interface AddressableFields {
        [key: string]: string | AddressableStatement
        statement: AddressableStatement
      }

      const addressableFields = config.transforms.addressablefields as AddressableFields

      const addressableStatement = addressableFields.statement as AddressableStatement

      const fieldsArray = addressableStatement.addressable


      const t: typeof updates = []

      //Process All Updates for this Transform
      for (const update of updates)
      {

        const addressableArray = [] as object[]

        //If 'Statement' configured, takes precedence over all others
        if (typeof config.transforms.addressablefields["statement"] !== undefined &&
          config.transforms.addressablefields["statement"] !== "")
        {
          for (const fieldItem of fieldsArray)
          {

            //  "statement": {
            //    "addressable": [
            //      {"field": "Email", "eq": "$.email"} //,
            //      //{ "field": "SMS", "eq": "$['Mobile Number']" }
            //    ]
            let s: string = ""

            const fieldVar = fieldItem.eq

            if (fieldVar.startsWith('$')) s = 'jsonpath'
            else if (fieldVar.startsWith('@')) s = 'csvcolumn'
            else if (s === "" && fieldVar.length > 3) s = 'static' //Should never see Static but there may be a use-case
            else S3DB_Logging("error", "933", `Error - Transform - AddressableFields.Statement invalid configuration.`)

            switch (s)
            {
              case 'static': {
                addressableArray.push({fieldItem})
                break
              }
              case 'jsonpath': {

                let j = applyJSONMap(update, {"AddressableFields": fieldVar})
                if (typeof j === 'undefined' || j === 'undefined' || j === "") j = "Not Found"

                const field = {
                  field: fieldItem.field,
                  eq: j
                }

                //addressable: [   //valid mutation example
                // { field: "email", eq: "john.doe1@acoustic.co" },
                // { field: "sms", eq: "+48555555555" }
                //]
                addressableArray.push(field)
                break
              }
              case 'csvcolumn':
                {
                  //strip preceding '@.'
                  let csvmapvalue = fieldVar.substring(2, fieldVar.length)
                  let colRef = csvmapvalue as keyof typeof update

                  let v = update[colRef] as string
                  if (typeof v === "undefined" || v === "") v = "Not Found"

                  const field = {
                    field: fieldItem.field,
                    eq: v
                  }

                  addressableArray.push({field})
                  break
                }
            }
          }

          Object.assign(update, {addressable: addressableArray})

        }
        else //No 'Statement' that would override everything, so process individual Field definition(s)
        {
          for (const [key, fieldItem] of Object.entries(addressableFields))
          {

            const addressableValue = fieldItem as string

            //"Email": "Email",
            //"Mobile Number": "Mobile Number",
            //"Example3": "999445599",
            //"Example4": "$.jsonfileValue", //example
            //"Example5": "@.csvfileColumn" //example
            let s: string = ""

            if (addressableValue.startsWith('$')) s = 'jsonpath'
            else if (addressableValue.startsWith('@')) s = 'csvcolumn'
            else if (s === "" && addressableValue.length > 3) s = 'static'
            else S3DB_Logging("error", "933", `Error - Transform - AddressableFields invalid configuration.`)

            switch (s)
            {
              case 'static': {
                addressableArray.push({addressable: addressableValue})
                break
              }
              case 'jsonpath': {
                let j = applyJSONMap(update, {addressable: addressableValue})
                if (typeof j === "undefined" || j === "") j = "Not Found"

                const field = {
                  field: fieldItem,
                  eq: j
                }

                addressableArray.push({addressable: field})
                break
              }
              case 'csvcolumn':
                {
                  //strip preceding '@.'
                  let csvmapvalue = addressableValue.substring(2, addressableValue.length)
                  let colRef = csvmapvalue as keyof typeof update

                  let v = update[colRef] as string
                  if (typeof v === "undefined" || v === "") v = "Not Found"

                  const field = {
                    field: fieldItem,
                    eq: v
                  }

                  addressableArray.push(field)

                  break
                }
            }
          }
          //})
          Object.assign(update, {addressable: {addressable: addressableArray}})

        }

        t.push(update)

      }

      //All Updates now transformed from consent transform
      if (t.length === updates.length)
      {
        updates = t
      }
      else
      {
        S3DB_Logging("error", "933", `Error - Transform - Applying AddressableFields Transform returns fewer records (${t.length}) than initial set ${updates.length}`)
        throw new Error(
          `Error - Transform - Applying AddressableFields Transform returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }

      S3DB_Logging("info", "952", `Transforms (Addressable Fields) applied: \n${JSON.stringify(updates)}`)

    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "934", `Exception - Applying AddressableFields Transform \n${e}`)
  }


  //  //else
  //  //{
  //  if (config.targetupdate.toLowerCase() === "connect")
  //  {
  //    S3DB_Logging("warn", "933", `Warning - No ContactKey or Addressable Field provided for a Connect Create or Update ${config.targetupdate} Contact.`)
  //    //ToDo: Possible throw exception here for missing addressable, contactID or Addressable Field 
  //  }
  ////}
  //Transform: Channel Consent
  //need loop through Config consent and build consent object.
  //"consent": { //Can use either mapping or a "statement" definition, if defined, a 'statement' definition overrides any other Consent config.
  //  "Email": "OPT_IN_UNVERIFIED", //Static Values: Use When Consent status is not in the Data
  //  "SMS": "OPT_OUT", //Static Values: Use When Consent status is not in the Data
  //  "WhatsApp": "$.whatsappconsent", //Dynamic Values: Use when consent data can be mapped from the data (JSONPath in this case) .
  //  "Email2": "@.csvfileColumn", //Dynamic Values: Use when consent data can be mapped from the data (CSV Column reference in this case).
  //  "statement": { //Mutually Exclusive: Use to define a Consent update to be used with this dataflow, useful to define a compound Group Consent statement as in the example in the description.
  //    "channels": [
  //      {"channel": "EMAIL", "status": "OPT_IN"},
  //      {"channel": "SMS", "status": "OPT_IN"}
  //    ]
  // Mutation:
  // consent: {
  //  channels: [
  //    { channel: EMAIL, status: OPT_IN_UNVERIFIED },
  //    { channel: SMS, status: OPT_IN }
  //    ]
  //  }
  try
  {

    if (typeof config.transforms.consent !== undefined &&
      Object.keys(config.transforms.consent).length > 0)
    {

      let s: string = ""

      const t: typeof updates = []
      const channelconsents: typeof config.transforms.consent = config.transforms.consent

      //Process All Updates for this Transform
      for (const update of updates)
      {

        const channelConsentsArray = [] as object[]

        //Consent: Either Channels or consentGroups
        //"consent": {   
        //  "channels": [
        //     { "channel": "EMAIL", "status": "OPT_IN" }
        //     ]
        //"consentGroups": [
        //  {
        //  "consentGroupId": "3a7134b8-dcb5-509a-b7ff-946b48333cc9",
        //  "status": "OPT_IN"
        //  }
        //] 
        //If 'Statement' configured, takes precedence over all others
        if (typeof config.transforms.consent["statement"] !== undefined &&
          config.transforms.consent["statement"] !== "")
        {
          //Consent Statement is not the same as AddressableFields, for Consent 'Statement" simply copy 
          // what was provided in the Customer Config as the Consent Statement to provide for the operation. 
          Object.assign(update, {consent: config.transforms.consent["statement"]})

        }
        else //No 'Statement', so process individual Field definition(s)
        {
          //for (const [key, consentChannel] of Object.entries(consentChannel))
          //{
          Object.keys(channelconsents).forEach((consentChannel) => {
            //"Email": "OPT_IN_UNVERIFIED",
            //"SMS": "OPT_OUT",
            //"WhatsApp": "$.jsonfileValue",
            //"Email2": "@.csvfileColumn"
            //"statement": {}

            //forUpdateOnly????
            //consent: {channels: {channel: EMAIL, status: OPT_IN} } 
            //From Create mutation doc.  
            //consent: {
            //  channels: [
            //    {channel: EMAIL, status: OPT_IN_UNVERIFIED}
            //    {channel: SMS, status: OPT_IN}
            //  ]
            //}
            let s: string = ""
            const consentValue = channelconsents[consentChannel]

            if (consentValue.startsWith('$')) s = 'jsonpath'
            else if (consentValue.startsWith('@')) s = 'csvcolumn'
            else if (s === "" && consentValue.length > 3) s = 'static'
            else S3DB_Logging("error", "933", `Error - Transform - Consent transform has an invalid configuration.`)

            switch (s)
            {
              case 'static': {
                channelConsentsArray.push({channel: consentChannel, status: consentValue})
                break
              }
              case 'jsonpath': {
                let j = applyJSONMap(update, {consentChannel: consentValue})
                if (typeof j === "undefined" || j === "") j = "Not Found"


                channelConsentsArray.push({channel: consentChannel, status: j})
                //Object.assign(u, {"consent": consentfromJPath})
                break
              }
              case 'csvcolumn':
                {
                  //strip preceding '@.'
                  let csvmapvalue = consentValue.substring(2, consentValue.length)
                  let colRef = csvmapvalue as keyof typeof update

                  let v = update[colRef] as string
                  if (typeof v === "undefined" || v === "") v = "Not Found"

                  channelConsentsArray.push({channel: consentChannel, status: v})
                  break
                }
            }

          })

          //consent: {
          //  channels: [
          //    {channel: EMAIL, status: OPT_IN_UNVERIFIED}
          //    {channel: SMS, status: OPT_IN}
          //  ]
          //}
          Object.assign(update, {consent: {channels: channelConsentsArray}})

        }

        t.push(update)

      }

      //All Updates now transformed from consent transform
      if (t.length === updates.length)
      {
        updates = t
      }
      else
      {
        S3DB_Logging("error", "933", `Error - Transform - Applying Consent Transform returns fewer records (${t.length}) than initial set ${updates.length}`)

        throw new Error(
          `Error - Transform - Applying Consent Transform returns fewer records (${t.length}) than initial set ${updates.length}`
        )
      }

      S3DB_Logging("info", "953", `Transforms (Channel Consent) applied: \n${JSON.stringify(updates)}`)
    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "934", `Exception - Applying Consent Transform \n${e}`)
  }

  /*
    //Transform: Audience Update
    //need loop through Config audienceupdate and build audience object.
  
    try
    {
      if (typeof config.transforms.audienceupdate !== undefined &&
        Object.keys(config.transforms.audienceupdate).length > 0)
      {
        let s: string = ""
  
        const t: typeof updates = []
        const audienceUpdates: typeof config.transforms.audienceupdate = config.transforms.audienceupdate
  
        //"audienceupdate": { //Static Values: Use When Audience status is not in the Data (use JSONMap/CSVMap when Consent is in data)
        //"AudienceXYZ": "Exited",
        //"AudienceABC": "Entered",
        //"AudiencePQR": "$.jsonfileValue",
        //"AudienceMNO": "@.csvfileColumn"
  
        //Object.assign(update, {"Audience": []})
  
        //Process All Updates for this Transform
        for (const update of updates)
        {
          const audienceUpdatesArray = [] as object[]
  
          Object.keys(audienceUpdates).forEach((audUpdate) => {
  
            let s: string = ""
            const audUpdateValue = audienceUpdates[audUpdate]
  
            if (audUpdateValue.startsWith('$')) s = 'jsonpath'
            else if (audUpdateValue.startsWith('@')) s = 'csvcolumn'
            else if (s === "" && audUpdateValue.length > 3) s = 'static'
            else S3DB_Logging("error", "933", `Error - Transform - AudienceUpdate transform has an invalid configuration.`)
  
            switch (s)
            {
              case 'static': {
                audienceUpdatesArray.push({"audience": audUpdate, "status": audUpdateValue})
                //Object.assign(update, {"Audience": audienceValue})
                break
              }
              case 'jsonpath': {
  
                let j = applyJSONMap(update, {audUpdate: audUpdateValue})
                if (typeof j === "undefined" || j === "") j = "Not Found"
                audienceUpdatesArray.push({"audience": audUpdate, "status": j})
                // const ajp = applyJSONMap(update, audienceValue)
                // Object.assign(u, {"Audience": ajp})
                break
              }
              case 'csvcolumn':
                {
                  //strip preceding '@.'
                  let csvmapvalue = audUpdateValue.substring(2, audUpdateValue.length)
                  let colRef = csvmapvalue as keyof typeof update
  
                  let v = update[colRef] as string
                  if (typeof v === "undefined" || v === "") v = "Not Found"
                  audienceUpdatesArray.push({"audience": audUpdate, "status": v})
                  break
                }
            }
  
            Object.assign(update, {audience: {audience: audienceUpdatesArray}})
  
          })
  
          t.push(update)
  
        }
  
        //All Updates now transformed from AudienceUpdates transform
        if (t.length === updates.length)
        {
          updates = t
        } else
        {
          S3DB_Logging("error", "933", `Error - Transform - Applying AudienceUpdates Transform returns fewer records (${t.length}) than initial set ${updates.length}`)
  
          throw new Error(
            `Error - Transform - Applying AudienceUpdates Transform returns fewer records (${t.length}) than initial set ${updates.length}`
          )
        }
  
  
      }
  
      S3DB_Logging("info", "954", `Transforms (Audience Update) applied: \n${JSON.stringify(updates)}`)
  
    } catch (e)
    {
      debugger //catch
  
      S3DB_Logging("exception", "934", `Exception - Applying AudienceUpdates Transform \n${e}`)
    }
  */
  //When processing an Aggregator file we need to remove "Customer" column that is added by the 
  // Aggregator. Which, we will not reach here if not an Aggregator, as transforms are not applied to
  // incoming data before sending to Aggregator.
  //Delete "Customer" column from data
  try
  {
    if (config.updates.toLowerCase() === "singular") //Denotes Aggregator is used, so remove Customer
    {

      const t: typeof updates = [] //start a "Processed for Aggregator" update set
      try
      {
        for (const jo of updates)
        {
          type dk = keyof typeof jo
          const k: dk = "Customer" as dk
          delete jo[k]

          t.push(jo)
        }
      } catch (e)
      {
        debugger //catch

        S3DB_Logging("exception", "933", `Exception - Transform - Removing Aggregator Surplus "Customer" Field \n${e}`)

      }

      if (t.length === updates.length)
      {
        updates = t
      }
      else
      {
        debugger //catch

        S3DB_Logging("error", "933", `Error - Transform for Aggregator Files - Removing Surplus Customer Field returns fewer records ${t.length} than initial set ${updates.length}`)

        throw new Error(
          `Error - Transform - Transform for Aggregator Files: Removing Surplus Customer Field returns fewer records ${t.length} than initial set ${updates.length}`
        )
      }

      S3DB_Logging("info", "919", `Transform (Aggregator Files - Removing Surplus Customer Field) applied: \n${JSON.stringify(t)}`)
    }

  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "934", `Exception - Applying Transform for Aggregator Files - Removing Surplus Customer Field \n${e}`)
  }


  //Transform: Ignore
  // Ignore must be last to take advantage of cleaning up any extraneous columns after previous transforms
  try
  {
    const igno = config.transforms.ignore as typeof config.transforms.ignore ?? []

    if (typeof config.transforms.ignore !== "undefined" &&
      config.transforms.ignore.length > 0)
    {

      const t: typeof updates = [] //start an 'ignore processed' update set
      try
      {
        for (const jo of updates)
        {

          for (const ig of igno)
          {
            type dk = keyof typeof jo
            const k: dk = ig as dk
            delete jo[k]
          }
          t.push(jo)
        }
      } catch (e)
      {
        debugger //catch

        S3DB_Logging("exception", "934", `Exception - Transform - Applying Ignore - \n${e}`)

      }

      if (t.length === updates.length)
      {
        updates = t
      }
      else
      {
        debugger //catch

        S3DB_Logging("error", "933", `Error - Transform - Applying Ignore returns fewer records ${t.length} than initial set ${updates.length}`)

        throw new Error(
          `Error - Transform - Applying Ignore returns fewer records ${t.length} than initial set ${updates.length}`
        )
      }

      S3DB_Logging("info", "936", `Transforms (Ignore) applied: \n${JSON.stringify(t)}`)
    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "934", `Exception - Applying Ignore Transform \n${e}`)
  }

  return updates
}
