import { get } from "lodash"

//Get Config file from S3 location that has triggered this invocation 
//Parse config into consts
//Get all S3 Object (File) 
//Parse each file into a large array (how large can this array be in Lambda? )
//              (better to parse up to a limit, process those updates then return to parsing for more?) 
//  Parse up to 100 csv rows into an Array (parse all files in this run into a large Array?)
//    Make Update call - POST Updates 
//    On success, delete array (or remove those entries successfully updated from the array)
//  Continue Parsing file(s) until next 100 Updates
//  Post next 100 Updates
//Stop/return once all data processed - no additional Files with Customer Prefix 


function getAndParseConfigFile () {

}

function getAllFilesToProcess () {
  //create array of all files to be processed

}

function parseRows () {
  //forEach file in the array pull each row into an array up to 100 entries
  //On completion of a file - rename the file 
}

function markFileAsProcessed () {

}

function postUpdates () {

}

const _a = {
  pod: {
    label: 'Pod',
    description: 'Pod Number for API Endpoint',
    default: '2',
    type: 'string',
    required: true
  },
  region: {
    label: 'Region',
    description: 'Region for API Endpoint, either US, EU, AP, or CA',
    choices: [
      { label: 'US', value: 'US' },
      { label: 'EU', value: 'EU' },
      { label: 'AP', value: 'AP' },
      { label: 'CA', value: 'CA' }
    ],
    default: 'US',
    type: 'string',
    required: true
  },
  tableName: {
    label: 'Acoustic Segment Table Name',
    description: `The Segment Table Name in Acoustic Campaign Data dialog.`,
    default: 'Segment Events Table Name',
    type: 'string',
    required: true
  },
  tableListId: {
    label: 'Acoustic Segment Table List Id',
    description: 'The Segment Table List Id from the Database-Relational Table dialog in Acoustic Campaign',
    default: '',
    type: 'string',
    required: true
  },
  a_clientId: {
    label: 'Acoustic App Definition ClientId',
    description: 'The Client Id from the App definition dialog in Acoustic Campaign',
    default: '',
    type: 'string',
    required: true
  },
  a_clientSecret: {
    label: 'Acoustic App Definition ClientSecret',
    description: 'The Client Secret from the App definition dialog in Acoustic Campaign',
    default: '',
    type: 'password',
    required: true
  },
  a_refreshToken: {
    label: 'Acoustic App Access Definition RefreshToken',
    description: 'The RefreshToken provided when defining access for the App in Acoustic Campaign',
    default: '',
    type: 'password',
    required: true
  },
  attributesMax: {
    label: 'Properties Max',
    description:
      'A safety against mapping too many attributes into the Event, ignore Event if number of Event Attributes exceeds this maximum. Note: Before increasing the default max number, consult the Acoustic Destination documentation.',
    default: 15,
    type: 'number',
    required: false
  }
}


export function parseSections (section: { [key: string]: string }, nestDepth: number) {
  const parseResults: { [key: string]: string } = {}
  try
  {
    //if (nestDepth > 5) return parseResults
    if (nestDepth > 10)
      throw new Error(
        'Event data exceeds nesting depth. Use Mapping to avoid nesting data attributes more than 3 levels deep')

    for (const key of Object.keys(section))
    {
      if (typeof section[key] === 'object')
      {
        nestDepth++
        const nested: { [key: string]: string } = parseSections(
          section[key] as {} as { [key: string]: string },
          nestDepth
        )
        for (const nestedKey of Object.keys(nested))
        {
          parseResults[`${key}.${nestedKey}`] = nested[nestedKey]
        }
      } else
      {
        parseResults[key] = section[key]
      }
    }
  } catch (e)
  {
    throw new Error(
      `Unexpected Exception while parsing Event payload.\n ${e}`
    )
  }
  return parseResults
}
const payload: { [key: string]: string } = {}

export function addUpdateEvents (payload: { [key: string]: string }, email: string, limit: number) {
  let eventName = ''
  let eventValue = ''
  let xmlRows = ''

  //Event Source
  const eventSource = get(payload, 'type', 'Null') + ' Event'

  //Timestamp
  // "timestamp": "2023-02-07T02:19:23.469Z"`
  const timestamp = get(payload, 'timestamp', 'Null')

  // let propertiesTraitsKV: { [key: string]: string } = {}
  // try {
  //   if (payload.key_value_pairs)
  //     propertiesTraitsKV = {
  //       ...propertiesTraitsKV,
  //       ...parseSections(payload.key_value_pairs as { [key: string]: string }, 0)
  //     }

  //   if (payload.array_data)
  //     propertiesTraitsKV = {
  //       ...propertiesTraitsKV,
  //       ...parseSections(payload.array_data as unknown as { [key: string]: string }, 0)
  //     }

  //   if (payload.traits)
  //     propertiesTraitsKV = {
  //       ...propertiesTraitsKV,
  //       ...parseSections(payload.traits as { [key: string]: string }, 0)
  //     }
  //   if (payload.properties)
  //     propertiesTraitsKV = {
  //       ...propertiesTraitsKV,
  //       ...parseSections(payload.properties as { [key: string]: string }, 0)
  //     }
  //   if (payload.context)
  //     propertiesTraitsKV = {
  //       ...propertiesTraitsKV,
  //       ...parseSections(payload.context as { [key: string]: string }, 0)
  //     }
  // } catch (e) {
  //   throw new Error(
  //     'Unexpected Exception processing payload \n ${e}')
  // }

  // //Check Size - Number of Attributes
  // const l = Object.keys(propertiesTraitsKV).length
  // if (l > limit) {
  //   throw new Error(
  //     `There are ${l} Attributes in this Event. This exceeds the max of ${limit}. Use Mapping to limit the number of Attributes and thereby reduce the Campaign Relational Table Rows consumed.`)
  // }

  //Audience
  //   Identify events generated by an audience have the audience key set to true or false based on whether the user is entering or exiting the audience:
  // {
  //   "type": "identify",
  //   "userId": u123,
  //   "traits": {
  //      "first_time_shopper": true // false when a user exits the audience
  //   }
  // }
  // Track events generated by an audience have a key for the audience name, and a key for the audience value:
  // {
  //   "type": "track",
  //   "userId": u123,
  //   "event": "Audience Entered", // "Audience Exited" when a user exits an audience
  //   "properties": {
  //      "audience_key": "first_time_shopper",
  //      "first_time_shopper": true // false when a user exits the audience
  //   }
  // }

  let ak = ''
  let av = ''

  // const getValue = (o: object, part: string) => Object.entries(o).find(([k, _v]) => k.includes(part))?.[1] as string
  // const getKey = (o: object, part: string) => Object.entries(o).find(([k, _v]) => k.includes(part))?.[0] as string

  // if (getValue(propertiesTraitsKV, 'computation_class')?.toLowerCase() === 'audience') {
  //   ak = getValue(propertiesTraitsKV, 'computation_key')
  //   av = getValue(propertiesTraitsKV, `${ak}`)

  //   //Audience determined, clean out parsed attributes, reduce redundant attributes
  //   let x = getKey(propertiesTraitsKV, 'computation_class')
  //   delete propertiesTraitsKV[`${x}`]
  //   x = getKey(propertiesTraitsKV, 'computation_key')
  //   delete propertiesTraitsKV[`${x}`]
  //   delete propertiesTraitsKV[`${ak}`]
  // }

  // if (getValue(propertiesTraitsKV, 'audience_key')) {
  //   ak = getValue(propertiesTraitsKV, 'audience_key')
  //   av = getValue(propertiesTraitsKV, `${ak}`)

  //   //Audience determined, clean out parsed attributes, reduce redundant attributes
  //   const x = getKey(propertiesTraitsKV, 'audience_key')
  //   delete propertiesTraitsKV[`${x}`]
  //   delete propertiesTraitsKV[`${ak}`]
  // }

  if (av !== '')
  {
    let audiStatus = av

    eventValue = audiStatus
    audiStatus = audiStatus.toString().toLowerCase()
    if (audiStatus === 'true') eventValue = 'Audience Entered'
    if (audiStatus === 'false') eventValue = 'Audience Exited'

    eventName = ak

    xmlRows += `  
      <ROW>
      <COLUMN name="EMAIL">           <![CDATA[${email}]]></COLUMN>
      <COLUMN name="EventSource">     <![CDATA[${eventSource}]]></COLUMN>  
      <COLUMN name="EventName">       <![CDATA[${eventName}]]></COLUMN>
      <COLUMN name="EventValue">      <![CDATA[${eventValue}]]></COLUMN>
      <COLUMN name="Event Timestamp"> <![CDATA[${timestamp}]]></COLUMN>
      </ROW>`
  }

  // //Wrap Properties and Traits into XML
  // for (const e in propertiesTraitsKV) {
  //   const eventName = e
  //   const eventValue = propertiesTraitsKV[e]

  //   xmlRows += `
  //    <ROW>
  //    <COLUMN name="Email">           <![CDATA[${email}]]></COLUMN>
  //    <COLUMN name="EventSource">     <![CDATA[${eventSource}]]></COLUMN>  
  //    <COLUMN name="EventName">       <![CDATA[${eventName}]]></COLUMN>
  //    <COLUMN name="EventValue">      <![CDATA[${eventValue}]]></COLUMN>
  //    <COLUMN name="Event Timestamp"> <![CDATA[${timestamp}]]></COLUMN>
  //    </ROW>`
  // }
  return xmlRows
}

const settings = ""

// export async function doPOST( request: RequestClient, settings: Settings, body: string, action: string
//   export async function doPOST( request: string, settings: string, body: string, action: string
// ) {
//   let resultTxt = ''
//   let res = ''

//   const postResults = await request(`https://api-campaign-${settings.region}-${settings.pod}.goacoustic.com/XMLAPI`, {
//     method: 'POST',
//     headers: {
//       Authorization: `Bearer ${getAccessToken()}`,
//       'Content-Type': 'text/xml',
//       'user-agent': `Segment Action (Acoustic Destination) ${action}`,
//       Connection: 'keep-alive',
//       'Accept-Encoding': 'gzip, deflate, br',
//       Accept: '*/*'
//     },
//     body: `${body}`
//   })

//   res = (await postResults.data) as string

//   //check for success, hard fails throw error, soft fails throw retryable error
//   resultTxt = res

//   if (resultTxt.toLowerCase().indexOf('<success>false</success>') > -1) {
//     const rx = /<FaultString>(.*)<\/FaultString>/gm
//     const r = rx.exec(resultTxt) as RegExpExecArray

//     const faultMsg = r[1].toLowerCase()

//     if (faultMsg.indexOf('max number of concurrent') > -1)
//       throw new Error('Currently exceeding Max number of concurrent API authenticated requests, retrying...')
//     }

//   return resultTxt
// }




// export function buildFile(payloads: Payload[]) {
//   payloads
//   //   const rows = []
//   //   const headers = ['audience_key']

//   // // Prepare header row
//   // if (payloads[0].identifier_data) {
//   //   for (const identifier of Object.getOwnPropertyNames(payloads[0].identifier_data)) {
//   //     headers.push(identifier)
//   //   }
//   // }
//   // 
//   // if (payloads[0].unhashed_identifier_data) {
//   //   for (const identifier of Object.getOwnPropertyNames(payloads[0].unhashed_identifier_data)) {
//   //     headers.push(identifier)
//   //   }
//   // }
//   // rows.push(headers.join(payloads[0].delimiter))

//   // // Prepare data rows
//   // for (const payload of payloads) {
//   //   const row = []
//   //   row.push(payload.audience_key)
//   //   if (payload.identifier_data) {
//   //     for (const identifier of Object.getOwnPropertyNames(payload.identifier_data)) {
//   //       row.push(payload.identifier_data[identifier] as string)
//   //     }
//   //   }

//   //   if (payload.unhashed_identifier_data) {
//   //     for (const identifier of Object.getOwnPropertyNames(payload.unhashed_identifier_data)) {
//   //       row.push(hash(payload.unhashed_identifier_data[identifier] as string))
//   //     }
//   //   }
//   //   rows.push(row.map(wrapQuotes).join(payload.delimiter))
//   // }

//   // 
//   //   const filename = payloads[0].filename
//   //   const fileContent = Buffer.from(rows.join('\n'))

//   //return { filename, fileContent }  

//   const filename = "eventfile.json"
//   const fileContent = Buffer.from("")
//   return { filename, fileContent }
// }

