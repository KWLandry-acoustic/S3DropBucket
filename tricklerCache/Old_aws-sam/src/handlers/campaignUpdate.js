import { get } from "lodash";
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
function getAndParseConfigFile() {
}
function getAllFilesToProcess() {
    //create array of all files to be processed
}
function parseRows() {
    //forEach file in the array pull each row into an array up to 100 entries
    //On completion of a file - rename the file 
}
function markFileAsProcessed() {
}
function postUpdates() {
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
        description: 'A safety against mapping too many attributes into the Event, ignore Event if number of Event Attributes exceeds this maximum. Note: Before increasing the default max number, consult the Acoustic Destination documentation.',
        default: 15,
        type: 'number',
        required: false
    }
};
export function parseSections(section, nestDepth) {
    const parseResults = {};
    try {
        //if (nestDepth > 5) return parseResults
        if (nestDepth > 10)
            throw new Error('Event data exceeds nesting depth. Use Mapping to avoid nesting data attributes more than 3 levels deep');
        for (const key of Object.keys(section)) {
            if (typeof section[key] === 'object') {
                nestDepth++;
                const nested = parseSections(section[key], nestDepth);
                for (const nestedKey of Object.keys(nested)) {
                    parseResults[`${key}.${nestedKey}`] = nested[nestedKey];
                }
            }
            else {
                parseResults[key] = section[key];
            }
        }
    }
    catch (e) {
        throw new Error(`Unexpected Exception while parsing Event payload.\n ${e}`);
    }
    return parseResults;
}
const payload = {};
export function addUpdateEvents(payload, email, limit) {
    let eventName = '';
    let eventValue = '';
    let xmlRows = '';
    //Event Source
    const eventSource = get(payload, 'type', 'Null') + ' Event';
    //Timestamp
    // "timestamp": "2023-02-07T02:19:23.469Z"`
    const timestamp = get(payload, 'timestamp', 'Null');
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
    let ak = '';
    let av = '';
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
    if (av !== '') {
        let audiStatus = av;
        eventValue = audiStatus;
        audiStatus = audiStatus.toString().toLowerCase();
        if (audiStatus === 'true')
            eventValue = 'Audience Entered';
        if (audiStatus === 'false')
            eventValue = 'Audience Exited';
        eventName = ak;
        xmlRows += `  
      <ROW>
      <COLUMN name="EMAIL">           <![CDATA[${email}]]></COLUMN>
      <COLUMN name="EventSource">     <![CDATA[${eventSource}]]></COLUMN>  
      <COLUMN name="EventName">       <![CDATA[${eventName}]]></COLUMN>
      <COLUMN name="EventValue">      <![CDATA[${eventValue}]]></COLUMN>
      <COLUMN name="Event Timestamp"> <![CDATA[${timestamp}]]></COLUMN>
      </ROW>`;
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
    return xmlRows;
}
const settings = "";
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
//   //   // TODO: verify multiple emails are handled
//   //   const filename = payloads[0].filename
//   //   const fileContent = Buffer.from(rows.join('\n'))
//   //return { filename, fileContent }  
//   const filename = "eventfile.json"
//   const fileContent = Buffer.from("")
//   return { filename, fileContent }
// }
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY2FtcGFpZ25VcGRhdGUuanMiLCJzb3VyY2VSb290IjoiL1VzZXJzL2t3bGFuZHJ5L0Ryb3Bib3gvRG9jdW1lbnRzL0EuQ2FtcGFpZ24vQ29kZUZvbGRlci9BLlRyaWNrbGVyQ2FjaGVfY29kZWNvbW1pdC90cmlja2xlckNhY2hlLyIsInNvdXJjZXMiOlsic3JjL2hhbmRsZXJzL2NhbXBhaWduVXBkYXRlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBLE9BQU8sRUFBRSxHQUFHLEVBQUUsTUFBTSxRQUFRLENBQUE7QUFFNUIsc0VBQXNFO0FBQ3RFLDBCQUEwQjtBQUMxQiwyQkFBMkI7QUFDM0IsOEVBQThFO0FBQzlFLHdHQUF3RztBQUN4Ryw0RkFBNEY7QUFDNUYsc0NBQXNDO0FBQ3RDLDRGQUE0RjtBQUM1RixtREFBbUQ7QUFDbkQseUJBQXlCO0FBQ3pCLGlGQUFpRjtBQUdqRixTQUFTLHFCQUFxQjtBQUU5QixDQUFDO0FBRUQsU0FBUyxvQkFBb0I7SUFDM0IsMkNBQTJDO0FBRTdDLENBQUM7QUFFRCxTQUFTLFNBQVM7SUFDZCx5RUFBeUU7SUFDekUsNENBQTRDO0FBQ2hELENBQUM7QUFFRCxTQUFTLG1CQUFtQjtBQUU1QixDQUFDO0FBRUQsU0FBUyxXQUFXO0FBRXBCLENBQUM7QUFFRCxNQUFNLEVBQUUsR0FBRztJQUNQLEdBQUcsRUFBRTtRQUNELEtBQUssRUFBRSxLQUFLO1FBQ1osV0FBVyxFQUFFLDZCQUE2QjtRQUMxQyxPQUFPLEVBQUUsR0FBRztRQUNaLElBQUksRUFBRSxRQUFRO1FBQ2QsUUFBUSxFQUFFLElBQUk7S0FDZjtJQUNELE1BQU0sRUFBRTtRQUNOLEtBQUssRUFBRSxRQUFRO1FBQ2YsV0FBVyxFQUFFLG1EQUFtRDtRQUNoRSxPQUFPLEVBQUU7WUFDUCxFQUFFLEtBQUssRUFBRSxJQUFJLEVBQUUsS0FBSyxFQUFFLElBQUksRUFBRTtZQUM1QixFQUFFLEtBQUssRUFBRSxJQUFJLEVBQUUsS0FBSyxFQUFFLElBQUksRUFBRTtZQUM1QixFQUFFLEtBQUssRUFBRSxJQUFJLEVBQUUsS0FBSyxFQUFFLElBQUksRUFBRTtZQUM1QixFQUFFLEtBQUssRUFBRSxJQUFJLEVBQUUsS0FBSyxFQUFFLElBQUksRUFBRTtTQUM3QjtRQUNELE9BQU8sRUFBRSxJQUFJO1FBQ2IsSUFBSSxFQUFFLFFBQVE7UUFDZCxRQUFRLEVBQUUsSUFBSTtLQUNmO0lBQ0QsU0FBUyxFQUFFO1FBQ1QsS0FBSyxFQUFFLDZCQUE2QjtRQUNwQyxXQUFXLEVBQUUsMERBQTBEO1FBQ3ZFLE9BQU8sRUFBRSwyQkFBMkI7UUFDcEMsSUFBSSxFQUFFLFFBQVE7UUFDZCxRQUFRLEVBQUUsSUFBSTtLQUNmO0lBQ0QsV0FBVyxFQUFFO1FBQ1gsS0FBSyxFQUFFLGdDQUFnQztRQUN2QyxXQUFXLEVBQUUsMEZBQTBGO1FBQ3ZHLE9BQU8sRUFBRSxFQUFFO1FBQ1gsSUFBSSxFQUFFLFFBQVE7UUFDZCxRQUFRLEVBQUUsSUFBSTtLQUNmO0lBQ0QsVUFBVSxFQUFFO1FBQ1YsS0FBSyxFQUFFLGtDQUFrQztRQUN6QyxXQUFXLEVBQUUsbUVBQW1FO1FBQ2hGLE9BQU8sRUFBRSxFQUFFO1FBQ1gsSUFBSSxFQUFFLFFBQVE7UUFDZCxRQUFRLEVBQUUsSUFBSTtLQUNmO0lBQ0QsY0FBYyxFQUFFO1FBQ2QsS0FBSyxFQUFFLHNDQUFzQztRQUM3QyxXQUFXLEVBQUUsdUVBQXVFO1FBQ3BGLE9BQU8sRUFBRSxFQUFFO1FBQ1gsSUFBSSxFQUFFLFVBQVU7UUFDaEIsUUFBUSxFQUFFLElBQUk7S0FDZjtJQUNELGNBQWMsRUFBRTtRQUNkLEtBQUssRUFBRSw2Q0FBNkM7UUFDcEQsV0FBVyxFQUFFLGlGQUFpRjtRQUM5RixPQUFPLEVBQUUsRUFBRTtRQUNYLElBQUksRUFBRSxVQUFVO1FBQ2hCLFFBQVEsRUFBRSxJQUFJO0tBQ2Y7SUFDRCxhQUFhLEVBQUU7UUFDYixLQUFLLEVBQUUsZ0JBQWdCO1FBQ3ZCLFdBQVcsRUFDVCwrTkFBK047UUFDak8sT0FBTyxFQUFFLEVBQUU7UUFDWCxJQUFJLEVBQUUsUUFBUTtRQUNkLFFBQVEsRUFBRSxLQUFLO0tBQ2hCO0NBQ0YsQ0FBQTtBQUdMLE1BQU0sVUFBVSxhQUFhLENBQUMsT0FBa0MsRUFBRSxTQUFpQjtJQUNqRixNQUFNLFlBQVksR0FBOEIsRUFBRSxDQUFBO0lBQ2xELElBQUk7UUFDRix3Q0FBd0M7UUFDeEMsSUFBSSxTQUFTLEdBQUcsRUFBRTtZQUNoQixNQUFNLElBQUksS0FBSyxDQUNiLHdHQUF3RyxDQUFDLENBQUE7UUFFN0csS0FBSyxNQUFNLEdBQUcsSUFBSSxNQUFNLENBQUMsSUFBSSxDQUFDLE9BQU8sQ0FBQyxFQUFFO1lBQ3RDLElBQUksT0FBTyxPQUFPLENBQUMsR0FBRyxDQUFDLEtBQUssUUFBUSxFQUFFO2dCQUNwQyxTQUFTLEVBQUUsQ0FBQTtnQkFDWCxNQUFNLE1BQU0sR0FBOEIsYUFBYSxDQUNyRCxPQUFPLENBQUMsR0FBRyxDQUFvQyxFQUMvQyxTQUFTLENBQ1YsQ0FBQTtnQkFDRCxLQUFLLE1BQU0sU0FBUyxJQUFJLE1BQU0sQ0FBQyxJQUFJLENBQUMsTUFBTSxDQUFDLEVBQUU7b0JBQzNDLFlBQVksQ0FBQyxHQUFHLEdBQUcsSUFBSSxTQUFTLEVBQUUsQ0FBQyxHQUFHLE1BQU0sQ0FBQyxTQUFTLENBQUMsQ0FBQTtpQkFDeEQ7YUFDRjtpQkFBTTtnQkFDTCxZQUFZLENBQUMsR0FBRyxDQUFDLEdBQUcsT0FBTyxDQUFDLEdBQUcsQ0FBQyxDQUFBO2FBQ2pDO1NBQ0Y7S0FDRjtJQUFDLE9BQU8sQ0FBQyxFQUFFO1FBQ1YsTUFBTSxJQUFJLEtBQUssQ0FDYix1REFBdUQsQ0FBQyxFQUFFLENBQzNELENBQUE7S0FDRjtJQUNELE9BQU8sWUFBWSxDQUFBO0FBQ3JCLENBQUM7QUFDRCxNQUFNLE9BQU8sR0FBOEIsRUFBRSxDQUFBO0FBRTdDLE1BQU0sVUFBVSxlQUFlLENBQUMsT0FBa0MsRUFBRSxLQUFhLEVBQUUsS0FBYTtJQUM5RixJQUFJLFNBQVMsR0FBRyxFQUFFLENBQUE7SUFDbEIsSUFBSSxVQUFVLEdBQUcsRUFBRSxDQUFBO0lBQ25CLElBQUksT0FBTyxHQUFHLEVBQUUsQ0FBQTtJQUVoQixjQUFjO0lBQ2QsTUFBTSxXQUFXLEdBQUcsR0FBRyxDQUFDLE9BQU8sRUFBRSxNQUFNLEVBQUUsTUFBTSxDQUFDLEdBQUcsUUFBUSxDQUFBO0lBRTNELFdBQVc7SUFDWCwyQ0FBMkM7SUFDM0MsTUFBTSxTQUFTLEdBQUcsR0FBRyxDQUFDLE9BQU8sRUFBRSxXQUFXLEVBQUUsTUFBTSxDQUFDLENBQUE7SUFFbkQseURBQXlEO0lBQ3pELFFBQVE7SUFDUixpQ0FBaUM7SUFDakMsNkJBQTZCO0lBQzdCLCtCQUErQjtJQUMvQixrRkFBa0Y7SUFDbEYsUUFBUTtJQUVSLDRCQUE0QjtJQUM1Qiw2QkFBNkI7SUFDN0IsK0JBQStCO0lBQy9CLHdGQUF3RjtJQUN4RixRQUFRO0lBRVIsd0JBQXdCO0lBQ3hCLDZCQUE2QjtJQUM3QiwrQkFBK0I7SUFDL0IseUVBQXlFO0lBQ3pFLFFBQVE7SUFDUiw0QkFBNEI7SUFDNUIsNkJBQTZCO0lBQzdCLCtCQUErQjtJQUMvQiw2RUFBNkU7SUFDN0UsUUFBUTtJQUNSLHlCQUF5QjtJQUN6Qiw2QkFBNkI7SUFDN0IsK0JBQStCO0lBQy9CLDBFQUEwRTtJQUMxRSxRQUFRO0lBQ1IsZ0JBQWdCO0lBQ2hCLHFCQUFxQjtJQUNyQix5REFBeUQ7SUFDekQsSUFBSTtJQUVKLHNDQUFzQztJQUN0QyxtREFBbUQ7SUFDbkQsbUJBQW1CO0lBQ25CLHFCQUFxQjtJQUNyQixrTUFBa007SUFDbE0sSUFBSTtJQUVKLFVBQVU7SUFDVix1SkFBdUo7SUFDdkosSUFBSTtJQUNKLHdCQUF3QjtJQUN4QixvQkFBb0I7SUFDcEIsZ0JBQWdCO0lBQ2hCLDBFQUEwRTtJQUMxRSxNQUFNO0lBQ04sSUFBSTtJQUNKLDRHQUE0RztJQUM1RyxJQUFJO0lBQ0oscUJBQXFCO0lBQ3JCLG9CQUFvQjtJQUNwQixvRkFBb0Y7SUFDcEYsb0JBQW9CO0lBQ3BCLDZDQUE2QztJQUM3QywwRUFBMEU7SUFDMUUsTUFBTTtJQUNOLElBQUk7SUFFSixJQUFJLEVBQUUsR0FBRyxFQUFFLENBQUE7SUFDWCxJQUFJLEVBQUUsR0FBRyxFQUFFLENBQUE7SUFFWCxxSEFBcUg7SUFDckgsbUhBQW1IO0lBRW5ILHlGQUF5RjtJQUN6Rix5REFBeUQ7SUFDekQsK0NBQStDO0lBRS9DLG9GQUFvRjtJQUNwRiw0REFBNEQ7SUFDNUQsc0NBQXNDO0lBQ3RDLHNEQUFzRDtJQUN0RCxzQ0FBc0M7SUFDdEMsdUNBQXVDO0lBQ3ZDLElBQUk7SUFFSixzREFBc0Q7SUFDdEQsc0RBQXNEO0lBQ3RELCtDQUErQztJQUUvQyxvRkFBb0Y7SUFDcEYseURBQXlEO0lBQ3pELHNDQUFzQztJQUN0Qyx1Q0FBdUM7SUFDdkMsSUFBSTtJQUVKLElBQUksRUFBRSxLQUFLLEVBQUUsRUFBRTtRQUNiLElBQUksVUFBVSxHQUFHLEVBQUUsQ0FBQTtRQUVuQixVQUFVLEdBQUcsVUFBVSxDQUFBO1FBQ3ZCLFVBQVUsR0FBRyxVQUFVLENBQUMsUUFBUSxFQUFFLENBQUMsV0FBVyxFQUFFLENBQUE7UUFDaEQsSUFBSSxVQUFVLEtBQUssTUFBTTtZQUFFLFVBQVUsR0FBRyxrQkFBa0IsQ0FBQTtRQUMxRCxJQUFJLFVBQVUsS0FBSyxPQUFPO1lBQUUsVUFBVSxHQUFHLGlCQUFpQixDQUFBO1FBRTFELFNBQVMsR0FBRyxFQUFFLENBQUE7UUFFZCxPQUFPLElBQUk7O2lEQUVrQyxLQUFLO2lEQUNMLFdBQVc7aURBQ1gsU0FBUztpREFDVCxVQUFVO2lEQUNWLFNBQVM7YUFDN0MsQ0FBQTtLQUNWO0lBRUQsd0NBQXdDO0lBQ3hDLHdDQUF3QztJQUN4Qyx3QkFBd0I7SUFDeEIsNkNBQTZDO0lBRTdDLGlCQUFpQjtJQUNqQixXQUFXO0lBQ1gsbUVBQW1FO0lBQ25FLDJFQUEyRTtJQUMzRSx1RUFBdUU7SUFDdkUsd0VBQXdFO0lBQ3hFLHVFQUF1RTtJQUN2RSxhQUFhO0lBQ2IsSUFBSTtJQUNKLE9BQU8sT0FBTyxDQUFBO0FBQ2hCLENBQUM7QUFFRCxNQUFNLFFBQVEsR0FBRyxFQUFFLENBQUE7QUFFbkIseUdBQXlHO0FBQ3pHLGtHQUFrRztBQUNsRyxNQUFNO0FBQ04sdUJBQXVCO0FBQ3ZCLGlCQUFpQjtBQUVqQix5SEFBeUg7QUFDekgsc0JBQXNCO0FBQ3RCLGlCQUFpQjtBQUNqQixxREFBcUQ7QUFDckQsb0NBQW9DO0FBQ3BDLHlFQUF5RTtBQUN6RSxrQ0FBa0M7QUFDbEMsZ0RBQWdEO0FBQ2hELHNCQUFzQjtBQUN0QixTQUFTO0FBQ1Qsc0JBQXNCO0FBQ3RCLE9BQU87QUFFUCw2Q0FBNkM7QUFFN0Msa0ZBQWtGO0FBQ2xGLG9CQUFvQjtBQUVwQiw0RUFBNEU7QUFDNUUsc0RBQXNEO0FBQ3RELHNEQUFzRDtBQUV0RCwwQ0FBMEM7QUFFMUMsNkRBQTZEO0FBQzdELGdIQUFnSDtBQUNoSCxRQUFRO0FBRVIscUJBQXFCO0FBQ3JCLElBQUk7QUFLSixtREFBbUQ7QUFDbkQsYUFBYTtBQUNiLHlCQUF5QjtBQUN6QiwwQ0FBMEM7QUFFMUMsNkJBQTZCO0FBQzdCLDBDQUEwQztBQUMxQyw2RkFBNkY7QUFDN0Ysb0NBQW9DO0FBQ3BDLFdBQVc7QUFDWCxTQUFTO0FBQ1QsUUFBUTtBQUNSLG1EQUFtRDtBQUNuRCxzR0FBc0c7QUFDdEcsb0NBQW9DO0FBQ3BDLFdBQVc7QUFDWCxTQUFTO0FBQ1Qsc0RBQXNEO0FBRXRELDRCQUE0QjtBQUM1Qix5Q0FBeUM7QUFDekMsd0JBQXdCO0FBQ3hCLHdDQUF3QztBQUN4Qyx3Q0FBd0M7QUFDeEMsMkZBQTJGO0FBQzNGLHFFQUFxRTtBQUNyRSxhQUFhO0FBQ2IsV0FBVztBQUVYLGlEQUFpRDtBQUNqRCxvR0FBb0c7QUFDcEcsb0ZBQW9GO0FBQ3BGLGFBQWE7QUFDYixXQUFXO0FBQ1gsZ0VBQWdFO0FBQ2hFLFNBQVM7QUFFVCxxREFBcUQ7QUFDckQsK0NBQStDO0FBQy9DLDBEQUEwRDtBQUUxRCx5Q0FBeUM7QUFFekMsc0NBQXNDO0FBQ3RDLHdDQUF3QztBQUN4QyxxQ0FBcUM7QUFDckMsSUFBSSJ9