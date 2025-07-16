/* eslint-disable no-debugger */
"use strict"
import {type CustomerConfig, s3dbConfig, S3DB_Logging} from './s3DropBucket'

interface ContactAttribute {
  name: string
  value: any
}

interface ContactInput {
  attributes: ContactAttribute[]
  consent?: {id: string}
}

interface CreateContactRecord {
  contactInput: ContactInput[]
}

interface CreateContactsVariables {
  contactInput: CreateContactRecord[]
}



//interface updateTo {
//  attributes: ContactInput []
//}
interface UpdateContactInput {
  key?: string
  addressable?: Array<{
    field: string
    eq: string
  }>
  to: {
    attributes: Array<{
      name: string
      value: string
    }>
    consent: {
        channels?: [
          {channel?: string, status?: string}
        ]
      consentGroups?: [
        {consentGroupId?: string, status?: string},
        {
          consentGroupId?: string
          channels?: [
            {channel?: string, status?: string}
          ]
        }
      ]
    }
  }
}

interface UpdateContactRecord {
  updateContactInputs: UpdateContactInput[]
}

interface UpdateContactsVariables {
  updateContactInputs: UpdateContactRecord[]
}



/*    Future notes:
    try
    {
      if (config.updatetype.toLowerCase() === "referenceset")
      {
        
    mutation = `mutation CreateImportJob {
      createImportJob(
          importInput: {
              fileFormat: DELIMITED
              delimiter: "\\n"
              importType: ADD_UPDATE
              notifications: null
              mappings: { columnIndex: null, attributeName: null }
              createSegment: true
              segmentName: "S3DBSegment"
              consent: {
                  enableOverrideExistingOptOut: null
                  channels: null
                  consentGroups: null
              }
              dateFormat: MONTH_DAY_YEAR_SLASH_SEPARATED
              dataSetId: "1234"
              dataSetName: "xyz"
              dataSetType: REFERENCE_SET
              jobName: "S3DBRefSet"
              fileLocation: { type: SFTP, folder: "S3DBFolder", filename: "S3DBFile" }
              attributes: { create: null }
              # When columnIndex is used for mapping, skipFirstRow determines whether the first row contains headers that should not be ingested as data When columnHeader is used for mapping, skipFirstRow must either not be supplied, or set to true.
              skipFirstRow: null
          }
      ) {
          id
      }
  }`
  
      mutation = {
          mutation: {
            createImportJob: {
              importInput: {
                fileFormat: "DELIMITED",
                delimiter: "\n",
                importType: "ADD_UPDATE",
                mappings: {
                  columnIndex: 0,
                  attributeName: "id"
                },
                createSegment: true,
                segmentName: "S3DBSegment",
                consent: {
                  enableOverrideExistingOptOut: false,
                  channels: [],
                  consentGroups: []
                },
                dateFormat: "MONTH_DAY_YEAR_SLASH_SEPARATED",
                dataSetId: "1234",
                dataSetName: "xyz",
                dataSetType: "REFERENCE_SET",
                jobName: "S3DBRefSet",
                fileLocation: {
                  type: "SFTP",
                  folder: "S3DBFolder",
                  filename: "S3DBFile"
                },
                attributes: {
                  create: []
                },
                skipFirstRow: false
              }
            },
            fields: {
              id: true,
              status: true,
              message: true
            }
          }
        }
  
        mutation.mutation.createImportJob.importInput.
  
        
  
        //let createVariables = {} as CreateContactsVariables
        variables = {dataSetId: config.datasetid, contactsInput: []} as CreateContactsVariables
  
        for (const upd in updates)
        {
          //Audience is just a placeholder for now, so removing until it is valid in graphQL type
          //let ccr: CreateContactsInput = {"to": {"audience":[], "consent":[], "attributes": []}}
  
          let ca: ContactAttribute = {name: "", value: ""}
          let ccr: CreateContactRecord = {attributes: []}
          let cci: CreateContactInput = {attributes: []}
  
          variables.contactsInput.push(cci)
  
          //Build ContactId, ContactKey, AddressableFields, Consent, Audience properties
          const u = updates[upd] as Record<string, any>
  
          //ToDo: Add logic CreateContact vs UpdateContact Key/Id/Addressable fields
          //Create:
          //No Key, No Addressable but UniqueId must be in the data
          //if (typeof u.contactId !== "undefined") Object.assign(variables.contactsInput[upd], {contactId: u.contactId})
          //else if (typeof u.contactKey !== "undefined") Object.assign(variables.contactsInput[upd], {key: u.contactKey})
          //else if (typeof u.addressable !== "undefined") Object.assign(variables.contactsInput[upd], {addressable: u.addressable})
  
          //if (typeof u.consent !== "undefined") Object.assign(variables.contactsInput, {consent: u.consent})
          if (typeof u.consent !== "undefined") Object.assign(cci, {consent: u.consent})
  
  
          //Audience is just a placeholder for now, so removing until it is valid in graphQL type
          //if (typeof u.audience !== "undefined") Object.assign(variables.contactsInput, {audience: u.audience})
  
          //Add S3DBConfirmation value as a means to quickly confirm Testing outcomes
          const now: Date = new Date()
          const date: string = now.toLocaleDateString()
          const time: string = now.toLocaleTimeString()
          cci.attributes.push({name: "S3DBConfirmation", value: date + " - " + time})
          //variables.contactsInput[upd].attributes[upd] = {name: "S3DBConfirmation", value: date + " - " + time}
  
  
          //Build Attribute Array from each row of inbound data
          for (const [key, value] of Object.entries(u))
          {
            //let v
            //if (typeof value === "string") v = value
            //else v = String(value)
  
            //Skip Payload Vars, Vars injected to carry Transformed Values and are already processed above,
            // and are not valid for the actual Update
            if (!["contactid", "contactkey", "addressable", "consent", "audience"].includes(key.toLowerCase()))
            {
              ca = {name: key, value: value}
              cci.attributes.push(ca)
            }
  
          }
          Object.assign(variables.contactsInput[upd], cci)
          //variables.contactsInput.push(cci)
        }
  
        S3DB_Logging("info", "817", `Create Multiple Contacts Mutation: \n${mutation} and Vars: \n${JSON.stringify(variables)}`)
        return {mutation, variables}
  
      }
    } catch (e)
    {
      S3DB_Logging("exception", "", `Exception - Build Mutations - CreateUpdateContacts - ${e}`)
      debugger //catch
    }
  
  */

export async function buildMutationsConnect (updates: object[], config: CustomerConfig) {

  try
  {
    if (config.updatetype.toLowerCase() === "createupdatecontacts")
    {

      //From Schema
      //mutation CreateContacts {
      //        createContacts(
      //          contactsInput: {
      //          attributes: {name: null, value: null}
      //            consent: {channels: null, consentGroups: null}
      //        }
      //        ) {
      //        items {
      //            contactKey
      //            message
      //            identifyingField {
      //              value
      //                attributeData {
      //                type
      //                decimalPrecision
      //                name
      //                category
      //                mapAs
      //                validateAs
      //                defaultDisplayText
      //                    tracking {
      //                  createdBy
      //                  createdAt
      //                  lastModifiedBy
      //                  lastModifiedAt
      //                }
      //                    identifyAs {
      //                  channels
      //                  key
      //                  index
      //                }
      //              }
      //            }
      //          }
      //        }
      //      }

      //create_mutation = `mutation S3DropBucketCreateMutation (
      //  $contactsData: [ContactCreateInput!]!
      //  ) 
      //  {
      //      createContacts(
      //      contactsInput: $contactsData
      //      ) {
      //            items {
      //                  contactKey
      //                  message
      //                  identifyingField {
      //                        value
      //                        attributeData {
      //                              type
      //                              decimalPrecision
      //                              name
      //                              category
      //                              mapAs
      //                              validateAs
      //                              identifyAs {
      //                                    channels
      //                                    key
      //                                    index
      //                              }
      //                              tracking {
      //                                    createdBy
      //                                    createdAt
      //                                    lastModifiedBy
      //                                    lastModifiedAt
      //                              }
      //                        }
      //                  }
      //            }
      //      }
      //  }`
      
      const create_mutation = {
        "query": `mutation S3DropBucketCreateMutation (
        $contactInput: [ContactCreateInput!]!
        ) 
        {
            createContacts(
            contactsInput: $contactInput
            ) {
                  items {
                        contactKey
                        message
                        identifyingField {
                              value
                              attributeData {
                                    type
                                    decimalPrecision
                                    name
                                    category
                                    mapAs
                                    validateAs
                                    identifyAs {
                                          channels
                                          key
                                          index
                                    }
                                    tracking {
                                          createdBy
                                          createdAt
                                          lastModifiedBy
                                          lastModifiedAt
                                    }
                              }
                        }
                  }
            }
        }`
      }


      //variables = {dataSetId: config.datasetid, contactsData: []} as CreateContactsVariables
      let createMutationVars: CreateContactsVariables = {contactInput: []}



      //Add S3DBConfirmation value as a means to quickly confirm Testing outcomes
      const now: Date = new Date()
      const date: string = now.toLocaleDateString()
      const time: string = now.toLocaleTimeString()

      let ca: ContactAttribute = {name: "S3DBConfirmation", value: date + " - " + time}
      //variables.contactsInput[upd].attributes[upd] = {name: "S3DBConfirmation", value: date + " - " + time}

      let cci: ContactInput = {attributes: [ca]}
      let ccr: CreateContactRecord = {contactInput: [cci]}


      
      for (const upd in updates)
      {
        
        //Build ContactId, AddressableFields, Consent, Audience properties
        const u = updates[upd] as Record<string, any>

        //if (typeof u.consent !== "undefined") Object.assign(variables.contactsInput, {consent: u.consent})
        if (typeof u.consent !== "undefined") Object.assign(cci, {consent: u.consent})

        if (typeof u.addressable !== "undefined" &&
          config.updatetype.toLowerCase() !== "createupdatecontacts") Object.assign(cci, {addressable: u.addressable})

        //Audience is just a placeholder for now, so removing until it is valid in graphQL type
        //if (typeof u.audience !== "undefined") Object.assign(variables.contactsInput, {audience: u.audience})

        
        
        //Build Attribute Array from each row of inbound data
        for (const [key, value] of Object.entries(u))
        {
          //let v
          //if (typeof value === "string") v = value
          //else v = String(value)

          //Skip Vars that were injected to carry ancillary Values into this portion of bulding out the mutation and which have already been 
          // processed above and are actually not valid for the actual Update
          if (!["contactid", "contactkey", "addressable", "consent", "audience"].includes(key.toLowerCase()))
          {
            ca = {name: key, value: value}
            cci.attributes.push(ca)
          }

        }

        // ToDo: add validation logic for variables structure
        ccr.contactInput.push(cci)
        
      }


      Object.assign(createMutationVars, ccr)

            
      const r = {...create_mutation, variables: createMutationVars}
      S3DB_Logging("info", "817", `CreateUpdate Multiple Contacts Mutation: \n${JSON.stringify(r)}`)

      return r

    }
  } catch (e)
  {

    debugger //catch

    S3DB_Logging("exception", "", `Exception - Build Mutations - CreateUpdateContacts - ${e}`)
  }


  

  //mutation UpdateContacts {
  //      updateContacts(
  //        updateContactInputs: {
  //        key: "email"
  //        addressable: null
  //        to: {
  //            attributes: null, 
  //            consent: {
  //                consentGroups: [
  //                      {consentGroupId: "", status: ""}
  //                ], 
  //                channels: [
  //                    {channel: "", status: ""}
  //                    {consentGroupId: ""}
  //                  ]
  //            }
  //          }
  //      }
  //      ) {
  //        modifiedCount
  //      }
  //    }


  //      consent: {
  //          consentGroups: [
  //            {consentGroupId: "1c3b3e8e-7386-5676-95d8-9c58d51a2334", status: OPT_IN}
  //            { consentGroupId: "e1f4cf1c-eed0-5d0c-a220-da5e198e6f05"
  //              channels: [{channel: EMAIL, status: OPT_IN_UNVERIFIED}]      }
  //          ]
  //        }



  try
  {
    if (config.updatetype.toLowerCase() === "updatecontacts") //Deprecated for now, until its known this is required aside from CreateUpdate
    {

      //From Schema
      //mutation UpdateContacts {
      //      updateContacts(
      //        updateContactInputs: {
      //        key: "email"
      //        addressable: null
      //        to: {attributes: null, consent: {consentGroups: null, channels: null}}
      //      }
      //      ) {
      //        modifiedCount
      //      }
      //    }


      //From Doc:
      //mutation {
      //  updateContacts(
      //    updateContactInputs: [
      //      {
      //        key: "contact_key_value"
      //        to: {
      //          attributes: [
      //            { name: "firstName", value: "John" }
      //            { name: "lastName", value: "Doe" }
      //            { name: "email", value: "john.doe@acoustic.co" }
      //          ]
      //          consent: { channels: [{ channel: EMAIL, status: OPT_IN }] }
      //        }
      //      }
      //      { key: "another_contact_key_value", to: { attributes: [{ name: "email", value: null }] } }
      //    ]
      //  ) {
      //    modifiedCount
      //  }
      //}

      //From Doc - Including Consent Groups examples
      //mutation {
      //  updateContacts(
      //    updateContactInputs: [
      //    {
      //      key: "contactId"
      //      to: {
      //        attributes: [
      //          {name: "firstName", value: "John"}
      //          {name: "lastName", value: "Doe"}
      //          {name: "email", value: "john.doe@acoustic.co"}
      //        ]
      //      consent: {
      //          consentGroups: [
      //            {consentGroupId: "1c3b3e8e-7386-5676-95d8-9c58d51a2334", status: OPT_IN}
      //            { consentGroupId: "e1f4cf1c-eed0-5d0c-a220-da5e198e6f05"
      //              channels: [{channel: EMAIL, status: OPT_IN_UNVERIFIED}]      }
      //          ]
      //        }
      //      }
      //    }
      //  ]
      //  ) {
      //    modifiedCount
      //  }
      //}

      const update_mutation = {
        query: `mutation S3DropBucketUpdateMutation (
        $contactInput: [ContactUpdateInput!]!
        ) 
        {
            updateContacts(updateContactInputs: $contactInput) {
              modifiedCount
            }
          }
        }`
      }
        

      let updateMutationVars = {updateContactInputs: []} as UpdateContactsVariables

      
      //Add S3DBConfirmation value as a means to quickly confirm Testing outcomes (and prime the struct) 
      const now: Date = new Date()
      const date: string = now.toLocaleDateString()
      const time: string = now.toLocaleTimeString()

      
      let uci: UpdateContactInput = {
        key: "",
        addressable: [],
        to: {
          attributes: [],
          consent: {
            channels: [{channel: "", status: ""}],
            consentGroups: [
              {consentGroupId: "", status: ""},
              {}
            ]

          }
        }
      }

      let ca = {name: "S3DBConfirmation", value: date + " - " + time} as ContactAttribute
      
      uci.to.attributes.push(ca)
              
      let ucr: UpdateContactRecord = {updateContactInputs: [uci]}


      for (const upd in updates)
      {
        //Audience is just a placeholder for now, so removing until it is valid in graphQL type
        //let uci: UpdateContactsInput = {"to": {"audience":[], "consent":[], "attributes": []}}

        //Build ContactId, AddressableFields, Consent, Audience properties
        const u = updates[upd] as Record<string, any>

        //ToDo: Add logic CreateContact vs UpdateContact which uses Key/Id/Addressable fields
        if (typeof u.contactId !== "undefined") Object.assign(updateMutationVars.updateContactInputs[upd], {contactId: u.contactId})
        else if (typeof u.addressable !== "undefined") Object.assign(updateMutationVars.updateContactInputs[upd], {addressable: u.addressable})

        if (typeof u.consent !== "undefined") Object.assign(uci.to, {consent: u.consent})


        //Build Attribute Array from each row of inbound data
        for (const [key, value] of Object.entries(u))
        {
          //let v
          //if (typeof value === "string") v = value as string
          //else v = String(value)
          //Skip Payload Vars, Vars injected to carry Transformed Values and are already processed above,
          // and are not valid for the actual Update
          if (!["contactid", "contactkey", "addressable", "consent", "audience"].includes(key.toLowerCase()))
          {
            ca = {name: key, value: value}
            uci.to.attributes.push(ca)
          }

        }

        Object.assign(updateMutationVars, ucr)

      }


      const r = {...update_mutation, variables: updateMutationVars}
      S3DB_Logging("info", "817", `Update Multiple Contacts Mutation: \n${JSON.stringify(r)}`)

      return r

    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - Build Mutations - UpdateContacts - ${e}`)
  }


} //Function Close

