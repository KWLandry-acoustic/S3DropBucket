/* eslint-disable no-debugger */
"use strict"
import {type CustomerConfig, S3DB_Logging} from './s3DropBucket'

export async function buildMutationsConnect (updates: object[], config: CustomerConfig) {

  let create_mutation
  let update_mutation
  let variables

  /*
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
  interface ContactInput {
    attributes: ContactAttribute[]
    //audience?: {id: string}
    consent?: {id: string}
  }

  interface ContactAttribute {
    name: string
    value: any
  }

  interface CreateContactRecord {
    consent?: {}
    attributes: Array<{
      name: string
      value: any
    }>
  }

  interface CreateContactsVariables {
    //dataSetId: string
    contactsData: CreateContactRecord[]
  }

  interface UpdateContactsVariables {
    //dataSetId: string
    contactsData: CreateContactRecord[]
  }


  create_mutation = `mutation S3DropBucketCreateMutation (
        $contactsData: [ContactCreateInput!]!
        ) 
        {
            createContacts(
            contactsInput: $contactsData
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

  update_mutation = `mutation S3DropBucketCreateMutation (
        $contactsData: [ContactCreateInput!]!
        ) 
        {
            createContacts(
            contactsInput: $contactsData
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

  try
  {
    if (config.updatetype.toLowerCase() === "createupdatecontacts")
    {

      //variables = {dataSetId: config.datasetid, contactsData: []} as CreateContactsVariables
      variables = {contactsData: []} as CreateContactsVariables

      for (const upd in updates)
      {
        //Audience is just a placeholder for now, so removing until it is valid in graphQL type
        //let ccr: CreateContactsInput = {"to": {"audience":[], "consent":[], "attributes": []}}
        let ca: ContactAttribute = {name: "", value: ""}
        let ccr: CreateContactRecord = {attributes: []}
        let cci: ContactInput = {attributes: []}

        variables.contactsData.push(cci)

        //Build ContactId, AddressableFields, Consent, Audience properties
        const u = updates[upd] as Record<string, any>

        //if (typeof u.consent !== "undefined") Object.assign(variables.contactsInput, {consent: u.consent})
        if (typeof u.consent !== "undefined") Object.assign(cci, {consent: u.consent})


        if (typeof u.addressable !== "undefined" &&
          config.updatetype.toLowerCase() !== "createupdatecontacts") Object.assign(cci, {addressable: u.addressable})




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
          //Skip Vars that were injected to carry ancillary Values into this portion of bulding out the mutation and which have already been 
          // processed above and are actually not valid for the actual Update
          if (!["contactid", "contactkey", "addressable", "consent", "audience"].includes(key.toLowerCase()))
          {
            ca = {name: key, value: value}
            cci.attributes.push(ca)
          }

        }

        // ToDo: add validation logic for variables structure
        Object.assign(variables.contactsData[upd], cci)
        //variables.contactsInput.push(cci)
      }

      S3DB_Logging("info", "817", `CreateUpdate Multiple Contacts Mutation: ${JSON.stringify({query: create_mutation, variables: variables})}`)
      return {query: create_mutation, variables: variables}

    }
  } catch (e)
  {

    debugger //catch

    S3DB_Logging("exception", "", `Exception - Build Mutations - CreateUpdateContacts - ${e}`)
  }


  try
  {
    if (config.updatetype.toLowerCase() === "updatecontacts") //Deprecated for now, until its known this is required aside from CreateUpdate
    {

      interface ContactAttribute {
        name: string
        value: any
      }
      interface UpdateContactRecord {
        attributes: Array<{
          name: string
          value: string
        }>
        audience?: Array<{}> //Audience is just a placeholder for now, so ignoring until it is valid in graphQL type
        consent?: Array<{}>
      }
      interface UpdateContactsVariables {
        contactsInput: UpdateContactsInput[]
      }

      interface UpdateContactsInput {
        contactId?: string
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
        }
      }

      create_mutation = `mutation updateContacts (
        $contactsInput: [UpdateContactInput!]!
        ) {
        updateContacts(
            updateContactInputs: $contactsInput
            ) 
        {
        modifiedCount 
        }
    }`



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
      variables = {contactsInput: []} as UpdateContactsVariables

      for (const upd in updates)
      {
        //Audience is just a placeholder for now, so removing until it is valid in graphQL type
        //let uci: UpdateContactsInput = {"to": {"audience":[], "consent":[], "attributes": []}}
        let ca: ContactAttribute = {"name": "", "value": ""}
        let ucr: UpdateContactRecord = {attributes: []}
        let uci: UpdateContactsInput = {to: {attributes: []}}

        variables.contactsInput.push(uci)

        //Build ContactId, AddressableFields, Consent, Audience properties
        const u = updates[upd] as Record<string, any>

        //ToDo: Add logic CreateContact vs UpdateContact Key/Id/Addressable fields
        if (typeof u.contactId !== "undefined") Object.assign(variables.contactsInput[upd], {contactId: u.contactId})
        else if (typeof u.addressable !== "undefined") Object.assign(variables.contactsInput[upd], {addressable: u.addressable})

        if (typeof u.consent !== "undefined") Object.assign(uci.to, {consent: u.consent})

        //Audience is just a placeholder for now, so removing until it is valid in graphQL type
        //if (typeof u.audience !== "undefined") Object.assign(variables.contactsInput, {audience: u.audience})
        //Add S3DBConfirmation value as a means to quickly confirm Testing outcomes
        const now: Date = new Date()
        const date: string = now.toLocaleDateString()
        const time: string = now.toLocaleTimeString()
        uci.to.attributes.push({name: "S3DBConfirmation", value: date + " - " + time})


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

        Object.assign(variables.contactsInput[upd], uci)
        //variables.contactsInput.push(uci)
      }

      S3DB_Logging("info", "817", `Update Multiple Contacts Mutation: \n${create_mutation} and Vars: \n${JSON.stringify(variables)}`)
      return {query: create_mutation, variables}

    }
  } catch (e)
  {
    debugger //catch

    S3DB_Logging("exception", "", `Exception - Build Mutations - UpdateContacts - ${e}`)
  }


} //Function Close

