interface ContactAttribute {
  name: string
  value: string | number | boolean | null
}

interface ContactTracking {
  createdAt: string
  lastModifiedAt: string
}

interface Contact {
  attributes: ContactAttribute[]
  tracking: ContactTracking
}

interface PageInfo {
  hasNextPage: boolean
  endCursor: string
}

interface ContactsResponse {
  nodes: Contact[]
  pageInfo: PageInfo
  totalCount: number
}

interface QueryVariables {
  createdAfter: string
  modifiedAfter: string
}

interface QueryResponse {
  contacts: ContactsResponse
}

const createHeaders = (): Headers => {
  const headers = new Headers()

  //headers.append("subscriptionId", "73e3348f21c2cf5c4ed8a20e4fa18a7f81ddc761")
  //headers.append("x-api-key", "a9a7e6b143e1449c96632f079f21011b_gka")
  //headers.append("x-acoustic-region", "us-east-1")
  //headers.append("creds", "areBarnesFoundation")

  headers.append("subscriptionId", "bb6758fc16bbfffbe4248214486c06cc3a924edf")
  headers.append("x-api-key", "b1fa7ef5024e4da3b0a40aed8331761c")
  headers.append("x-acoustic-region", "us-east-1")
  headers.append("creds", "areServicesSupport")


  headers.append("Content-Type", "application/json")
  return headers
}

const fetchContactsByDate = async (
  createdAfter: string = "2025-01-01T00:00:00Z",
  modifiedAfter: string = "2025-05-01T00:00:00Z",
  pageSize: number = 10
): Promise<QueryResponse> => {
  const schemaUrl = "https://connect-gql-us-1.goacoustic.com"
  
  // Since we can't filter by tracking.createdAt directly, we'll just get contacts and filter in memory
  const graphql = JSON.stringify({
    query: `query GetContacts($pageSize: Float!) {
      contacts(first: $pageSize) {
        nodes {
          attributes {
            name
            value
          }
          tracking {
            createdAt
            lastModifiedAt
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
        totalCount
      }
    }`,
    variables: {
      pageSize: 100 // Get more contacts since we'll filter client-side
    }
  })

  const response = await fetch(schemaUrl, {
    method: 'POST',
    headers: createHeaders(),
    body: graphql
  })

  const data = await response.json()
  
  // Filter contacts by date client-side
  if (data.data && data.data.contacts && data.data.contacts.nodes) {
    const createdDate = new Date(createdAfter)
    const modifiedDate = new Date(modifiedAfter)
    
    data.data.contacts.nodes = data.data.contacts.nodes.filter(contact => {
      const contactCreatedAt = new Date(contact.tracking.createdAt)
      const contactModifiedAt = new Date(contact.tracking.lastModifiedAt)
      
      return contactCreatedAt >= createdDate && contactModifiedAt >= modifiedDate
    })
  }

  return data
}

// Run with specific date parameters
fetchContactsByDate("2025-05-20T00:00:00Z", "2024-01-01T00:00:00Z")
  .then(data => {
    console.log(`Found ${data?.contacts?.nodes?.length || 0} contacts created after May 20, 2025`)
    console.log(JSON.stringify(data, null, 2))
  })
  .catch(error => console.error('Error:', error))
