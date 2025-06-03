const myHeaders = new Headers();
myHeaders.append("subscriptionId", "73e3348f21c2cf5c4ed8a20e4fa18a7f81ddc761");
myHeaders.append("x-api-key", "a9a7e6b143e1449c96632f079f21011b_gka");
myHeaders.append("x-acoustic-region", "us-east-1");
myHeaders.append("creds", "areBarnesFoundation");
myHeaders.append("Content-Type", "application/json");

const schemaurl = "https://connect-gql-us-1.goacoustic.com";

const graphql = JSON.stringify({
  query: `query FilteredContacts($createdAfter: String, $modifiedAfter: String) {
    contacts(
      filter: [
        { field: "createdOn", after: $createdAfter },
        { field: "updatedOn", after: $modifiedAfter }
      ]
      first: 10
    ) {
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
    "createdAfter": "2024-01-01T00:00:00Z",
    "modifiedAfter": "2024-05-01T00:00:00Z"
  }
});

// Execute the fetch
fetch(schemaurl, {
  method: 'POST',
  headers: myHeaders,
  body: graphql
})
.then(response => response.json())
.then(data => console.log(JSON.stringify(data, null, 2)))
.catch(error => console.error('Error:', error));