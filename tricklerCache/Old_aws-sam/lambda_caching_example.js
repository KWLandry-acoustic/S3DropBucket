// 
// 
// // 
// //Caching of AuthToken
// // https://medium.com/@rajvirsinghrai/caching-auth-token-in-aws-lambda-using-ssm-4f214a986df6
// // 
export {};
// import { GetParameterCommand, PutParameterCommand, SSMClient } from "@aws-sdk/client-ssm";
// // const axios = require("axios").default;
// // const url = require("url");
// // import jwt_decode from "jwt-decode";
// export let token: string;
// const ssm = new SSMClient({ region: "your region" });
// export const handler = async function (event: any) {
//     console.log(event);
//     const token = await getToken();
// }
// export const getToken = async function () {
//     if (!token) {
//         token = await getParameterWorker("cached-token", true);
//         if (token === "Not_Found") {
//             console.log(" Token not found in SSM");
//             //   token= await getAuthToken();
//             token = await getAccessToken();
//             putParameterWorker(token);
//         } else {
//             console.log("Using SSM cachedtoken");
//         }
//     } else {
//         console.log("Using cached token");
//     }
//     return token;
// };
// // export const getAuthToken = async function (
// // ) {
// //   const options = {
// //     method: "POST",
// //     url: `https://"yourdomain"/oauth/token`,
// //     headers: { "content-type": "application/x-www-form-urlencoded" },
// //     data: new URLSearchParams({
// //       grant_type: "yourclient_credentials",
// //       client_id: "yourclientId",
// //       client_secret: "yourclientSecret",
// //     })
// //   };
// //   try {
// //     const response = await axios.request(options);
// //     return response.data.access_token;
// //   } catch (e) {
// //     console.error(`Unable to retrieve token: ${e}`);
// //     return "";
// //   }
// // };
// export const getParameterWorker = async (name: string, decrypt: boolean): Promise<string> => {
//     const command = new GetParameterCommand({ Name: name, WithDecryption: decrypt });
//     let result;
//     try {
//         result = await ssm.send(command);
//     } catch (error) {
//         console.log(error);
//         return "Not_Found";
//     }
//     return result?.Parameter?.Value ?? "";
// };
// export const putParameterWorker = async (authToken: any): Promise<void> => {
//     const decodedToken: { exp: number } = jwt_decode(authToken);
//     var tokenExpiryDate = new Date(decodedToken.exp * 1000);
//     const command = new PutParameterCommand({
//         Name: "cached-token",
//         Type: "SecureString",
//         Value: authToken,
//         Overwrite: true,
//         Tier: "Advanced",
//         Policies: JSON.stringify([
//             {
//                 Type: "Expiration",
//                 Version: "1.0",
//                 Attributes: {
//                     Timestamp: tokenExpiryDate
//                 }
//             }
//         ])
//     });
//     await ssm.send(command);
// };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibGFtYmRhX2NhY2hpbmdfZXhhbXBsZS5qcyIsInNvdXJjZVJvb3QiOiIvVXNlcnMva3dsYW5kcnkvRHJvcGJveC9Eb2N1bWVudHMvQS5DYW1wYWlnbi9Db2RlRm9sZGVyL0EuVHJpY2tsZXJDYWNoZV9jb2RlY29tbWl0L3RyaWNrbGVyQ2FjaGUvIiwic291cmNlcyI6WyJsYW1iZGFfY2FjaGluZ19leGFtcGxlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBLEdBQUc7QUFDSCxHQUFHO0FBQ0gsTUFBTTtBQUNOLHlCQUF5QjtBQUN6QixnR0FBZ0c7QUFDaEcsTUFBTTs7QUFFTiw2RkFBNkY7QUFDN0YsNkNBQTZDO0FBQzdDLGlDQUFpQztBQUNqQywwQ0FBMEM7QUFFMUMsNEJBQTRCO0FBQzVCLHdEQUF3RDtBQUV4RCx1REFBdUQ7QUFDdkQsMEJBQTBCO0FBQzFCLHNDQUFzQztBQUN0QyxJQUFJO0FBQ0osOENBQThDO0FBQzlDLG9CQUFvQjtBQUNwQixrRUFBa0U7QUFDbEUsdUNBQXVDO0FBQ3ZDLHNEQUFzRDtBQUN0RCxnREFBZ0Q7QUFDaEQsOENBQThDO0FBQzlDLHlDQUF5QztBQUN6QyxtQkFBbUI7QUFDbkIsb0RBQW9EO0FBQ3BELFlBQVk7QUFDWixlQUFlO0FBQ2YsNkNBQTZDO0FBQzdDLFFBQVE7QUFDUixvQkFBb0I7QUFDcEIsS0FBSztBQUNMLGtEQUFrRDtBQUNsRCxTQUFTO0FBQ1QseUJBQXlCO0FBQ3pCLHlCQUF5QjtBQUN6QixrREFBa0Q7QUFDbEQsMkVBQTJFO0FBQzNFLHFDQUFxQztBQUNyQyxpREFBaUQ7QUFDakQsc0NBQXNDO0FBQ3RDLDhDQUE4QztBQUM5QyxZQUFZO0FBQ1osVUFBVTtBQUVWLGFBQWE7QUFDYix3REFBd0Q7QUFDeEQsNENBQTRDO0FBQzVDLHFCQUFxQjtBQUNyQiwwREFBMEQ7QUFDMUQsb0JBQW9CO0FBQ3BCLFNBQVM7QUFDVCxRQUFRO0FBQ1IsaUdBQWlHO0FBQ2pHLHdGQUF3RjtBQUN4RixrQkFBa0I7QUFDbEIsWUFBWTtBQUNaLDRDQUE0QztBQUM1Qyx3QkFBd0I7QUFDeEIsOEJBQThCO0FBQzlCLDhCQUE4QjtBQUM5QixRQUFRO0FBQ1IsNkNBQTZDO0FBQzdDLEtBQUs7QUFFTCwrRUFBK0U7QUFDL0UsbUVBQW1FO0FBQ25FLCtEQUErRDtBQUMvRCxnREFBZ0Q7QUFDaEQsZ0NBQWdDO0FBQ2hDLGdDQUFnQztBQUNoQyw0QkFBNEI7QUFDNUIsMkJBQTJCO0FBQzNCLDRCQUE0QjtBQUM1QixxQ0FBcUM7QUFDckMsZ0JBQWdCO0FBQ2hCLHNDQUFzQztBQUN0QyxrQ0FBa0M7QUFDbEMsZ0NBQWdDO0FBQ2hDLGlEQUFpRDtBQUNqRCxvQkFBb0I7QUFDcEIsZ0JBQWdCO0FBQ2hCLGFBQWE7QUFDYixVQUFVO0FBQ1YsK0JBQStCO0FBQy9CLEtBQUsifQ==