// src/handlers/s3-json-logger.ts
import { S3Client, GetObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";
import fetch from "node-fetch";
var s3 = new S3Client({ region: "us-east-1" });
var s3JsonLoggerHandler = async (event, context) => {
  console.log("ENVIRONMENT VARIABLES\n" + JSON.stringify(process.env, null, 2));
  console.log("Processing Trigger from Event: ", event.Records[0].responseElements["x-amz-request-id"]);
  if (event.Records.length > 1)
    throw new Error(`Expecting only a single S3 Object from a Triggered S3 write of a new Object, received ${event.Records.length} Objects`);
  else
    console.log("Num of Events to be processed: ", event.Records.length);
  const getS3Obj = async () => {
    const data = await s3.send(
      new GetObjectCommand({
        Key: event.Records[0].s3.object.key,
        Bucket: event.Records[0].s3.bucket.name
      })
    );
    console.log("Received the following Object: \n", data.Body?.toString());
    const del = await s3.send(
      new DeleteObjectCommand({
        Key: event.Records[0].s3.object.key,
        Bucket: event.Records[0].s3.bucket.name
      })
    );
    console.log(`Response from deleteing Object ${event.Records[0].responseElements["x-amz-request-id"]} 
 ${del.$metadata.toString()}`);
  };
  getS3Obj();
};
var s3_json_logger_default = s3JsonLoggerHandler;
async function postCampaign(xmlCalls) {
  const accessToken = getAccessToken();
  console.log("Access Token: ", accessToken);
  try {
    const r = await fetch("https://api-campaign-us-6.goacoustic.com/XMLAPI", {
      method: "POST",
      // body: JSON.stringify(xmlCalls),
      body: xmlCalls,
      headers: {
        "Authorization": `Bearer: ${accessToken}`,
        "Content-Type": "application/json"
      }
    }).then((res) => res.json()).then((json) => {
      console.log("Return from POST Campaign: \n", json);
      return JSON;
    });
  } catch (e) {
    console.log("Exception during POST to Campaign: \n", e);
  }
}
async function getAccessToken() {
  const ac = {
    accessToken: "",
    clientId: "",
    clientSecret: "",
    refreshToken: "",
    refreshTokenUrl: ""
  };
  ac.accessToken = "";
  ac.clientId = "1853dc2f-1a79-4219-b538-edb018be9d52";
  ac.clientSecret = "329f1765-0731-4c9e-a5da-0e8f48559f45";
  ac.refreshToken = "r7nyDaWJ6GYdH5l6mlR9uqFqqrWZvwKD9RSq-hFgTMdMS1";
  ac.refreshTokenUrl = `https://api-campaign-us-6.goacoustic.com/oauth/token`;
  try {
    const rat = await fetch(ac.refreshTokenUrl, {
      method: "POST",
      body: new URLSearchParams({
        refresh_token: ac.refreshToken,
        client_id: ac.clientId,
        client_secret: ac.clientSecret,
        grant_type: "refresh_token"
      }),
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "S3 TricklerCache GetAccessToken"
      }
    });
    const ratResp = await rat.json();
    ac.accessToken = ratResp.access_token;
    return { accessToken: ac.accessToken, refreshToken: ac.refreshToken };
  } catch (e) {
    console.log("Exception in getAccessToken: \n", e);
  }
}
export {
  s3_json_logger_default as default,
  getAccessToken,
  postCampaign,
  s3JsonLoggerHandler
};
//# sourceMappingURL=s3-json-logger.mjs.map
