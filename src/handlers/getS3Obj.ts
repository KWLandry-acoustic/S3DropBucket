
import { S3, S3Client, S3ClientConfig, GetObjectCommand, GetObjectCommandOutput } from "@aws-sdk/client-s3"
import { Handler, S3Event, Context } from "aws-lambda"

export interface S3Object {
    Bucket: string
    Key: string
    Region: string
}

export interface s3Obj {
    s3: {
        object: { key: any; };
        bucket: { name: any; };
    };
}

const s3 = new S3Client({ region: "us-east-1", });


export async function getS3Obj(event: S3Event, r: s3Obj) {

const get = async () => {


    const data = await s3.send(
        new GetObjectCommand({
            Key: r.s3.object.key,
            Bucket: r.s3.bucket.name
        })
    )    

    console.log(data.Body?.toString());
    process.exit(0);
};

get();
}

