"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
let processFilePromises = [];
// Create a client to read objects from S3
const s3 = new S3Client({ region: "us-east-1", });
export function getAllS3Obj(event, r) {
    return __awaiter(this, void 0, void 0, function* () {
        //Wait for 99 or 1 minute more than last record received. 
        // if (event.Records.length = 1) {
        //     // console.log(await lambdaWait(2000));
        // } else {
        event.Records.forEach((r) => __awaiter(this, void 0, void 0, function* () {
            const command = new GetObjectCommand({
                Key: r.s3.object.key,
                Bucket: r.s3.bucket.name
            });
            processFilePromises.push(s3.send(command));
            console.log('Processed Event: \n' + JSON.stringify(event, null, 2));
        }));
        yield Promise.all(processFilePromises);
    });
}
function lambdaWait(n) {
    return new Promise((resolve, reject) => {
        setTimeout(() => resolve("hello"), n);
    });
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZ2V0QWxsUzNPYmouanMiLCJzb3VyY2VSb290IjoiL1VzZXJzL2t3bGFuZHJ5L0Ryb3Bib3gvRG9jdW1lbnRzL0EuQ2FtcGFpZ24vQ29kZUZvbGRlci9BLlRyaWNrbGVyQ2FjaGVfY29kZWNvbW1pdC90cmlja2xlckNhY2hlLyIsInNvdXJjZXMiOlsic3JjL2hhbmRsZXJzL2dldEFsbFMzT2JqLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBLFlBQVksQ0FBQzs7Ozs7Ozs7OztBQUNiLE9BQU8sRUFBTSxRQUFRLEVBQWtCLGdCQUFnQixFQUEwQixNQUFNLG9CQUFvQixDQUFBO0FBSTNHLElBQUksbUJBQW1CLEdBQVMsRUFBRSxDQUFBO0FBZ0JsQywwQ0FBMEM7QUFDMUMsTUFBTSxFQUFFLEdBQUcsSUFBSSxRQUFRLENBQUMsRUFBRSxNQUFNLEVBQUUsV0FBVyxHQUFHLENBQUMsQ0FBQztBQUVsRCxNQUFNLFVBQWdCLFdBQVcsQ0FBQyxLQUFjLEVBQUUsQ0FBUTs7UUFJdEQsMERBQTBEO1FBRTFELGtDQUFrQztRQUNsQyw4Q0FBOEM7UUFDOUMsV0FBVztRQUlYLEtBQUssQ0FBQyxPQUFPLENBQUMsT0FBTyxDQUFDLENBQU8sQ0FBUSxFQUFFLEVBQUU7WUFHckMsTUFBTSxPQUFPLEdBQUcsSUFBSSxnQkFBZ0IsQ0FBQztnQkFDakMsR0FBRyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsTUFBTSxDQUFDLEdBQUc7Z0JBQ3BCLE1BQU0sRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLE1BQU0sQ0FBQyxJQUFJO2FBQzNCLENBQUMsQ0FBQTtZQUNGLG1CQUFtQixDQUFDLElBQUksQ0FBQyxFQUFFLENBQUMsSUFBSSxDQUFDLE9BQU8sQ0FBQyxDQUFDLENBQUE7WUFFMUMsT0FBTyxDQUFDLEdBQUcsQ0FBQyxxQkFBcUIsR0FBRyxJQUFJLENBQUMsU0FBUyxDQUFDLEtBQUssRUFBRSxJQUFJLEVBQUUsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUN4RSxDQUFDLENBQUEsQ0FBQyxDQUFBO1FBR0YsTUFBTSxPQUFPLENBQUMsR0FBRyxDQUFDLG1CQUFtQixDQUFDLENBQUM7SUFFM0MsQ0FBQztDQUFBO0FBR0QsU0FBUyxVQUFVLENBQUMsQ0FBUztJQUN6QixPQUFPLElBQUksT0FBTyxDQUFDLENBQUMsT0FBTyxFQUFFLE1BQU0sRUFBRSxFQUFFO1FBQ25DLFVBQVUsQ0FBQyxHQUFHLEVBQUUsQ0FBQyxPQUFPLENBQUMsT0FBTyxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUE7SUFDekMsQ0FBQyxDQUFDLENBQUM7QUFDUCxDQUFDIn0=