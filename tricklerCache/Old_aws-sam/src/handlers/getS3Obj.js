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
const s3 = new S3Client({ region: "us-east-1", });
export function getS3Obj(event, r) {
    return __awaiter(this, void 0, void 0, function* () {
        const get = () => __awaiter(this, void 0, void 0, function* () {
            var _a;
            const data = yield s3.send(new GetObjectCommand({
                Key: r.s3.object.key,
                Bucket: r.s3.bucket.name
            }));
            console.log((_a = data.Body) === null || _a === void 0 ? void 0 : _a.toString());
            process.exit(0);
        });
        get();
    });
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZ2V0UzNPYmouanMiLCJzb3VyY2VSb290IjoiL1VzZXJzL2t3bGFuZHJ5L0Ryb3Bib3gvRG9jdW1lbnRzL0EuQ2FtcGFpZ24vQ29kZUZvbGRlci9BLlRyaWNrbGVyQ2FjaGVfY29kZWNvbW1pdC90cmlja2xlckNhY2hlLyIsInNvdXJjZXMiOlsic3JjL2hhbmRsZXJzL2dldFMzT2JqLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7OztBQUNBLE9BQU8sRUFBTSxRQUFRLEVBQWtCLGdCQUFnQixFQUEwQixNQUFNLG9CQUFvQixDQUFBO0FBZ0IzRyxNQUFNLEVBQUUsR0FBRyxJQUFJLFFBQVEsQ0FBQyxFQUFFLE1BQU0sRUFBRSxXQUFXLEdBQUcsQ0FBQyxDQUFDO0FBR2xELE1BQU0sVUFBZ0IsUUFBUSxDQUFDLEtBQWMsRUFBRSxDQUFROztRQUV2RCxNQUFNLEdBQUcsR0FBRyxHQUFTLEVBQUU7O1lBR25CLE1BQU0sSUFBSSxHQUFHLE1BQU0sRUFBRSxDQUFDLElBQUksQ0FDdEIsSUFBSSxnQkFBZ0IsQ0FBQztnQkFDakIsR0FBRyxFQUFFLENBQUMsQ0FBQyxFQUFFLENBQUMsTUFBTSxDQUFDLEdBQUc7Z0JBQ3BCLE1BQU0sRUFBRSxDQUFDLENBQUMsRUFBRSxDQUFDLE1BQU0sQ0FBQyxJQUFJO2FBQzNCLENBQUMsQ0FDTCxDQUFBO1lBRUQsT0FBTyxDQUFDLEdBQUcsQ0FBQyxNQUFBLElBQUksQ0FBQyxJQUFJLDBDQUFFLFFBQVEsRUFBRSxDQUFDLENBQUM7WUFDbkMsT0FBTyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUNwQixDQUFDLENBQUEsQ0FBQztRQUVGLEdBQUcsRUFBRSxDQUFDO0lBQ04sQ0FBQztDQUFBIn0=