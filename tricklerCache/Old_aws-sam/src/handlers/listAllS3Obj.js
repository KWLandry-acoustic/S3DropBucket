var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
import { S3Client, 
// This command supersedes the ListObjectsCommand and is the recommended way to list objects.
ListObjectsV2Command, } from "@aws-sdk/client-s3";
const client = new S3Client({});
export const main = () => __awaiter(void 0, void 0, void 0, function* () {
    const command = new ListObjectsV2Command({
        Bucket: "my-bucket",
        // The default and maximum number of keys returned is 1000. This limits it to
        // one for demonstration purposes.
        MaxKeys: 1,
    });
    try {
        let isTruncated = true;
        console.log("Your bucket contains the following objects:\n");
        let contents = "";
        while (isTruncated) {
            const { Contents, IsTruncated, NextContinuationToken } = yield client.send(command);
            const contentsList = Contents === null || Contents === void 0 ? void 0 : Contents.map((c) => ` • ${c.Key}`).join("\n");
            contents += contentsList + "\n";
            isTruncated = IsTruncated;
            command.input.ContinuationToken = NextContinuationToken;
        }
        console.log(contents);
    }
    catch (err) {
        console.error(err);
    }
});
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibGlzdEFsbFMzT2JqLmpzIiwic291cmNlUm9vdCI6Ii9Vc2Vycy9rd2xhbmRyeS9Ecm9wYm94L0RvY3VtZW50cy9BLkNhbXBhaWduL0NvZGVGb2xkZXIvQS5Ucmlja2xlckNhY2hlX2NvZGVjb21taXQvdHJpY2tsZXJDYWNoZS8iLCJzb3VyY2VzIjpbInNyYy9oYW5kbGVycy9saXN0QWxsUzNPYmoudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7O0FBQUEsT0FBTyxFQUNILFFBQVE7QUFDUiw2RkFBNkY7QUFDN0Ysb0JBQW9CLEdBQ3JCLE1BQU0sb0JBQW9CLENBQUM7QUFFNUIsTUFBTSxNQUFNLEdBQUcsSUFBSSxRQUFRLENBQUMsRUFBRSxDQUFDLENBQUM7QUFFaEMsTUFBTSxDQUFDLE1BQU0sSUFBSSxHQUFHLEdBQVMsRUFBRTtJQUM3QixNQUFNLE9BQU8sR0FBRyxJQUFJLG9CQUFvQixDQUFDO1FBQ3ZDLE1BQU0sRUFBRSxXQUFXO1FBQ25CLDZFQUE2RTtRQUM3RSxrQ0FBa0M7UUFDbEMsT0FBTyxFQUFFLENBQUM7S0FDWCxDQUFDLENBQUM7SUFFSCxJQUFJO1FBQ0YsSUFBSSxXQUFXLEdBQUcsSUFBZSxDQUFBO1FBRWpDLE9BQU8sQ0FBQyxHQUFHLENBQUMsK0NBQStDLENBQUMsQ0FBQTtRQUM1RCxJQUFJLFFBQVEsR0FBRyxFQUFFLENBQUM7UUFFbEIsT0FBTyxXQUFXLEVBQUU7WUFDbEIsTUFBTSxFQUFFLFFBQVEsRUFBRSxXQUFXLEVBQUUscUJBQXFCLEVBQUUsR0FBRyxNQUFNLE1BQU0sQ0FBQyxJQUFJLENBQUMsT0FBTyxDQUFDLENBQUM7WUFDcEYsTUFBTSxZQUFZLEdBQUcsUUFBUSxhQUFSLFFBQVEsdUJBQVIsUUFBUSxDQUFFLEdBQUcsQ0FBQyxDQUFDLENBQUMsRUFBRSxFQUFFLENBQUMsTUFBTSxDQUFDLENBQUMsR0FBRyxFQUFFLEVBQUUsSUFBSSxDQUFDLElBQUksQ0FBQyxDQUFDO1lBQ3BFLFFBQVEsSUFBSSxZQUFZLEdBQUcsSUFBSSxDQUFDO1lBQ2hDLFdBQVcsR0FBRyxXQUFzQixDQUFBO1lBQ3BDLE9BQU8sQ0FBQyxLQUFLLENBQUMsaUJBQWlCLEdBQUcscUJBQXFCLENBQUM7U0FDekQ7UUFDRCxPQUFPLENBQUMsR0FBRyxDQUFDLFFBQVEsQ0FBQyxDQUFDO0tBRXZCO0lBQUMsT0FBTyxHQUFHLEVBQUU7UUFDWixPQUFPLENBQUMsS0FBSyxDQUFDLEdBQUcsQ0FBQyxDQUFDO0tBQ3BCO0FBQ0gsQ0FBQyxDQUFBLENBQUMifQ==