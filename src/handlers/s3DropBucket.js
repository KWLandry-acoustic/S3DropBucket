'use strict';
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.postToCampaign = exports.getAccessToken = exports.s3DropBucketSFTPHandler = exports.S3DropBucketQueueProcessorHandler = exports.s3DropBucketHandler = void 0;
var client_s3_1 = require("@aws-sdk/client-s3");
var client_firehose_1 = require("@aws-sdk/client-firehose");
var client_scheduler_1 = require("@aws-sdk/client-scheduler"); // ES Modules import
// import JSONParserTransform from '@streamparser/json-node/jsonparser.js'
var uuid_1 = require("uuid");
var csv_parse_1 = require("csv-parse");
var stream_transform_1 = require("stream-transform");
var jsonpath_1 = require("jsonpath");
var client_sqs_1 = require("@aws-sdk/client-sqs");
var ssh2_sftp_client_1 = require("ssh2-sftp-client");
var s3db_version = "3.3.003 - ".concat(new Date().toUTCString());
var s3 = {};
var SFTPClient = new ssh2_sftp_client_1.default();
//For when needed to reference Lambda execution environment /tmp folder 
// import { ReadStream, close } from 'fs'
var sqsClient = new client_sqs_1.SQSClient({});
var localTesting = false;
var chunks = [];
var xmlRows = '';
var batchCount = 0;
var recs = 0;
var customersConfig = {};
var s3db_cc = {};
var processS3ObjectStreamResolution = {
    Key: "",
    Processed: "",
    OnDataBatchingResult: "",
    OnDataStoreAndQueueWorkResult: {
        StoreAndQueueWorkResult: {
            AddWorkToS3WorkBucketResults: {
                versionId: "",
                S3ProcessBucketResult: "",
                AddWorkToS3ProcessBucket: ""
            },
            AddWorkToSQSWorkQueueResults: {
                SQSWriteResult: "",
                AddToSQSQueue: ""
            },
            StoreQueueWorkException: "",
            StoreS3WorkException: ""
        }
    },
    OnEndStreamEndResult: {
        StoreAndQueueWorkResult: {
            AddWorkToS3WorkBucketResults: {
                versionId: "",
                S3ProcessBucketResult: "",
                AddWorkToS3ProcessBucket: ""
            },
            AddWorkToSQSWorkQueueResults: {
                SQSWriteResult: "",
                AddToSQSQueue: ""
            },
            StoreQueueWorkException: "",
            StoreS3WorkException: ""
        }
    },
    StreamEndResult: "",
    StreamException: "",
    OnEndRecordStatus: "",
    OnDataReadStreamException: "",
    OnEndNoRecordsException: "",
    ProcessS3ObjectStreamCatch: "",
    OnClose_Result: "",
    StreamReturnLocation: "",
    PutToFireHoseAggregatorResult: "",
    PutToFireHoseException: "",
    OnEnd_PutToFireHoseAggregator: "",
    DeleteResult: ""
};
var sqsBatchFail = {
    batchItemFailures: [
        {
            itemIdentifier: ''
        }
    ]
};
sqsBatchFail.batchItemFailures.pop();
var tcLogInfo = true;
var s3dbLogDebug = false;
var s3dbLogVerbose = false;
var tcSelectiveDebug; //call out selective debug as an option
//For local testing
var testS3Key;
var testS3Bucket;
testS3Bucket = "s3dropbucket-configs";
// testS3Key = "TestData/visualcrossing_00213.csv"
// testS3Key = "TestData/pura_2024_02_25T00_00_00_090Z.json"
// testS3Key = "TestData/pura_S3DropBucket_Aggregator-8-2024-03-23-09-23-55-123cb0f9-9552-3303-a451-a65dca81d3c4_json_update_53_99.xml"
//testS3Key = "TestData/alerusrepsignature_sampleformatted_json_update_1_1.xml"
//testS3Key = "TestData/alerusrepsignature_advisors_2_json_update-3c74bfb2-1997-4653-bd8e-73bf030b4f2d_26_14.xml"
//  Core - Key Set of Test Datasets 
//testS3Key = "TestData/cloroxweather_99706.csv"
//testS3Key = "TestData/pura_S3DropBucket_Aggregator-8-2024-03-19-16-42-48-46e884aa-8c6a-3ff9-8d32-c329395cf311.json"
//testS3Key = "TestData/pura_2024_02_26T05_53_26_084Z.json"
//testS3Key = "TestData/alerusrepsignature_sample.json"
//testS3Key = "TestData/alerusrepsignature_advisors.json"
// testS3Key = "TestData/alerusrepsignature_sampleformatted.json"
// testS3Key = "TestData/alerusrepsignature_sample - min.json"
testS3Key = "TestData/alerusreassignrepsignature_advisors.json";
var vid;
var et;
/**
 * A Lambda function to process the Event payload received from S3.
 */
var s3DropBucketHandler = function (event, context) { return __awaiter(void 0, void 0, void 0, function () {
    var _a, d, _loop_1, _i, _b, r, state_1, maintenance, l, filesProcessed, n, osr, osrl, k, p, logMsg, logKey, fireHoseLog;
    var _c, _d, _e, _f, _g;
    return __generator(this, function (_h) {
        switch (_h.label) {
            case 0:
                //Ignore Aggregation Error Files 
                if (event.Records[0].s3.object.key.indexOf('AggregationError') > -1)
                    return [2 /*return*/, ""
                        //ToDo: Make AWS Region Config Option for portability/infrastruct dedication
                        //const s3 = new S3Client( {region: 'us-east-1'} )
                    ];
                //ToDo: Make AWS Region Config Option for portability/infrastruct dedication
                //const s3 = new S3Client( {region: 'us-east-1'} )
                s3 = new client_s3_1.S3Client({ region: process.env.s3DropBucketRegion });
                if (!(process.env["EventEmitterMaxListeners"] === undefined ||
                    process.env["EventEmitterMaxListeners"] === '' ||
                    process.env["EventEmitterMaxListeners"] === null)) return [3 /*break*/, 2];
                return [4 /*yield*/, getValidateS3DropBucketConfig()];
            case 1:
                s3db_cc = _h.sent();
                _h.label = 2;
            case 2:
                if (s3db_cc.SelectiveDebug.indexOf("_98,") > -1)
                    console.info("Selective Debug 98 - S3DropBucket Options: ".concat(JSON.stringify(s3db_cc), " "));
                if (s3db_cc.SelectiveDebug.indexOf("_99,") > -1)
                    console.info("Selective Debug 99 - S3DropBucket Logging Options: ".concat(s3db_cc.SelectiveDebug, " "));
                if (s3db_cc.SelectiveDebug.indexOf("_909,") > -1)
                    console.info("Selective Debug 909 - Environment Vars: ".concat(JSON.stringify(process.env), " "));
                if (event.Records[0].s3.object.key.indexOf('Aggregator') > -1) {
                    if (s3db_cc.SelectiveDebug.indexOf("_925,") > -1)
                        console.info("Selective Debug 925 - Processing an Aggregated File ".concat(event.Records[0].s3.object.key));
                }
                if (!(!event.Records[0].s3.object.key || event.Records[0].s3.object.key === 'devtest.csv')) return [3 /*break*/, 6];
                if (!(testS3Key !== undefined && testS3Key !== null &&
                    testS3Bucket !== undefined && testS3Bucket !== null)) return [3 /*break*/, 3];
                event.Records[0].s3.object.key = testS3Key;
                event.Records[0].s3.bucket.name = testS3Bucket;
                return [3 /*break*/, 5];
            case 3:
                _a = event.Records[0].s3.object;
                return [4 /*yield*/, getAnS3ObjectforTesting(event.Records[0].s3.bucket.name)];
            case 4:
                _a.key = (_c = _h.sent()) !== null && _c !== void 0 ? _c : "";
                _h.label = 5;
            case 5:
                localTesting = true;
                return [3 /*break*/, 7];
            case 6:
                testS3Key = '';
                testS3Bucket = '';
                localTesting = false;
                _h.label = 7;
            case 7:
                if (!(s3db_cc.S3DropBucketPurgeCount > 0)) return [3 /*break*/, 9];
                console.warn("Purge Requested, Only action will be to Purge ".concat(s3db_cc.S3DropBucketPurge, " of ").concat(s3db_cc.S3DropBucketPurgeCount, " Records. "));
                return [4 /*yield*/, purgeBucket(Number(s3db_cc.S3DropBucketPurgeCount), s3db_cc.S3DropBucketPurge)];
            case 8:
                d = _h.sent();
                return [2 /*return*/, d];
            case 9:
                if (s3db_cc.S3DropBucketQuiesce) {
                    if (!localTesting) {
                        console.warn("S3DropBucket Quiesce is in effect, new Files from ".concat(s3db_cc.S3DropBucket, " will be ignored and not processed. \nTo Process files that have arrived during a Quiesce of the Cache, reference the S3DropBucket Guide appendix for AWS cli commands."));
                        return [2 /*return*/];
                    }
                }
                if (s3db_cc.SelectiveDebug.indexOf("_100,") > -1)
                    console.info("(100) Received S3DropBucket Event Batch.There are ".concat(event.Records.length, " S3DropBucket Event Records in this batch. (Event Id: ").concat(event.Records[0].responseElements['x-amz-request-id'], ")."));
                _loop_1 = function (r) {
                    var key, bucket, e_1, getWork, e_2, e_3;
                    return __generator(this, function (_j) {
                        switch (_j.label) {
                            case 0:
                                key = (_d = r.s3.object.key) !== null && _d !== void 0 ? _d : '';
                                bucket = (_e = r.s3.bucket.name) !== null && _e !== void 0 ? _e : '';
                                if (s3db_cc.prefixFocus !== "" && key.indexOf(s3db_cc.prefixFocus) < 0) {
                                    return [2 /*return*/, { value: void 0 }];
                                }
                                //ToDo: Resolve Duplicates Issue - S3 allows Duplicate Object Names but Delete marks all Objects of same Name Deleted. 
                                //   Which causes an issue with Key Not Found after an Object of Name A is processed and deleted, then another Object of Name A comes up in a Trigger.
                                vid = (_f = r.s3.object.versionId) !== null && _f !== void 0 ? _f : undefined;
                                et = (_g = r.s3.object.eTag) !== null && _g !== void 0 ? _g : "";
                                _j.label = 1;
                            case 1:
                                _j.trys.push([1, 3, , 4]);
                                return [4 /*yield*/, getCustomerConfig(key)];
                            case 2:
                                customersConfig = _j.sent();
                                return [3 /*break*/, 4];
                            case 3:
                                e_1 = _j.sent();
                                console.error("Exception - Pulling Customer Config \n".concat(e_1, " "));
                                return [2 /*return*/, "break"];
                            case 4:
                                //Initial work out for writing logs to S3 Bucket
                                try {
                                    if (key.indexOf('S3DropBucket-LogsS3DropBucket_Aggregator') > -1)
                                        console.warn("Warning -- Found Invalid Aggregator File Name - ".concat(key));
                                    if (s3db_cc.SelectiveDebug.indexOf("_101,") > -1)
                                        console.info("(101) Processing inbound data for ".concat(customersConfig.Customer, " - ").concat(key));
                                }
                                catch (e) {
                                    throw new Error("Exception - Retrieving Customer Config for ".concat(key, " \n").concat(e));
                                }
                                _j.label = 5;
                            case 5:
                                _j.trys.push([5, 8, , 9]);
                                if (!(key.indexOf('.xml') > -1)) return [3 /*break*/, 7];
                                console.warn("Warning -- Found Invalid Aggregator File Name - ".concat(key));
                                return [4 /*yield*/, getS3Work(key, bucket)
                                        .then(function (work) { return __awaiter(void 0, void 0, void 0, function () {
                                        var workSet, reQueueWorkRes;
                                        return __generator(this, function (_a) {
                                            switch (_a.label) {
                                                case 0:
                                                    workSet = work.split('</Envelope>');
                                                    return [4 /*yield*/, packageUpdates(workSet, key, customersConfig)];
                                                case 1:
                                                    reQueueWorkRes = _a.sent();
                                                    return [2 /*return*/];
                                            }
                                        });
                                    }); })];
                            case 6:
                                getWork = _j.sent();
                                if (s3db_cc.SelectiveDebug.indexOf("_101,") > -1)
                                    console.info("(101) Processing inbound data for ".concat(customersConfig.Customer, " - ").concat(key));
                                _j.label = 7;
                            case 7: return [3 /*break*/, 9];
                            case 8:
                                e_2 = _j.sent();
                                throw new Error("Exception - ReQueing Work from ".concat(bucket, " for ").concat(key, " \n").concat(e_2));
                            case 9:
                                batchCount = 0;
                                recs = 0;
                                _j.label = 10;
                            case 10:
                                _j.trys.push([10, 12, , 13]);
                                return [4 /*yield*/, processS3ObjectContentStream(key, bucket, customersConfig)
                                        .then(function (res) { return __awaiter(void 0, void 0, void 0, function () {
                                        var delResultCode, streamResults, e_4;
                                        var _a, _b;
                                        return __generator(this, function (_c) {
                                            switch (_c.label) {
                                                case 0:
                                                    streamResults = processS3ObjectStreamResolution;
                                                    streamResults.Key = key;
                                                    streamResults.Processed = res.OnEndRecordStatus;
                                                    console.info("Completed Processing Content Stream - ".concat(processS3ObjectStreamResolution.Key, " ").concat(processS3ObjectStreamResolution.Processed));
                                                    if (s3db_cc.SelectiveDebug.indexOf("_103,") > -1)
                                                        console.info("(103) Completed processing all records of the S3 Object ".concat(key, ". ").concat(res.OnEndRecordStatus));
                                                    //Don't delete the test data
                                                    if (localTesting)
                                                        key = 'TestData/S3Object_DoNotDelete';
                                                    if (!(((res === null || res === void 0 ? void 0 : res.PutToFireHoseAggregatorResult) === "200") ||
                                                        (((_a = res.OnEndStreamEndResult.StoreAndQueueWorkResult.AddWorkToS3WorkBucketResults) === null || _a === void 0 ? void 0 : _a.S3ProcessBucketResult) === "200" &&
                                                            ((_b = res.OnEndStreamEndResult.StoreAndQueueWorkResult.AddWorkToSQSWorkQueueResults) === null || _b === void 0 ? void 0 : _b.SQSWriteResult) === "200"))) return [3 /*break*/, 4];
                                                    _c.label = 1;
                                                case 1:
                                                    _c.trys.push([1, 3, , 4]);
                                                    return [4 /*yield*/, deleteS3Object(key, bucket)
                                                            .catch(function (e) {
                                                            console.error("Exception - DeleteS3Object - ".concat(e));
                                                        })];
                                                case 2:
                                                    //Once File successfully processed delete the original S3 Object
                                                    delResultCode = _c.sent();
                                                    if (delResultCode !== '204') {
                                                        streamResults = __assign(__assign({}, streamResults), { DeleteResult: JSON.stringify(delResultCode) });
                                                        if (s3db_cc.SelectiveDebug.indexOf("_104,") > -1)
                                                            console.error("Processing Successful, but Unsuccessful Delete of ".concat(key, ", Expected 204 result code, received ").concat(delResultCode));
                                                    }
                                                    else {
                                                        if (s3db_cc.SelectiveDebug.indexOf("_104,") > -1)
                                                            console.info("(104) Processing Successful, Delete of ".concat(key, " Successful (Result ").concat(delResultCode, ")."));
                                                        streamResults = __assign(__assign({}, streamResults), { DeleteResult: "Successful Delete of ".concat(key, "  (Result ").concat(JSON.stringify(delResultCode), ")") });
                                                    }
                                                    return [3 /*break*/, 4];
                                                case 3:
                                                    e_4 = _c.sent();
                                                    console.error("Exception - Deleting S3 Object after successful processing of the Content Stream for ".concat(key, " \n").concat(e_4));
                                                    return [3 /*break*/, 4];
                                                case 4: return [2 /*return*/, streamResults];
                                            }
                                        });
                                    }); })
                                        .catch(function (e) {
                                        var r = "Exception - Process S3 Object Stream Catch - \n".concat(e);
                                        console.error(r);
                                        processS3ObjectStreamResolution = __assign(__assign({}, processS3ObjectStreamResolution), { ProcessS3ObjectStreamCatch: r });
                                        return processS3ObjectStreamResolution;
                                    })];
                            case 11:
                                processS3ObjectStreamResolution = _j.sent();
                                return [3 /*break*/, 13];
                            case 12:
                                e_3 = _j.sent();
                                console.error("Exception - Processing S3 Object Content Stream for ".concat(key, " \n").concat(e_3));
                                return [3 /*break*/, 13];
                            case 13:
                                if (s3db_cc.SelectiveDebug.indexOf("_903,") > -1)
                                    console.info("Selective Debug 903 - Returned from Processing S3 Object Content Stream for ".concat(key, ". Result: ").concat(JSON.stringify(processS3ObjectStreamResolution)));
                                return [2 /*return*/];
                        }
                    });
                };
                _i = 0, _b = event.Records;
                _h.label = 10;
            case 10:
                if (!(_i < _b.length)) return [3 /*break*/, 13];
                r = _b[_i];
                return [5 /*yield**/, _loop_1(r)];
            case 11:
                state_1 = _h.sent();
                if (typeof state_1 === "object")
                    return [2 /*return*/, state_1.value];
                if (state_1 === "break")
                    return [3 /*break*/, 13];
                _h.label = 12;
            case 12:
                _i++;
                return [3 /*break*/, 10];
            case 13:
                //Check for important Config updates (which caches the config in Lambdas long-running cache)
                checkForS3DBConfigUpdates();
                if (!(event.Records[0].s3.bucket.name && s3db_cc.S3DropBucketMaintHours > 0)) return [3 /*break*/, 15];
                return [4 /*yield*/, maintainS3DropBucket(customersConfig)];
            case 14:
                maintenance = _h.sent();
                l = maintenance[0];
                // console.info( `- ${ l } File(s) met criteria and are marked for reprocessing ` )
                if (s3db_cc.SelectiveDebug.indexOf("_926,") > -1) {
                    filesProcessed = maintenance[1];
                    if (l > 0)
                        console.info("Selective Debug 926 - ".concat(l, " Files met criteria and are returned for Processing: \n").concat(filesProcessed));
                    else
                        console.info("Selective Debug 926 - No files met the criteria to return for Reprocessing");
                }
                _h.label = 15;
            case 15:
                n = new Date().toISOString();
                osr = n + "  -  " + JSON.stringify(processS3ObjectStreamResolution) + '\n\n';
                osrl = osr.length;
                if (osrl > 10000) //100K Characters
                 {
                    osr = "Excessive Length of ProcessS3ObjectStreamResolution: ".concat(osrl, " Truncated: \n ").concat(osr.substring(0, 1000), " ... ").concat(osr.substring(osrl - 1000, osrl));
                    if (s3db_cc.SelectiveDebug.indexOf("_920,") > -1)
                        console.warn("Selective Debug 920 - \n ".concat(JSON.stringify(osr), " "));
                    return [2 /*return*/, osr];
                }
                k = processS3ObjectStreamResolution.Key;
                p = processS3ObjectStreamResolution.Processed;
                if (s3db_cc.SelectiveDebug.indexOf("_105,") > -1)
                    console.info("(105) Completing S3DropBucket Processing of Request Id ".concat(event.Records[0].responseElements['x-amz-request-id'], " for ").concat(k, " \n").concat(p));
                if (s3db_cc.SelectiveDebug.indexOf("_920,") > -1)
                    console.info("Selective Debug 920 - \n".concat(JSON.stringify(osr)));
                if (!s3db_cc.S3DropBucketLog) return [3 /*break*/, 17];
                logMsg = [osr];
                logKey = "S3DropBucket_Log_".concat(new Date().toISOString().replace(/:/g, '_'));
                return [4 /*yield*/, putToFirehose(logMsg, logKey, 'S3DropBucket_Logs_')];
            case 16:
                fireHoseLog = _h.sent();
                console.info("Write S3DropBucket Log to FireHose aggregation - ".concat(JSON.stringify(fireHoseLog)));
                _h.label = 17;
            case 17:
                processS3ObjectStreamResolution = {};
                return [2 /*return*/, osr];
        }
    });
}); };
exports.s3DropBucketHandler = s3DropBucketHandler;
exports.default = exports.s3DropBucketHandler;
function processS3ObjectContentStream(key, bucket, custConfig) {
    return __awaiter(this, void 0, void 0, function () {
        var s3C, streamResult;
        var _this = this;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    if (s3dbLogDebug)
                        console.info("Processing S3 Content Stream for ".concat(key));
                    s3C = {
                        Key: key,
                        Bucket: bucket
                    };
                    streamResult = processS3ObjectStreamResolution;
                    return [4 /*yield*/, s3.send(new client_s3_1.GetObjectCommand(s3C))
                            .then(function (getS3StreamResult) { return __awaiter(_this, void 0, void 0, function () {
                            var errMsg, s3ContentReadableStream, t, csvParser, jsonSep, jsonParser, packageResult;
                            var _this = this;
                            return __generator(this, function (_a) {
                                switch (_a.label) {
                                    case 0:
                                        if (getS3StreamResult.$metadata.httpStatusCode != 200) {
                                            errMsg = JSON.stringify(getS3StreamResult.$metadata);
                                            //throw new Error( `Get S3 Object Command failed for ${ key }. Result is ${ errMsg }` )
                                            return [2 /*return*/, __assign(__assign({}, streamResult), { ProcessS3ObjectStreamError: "Get S3 Object Command failed for ".concat(key, ". Result is ").concat(errMsg) })];
                                        }
                                        s3ContentReadableStream = getS3StreamResult.Body;
                                        t = (0, stream_transform_1.transform)(function (data) {
                                            //"The \"chunk\" argument must be of type string or an instance of Buffer or Uint8Array. Received an instance of Object"
                                            var r;
                                            if (Buffer.isBuffer(data))
                                                r = data.toString('utf8');
                                            else
                                                r = JSON.stringify(data) + '\n';
                                            return r;
                                        });
                                        if (key.indexOf('aggregate_') < 0 && custConfig.format.toLowerCase() === 'csv') {
                                            csvParser = (0, csv_parse_1.parse)({
                                                delimiter: ',',
                                                columns: true,
                                                comment: '#',
                                                trim: true,
                                                skip_records_with_error: true,
                                            });
                                            s3ContentReadableStream = s3ContentReadableStream.pipe(csvParser).pipe(t);
                                            //#region
                                            // s3ContentReadableStream = s3ContentReadableStream.pipe(csvParser), { end: false })
                                            // .on('error', function (err) {
                                            //     console.error(`CSVParser(${key}) - Error ${err}`)
                                            // })
                                            // .on('end', function (e: string) {
                                            //     console.info(`CSVParser(${key}) - OnEnd - Message: ${e} \nDebugData: ${JSON.stringify(debugData)}`)
                                            // })
                                            // .on('finish', function (f: string) {
                                            //     console.info(`CSVParser(${key}) - OnFinish ${f}`)
                                            // })
                                            // .on('close', function (c: string) {
                                            //     console.info(`CSVParser(${key}) - OnClose ${c}`)
                                            //     console.info(`Stream Closed \n${JSON.stringify(debugData)}`)
                                            // })
                                            // .on('skip', async function (err) {
                                            //     console.info(`CSVParse(${key}) - Invalid Record \nError: ${err.code} for record ${err.lines}.\nOne possible cause is a field containing commas ',' and not properly Double-Quoted. \nContent: ${err.record} \nMessage: ${err.message} \nStack: ${err.stack} `)
                                            // })
                                            // .on('data', function (f: string) {
                                            //     console.info(`CSVParse(${key}) - OnData ${f}`)
                                            //
                                            // })
                                            //#region
                                        }
                                        else {
                                            s3ContentReadableStream = s3ContentReadableStream.pipe(t);
                                        }
                                        //Placeholder - Everything should be JSON by the time we get here 
                                        if (custConfig.format.toLowerCase() === 'json') {
                                        }
                                        jsonSep = s3db_cc.jsonSeparator;
                                        if (custConfig.separator && key.indexOf('aggregate_') < 0)
                                            jsonSep = custConfig.separator;
                                        jsonParser = new JSONParser({
                                            // numberBufferSize: 64,        //64, //0, //undefined, // set to 0 to don't buffer.
                                            stringBufferSize: undefined,
                                            //separator: '\n',               // separator between object. For example `\n` for nd-js.
                                            //separator: `''`,               // separator between object. For example `\n` for nd-js.
                                            separator: jsonSep,
                                            paths: ['$'],
                                            keepStack: false,
                                            emitPartialTokens: false // whether to emit tokens mid-parsing.
                                        }) //, { objectMode: true })
                                        ;
                                        //At this point, all data, whether a json file or a csv file parsed by csvparser should come through
                                        //  as a series of Objects (One at a time in the Stream) with a line break after each Object.
                                        //   {data:data, data:data, .....}
                                        //   {data:data, data:data, .....}
                                        //   {data:data, data:data, .....}
                                        //
                                        // Later, OnData processing populates an Array with each line/Object, so the
                                        // final format will be an Array of Objects:
                                        // [ {data:data, data:data, .....},
                                        //  { data: data, data: data, .....},
                                        //  { data: data, data: data, .....}, ... ]
                                        //
                                        // s3ContentReadableStream = s3ContentReadableStream.pipe(t).pipe(jsonParser)
                                        s3ContentReadableStream = s3ContentReadableStream.pipe(jsonParser);
                                        s3ContentReadableStream.setMaxListeners(Number(s3db_cc.EventEmitterMaxListeners));
                                        chunks = [];
                                        batchCount = 0;
                                        recs = 0;
                                        // const readStream = await new Promise(async (resolve, reject) => {
                                        return [4 /*yield*/, new Promise(function (resolve, reject) { return __awaiter(_this, void 0, void 0, function () {
                                                return __generator(this, function (_a) {
                                                    s3ContentReadableStream
                                                        .on('error', function (err) {
                                                        return __awaiter(this, void 0, void 0, function () {
                                                            var errMessage;
                                                            return __generator(this, function (_a) {
                                                                errMessage = "An error has stopped Content Parsing at record ".concat(recs++, " for s3 object ").concat(key, ". Separator is ").concat(jsonSep, ".\n").concat(err, " \n").concat(chunks);
                                                                console.error(errMessage);
                                                                chunks = [];
                                                                batchCount = 0;
                                                                recs = 0;
                                                                streamResult = __assign(__assign({}, streamResult), { StreamException: "s3ContentReadableStreamErrorMessage ".concat(JSON.stringify(errMessage)) });
                                                                throw new Error("Error on Readable Stream for s3DropBucket Object ".concat(key, ".\nError Message: ").concat(errMessage, " "));
                                                            });
                                                        });
                                                    })
                                                        .on('data', function (s3Chunk) {
                                                        return __awaiter(this, void 0, void 0, function () {
                                                            var oa, a, e, e_5;
                                                            return __generator(this, function (_a) {
                                                                switch (_a.label) {
                                                                    case 0:
                                                                        if (key.toLowerCase().indexOf('aggregat') < 0
                                                                            && recs > custConfig.updateMaxRows)
                                                                            throw new Error("The number of Updates in this batch (".concat(recs, ") Exceeds Max Row Updates allowed in the Customers Config (").concat(custConfig.updateMaxRows, ").  ").concat(key, " will not be deleted from ").concat(s3db_cc.S3DropBucket, " to allow for review and possible restaging."));
                                                                        if (s3db_cc.SelectiveDebug.indexOf("_913,") > -1)
                                                                            console.info("Selective Debug 913 - s3ContentStream OnData - Another Batch ".concat(batchCount + 1, " of ").concat(Object.values(s3Chunk).length, " Updates read from ").concat(key, " "));
                                                                        try {
                                                                            oa = s3Chunk.value;
                                                                            //What's possible
                                                                            //  {} a single Object - Pura
                                                                            //  [{},{},{}] An Array of Objects - Alerus
                                                                            //  A list of Objects - (Aggregated files) Line delimited
                                                                            //      [{}
                                                                            //      {}
                                                                            //       ...
                                                                            //      {}]
                                                                            // An array of Strings (CSV Parsed)
                                                                            //  [{"key1":"value1","key2":"value2"},{"key1":"value1","key2":"value2"},...]
                                                                            //
                                                                            //Build a consistent Object of an Array of Objects
                                                                            // [{},{},{},...]
                                                                            if (Array.isArray(oa)) {
                                                                                for (a in oa) {
                                                                                    e = oa[a];
                                                                                    if (typeof e === "string") {
                                                                                        e = JSON.parse(e);
                                                                                    }
                                                                                    chunks.push(e);
                                                                                }
                                                                            }
                                                                            else {
                                                                                //chunks.push( JSON.stringify( oa ) )
                                                                                chunks.push(oa);
                                                                            }
                                                                        }
                                                                        catch (e) {
                                                                            console.error("Exception - ReadStream-OnData - Chunk aggregation for ".concat(key, " \nBatch ").concat(batchCount, " of ").concat(recs, " Updates. \n").concat(e, " "));
                                                                            streamResult = __assign(__assign({}, streamResult), { OnDataReadStreamException: "Exception - First Catch - ReadStream-OnData Processing for ".concat(key, " \nBatch ").concat(batchCount, " of ").concat(recs, " Updates. \n").concat(e, " ") });
                                                                        }
                                                                        _a.label = 1;
                                                                    case 1:
                                                                        _a.trys.push([1, 5, , 6]);
                                                                        _a.label = 2;
                                                                    case 2:
                                                                        if (!(chunks.length > 98)) return [3 /*break*/, 4];
                                                                        return [4 /*yield*/, packageUpdates(chunks, key, custConfig)];
                                                                    case 3:
                                                                        packageResult = _a.sent();
                                                                        streamResult = __assign(__assign({}, streamResult), { OnDataBatchingResult: JSON.stringify(packageResult) });
                                                                        return [3 /*break*/, 2];
                                                                    case 4: return [3 /*break*/, 6];
                                                                    case 5:
                                                                        e_5 = _a.sent();
                                                                        console.error("Exception - ReadStream-OnData - Batch Packaging for ".concat(key, " \nBatch ").concat(batchCount, " of ").concat(recs, " Updates. \n").concat(e_5, " "));
                                                                        streamResult = __assign(__assign({}, streamResult), { OnDataReadStreamException: "Exception - Second Catch - ReadStream-OnData Processing for ".concat(key, " \nBatch ").concat(batchCount, " of ").concat(recs, " Updates. \n").concat(e_5, " ") });
                                                                        return [3 /*break*/, 6];
                                                                    case 6: return [2 /*return*/];
                                                                }
                                                            });
                                                        });
                                                    })
                                                        .on('end', function () {
                                                        return __awaiter(this, void 0, void 0, function () {
                                                            var pfhRes, e_6, e_7, sErr, streamEndResult;
                                                            return __generator(this, function (_a) {
                                                                switch (_a.label) {
                                                                    case 0:
                                                                        if (recs < 1 && chunks.length < 1) {
                                                                            streamResult = __assign(__assign({}, streamResult), { OnEndNoRecordsException: "Exception - No records returned from parsing file.Check the content as well as the configured file format(".concat(custConfig.format, ") matches the content of the file.") });
                                                                            // console.error(`Exception - ${ JSON.stringify(streamResult) } `)
                                                                            //throw new Error( `Exception - onEnd ${ JSON.stringify( streamResult ) } ` )
                                                                            return [2 /*return*/, streamResult];
                                                                        }
                                                                        _a.label = 1;
                                                                    case 1:
                                                                        _a.trys.push([1, 8, , 9]);
                                                                        if (!((chunks.length > 0) &&
                                                                            (custConfig.updates.toLowerCase() === 'multiple') ||
                                                                            key.toLowerCase().indexOf('aggregat') > 0)) return [3 /*break*/, 3];
                                                                        return [4 /*yield*/, packageUpdates(chunks, key, custConfig)
                                                                                .then(function (res) {
                                                                                //console.info( `Return Await PackageResult from PackageUpdates: ${ JSON.stringify( res ) }` )
                                                                                return res;
                                                                            })];
                                                                    case 2:
                                                                        packageResult = _a.sent();
                                                                        streamResult = __assign(__assign({}, streamResult), { OnEndStreamEndResult: packageResult });
                                                                        _a.label = 3;
                                                                    case 3:
                                                                        if (!(chunks.length > 0 &&
                                                                            custConfig.updates.toLowerCase() === 'singular' &&
                                                                            key.toLowerCase().indexOf('aggregat') < 0)) return [3 /*break*/, 7];
                                                                        pfhRes = void 0;
                                                                        _a.label = 4;
                                                                    case 4:
                                                                        _a.trys.push([4, 6, , 7]);
                                                                        return [4 /*yield*/, putToFirehose(chunks, key, custConfig.Customer)
                                                                                .then(function (res) {
                                                                                var fRes = res;
                                                                                streamResult = __assign(__assign({}, streamResult), fRes);
                                                                                return streamResult;
                                                                            })];
                                                                    case 5:
                                                                        pfhRes = _a.sent();
                                                                        return [3 /*break*/, 7];
                                                                    case 6:
                                                                        e_6 = _a.sent();
                                                                        console.error("Exception - PutToFirehose Call - \n".concat(e_6, " "));
                                                                        streamResult = __assign(__assign({}, streamResult), { PutToFireHoseException: "Exception - PutToFirehose \n".concat(e_6, " ") });
                                                                        return [2 /*return*/, streamResult];
                                                                    case 7: return [3 /*break*/, 9];
                                                                    case 8:
                                                                        e_7 = _a.sent();
                                                                        sErr = "Exception - ReadStream OnEnd Processing - \n".concat(e_7, " ");
                                                                        // console.error(sErr)
                                                                        return [2 /*return*/, __assign(__assign({}, streamResult), { OnEndStreamResult: sErr })];
                                                                    case 9:
                                                                        streamEndResult = "S3 Content Stream Ended for ".concat(key, ".Processed ").concat(recs, " records as ").concat(batchCount, " batches.");
                                                                        streamResult = __assign(__assign({}, streamResult), { StreamEndResult: streamEndResult, OnEndRecordStatus: "Processed ".concat(recs, " records as ").concat(batchCount, " batches.") });
                                                                        if (s3db_cc.SelectiveDebug.indexOf("_902,") > -1)
                                                                            console.info("Selective Debug 902: Content Stream OnEnd for (".concat(key, ") - Store and Queue Work of ").concat(batchCount, " Batches of ").concat(recs, " records - Stream Result: \n").concat(JSON.stringify(streamResult), " "));
                                                                        //chunks = []
                                                                        //batchCount = 0
                                                                        //recs = 0
                                                                        resolve(__assign({}, streamResult));
                                                                        return [2 /*return*/];
                                                                }
                                                            });
                                                        });
                                                    })
                                                        .on('close', function () {
                                                        return __awaiter(this, void 0, void 0, function () {
                                                            return __generator(this, function (_a) {
                                                                streamResult = __assign(__assign({}, streamResult), { OnClose_Result: "S3 Content Stream Closed for ".concat(key) });
                                                                return [2 /*return*/];
                                                            });
                                                        });
                                                    });
                                                    if (s3db_cc.SelectiveDebug.indexOf("_102,") > -1)
                                                        console.info("(102) S3 Content Stream Opened for ".concat(key));
                                                    return [2 /*return*/];
                                                });
                                            }); })
                                                .then(function (r) {
                                                return __assign(__assign({}, streamResult), { ReturnLocation: "ReadStream Then Clause.\n".concat(r, " ") });
                                            })
                                                .catch(function (e) {
                                                var err = "Exception - ReadStream(catch) - Process S3 Object Content Stream for ".concat(key, ". \n").concat(e, " ");
                                                //console.error( err )
                                                //throw new Error( err )
                                                return __assign(__assign({}, streamResult), { OnCloseReadStreamException: "".concat(err) });
                                            })
                                            // return { ...readStream, ReturnLocation: `...End of ReadStream Promise` }
                                        ];
                                    case 1:
                                        // const readStream = await new Promise(async (resolve, reject) => {
                                        _a.sent();
                                        // return { ...readStream, ReturnLocation: `...End of ReadStream Promise` }
                                        return [2 /*return*/, __assign(__assign({}, streamResult), { ReturnLocation: "...End of ReadStream Promise" })];
                                }
                            });
                        }); })
                            .catch(function (e) {
                            // console.error(`Exception(error) - Process S3 Object Content Stream for ${ key }.\nResults: ${ JSON.stringify(streamResult) }.\n${ e } `)
                            //throw new Error( `Exception(throw) - ReadStream - For ${ key }.\nResults: ${ JSON.stringify( streamResult ) }.\n${ e } ` )
                            return __assign(__assign({}, streamResult), { ReadStreamException: "Exception(throw) - ReadStream - For ".concat(key, ".\nResults: ").concat(JSON.stringify(streamResult), ".\n").concat(e, " ") });
                        })
                        // return processS3ObjectResults
                    ];
                case 1:
                    // processS3ObjectResults
                    processS3ObjectStreamResolution = _a.sent();
                    // return processS3ObjectResults
                    return [2 /*return*/, processS3ObjectStreamResolution];
            }
        });
    });
}
function putToFirehose(chunks, key, cust) {
    return __awaiter(this, void 0, void 0, function () {
        var client, fireHoseStream, putFirehoseResp, _loop_2, _a, _b, _c, _i, j, e_8;
        return __generator(this, function (_d) {
            switch (_d.label) {
                case 0:
                    client = new client_firehose_1.FirehoseClient();
                    fireHoseStream = "S3DropBucket_Aggregator";
                    if (cust === "S3DropBucket_Logs_")
                        fireHoseStream = 'S3DropBucket_Log';
                    putFirehoseResp = {};
                    _d.label = 1;
                case 1:
                    _d.trys.push([1, 6, , 7]);
                    _loop_2 = function (j) {
                        var jo, fd, fp, fireCommand, firehosePutResult, e_9;
                        return __generator(this, function (_e) {
                            switch (_e.label) {
                                case 0:
                                    jo = chunks[j];
                                    //const j = JSON.parse( chunks[ fhchunk ] )
                                    if (cust !== "S3DropBucket_Log_")
                                        cust === Object.assign(jo, { "Customer": cust });
                                    fd = Buffer.from(JSON.stringify(jo), 'utf-8');
                                    fp = {
                                        DeliveryStreamName: fireHoseStream,
                                        Record: {
                                            Data: fd
                                        }
                                    };
                                    fireCommand = new client_firehose_1.PutRecordCommand(fp);
                                    _e.label = 1;
                                case 1:
                                    _e.trys.push([1, 3, , 4]);
                                    return [4 /*yield*/, client.send(fireCommand)
                                            .then(function (res) {
                                            if (fireHoseStream !== 'S3DropBucket_Log') {
                                                if (s3db_cc.SelectiveDebug.indexOf('_922,') > -1)
                                                    console.info("Selective Debug 922 - Put to Firehose Aggregator for ".concat(key, " - \n").concat(JSON.stringify(fd), " \nResult: ").concat(JSON.stringify(res), " "));
                                                if (res.$metadata.httpStatusCode === 200) {
                                                    firehosePutResult = __assign(__assign({}, firehosePutResult), { PutToFireHoseAggregatorResult: "".concat(res.$metadata.httpStatusCode) });
                                                    firehosePutResult = __assign(__assign({}, firehosePutResult), { OnEnd_PutToFireHoseAggregator: "Successful Put to Firehose Aggregator for ".concat(key, ".\n").concat(JSON.stringify(res), " \n").concat(res.RecordId, " ") });
                                                }
                                                else {
                                                    firehosePutResult = __assign(__assign({}, firehosePutResult), { PutToFireHoseAggregatorResult: "UnSuccessful Put to Firehose Aggregator for ".concat(key, " \n ").concat(JSON.stringify(res), " ") });
                                                }
                                            }
                                            return firehosePutResult;
                                        })
                                            .catch(function (e) {
                                            console.error("Exception - Put to Firehose Aggregator(Promise -catch) for ".concat(key, " \n").concat(e, " "));
                                            firehosePutResult = __assign(__assign({}, firehosePutResult), { PutToFireHoseException: "Exception - Put to Firehose Aggregator for ".concat(key, " \n").concat(e, " ") });
                                            return firehosePutResult;
                                        })];
                                case 2:
                                    putFirehoseResp = _e.sent();
                                    return [3 /*break*/, 4];
                                case 3:
                                    e_9 = _e.sent();
                                    console.error("Exception - PutToFirehose \n".concat(e_9, " "));
                                    return [3 /*break*/, 4];
                                case 4: return [2 /*return*/];
                            }
                        });
                    };
                    _a = chunks;
                    _b = [];
                    for (_c in _a)
                        _b.push(_c);
                    _i = 0;
                    _d.label = 2;
                case 2:
                    if (!(_i < _b.length)) return [3 /*break*/, 5];
                    _c = _b[_i];
                    if (!(_c in _a)) return [3 /*break*/, 4];
                    j = _c;
                    return [5 /*yield**/, _loop_2(j)];
                case 3:
                    _d.sent();
                    _d.label = 4;
                case 4:
                    _i++;
                    return [3 /*break*/, 2];
                case 5: return [2 /*return*/, putFirehoseResp];
                case 6:
                    e_8 = _d.sent();
                    console.error("Exception - Put to Firehose Aggregator(try-catch) for ".concat(key, " \n").concat(e_8, " "));
                    return [3 /*break*/, 7];
                case 7: return [2 /*return*/];
            }
        });
    });
}
/**
 * A Lambda function to process the Event payload received from SQS - AWS Queues.
 */
var S3DropBucketQueueProcessorHandler = function (event, context) { return __awaiter(void 0, void 0, void 0, function () {
    var d, custconfig, postResult, tqm, _i, _a, q, _b, work, d, e_10, maintenance, l, e_11;
    var _c, _d;
    return __generator(this, function (_e) {
        switch (_e.label) {
            case 0:
                //ToDo: Build aggregate results and outcomes block  
                s3 = new client_s3_1.S3Client({ region: process.env.s3DropBucketRegion });
                if (!(process.env["WorkQueueVisibilityTimeout"] === undefined ||
                    process.env["WorkQueueVisibilityTimeout"] === '' ||
                    process.env["WorkQueueVisibilityTimeout"] === null)) return [3 /*break*/, 2];
                return [4 /*yield*/, getValidateS3DropBucketConfig()];
            case 1:
                s3db_cc = _e.sent();
                _e.label = 2;
            case 2:
                if (s3db_cc.SelectiveDebug.indexOf("_98,") > -1)
                    console.info("Selective Debug 98 - S3DropBucket Options: ".concat(JSON.stringify(s3db_cc), " "));
                if (s3db_cc.SelectiveDebug.indexOf("_99,") > -1)
                    console.info("Selective Debug 99 - S3DropBucket Logging Options: ".concat(s3db_cc.SelectiveDebug, " "));
                if (s3db_cc.SelectiveDebug.indexOf("_909,") > -1)
                    console.info("Selective Debug 909 - Environment Vars: ".concat(JSON.stringify(process.env), " "));
                if (s3db_cc.WorkQueueQuiesce) {
                    console.info("WorkQueue Quiesce is in effect, no New Work will be Queued up in the SQS Process Queue.");
                    return [2 /*return*/];
                }
                if (!(s3db_cc.WorkQueueBucketPurgeCount > 0)) return [3 /*break*/, 4];
                console.info("Purge Requested, Only action will be to Purge ".concat(s3db_cc.WorkQueueBucketPurge, " of ").concat(s3db_cc.WorkQueueBucketPurgeCount, " Records. "));
                return [4 /*yield*/, purgeBucket(Number(process.env["WorkQueueBucketPurgeCount"]), process.env["WorkQueueBucketPurge"])];
            case 3:
                d = _e.sent();
                return [2 /*return*/, d];
            case 4:
                postResult = 'false';
                if (s3db_cc.SelectiveDebug.indexOf("_506,") > -1)
                    console.info("(506) Received SQS Events Batch of ".concat(event.Records.length, " records."));
                if (s3db_cc.SelectiveDebug.indexOf("_904,") > -1)
                    console.info("Selective Debug 904 - Received ".concat(event.Records.length, " Work Queue Records. The set of Records are: \n").concat(JSON.stringify(event), " "));
                //Empty BatchFail array 
                sqsBatchFail.batchItemFailures.forEach(function () {
                    sqsBatchFail.batchItemFailures.pop();
                });
                tqm = {
                    workKey: '',
                    versionId: '',
                    marker: '',
                    attempts: 0,
                    batchCount: '',
                    updateCount: '',
                    lastQueued: '',
                    custconfig: {
                        Customer: '',
                        format: '',
                        separator: '',
                        updates: '',
                        listId: '',
                        listName: '',
                        listType: '',
                        DBKey: '',
                        LookupKeys: '',
                        pod: '',
                        region: '',
                        updateMaxRows: 0,
                        refreshToken: '',
                        clientId: '',
                        clientSecret: '',
                        sftp: {
                            user: '',
                            password: '',
                            filepattern: '',
                            schedule: ''
                        },
                        transforms: {
                            jsonMap: {},
                            csvMap: {},
                            ignore: [],
                            script: []
                        }
                    }
                };
                _i = 0, _a = event.Records;
                _e.label = 5;
            case 5:
                if (!(_i < _a.length)) return [3 /*break*/, 20];
                q = _a[_i];
                tqm = JSON.parse(q.body);
                if (!(tqm.workKey === '')) return [3 /*break*/, 7];
                _b = tqm;
                return [4 /*yield*/, getAnS3ObjectforTesting(s3db_cc.S3DropBucketWorkBucket)];
            case 6:
                _b.workKey = (_c = _e.sent()) !== null && _c !== void 0 ? _c : "";
                _e.label = 7;
            case 7:
                if (tqm.workKey === 'devtest.xml') {
                    //tqm.workKey = await getAnS3ObjectforTesting( tcc.s3DropBucketWorkBucket! ) ?? ""
                    tqm.workKey = testS3Key;
                    s3db_cc.S3DropBucketWorkBucket = testS3Bucket;
                    localTesting = true;
                }
                else {
                    testS3Key = '';
                    testS3Bucket = '';
                    localTesting = false;
                }
                //
                //
                if (s3db_cc.SelectiveDebug.indexOf("_507,") > -1)
                    console.info("(507) Processing Work off the Queue - ".concat(tqm.workKey));
                if (s3db_cc.SelectiveDebug.indexOf("_911,") > -1)
                    console.info("Selective Debug 911 - Processing a Batch Item. SQS Event Message: ".concat(JSON.stringify(q)));
                return [4 /*yield*/, getCustomerConfig(tqm.workKey)];
            case 8:
                custconfig = (_e.sent());
                _e.label = 9;
            case 9:
                _e.trys.push([9, 18, , 19]);
                return [4 /*yield*/, getS3Work(tqm.workKey, s3db_cc.S3DropBucketWorkBucket)];
            case 10:
                work = _e.sent();
                if (!(work.length > 0)) return [3 /*break*/, 16];
                if (s3db_cc.SelectiveDebug.indexOf("_512,") > -1)
                    console.info("Selective Debug 512 - S3 Retrieve results for Work file ".concat(tqm.workKey, ": ").concat(JSON.stringify(work)));
                return [4 /*yield*/, postToCampaign(work, custconfig, tqm.updateCount)
                    //  postResult can contain: 
                    //retry
                    //unsuccessful post
                    //partially successful
                    //successfully posted
                ];
            case 11:
                postResult = _e.sent();
                //  postResult can contain: 
                //retry
                //unsuccessful post
                //partially successful
                //successfully posted
                if (s3db_cc.SelectiveDebug.indexOf("_908,") > -1)
                    console.info("Selective Debug 908 - POST Result for ".concat(tqm.workKey, ": ").concat(postResult, " "));
                if (!(postResult.indexOf('retry') > -1)) return [3 /*break*/, 12];
                console.warn("Retry Marked for ".concat(tqm.workKey, ". Returning Work Item ").concat(q.messageId, " to Process Queue (Total Retry Count: ").concat(sqsBatchFail.batchItemFailures.length + 1, "). \n").concat(postResult, " "));
                //Add to BatchFail array to Retry processing the work 
                sqsBatchFail.batchItemFailures.push({ itemIdentifier: q.messageId });
                if (s3db_cc.SelectiveDebug.indexOf("_509,") > -1)
                    console.warn("(509) - ".concat(tqm.workKey, " added back to Queue for Retry \n").concat(JSON.stringify(sqsBatchFail), " "));
                return [3 /*break*/, 15];
            case 12:
                if (!(postResult.toLowerCase().indexOf('unsuccessful post') > -1)) return [3 /*break*/, 13];
                console.error("Error - Unsuccessful POST (Hard Failure) for ".concat(tqm.workKey, ": \n").concat(postResult, "\nCustomer: ").concat(custconfig.Customer, ", ListId: ").concat(custconfig.listId, " ListName: ").concat(custconfig.listName, " "));
                return [3 /*break*/, 15];
            case 13:
                if (postResult.toLowerCase().indexOf('partially successful') > -1) {
                    if (s3db_cc.SelectiveDebug.indexOf("_508,") > -1)
                        console.info("(508) Most Work was Successfully Posted to Campaign (work file (".concat(tqm.workKey, ", updated ").concat(tqm.custconfig.listName, " from ").concat(tqm.workKey, ", however there were some exceptions: \n").concat(postResult, " "));
                }
                else if (postResult.toLowerCase().indexOf('successfully posted') > -1) {
                    if (s3db_cc.SelectiveDebug.indexOf("_508,") > -1)
                        console.info("(508) Work Successfully Posted to Campaign (work file (".concat(tqm.workKey, ", updated ").concat(tqm.custconfig.listName, " from ").concat(tqm.workKey, ", \n").concat(postResult, " \nThe Work will be deleted from the S3 Process Queue"));
                }
                return [4 /*yield*/, deleteS3Object(tqm.workKey, s3db_cc.S3DropBucketWorkBucket)];
            case 14:
                d = _e.sent();
                if (d === '204') {
                    if (s3db_cc.SelectiveDebug.indexOf("_924,") > -1)
                        console.info("Selective Debug 924 - Successful Deletion of Queued Work file: ".concat(tqm.workKey));
                }
                else if (s3db_cc.SelectiveDebug.indexOf("_924,") > -1)
                    console.error("Selective Debug 924 - Failed to Delete ".concat(tqm.workKey, ". Expected '204' but received ").concat(d, " "));
                if (s3db_cc.SelectiveDebug.indexOf("_511,") > -1)
                    console.info("(511) Processed ".concat(tqm.updateCount, " Updates from ").concat(tqm.workKey));
                if (s3db_cc.SelectiveDebug.indexOf("_510,") > -1)
                    console.info("(510) Processed ".concat(event.Records.length, " Work Queue Events. Posted: ").concat(postResult, ". \nItems Retry Count: ").concat(sqsBatchFail.batchItemFailures.length, " \nItems Retry List: ").concat(JSON.stringify(sqsBatchFail), " "));
                _e.label = 15;
            case 15: return [3 /*break*/, 17];
            case 16: throw new Error("Failed to retrieve work file(".concat(tqm.workKey, ") "));
            case 17: return [3 /*break*/, 19];
            case 18:
                e_10 = _e.sent();
                console.error("Exception - Processing a Work File (".concat(tqm.workKey, " off the Work Queue - \n").concat(e_10, "} "));
                return [3 /*break*/, 19];
            case 19:
                _i++;
                return [3 /*break*/, 5];
            case 20:
                maintenance = [];
                if (!(s3db_cc.S3DropBucketWorkQueueMaintHours > 0)) return [3 /*break*/, 24];
                _e.label = 21;
            case 21:
                _e.trys.push([21, 23, , 24]);
                return [4 /*yield*/, maintainS3DropBucketQueueBucket()];
            case 22:
                maintenance = (_d = _e.sent()) !== null && _d !== void 0 ? _d : [0, ''];
                if (s3db_cc.SelectiveDebug.indexOf("_927,") > -1) {
                    l = maintenance[0];
                    if (l > 0)
                        console.info("Selective Debug 927 - ReQueued Work Files: \n".concat(maintenance, " "));
                    else
                        console.info("Selective Debug 927 - No Work files met criteria to ReQueue");
                }
                return [3 /*break*/, 24];
            case 23:
                e_11 = _e.sent();
                debugger;
                return [3 /*break*/, 24];
            case 24:
                if (s3db_cc.SelectiveDebug.indexOf("_912,") > -1)
                    console.info("Selective Debug 912 - ".concat(tqm.workKey, " returned to Work Queue for Retry \n").concat(JSON.stringify(sqsBatchFail), " "));
                //ToDo: For Queue Processing - Complete the Final Processing Outcomes messaging for Queue Processing 
                // if (tcc.SelectiveDebug.indexOf("_921,") > -1) console.info(`Selective Debug 921 - \n${ JSON.stringify(processS3ObjectStreamResolution) } `)
                return [2 /*return*/, sqsBatchFail
                    //For debugging - report no fails 
                    // return {
                    //     batchItemFailures: [
                    //         {
                    //             itemIdentifier: ''
                    //         }
                    //     ]
                    // }
                ];
        }
    });
}); };
exports.S3DropBucketQueueProcessorHandler = S3DropBucketQueueProcessorHandler;
var s3DropBucketSFTPHandler = function (event, context) { return __awaiter(void 0, void 0, void 0, function () {
    var client, input, command, response, _i, _a, q, tqm, _b;
    var _c;
    return __generator(this, function (_d) {
        switch (_d.label) {
            case 0:
                if (!(process.env["WorkQueueVisibilityTimeout"] === undefined ||
                    process.env["WorkQueueVisibilityTimeout"] === '' ||
                    process.env["WorkQueueVisibilityTimeout"] === null)) return [3 /*break*/, 2];
                return [4 /*yield*/, getValidateS3DropBucketConfig()];
            case 1:
                s3db_cc = _d.sent();
                _d.label = 2;
            case 2:
                console.info("S3 Dropbucket SFTP Processor Selective Debug Set is: ".concat(s3db_cc.SelectiveDebug, " "));
                if (s3db_cc.SelectiveDebug.indexOf("_98,") > -1)
                    console.info("Selective Debug 98 - Process Environment Vars: ".concat(JSON.stringify(process.env), " "));
                console.info("SFTP  Received Event: ".concat(JSON.stringify(event), " "));
                client = new client_scheduler_1.SchedulerClient();
                input = {
                    // GroupName: "STRING_VALUE",
                    NamePrefix: "STRING_VALUE",
                    // State: "STRING_VALUE",
                    NextToken: "STRING_VALUE",
                    MaxResults: Number("int"),
                };
                command = new client_scheduler_1.ListSchedulesCommand(input);
                return [4 /*yield*/, client.send(command)
                    // { // ListSchedulesOutput
                    //   NextToken: "STRING_VALUE",
                    //   Schedules: [ // ScheduleList // required
                    //     { // ScheduleSummary
                    //       Arn: "STRING_VALUE",
                    //       Name: "STRING_VALUE",
                    //       GroupName: "STRING_VALUE",
                    //       State: "STRING_VALUE",
                    //       CreationDate: new Date("TIMESTAMP"),
                    //       LastModificationDate: new Date("TIMESTAMP"),
                    //       Target: { // TargetSummary
                    //         Arn: "STRING_VALUE", // required
                    //       },
                    //     },
                    //   ],
                    // };
                    //Avoid following code until refactoring completed
                ];
            case 3:
                response = _d.sent();
                // { // ListSchedulesOutput
                //   NextToken: "STRING_VALUE",
                //   Schedules: [ // ScheduleList // required
                //     { // ScheduleSummary
                //       Arn: "STRING_VALUE",
                //       Name: "STRING_VALUE",
                //       GroupName: "STRING_VALUE",
                //       State: "STRING_VALUE",
                //       CreationDate: new Date("TIMESTAMP"),
                //       LastModificationDate: new Date("TIMESTAMP"),
                //       Target: { // TargetSummary
                //         Arn: "STRING_VALUE", // required
                //       },
                //     },
                //   ],
                // };
                //Avoid following code until refactoring completed
                if (new Date().getTime() > 0)
                    return [2 /*return*/];
                console.info("Received SFTP SQS Events Batch of ".concat(event.Records.length, " records."));
                if (s3db_cc.SelectiveDebug.indexOf("_4,") > -1)
                    console.info("Selective Debug 4 - Received ".concat(event.Records.length, " SFTP Queue Records.Records are: \n").concat(JSON.stringify(event), " "));
                // event.Records.forEach((i) => {
                //     sqsBatchFail.batchItemFailures.push({ itemIdentifier: i.messageId })
                // })
                //Empty BatchFail array 
                sqsBatchFail.batchItemFailures.forEach(function () {
                    sqsBatchFail.batchItemFailures.pop();
                });
                _i = 0, _a = event.Records;
                _d.label = 4;
            case 4:
                if (!(_i < _a.length)) return [3 /*break*/, 8];
                q = _a[_i];
                tqm = JSON.parse(q.body);
                tqm.workKey = JSON.parse(q.body).workKey;
                if (!(tqm.workKey === 'process_2_pura_2023_10_27T15_11_40_732Z.csv')) return [3 /*break*/, 6];
                _b = tqm;
                return [4 /*yield*/, getAnS3ObjectforTesting(s3db_cc.S3DropBucket)];
            case 5:
                _b.workKey = (_c = _d.sent()) !== null && _c !== void 0 ? _c : "";
                _d.label = 6;
            case 6:
                console.info("Processing Work Queue for ".concat(tqm.workKey));
                if (s3db_cc.SelectiveDebug.indexOf("_11,") > -1)
                    console.info("Selective Debug 11 - SQS Events - Processing Batch Item ".concat(JSON.stringify(q), " "));
                _d.label = 7;
            case 7:
                _i++;
                return [3 /*break*/, 4];
            case 8:
                console.info("Processed ".concat(event.Records.length, " SFTP Requests.Items Fail Count: ").concat(sqsBatchFail.batchItemFailures.length, "\nItems Failed List: ").concat(JSON.stringify(sqsBatchFail)));
                return [2 /*return*/, sqsBatchFail
                    //For debugging - report no fails 
                    // return {
                    //     batchItemFailures: [
                    //         {
                    //             itemIdentifier: ''
                    //         }
                    //     ]
                    // }
                ];
        }
    });
}); };
exports.s3DropBucketSFTPHandler = s3DropBucketSFTPHandler;
function sftpConnect(options) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            console.info("Connecting to ".concat(options.host, ": ").concat(options.port));
            try {
                // await sftpClient.connect(options)
            }
            catch (err) {
                console.info('Failed to connect:', err);
            }
            return [2 /*return*/];
        });
    });
}
function sftpDisconnect() {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            return [2 /*return*/];
        });
    });
}
function sftpListFiles(remoteDir, fileGlob) {
    return __awaiter(this, void 0, void 0, function () {
        var fileObjects, fileNames, _i, fileObjects_1, file;
        return __generator(this, function (_a) {
            console.info("Listing ".concat(remoteDir, " ..."));
            fileObjects = [];
            try {
                // fileObjects = await sftpClient.list(remoteDir, fileGlob)
            }
            catch (err) {
                console.info('Listing failed:', err);
            }
            fileNames = [];
            for (_i = 0, fileObjects_1 = fileObjects; _i < fileObjects_1.length; _i++) {
                file = fileObjects_1[_i];
                if (file.type === 'd') {
                    console.info("".concat(new Date(file.modifyTime).toISOString(), " PRE ").concat(file.name));
                }
                else {
                    console.info("".concat(new Date(file.modifyTime).toISOString(), " ").concat(file.size, " ").concat(file.name));
                }
                fileNames.push(file.name);
            }
            return [2 /*return*/, fileNames];
        });
    });
}
function sftpUploadFile(localFile, remoteFile) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            console.info("Uploading ".concat(localFile, " to ").concat(remoteFile, " ..."));
            try {
                // await sftpClient.put(localFile, remoteFile)
            }
            catch (err) {
                console.error('Uploading failed:', err);
            }
            return [2 /*return*/];
        });
    });
}
function sftpDownloadFile(remoteFile, localFile) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            console.info("Downloading ".concat(remoteFile, " to ").concat(localFile, " ..."));
            try {
                // await sftpClient.get(remoteFile, localFile)
            }
            catch (err) {
                console.error('Downloading failed:', err);
            }
            return [2 /*return*/];
        });
    });
}
function sftpDeleteFile(remoteFile) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            console.info("Deleting ".concat(remoteFile));
            try {
                // await sftpClient.delete(remoteFile)
            }
            catch (err) {
                console.error('Deleting failed:', err);
            }
            return [2 /*return*/];
        });
    });
}
function checkForS3DBConfigUpdates() {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    if (s3dbLogDebug)
                        console.info("Checking for S3DropBucket Config updates");
                    return [4 /*yield*/, getValidateS3DropBucketConfig()];
                case 1:
                    s3db_cc = _a.sent();
                    if (s3db_cc.SelectiveDebug.indexOf("_901,") > -1)
                        console.info("Selective Debug 901 - Refreshed S3DropBucket Queue Config \n ".concat(JSON.stringify(s3db_cc), " "));
                    return [2 /*return*/];
            }
        });
    });
}
function getValidateS3DropBucketConfig() {
    return __awaiter(this, void 0, void 0, function () {
        var getObjectCmd, s3dbcr, s3dbc, e_12;
        var _this = this;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    debugger;
                    if (!process.env.S3DropBucketConfigBucket)
                        process.env.S3DropBucketConfigBucket = 's3dropbucket-configs';
                    if (!process.env.S3DropBucketConfigFile)
                        process.env.S3DropBucketConfigFile = 's3dropbucket_config.jsonc';
                    getObjectCmd = {
                        Bucket: process.env.S3DropBucketConfigBucket,
                        Key: process.env.S3DropBucketConfigFile
                    };
                    s3dbc = {};
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 3, , 4]);
                    return [4 /*yield*/, s3.send(new client_s3_1.GetObjectCommand(getObjectCmd))
                            .then(function (getConfigS3Result) { return __awaiter(_this, void 0, void 0, function () {
                            var _a;
                            return __generator(this, function (_b) {
                                switch (_b.label) {
                                    case 0: return [4 /*yield*/, ((_a = getConfigS3Result.Body) === null || _a === void 0 ? void 0 : _a.transformToString('utf8'))];
                                    case 1:
                                        s3dbcr = (_b.sent());
                                        //Parse comments out of the json before returning parsed config json
                                        s3dbcr = s3dbcr.replaceAll(new RegExp(/[^:](\/\/.*(,|$|")?)/g), '');
                                        s3dbcr = s3dbcr.replaceAll(' ', '');
                                        s3dbcr = s3dbcr.replaceAll('\n', '');
                                        return [2 /*return*/, JSON.parse(s3dbcr)];
                                }
                            });
                        }); })];
                case 2:
                    s3dbc = _a.sent();
                    return [3 /*break*/, 4];
                case 3:
                    e_12 = _a.sent();
                    console.error("Exception - Pulling S3DropBucket Config File (bucket:".concat(getObjectCmd.Bucket, "  key:").concat(getObjectCmd.Key, ") \nResult: ").concat(s3dbcr, " \nException: \n").concat(e_12, " "));
                    return [2 /*return*/, {}];
                case 4:
                    try {
                        if (s3dbc.LOGLEVEL !== undefined && s3dbc.LOGLEVEL.toLowerCase().indexOf('debug') > -1) {
                            s3dbLogDebug = true;
                            process.env.s3dbLogDebug = "true";
                        }
                        if (s3dbc.LOGLEVEL !== undefined && s3dbc.LOGLEVEL.toLowerCase().indexOf('verbose') > -1) {
                            s3dbLogVerbose = true;
                            process.env["s3dbLogVerbose"] = "true";
                        }
                        if (s3dbc.SelectiveDebug !== undefined)
                            process.env["SelectiveDebug"] = s3dbc.SelectiveDebug;
                        //Perhaps validate that the string contains commas and underscores as needed, 
                        if (!s3dbc.S3DropBucket || s3dbc.S3DropBucket === "") {
                            throw new Error("Exception - S3DropBucket Configuration is not correct: ".concat(s3dbc.S3DropBucket, "."));
                        }
                        else
                            process.env["s3DropBucket"] = s3dbc.S3DropBucket;
                        if (!s3dbc.S3DropBucketConfigs || s3dbc.S3DropBucketConfigs === "") {
                            throw new Error("Exception -S3DropBucketConfigs definition is not correct: ".concat(s3dbc.S3DropBucketConfigs, "."));
                        }
                        else
                            process.env["s3DropBucketConfigs"] = s3dbc.S3DropBucketConfigs;
                        if (!s3dbc.S3DropBucketWorkBucket || s3dbc.S3DropBucketWorkBucket === "") {
                            throw new Error("Exception - S3DropBucket Work Bucket Configuration is not correct: ".concat(s3dbc.S3DropBucketWorkBucket, " "));
                        }
                        else
                            process.env["s3DropBucketWorkBucket"] = s3dbc.S3DropBucketWorkBucket;
                        if (!s3dbc.S3DropBucketWorkQueue || s3dbc.S3DropBucketWorkQueue === "") {
                            throw new Error("Exception -S3DropBucket Work Queue Configuration is not correct: ".concat(s3dbc.S3DropBucketWorkQueue, " "));
                        }
                        else
                            process.env["s3DropBucketWorkQueue"] = s3dbc.S3DropBucketWorkQueue;
                        // if (tc.SQS_QUEUE_URL !== undefined) tcc.SQS_QUEUE_URL = tc.SQS_QUEUE_URL
                        // else throw new Error(`S3DropBucket Config invalid definition: SQS_QUEUE_URL - ${ tc.SQS_QUEUE_URL } `)
                        if (s3dbc.xmlapiurl != undefined)
                            process.env["xmlapiurl"] = s3dbc.xmlapiurl;
                        else
                            throw new Error("S3DropBucket Config invalid definition: xmlapiurl - ".concat(s3dbc.xmlapiurl, " "));
                        if (s3dbc.restapiurl !== undefined)
                            process.env["restapiurl"] = s3dbc.restapiurl;
                        else
                            throw new Error("S3DropBucket Config invalid definition: restapiurl - ".concat(s3dbc.restapiurl, " "));
                        if (s3dbc.authapiurl !== undefined)
                            process.env["authapiurl"] = s3dbc.authapiurl;
                        else
                            throw new Error("S3DropBucket Config invalid definition: authapiurl - ".concat(s3db_cc.authapiurl, " "));
                        //Default Seperator 
                        if (s3dbc.jsonSeparator !== undefined) {
                            if (s3dbc.jsonSeparator.toLowerCase() === "null")
                                s3dbc.jsonSeparator = "''";
                            if (s3dbc.jsonSeparator.toLowerCase() === "empty")
                                s3dbc.jsonSeparator = "\"\"";
                            if (s3dbc.jsonSeparator.toLowerCase() === "\n")
                                s3dbc.jsonSeparator = '\n';
                        }
                        else
                            s3dbc.jsonSeparator = '\n';
                        process.env['jsonSeparator'] = s3dbc.jsonSeparator;
                        if (s3dbc.WorkQueueQuiesce !== undefined) {
                            process.env["WorkQueueQuiesce"] = s3dbc.WorkQueueQuiesce.toString();
                        }
                        else
                            throw new Error("S3DropBucket Config invalid definition: WorkQueueQuiesce - ".concat(s3dbc.WorkQueueQuiesce, " "));
                        //deprecated in favor of using AWS interface to set these on the queue
                        // if (tc.WorkQueueVisibilityTimeout !== undefined)
                        //     process.env.WorkQueueVisibilityTimeout = tc.WorkQueueVisibilityTimeout.toFixed()
                        // else
                        //     throw new Error(
                        //         `S3DropBucket Config invalid definition: WorkQueueVisibilityTimeout - ${ tc.WorkQueueVisibilityTimeout } `,
                        //     )
                        // if (tc.WorkQueueWaitTimeSeconds !== undefined)
                        //     process.env.WorkQueueWaitTimeSeconds = tc.WorkQueueWaitTimeSeconds.toFixed()
                        // else
                        //     throw new Error(
                        //         `S3DropBucket Config invalid definition: WorkQueueWaitTimeSeconds - ${ tc.WorkQueueWaitTimeSeconds } `,
                        //     )
                        // if (tc.RetryQueueVisibilityTimeout !== undefined)
                        //     process.env.RetryQueueVisibilityTimeout = tc.WorkQueueWaitTimeSeconds.toFixed()
                        // else
                        //     throw new Error(
                        //         `S3DropBucket Config invalid definition: RetryQueueVisibilityTimeout - ${ tc.RetryQueueVisibilityTimeout } `,
                        //     )
                        // if (tc.RetryQueueInitialWaitTimeSeconds !== undefined)
                        //     process.env.RetryQueueInitialWaitTimeSeconds = tc.RetryQueueInitialWaitTimeSeconds.toFixed()
                        // else
                        //     throw new Error(
                        //         `S3DropBucket Config invalid definition: RetryQueueInitialWaitTimeSeconds - ${ tc.RetryQueueInitialWaitTimeSeconds } `,
                        //     )
                        if (s3dbc.MaxBatchesWarning !== undefined)
                            process.env["RetryQueueInitialWaitTimeSeconds"] = s3dbc.MaxBatchesWarning.toFixed();
                        else
                            throw new Error("S3DropBucket Config invalid definition: MaxBatchesWarning - ".concat(s3dbc.MaxBatchesWarning, " "));
                        if (s3dbc.S3DropBucketQuiesce !== undefined) {
                            process.env["DropBucketQuiesce"] = s3dbc.S3DropBucketQuiesce.toString();
                        }
                        else
                            throw new Error("S3DropBucket Config invalid definition: DropBucketQuiesce - ".concat(s3dbc.S3DropBucketQuiesce, " "));
                        if (s3dbc.S3DropBucketMaintHours !== undefined) {
                            process.env["DropBucketMaintHours"] = s3dbc.S3DropBucketMaintHours.toString();
                        }
                        else
                            s3dbc.S3DropBucketMaintHours = -1;
                        if (s3dbc.S3DropBucketMaintLimit !== undefined) {
                            process.env["DropBucketMaintLimit"] = s3dbc.S3DropBucketMaintLimit.toString();
                        }
                        else
                            s3dbc.S3DropBucketMaintLimit = 0;
                        if (s3dbc.S3DropBucketMaintConcurrency !== undefined) {
                            process.env["DropBucketMaintConcurrency"] = s3dbc.S3DropBucketMaintConcurrency.toString();
                        }
                        else
                            s3dbc.S3DropBucketMaintLimit = 1;
                        if (s3dbc.S3DropBucketWorkQueueMaintHours !== undefined) {
                            process.env["DropBucketWorkQueueMaintHours"] = s3dbc.S3DropBucketWorkQueueMaintHours.toString();
                        }
                        else
                            s3dbc.S3DropBucketWorkQueueMaintHours = -1;
                        if (s3dbc.S3DropBucketWorkQueueMaintLimit !== undefined) {
                            process.env["DropBucketWorkQueueMaintLimit"] = s3dbc.S3DropBucketWorkQueueMaintLimit.toString();
                        }
                        else
                            s3dbc.S3DropBucketWorkQueueMaintLimit = 0;
                        if (s3dbc.S3DropBucketWorkQueueMaintConcurrency !== undefined) {
                            process.env["DropBucketWorkQueueMaintConcurrency"] = s3dbc.S3DropBucketWorkQueueMaintConcurrency.toString();
                        }
                        else
                            s3dbc.S3DropBucketWorkQueueMaintConcurrency = 1;
                        if (s3dbc.S3DropBucketLog !== undefined) {
                            process.env["S3DropBucketLog"] = s3dbc.S3DropBucketLog.toString();
                        }
                        else
                            s3dbc.S3DropBucketLog = false;
                        if (s3dbc.S3DropBucketLogBucket !== undefined) {
                            process.env["S3DropBucketLogBucket"] = s3dbc.S3DropBucketLogBucket.toString();
                        }
                        else
                            s3dbc.S3DropBucketLogBucket = '';
                        if (s3dbc.S3DropBucketWorkQueueMaintConcurrency !== undefined) {
                            process.env["DropBucketWorkQueueMaintConcurrency"] = s3dbc.S3DropBucketWorkQueueMaintConcurrency.toString();
                        }
                        else
                            s3dbc.S3DropBucketWorkQueueMaintConcurrency = 1;
                        if (s3dbc.S3DropBucketPurge !== undefined)
                            process.env["DropBucketPurge"] = s3dbc.S3DropBucketPurge;
                        else
                            throw new Error("S3DropBucket Config invalid definition: DropBucketPurge - ".concat(s3dbc.S3DropBucketPurge, " "));
                        if (s3dbc.S3DropBucketPurgeCount !== undefined)
                            process.env["DropBucketPurgeCount"] = s3dbc.S3DropBucketPurgeCount.toFixed();
                        else
                            throw new Error("S3DropBucket Config invalid definition: DropBucketPurgeCount - ".concat(s3dbc.S3DropBucketPurgeCount, " "));
                        if (s3dbc.QueueBucketQuiesce !== undefined) {
                            process.env["QueueBucketQuiesce"] = s3dbc.QueueBucketQuiesce.toString();
                        }
                        else
                            throw new Error("S3DropBucket Config invalid definition: QueueBucketQuiesce - ".concat(s3dbc.QueueBucketQuiesce, " "));
                        if (s3dbc.WorkQueueBucketPurge !== undefined)
                            process.env["WorkQueueBucketPurge"] = s3dbc.WorkQueueBucketPurge;
                        else
                            throw new Error("S3DropBucket Config invalid definition: WorkQueueBucketPurge - ".concat(s3dbc.WorkQueueBucketPurge, " "));
                        if (s3dbc.WorkQueueBucketPurgeCount !== undefined)
                            process.env["WorkQueueBucketPurgeCount"] = s3dbc.WorkQueueBucketPurgeCount.toFixed();
                        else
                            throw new Error("S3DropBucket Config invalid definition: WorkQueueBucketPurgeCount - ".concat(s3dbc.WorkQueueBucketPurgeCount, " "));
                        if (s3dbc.prefixFocus !== undefined && s3dbc.prefixFocus != "") {
                            process.env["S3DropBucketFocusPrefix"] = s3dbc.prefixFocus;
                            console.warn("A Prefix Focus has been configured. Only S3DropBucket Objects with the prefix \"".concat(s3dbc.prefixFocus, "\" will be processed."));
                        }
                    }
                    catch (e) {
                        throw new Error("Exception - Parsing S3DropBucket Config File ".concat(e, " "));
                    }
                    if (s3dbc.SelectiveDebug.indexOf("_901,") > -1)
                        console.info("Selective Debug 901 - Pulled s3dropbucket_config.jsonc: \n".concat(JSON.stringify(s3dbc), " "));
                    return [2 /*return*/, s3dbc];
            }
        });
    });
}
function getCustomerConfig(filekey) {
    var _a;
    return __awaiter(this, void 0, void 0, function () {
        var r, customer, configJSON, getObjectCommand, ccr, e_13;
        var _this = this;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0:
                    // Retrieve file's prefix as the Customer Name 
                    if (!filekey)
                        throw new Error("Exception - Cannot resolve Customer Config without a valid Customer Prefix (file is ".concat(filekey, ")"));
                    while (filekey.indexOf('/') > -1) //remove any folders from name
                     {
                        filekey = (_a = filekey.split('/').at(-1)) !== null && _a !== void 0 ? _a : filekey;
                    }
                    r = new RegExp(/\d{4}_\d{2}_\d{2}T.*Z.*/, 'gm');
                    if (filekey.match(r)) {
                        filekey = filekey.replace(r, ''); //remove timestamp from name
                    }
                    customer = filekey;
                    //const customer = filekey.split('_')[0] + '_'      //initial treatment, get prefix up to first underscore
                    if (customer === '_' || customer.length < 4) //shouldn't need this customer.indexOf('_') < 0 || 
                     {
                        throw new Error("Exception - Customer cannot be determined from S3 Object Name '".concat(filekey, "'      \n      "));
                    }
                    configJSON = {};
                    getObjectCommand = {
                        Key: "".concat(customer, "config.jsonc"),
                        //Bucket: 's3dropbucket-configs'
                        Bucket: process.env.S3DropBucketConfigBucket
                    };
                    _b.label = 1;
                case 1:
                    _b.trys.push([1, 3, , 4]);
                    return [4 /*yield*/, s3.send(new client_s3_1.GetObjectCommand(getObjectCommand))
                            .then(function (getConfigS3Result) { return __awaiter(_this, void 0, void 0, function () {
                            var _a;
                            return __generator(this, function (_b) {
                                switch (_b.label) {
                                    case 0: return [4 /*yield*/, ((_a = getConfigS3Result.Body) === null || _a === void 0 ? void 0 : _a.transformToString('utf8'))];
                                    case 1:
                                        ccr = (_b.sent());
                                        if (s3db_cc.SelectiveDebug.indexOf("_910,") > -1)
                                            console.info("Selective Debug 910 - Customers (".concat(customer, ") Config: \n ").concat(ccr, " "));
                                        //Parse comments out of the json before parse
                                        ccr = ccr.replaceAll(new RegExp(/[^:](\/\/.*(,|$|")?)/g), '');
                                        ccr = ccr.replaceAll(' ', '');
                                        ccr = ccr.replaceAll('\n', '');
                                        configJSON = JSON.parse(ccr);
                                        return [2 /*return*/];
                                }
                            });
                        }); })
                            .catch(function (e) {
                            var err = JSON.stringify(e);
                            if (err.indexOf('specified key does not exist') > -1)
                                throw new Error("Exception - Customer Config - ".concat(customer, "config.jsonc does not exist on ").concat(s3db_cc.S3DropBucketConfigs, " bucket \nException ").concat(e, " "));
                            if (err.indexOf('NoSuchKey') > -1)
                                throw new Error("Exception - Customer Config Not Found(".concat(customer, "config.jsonc) on ").concat(s3db_cc.S3DropBucketConfigs, ". \nException ").concat(e, " "));
                            throw new Error("Exception - Retrieving Config (".concat(customer, "config.jsonc) from ").concat(s3db_cc.S3DropBucketConfigs, " \nException ").concat(e, " "));
                        })];
                case 2:
                    _b.sent();
                    return [3 /*break*/, 4];
                case 3:
                    e_13 = _b.sent();
                    debugger;
                    throw new Error("Exception - Pulling Customer Config \n".concat(ccr, " \n").concat(e_13, " "));
                case 4: return [4 /*yield*/, validateCustomerConfig(configJSON)];
                case 5:
                    customersConfig = _b.sent();
                    return [2 /*return*/, customersConfig];
            }
        });
    });
}
function validateCustomerConfig(config) {
    return __awaiter(this, void 0, void 0, function () {
        var tmpMap, jm, m, p, v;
        return __generator(this, function (_a) {
            if (!config || config === null) {
                throw new Error('Invalid  CustomerConfig - empty or null config');
            }
            if (!config.Customer) {
                throw new Error('Invalid  CustomerConfig - Customer is not defined');
            }
            else if (config.Customer.length < 4 ||
                !config.Customer.endsWith('_')) {
                throw new Error("Invalid  CustomerConfig - Customer string is not valid, must be at least 3 characters and a trailing underscore, '_'");
            }
            if (!config.clientId) {
                throw new Error('Invalid Customer Config - ClientId is not defined');
            }
            if (!config.clientSecret) {
                throw new Error('Invalid Customer Config - ClientSecret is not defined');
            }
            if (!config.format) {
                throw new Error('Invalid Customer Config - Format is not defined');
            }
            if (!config.updates) {
                throw new Error('Invalid Customer Config - Updates is not defined');
            }
            if (!config.listId) {
                throw new Error('Invalid Customer Config - ListId is not defined');
            }
            if (!config.listName) {
                throw new Error('Invalid Customer Config - ListName is not defined');
            }
            if (!config.pod) {
                throw new Error('Invalid Customer Config - Pod is not defined');
            }
            if (!config.region) //Campaign POD Region 
             {
                throw new Error('Invalid Customer Config - Region is not defined');
            }
            if (!config.refreshToken) {
                throw new Error('Invalid Customer Config - RefreshToken is not defined');
            }
            if (!config.format.toLowerCase().match(/^(?:csv|json)$/gim)) {
                throw new Error("Invalid Customer Config - Format is not 'CSV' or 'JSON' ");
            }
            if (!config.separator) {
                //see: https://www.npmjs.com/package/@streamparser/json-node
                //JSONParser / Node separator option: null = `''` empty = '', otherwise a separator eg. '\n'
                config.separator = '\n';
            }
            //Customer specific separator 
            if (config.separator.toLowerCase() === "null")
                config.separator = "''";
            if (config.separator.toLowerCase() === "empty")
                config.separator = "\"\"";
            if (config.separator.toLowerCase() === "\n")
                config.separator = '\n';
            if (!config.updates.toLowerCase().match(/^(?:singular|multiple|bulk)$/gim)) {
                throw new Error("Invalid Customer Config - Updates is not 'Singular' or 'Multiple' ");
            }
            if (!config.pod.match(/^(?:0|1|2|3|4|5|6|7|8|9|a|b)$/gim)) {
                throw new Error('Invalid Customer Config - Pod is not 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, A, or B. ');
            }
            //
            //    XMLAPI Endpoints
            //Pod 1 - https://api-campaign-us-1.goacoustic.com/XMLAPI
            //Pod 2 - https://api-campaign-us-2.goacoustic.com/XMLAPI
            //Pod 3 - https://api-campaign-us-3.goacoustic.com/XMLAPI
            //Pod 4 - https://api-campaign-us-4.goacoustic.com/XMLAPI
            //Pod 5 - https://api-campaign-us-5.goacoustic.com/XMLAPI
            //Pod 6 - https://api-campaign-eu-1.goacoustic.com/XMLAPI
            //Pod 7 - https://api-campaign-ap-2.goacoustic.com/XMLAPI
            //Pod 8 - https://api-campaign-ca-1.goacoustic.com/XMLAPI
            //Pod 9 - https://api-campaign-us-6.goacoustic.com/XMLAPI
            //Pod A - https://api-campaign-ap-1.goacoustic.com/XMLAPI
            //pod B - https://api-campaign-ap-3.goacoustic.com/XMLAPI
            switch (config.pod.toLowerCase()) {
                case '6':
                    config.pod = '1';
                    break;
                case '7':
                    config.pod = '2';
                    break;
                case '8':
                    config.pod = '1';
                    break;
                case '9':
                    config.pod = '6';
                    break;
                case 'a':
                    config.pod = '1';
                    break;
                case 'b':
                    config.pod = '3';
                    break;
                default:
                    break;
            }
            if (!config.region.toLowerCase().match(/^(?:us|eu|ap|ca)$/gim)) {
                throw new Error("Invalid Customer Config - Region is not 'US', 'EU', CA' or 'AP'. ");
            }
            if (!config.listType) {
                throw new Error('Invalid Customer Config - ListType is not defined');
            }
            if (!config.listType.toLowerCase().match(/^(?:relational|dbkeyed|dbnonkeyed)$/gim)) {
                throw new Error("Invalid Customer Config - ListType must be either 'Relational', 'DBKeyed' or 'DBNonKeyed'. ");
            }
            if (config.listType.toLowerCase() == 'dbkeyed' && !config.DBKey) {
                throw new Error("Invalid Customer Config - Update set as Database Keyed but DBKey is not defined. ");
            }
            if (config.listType.toLowerCase() == 'dbnonkeyed' && !config.LookupKeys) {
                throw new Error("Invalid Customer Config - Update set as Database NonKeyed but lookupKeys is not defined. ");
            }
            if (!config.sftp) {
                config.sftp = { user: "", password: "", filepattern: "", schedule: "" };
            }
            if (config.sftp.user && config.sftp.user !== '') { }
            if (config.sftp.password && config.sftp.password !== '') { }
            if (config.sftp.filepattern && config.sftp.filepattern !== '') { }
            if (config.sftp.schedule && config.sftp.schedule !== '') { }
            if (!config.transforms) {
                Object.assign(config, { "transforms": {} });
            }
            if (!config.transforms.jsonMap) {
                Object.assign(config.transforms, { jsonMap: { "none": "" } });
            }
            if (!config.transforms.csvMap) {
                Object.assign(config.transforms, { csvMap: { "none": "" } });
            }
            if (!config.transforms.ignore) {
                Object.assign(config.transforms, { ignore: [] });
            }
            if (!config.transforms.script) {
                Object.assign(config.transforms, { script: "" });
            }
            // if (Object.keys(config.transform[0].jsonMap)[0].indexOf('none'))
            if (!config.transforms.jsonMap.hasOwnProperty("none")) {
                tmpMap = {};
                jm = config.transforms.jsonMap;
                for (m in jm) {
                    try {
                        p = jm[m];
                        v = jsonpath_1.default.parse(p);
                        tmpMap[m] = jm[m];
                        // tmpmap2.m = jm.m
                    }
                    catch (e) {
                        console.error("Invalid JSONPath defined in Customer config: ".concat(m, ": \"").concat(m, "\", \nInvalid JSONPath - ").concat(e, " "));
                    }
                }
                config.transforms.jsonMap = tmpMap;
            }
            return [2 /*return*/, config];
        });
    });
}
function packageUpdates(workSet, key, custConfig) {
    return __awaiter(this, void 0, void 0, function () {
        var updates, sqwResult, c, e_14;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    updates = [];
                    sqwResult = {};
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 5, , 6]);
                    _a.label = 2;
                case 2:
                    if (!(chunks.length > 0)) return [3 /*break*/, 4];
                    updates = [];
                    while (chunks.length > 0 && updates.length < 100) {
                        c = chunks.pop();
                        updates.push(c);
                    }
                    return [4 /*yield*/, storeAndQueueWork(updates, key, custConfig)
                            .then(function (res) {
                            //console.info( `Debug Await StoreAndQueueWork Result: ${ JSON.stringify( res ) }` )
                            return res;
                        })
                        //console.info( `Debug sqwResult ${ JSON.stringify( sqwResult ) }` )
                    ];
                case 3:
                    sqwResult = _a.sent();
                    return [3 /*break*/, 2];
                case 4:
                    if (s3db_cc.SelectiveDebug.indexOf("_918,") > -1)
                        console.info("Selective Debug 918: PackageUpdates StoreAndQueueWork for ".concat(key, ". \nFor a total of ").concat(recs, " Updates in ").concat(batchCount, " Batches.  Result: \n").concat(JSON.stringify(sqwResult), " "));
                    return [3 /*break*/, 6];
                case 5:
                    e_14 = _a.sent();
                    debugger;
                    console.error("Exception - packageUpdates for ".concat(key, " \n").concat(e_14, " "));
                    sqwResult = __assign(__assign({}, sqwResult), { StoreQueueWorkException: "Exception - PackageUpdates StoreAndQueueWork for ".concat(key, " \nBatch ").concat(batchCount, " of ").concat(recs, " Updates. \n").concat(e_14, " ") });
                    return [3 /*break*/, 6];
                case 6: return [2 /*return*/, { StoreAndQueueWorkResult: sqwResult }];
            }
        });
    });
}
function storeAndQueueWork(updates, s3Key, config) {
    var _a, _b;
    return __awaiter(this, void 0, void 0, function () {
        var updateCount, key, addWorkToS3WorkBucketResult, addWorkToSQSWorkQueueResult, v, e_15, sqwError, marker, e_16, sqwError;
        return __generator(this, function (_c) {
            switch (_c.label) {
                case 0:
                    batchCount++;
                    if (batchCount > s3db_cc.MaxBatchesWarning)
                        console.warn("Warning: Updates from the S3 Object(".concat(s3Key, ") are exceeding(").concat(batchCount, ") the Warning Limit of ").concat(s3db_cc.MaxBatchesWarning, " Batches per Object."));
                    updateCount = updates.length;
                    //Customers marked as "Singular" updates files are not transformed, but sent to Firehose before this,
                    //  therefore need to transform Aggregate files as well as files marked as "Multiple" updates
                    try {
                        updates = transforms(updates, config);
                    }
                    catch (e) {
                        throw new Error("Exception - Transforms - ".concat(e));
                    }
                    if (customersConfig.listType.toLowerCase() === 'dbkeyed' ||
                        customersConfig.listType.toLowerCase() === 'dbnonkeyed') {
                        xmlRows = convertJSONToXML_DBUpdates(updates, config);
                    }
                    if (customersConfig.listType.toLowerCase() === 'relational') {
                        xmlRows = convertJSONToXML_RTUpdates(updates, config);
                    }
                    if (s3Key.indexOf('TestData') > -1) {
                        //strip /testdata folder from key
                        s3Key = (_a = s3Key.split('/').at(-1)) !== null && _a !== void 0 ? _a : s3Key;
                    }
                    key = s3Key;
                    while (key.indexOf('/') > -1) {
                        key = (_b = key.split('/').at(-1)) !== null && _b !== void 0 ? _b : key;
                    }
                    key = key.replace('.', '_');
                    key = "".concat(key, "-update-").concat(batchCount, "-").concat(updateCount, "-").concat((0, uuid_1.v4)(), ".xml");
                    //if ( Object.values( updates ).length !== recs )
                    //{
                    //    if ( tcc.SelectiveDebug.indexOf( "_900," ) > -1 ) console.error( `(900) Recs Count ${ recs } does not reflect Updates Count ${ Object.values( updates ).length } ` )
                    //}
                    if (s3dbLogDebug)
                        console.info("Queuing Work File ".concat(key, " for ").concat(s3Key, ". Batch ").concat(batchCount, " of ").concat(updateCount, " records)"));
                    v = '';
                    _c.label = 1;
                case 1:
                    _c.trys.push([1, 3, , 4]);
                    return [4 /*yield*/, addWorkToS3WorkBucket(xmlRows, key)
                            .then(function (res) {
                            return res; //{"AddWorktoS3Results": res}
                        })
                            .catch(function (err) {
                            console.error("Exception - AddWorkToS3WorkBucket ".concat(err));
                        })];
                case 2:
                    addWorkToS3WorkBucketResult = _c.sent();
                    return [3 /*break*/, 4];
                case 3:
                    e_15 = _c.sent();
                    sqwError = "Exception - StoreAndQueueWork Add work to S3 Bucket exception \n".concat(e_15, " ");
                    console.error(sqwError);
                    return [2 /*return*/, { StoreS3WorkException: sqwError, StoreQueueWorkException: '', AddWorkToS3WorkBucketResults: JSON.stringify(addWorkToS3WorkBucketResult) }];
                case 4:
                    marker = 'Initially Queued on ' + new Date();
                    _c.label = 5;
                case 5:
                    _c.trys.push([5, 7, , 8]);
                    return [4 /*yield*/, addWorkToSQSWorkQueue(config, key, v, batchCount, updates.length.toString(), marker)
                            .then(function (res) {
                            return res;
                            //     {
                            //         sqsWriteResult: "200",
                            //         workQueuedSuccess: true,
                            //         SQSSendResult: "{\"$metadata\":{\"httpStatusCode\":200,\"requestId\":\"e70fba06-94f2-5608-b104-e42dc9574636\",\"attempts\":1,\"totalRetryDelay\":0},\"MD5OfMessageAttributes\":\"0bca0dfda87c206313963daab8ef354a\",\"MD5OfMessageBody\":\"940f4ed5927275bc93fc945e63943820\",\"MessageId\":\"cf025cb3-dce3-4564-89a5-23dcae86dd42\"}",
                            // }
                        })];
                case 6:
                    addWorkToSQSWorkQueueResult = _c.sent();
                    return [3 /*break*/, 8];
                case 7:
                    e_16 = _c.sent();
                    sqwError = "Exception - StoreAndQueueWork Add work to SQS Queue exception \n".concat(e_16, " ");
                    console.error(sqwError);
                    return [2 /*return*/, { StoreQueueWorkException: sqwError, StoreS3WorkException: '' }];
                case 8:
                    if (s3db_cc.SelectiveDebug.indexOf("_915,") > -1)
                        console.info("Selective Debug 915 - Results of Store and Queue of Updates - Add to Process Bucket: ".concat(JSON.stringify(addWorkToS3WorkBucketResult), " \n Add to Process Queue: ").concat(JSON.stringify(addWorkToSQSWorkQueueResult), " "));
                    return [2 /*return*/, { AddWorkToS3WorkBucketResults: addWorkToS3WorkBucketResult, AddWorkToSQSWorkQueueResults: addWorkToSQSWorkQueueResult }];
            }
        });
    });
}
function convertJSONToXML_RTUpdates(updates, config) {
    if (updates.length < 1) {
        throw new Error("Exception - Convert JSON to XML for RT - No Updates(".concat(updates.length, ") were passed to process into XML. Customer ").concat(config.Customer, " "));
    }
    xmlRows = "<Envelope> <Body> <InsertUpdateRelationalTable> <TABLE_ID> ".concat(config.listId, " </TABLE_ID><ROWS>");
    var r = 0;
    for (var upd in updates) {
        recs++;
        //const updAtts = JSON.parse( updates[ upd ] )
        var updAtts = updates[upd];
        r++;
        xmlRows += "<ROW>";
        // Object.entries(jo).forEach(([key, value]) => {
        for (var uv in updAtts) {
            // console.info(`Record ${r} as ${key}: ${value}`)
            xmlRows += "<COLUMN name=\"".concat(uv, "\"> <![CDATA[").concat(updAtts[uv], "]]> </COLUMN>");
        }
        xmlRows += "</ROW>";
    }
    //Tidy up the XML
    xmlRows += "</ROWS></InsertUpdateRelationalTable></Body></Envelope>";
    if (s3dbLogDebug)
        console.info("Converting S3 Content to XML RT Updates. Packaging ".concat(Object.values(updates).length, " rows as updates to ").concat(config.Customer, "'s ").concat(config.listName));
    if (s3db_cc.SelectiveDebug.indexOf("_906,") > -1)
        console.info("Selective Debug 906 - JSON to be converted to XML RT Updates(".concat(config.Customer, " - ").concat(config.listName, "): ").concat(JSON.stringify(updates)));
    if (s3db_cc.SelectiveDebug.indexOf("_917,") > -1)
        console.info("Selective Debug 917 - XML from JSON for RT Updates (".concat(config.Customer, " - ").concat(config.listName, "): ").concat(xmlRows));
    return xmlRows;
}
function convertJSONToXML_DBUpdates(updates, config) {
    if (updates.length < 1) {
        throw new Error("Exception - Convert JSON to XML for DB - No Updates (".concat(updates.length, ") were passed to process. Customer ").concat(config.Customer, " "));
    }
    xmlRows = "<Envelope><Body>";
    var r = 0;
    try {
        for (var upd in updates) {
            r++;
            var updAtts = updates[upd];
            //const s = JSON.stringify(updAttr )
            xmlRows += "<AddRecipient><LIST_ID>".concat(config.listId, "</LIST_ID><CREATED_FROM>0</CREATED_FROM><UPDATE_IF_FOUND>true</UPDATE_IF_FOUND>");
            // If Keyed, then Column that is the key must be present in Column Set
            // If Not Keyed must add Lookup Fields
            // Use SyncFields as 'Lookup" values, Columns hold the Updates while SyncFields hold the 'lookup' values.
            //Only needed on non-keyed(In Campaign use DB -> Settings -> LookupKeys to find what fields are Lookup Keys)
            if (config.listType.toLowerCase() === 'dbnonkeyed') {
                var lk = config.LookupKeys.split(',');
                xmlRows += "<SYNC_FIELDS>";
                for (var k in lk) {
                    //lk.forEach( k => {
                    k = k.trim();
                    var sf = "<SYNC_FIELD><NAME>".concat(k, "</NAME><VALUE><![CDATA[").concat(updAtts[k], "]]></VALUE></SYNC_FIELD>");
                    xmlRows += sf;
                } //)
                xmlRows += "</SYNC_FIELDS>";
            }
            //
            if (config.listType.toLowerCase() === 'dbkeyed') {
                //Placeholder
                //Don't need to do anything with DBKey, it's superfluous but documents the keys of the keyed DB
            }
            for (var uv in updAtts) {
                xmlRows += "<COLUMN><NAME>".concat(uv, "</NAME><VALUE><![CDATA[").concat(updAtts[uv], "]]></VALUE></COLUMN>");
            }
            xmlRows += "</AddRecipient>";
        }
    }
    catch (e) {
        console.error("Exception - ConvertJSONtoXML_DBUpdates - \n".concat(e));
    }
    xmlRows += "</Body></Envelope>";
    if (s3dbLogDebug)
        console.info("Converting S3 Content to XML DB Updates. Packaging ".concat(Object.values(updates).length, " rows as updates to ").concat(config.Customer, "'s ").concat(config.listName));
    if (s3db_cc.SelectiveDebug.indexOf("_916,") > -1)
        console.info("Selective Debug 916 - JSON to be converted to XML DB Updates: ".concat(JSON.stringify(updates)));
    if (s3db_cc.SelectiveDebug.indexOf("_917,") > -1)
        console.info("Selective Debug 917 - XML from JSON for DB Updates: ".concat(xmlRows));
    return xmlRows;
}
function transforms(updates, config) {
    //Apply Transforms
    var _a;
    //Clorox Weather Data
    //Add dateday column
    // Prep to add transform in Config file:
    //Get Method to apply - const method = config.transforms.method (if "dateDay") )
    //Get column to update - const column = config.transforms.methods[0].updColumn
    //Get column to reference const refColumn = config.transforms.method.refColumn
    if (config.Customer.toLowerCase().indexOf('kingsfordweather_') > -1) {
        var t = [];
        var days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
        for (var _i = 0, updates_1 = updates; _i < updates_1.length; _i++) {
            var jo = updates_1[_i];
            //const j = JSON.parse( l )
            var d = (_a = jo.datetime) !== null && _a !== void 0 ? _a : "";
            if (d !== "") {
                var dt = new Date(d);
                var day = { "dateday": days[dt.getDay()] };
                Object.assign(jo, day);
                //t.push( JSON.stringify( l ) )
                t.push(jo);
            }
        }
        if (t.length !== updates.length) {
            throw new Error("Error - Transform - Applying Clorox Custom Transform returns fewer records (".concat(t.length, ") than initial set ").concat(updates.length));
        }
        else
            updates = t;
    }
    //Apply the JSONMap -
    //  JSONPath statements
    //      "jsonMap": {
    //          "email": "$.uniqueRecipient",
    //              "zipcode": "$.context.traits.address.postalCode"
    //      },
    if (Object.keys(config.transforms.jsonMap).indexOf('none') < 0) {
        var r = [];
        try {
            var jmr = void 0;
            for (var _b = 0, updates_2 = updates; _b < updates_2.length; _b++) {
                var jo = updates_2[_b];
                //const jo = JSON.parse( l )
                jmr = applyJSONMap(jo, config.transforms.jsonMap);
                r.push(jmr);
            }
        }
        catch (e) {
            console.error("Exception - Transform - Applying JSONMap \n".concat(e));
        }
        if (r.length !== updates.length) {
            throw new Error("Error - Transform - Applying JSONMap returns fewer records (".concat(r.length, ") than initial set ").concat(updates.length));
        }
        else
            updates = r;
    }
    //Apply CSVMap
    // "csvMap": { //Mapping when processing CSV files
    //       "Col_AA": "COL_XYZ", //Write Col_AA with data from Col_XYZ in the CSV file
    //       "Col_BB": "COL_MNO",
    //       "Col_CC": "COL_GHI",
    //       "Col_DD": 1, //Write Col_DD with data from the 1st column of data in the CSV file.
    //       "Col_DE": 2,
    //       "Col_DF": 3
    // },
    if (Object.keys(config.transforms.csvMap).indexOf('none') < 0) {
        var c = [];
        try {
            var _loop_3 = function (jo) {
                //const jo = JSON.parse( l )
                var map = config.transforms.csvMap;
                Object.entries(map).forEach(function (_a) {
                    var _b, _c;
                    var k = _a[0], v = _a[1];
                    if (typeof v !== "number")
                        jo[k] = (_b = jo[v]) !== null && _b !== void 0 ? _b : "";
                    else {
                        var vk = Object.keys(jo)[v];
                        // const vkk = vk[v]
                        jo[k] = (_c = jo[vk]) !== null && _c !== void 0 ? _c : "";
                    }
                });
                c.push(jo);
            };
            for (var _c = 0, updates_3 = updates; _c < updates_3.length; _c++) {
                var jo = updates_3[_c];
                _loop_3(jo);
            }
        }
        catch (e) {
            console.error("Exception - Transforms - Applying CSVMap \n".concat(e));
        }
        if (c.length !== updates.length) {
            throw new Error("Error - Transform - Applying CSVMap returns fewer records (".concat(c.length, ") than initial set ").concat(updates.length));
        }
        else
            updates = c;
    }
    // Ignore must be last to take advantage of cleaning up any extraneous columns after previous transforms
    if (config.transforms.ignore.length > 0) {
        var i = [];
        try {
            for (var _d = 0, updates_4 = updates; _d < updates_4.length; _d++) {
                var jo = updates_4[_d];
                //const jo = JSON.parse( l )
                for (var _e = 0, _f = config.transforms.ignore; _e < _f.length; _e++) {
                    var ig = _f[_e];
                    // const { [keyToRemove]: removedKey, ...newObject } = originalObject;
                    // const { [ig]: , ...i } = jo
                    delete jo[ig];
                }
                i.push(jo);
            }
        }
        catch (e) {
            console.error("Exception - Transform - Applying Ignore - \n".concat(e));
        }
        if (i.length !== updates.length) {
            throw new Error("Error - Transform - Applying Ignore returns fewer records ".concat(i.length, " than initial set ").concat(updates.length));
        }
        else
            updates = i;
    }
    return updates;
}
function applyJSONMap(jsonObj, map) {
    // j.forEach((o: object) => {
    //     const a = applyJSONMap([o], config.transforms[0].jsonMap)
    //     am.push(a)
    //     chunks = am
    // })
    Object.entries(map).forEach(function (_a) {
        var _b;
        var k = _a[0], v = _a[1];
        try {
            var j = jsonpath_1.default.value(jsonObj, v);
            if (!j) {
                if (s3db_cc.SelectiveDebug.indexOf("_930,") > -1)
                    console.warn("Selective Debug 930 - Warning: Data not Found for JSONPath statement ".concat(k, ": ").concat(v, ",  \nTarget Data: \n").concat(JSON.stringify(jsonObj), " "));
            }
            else {
                // Object.assign(jsonObj, { [k]: jsonpath.value(jsonObj, v) })
                Object.assign(jsonObj, (_b = {}, _b[k] = j, _b));
            }
        }
        catch (e) {
            console.error("Error parsing data for JSONPath statement ".concat(k, " ").concat(v, ", ").concat(e, " \nTarget Data: \n").concat(JSON.stringify(jsonObj), " "));
        }
        // const a1 = jsonpath.parse(value)
        // const a2 = jsonpath.parent(s3Chunk, value)
        // const a3 = jsonpath.paths(s3Chunk, value)
        // const a4 = jsonpath.query(s3Chunk, value)
        // const a6 = jsonpath.value(s3Chunk, value)
        //Confirms Update was accomplished 
        // const j = jsonpath.query(s3Chunk, v)
        // console.info(`${ j } `)
    });
    return jsonObj;
}
function addWorkToS3WorkBucket(queueUpdates, key) {
    var _a;
    return __awaiter(this, void 0, void 0, function () {
        var s3WorkPutInput, s3ProcessBucketResult, addWorkToS3ProcessBucket, e_17, vid, aw3pbr;
        var _this = this;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0:
                    if (s3db_cc.QueueBucketQuiesce) {
                        console.warn("Work/Process Bucket Quiesce is in effect, no New Work Files are being written to the S3 Queue Bucket. This work file is for ".concat(key));
                        return [2 /*return*/, { versionId: '', S3ProcessBucketResult: '', AddWorkToS3ProcessBucket: 'In Quiesce' }];
                    }
                    s3WorkPutInput = {
                        Body: queueUpdates,
                        Bucket: s3db_cc.S3DropBucketWorkBucket,
                        Key: key,
                    };
                    if (s3dbLogDebug)
                        console.info("Write Work to S3 Process Queue for ".concat(key));
                    _b.label = 1;
                case 1:
                    _b.trys.push([1, 3, , 4]);
                    return [4 /*yield*/, s3.send(new client_s3_1.PutObjectCommand(s3WorkPutInput))
                            .then(function (s3PutResult) { return __awaiter(_this, void 0, void 0, function () {
                            return __generator(this, function (_a) {
                                if (s3PutResult.$metadata.httpStatusCode !== 200) {
                                    throw new Error("Failed to write Work File to S3 Process Store (Result ".concat(s3PutResult, ") for ").concat(key, " of ").concat(queueUpdates.length, " characters"));
                                }
                                return [2 /*return*/, s3PutResult];
                            });
                        }); })
                            .catch(function (err) {
                            throw new Error("PutObjectCommand Results Failed for (".concat(key, " of ").concat(queueUpdates.length, " characters) to S3 Processing bucket (").concat(s3db_cc.S3DropBucketWorkBucket, "): \n").concat(err));
                            //return {StoreS3WorkException: err}
                        })];
                case 2:
                    addWorkToS3ProcessBucket = _b.sent();
                    return [3 /*break*/, 4];
                case 3:
                    e_17 = _b.sent();
                    throw new Error("Exception - Put Object Command for writing work(".concat(key, " to S3 Processing bucket(").concat(s3db_cc.S3DropBucketWorkBucket, "): \n").concat(e_17));
                case 4:
                    s3ProcessBucketResult = JSON.stringify(addWorkToS3ProcessBucket.$metadata.httpStatusCode, null, 2);
                    vid = (_a = addWorkToS3ProcessBucket.VersionId) !== null && _a !== void 0 ? _a : "";
                    if (s3db_cc.SelectiveDebug.indexOf("_907,") > -1)
                        console.info("Selective Debug 907 - Added Work File ".concat(key, " to Work Bucket (").concat(s3db_cc.S3DropBucketWorkBucket, ") \n").concat(JSON.stringify(addWorkToS3ProcessBucket)));
                    aw3pbr = {
                        versionId: vid,
                        AddWorkToS3ProcessBucket: JSON.stringify(addWorkToS3ProcessBucket),
                        S3ProcessBucketResult: s3ProcessBucketResult
                    };
                    return [2 /*return*/, aw3pbr];
            }
        });
    });
}
function addWorkToSQSWorkQueue(config, key, versionId, batch, recCount, marker) {
    return __awaiter(this, void 0, void 0, function () {
        var sqsQMsgBody, sqsParams, sqsSendResult, sqsWriteResult, e_18;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    if (s3db_cc.QueueBucketQuiesce) {
                        console.warn("Work/Process Bucket Quiesce is in effect, no New Work Files are being written to the SQS Queue of S3 Work Bucket. This work file is for ".concat(key));
                        return [2 /*return*/, { versionId: '', S3ProcessBucketResult: '', AddWorkToS3ProcessBucket: 'In Quiesce' }];
                    }
                    sqsQMsgBody = {};
                    sqsQMsgBody.workKey = key;
                    sqsQMsgBody.versionId = versionId;
                    sqsQMsgBody.marker = marker;
                    sqsQMsgBody.attempts = 1;
                    sqsQMsgBody.batchCount = batch.toString();
                    sqsQMsgBody.updateCount = recCount;
                    sqsQMsgBody.custconfig = config;
                    sqsQMsgBody.lastQueued = Date.now().toString();
                    sqsParams = {
                        MaxNumberOfMessages: 1,
                        QueueUrl: s3db_cc.S3DropBucketWorkQueue,
                        //Defer to setting these on the Queue in AWS SQS Interface
                        // VisibilityTimeout: parseInt(tcc.WorkQueueVisibilityTimeout),
                        // WaitTimeSeconds: parseInt(tcc.WorkQueueWaitTimeSeconds),
                        MessageAttributes: {
                            FirstQueued: {
                                DataType: 'String',
                                StringValue: Date.now().toString(),
                            },
                            Retry: {
                                DataType: 'Number',
                                StringValue: '0',
                            },
                        },
                        MessageBody: JSON.stringify(sqsQMsgBody),
                    };
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 3, , 4]);
                    return [4 /*yield*/, sqsClient.send(new client_sqs_1.SendMessageCommand(sqsParams))
                            .then(function (sqsSendMessageResult) {
                            sqsWriteResult = JSON.stringify(sqsSendMessageResult.$metadata.httpStatusCode, null, 2);
                            if (sqsWriteResult !== '200') {
                                var storeQueueWorkException = "Failed writing to SQS Process Queue (queue URL: ".concat(sqsParams.QueueUrl, "), ").concat(sqsQMsgBody.workKey, ", SQS Params").concat(JSON.stringify(sqsParams), ")");
                                return { StoreQueueWorkException: storeQueueWorkException };
                            }
                            sqsSendResult = sqsSendMessageResult;
                            if (s3db_cc.SelectiveDebug.indexOf("_914,") > -1)
                                console.info("Selective Debug 914 - Queued Work to SQS Process Queue (".concat(sqsQMsgBody.workKey, ") - Result: ").concat(sqsWriteResult, " "));
                            return sqsSendMessageResult;
                        })
                            .catch(function (err) {
                            debugger;
                            var storeQueueWorkException = "Failed writing to SQS Process Queue (".concat(err, ") \nQueue URL: ").concat(sqsParams.QueueUrl, ")\nWork to be Queued: ").concat(sqsQMsgBody.workKey, "\nSQS Params: ").concat(JSON.stringify(sqsParams));
                            console.error("Failed to Write to SQS Process Queue. \n".concat(storeQueueWorkException));
                            return { StoreQueueWorkException: storeQueueWorkException };
                        })];
                case 2:
                    _a.sent();
                    return [3 /*break*/, 4];
                case 3:
                    e_18 = _a.sent();
                    console.error("Exception - Writing to SQS Process Queue - (queue URL".concat(sqsParams.QueueUrl, "), ").concat(sqsQMsgBody.workKey, ", SQS Params").concat(JSON.stringify(sqsParams), ") - Error: ").concat(e_18));
                    return [3 /*break*/, 4];
                case 4:
                    if (s3db_cc.SelectiveDebug.indexOf("_907,") > -1)
                        console.info("Selective Debug 907 - Queued Work ".concat(key, " (").concat(recCount, " updates) to the Work Queue (").concat(s3db_cc.S3DropBucketWorkQueue, ") \nSQS Params: \n").concat(JSON.stringify(sqsParams), " \nresults: \n").concat(JSON.stringify({
                            SQSWriteResult: sqsWriteResult,
                            AddToSQSQueue: JSON.stringify(sqsSendResult)
                        })));
                    return [2 /*return*/, {
                            SQSWriteResult: sqsWriteResult,
                            AddToSQSQueue: JSON.stringify(sqsSendResult)
                        }];
            }
        });
    });
}
function getS3Work(s3Key, bucket) {
    return __awaiter(this, void 0, void 0, function () {
        var getObjectCmd, work, e_19, err;
        var _this = this;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    if (s3dbLogDebug)
                        console.info("Debug - GetS3Work Key: ".concat(s3Key));
                    getObjectCmd = {
                        Bucket: bucket,
                        Key: s3Key
                    };
                    work = '';
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 3, , 4]);
                    return [4 /*yield*/, s3.send(new client_s3_1.GetObjectCommand(getObjectCmd))
                            .then(function (getS3Result) { return __awaiter(_this, void 0, void 0, function () {
                            var _a;
                            return __generator(this, function (_b) {
                                switch (_b.label) {
                                    case 0: return [4 /*yield*/, ((_a = getS3Result.Body) === null || _a === void 0 ? void 0 : _a.transformToString('utf8'))];
                                    case 1:
                                        work = (_b.sent());
                                        if (s3dbLogDebug)
                                            console.info("Work Pulled (".concat(work.length, " chars): ").concat(s3Key));
                                        return [2 /*return*/];
                                }
                            });
                        }); })];
                case 2:
                    _a.sent();
                    return [3 /*break*/, 4];
                case 3:
                    e_19 = _a.sent();
                    err = JSON.stringify(e_19);
                    if (err.toLowerCase().indexOf('nosuchkey') > -1)
                        throw new Error("Exception - Work Not Found on S3 Process Queue (".concat(s3Key, ". Work will not be marked for Retry. \n").concat(e_19));
                    else
                        throw new Error("Exception - Retrieving Work from S3 Process Queue for ".concat(s3Key, ".  \n ").concat(e_19));
                    return [3 /*break*/, 4];
                case 4: return [2 /*return*/, work];
            }
        });
    });
}
function saveS3Work(s3Key, body, bucket) {
    return __awaiter(this, void 0, void 0, function () {
        var putObjectCmd, saveS3, e_20;
        var _this = this;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    if (s3dbLogDebug)
                        console.info("Debug - SaveS3Work Key: ".concat(s3Key));
                    putObjectCmd = {
                        Bucket: bucket,
                        Key: s3Key,
                        Body: body
                        // ContentLength: Number(`${body.length}`),
                    };
                    saveS3 = '';
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 3, , 4]);
                    return [4 /*yield*/, s3.send(new client_s3_1.PutObjectCommand(putObjectCmd))
                            .then(function (getS3Result) { return __awaiter(_this, void 0, void 0, function () {
                            var _a;
                            return __generator(this, function (_b) {
                                switch (_b.label) {
                                    case 0: return [4 /*yield*/, ((_a = getS3Result.Body) === null || _a === void 0 ? void 0 : _a.transformToString('utf8'))];
                                    case 1:
                                        saveS3 = (_b.sent());
                                        if (s3dbLogDebug)
                                            console.info("Work Saved (".concat(saveS3.length, " chars): ").concat(s3Key));
                                        return [2 /*return*/];
                                }
                            });
                        }); })];
                case 2:
                    _a.sent();
                    return [3 /*break*/, 4];
                case 3:
                    e_20 = _a.sent();
                    throw new Error("Exception - Saving Work for ".concat(s3Key, ". \n ").concat(e_20));
                case 4: return [2 /*return*/, saveS3];
            }
        });
    });
}
function deleteS3Object(s3ObjKey, bucket) {
    return __awaiter(this, void 0, void 0, function () {
        var delRes, s3D, d, e_21;
        var _this = this;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    delRes = '';
                    s3D = {
                        Key: s3ObjKey,
                        Bucket: bucket
                    };
                    d = new client_s3_1.DeleteObjectCommand(s3D);
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 3, , 4]);
                    return [4 /*yield*/, s3.send(d)
                            .then(function (s3DelResult) { return __awaiter(_this, void 0, void 0, function () {
                            return __generator(this, function (_a) {
                                delRes = JSON.stringify(s3DelResult.$metadata.httpStatusCode, null, 2);
                                return [2 /*return*/];
                            });
                        }); })
                            .catch(function (e) {
                            console.error("Exception - Attempting S3 Delete Command for ".concat(s3ObjKey, ": \n ").concat(e, " "));
                            return delRes;
                        })];
                case 2:
                    _a.sent();
                    return [3 /*break*/, 4];
                case 3:
                    e_21 = _a.sent();
                    console.error("Exception - Attempting S3 Delete Command for ".concat(s3ObjKey, ": \n ").concat(e_21, " "));
                    return [3 /*break*/, 4];
                case 4: return [2 /*return*/, delRes];
            }
        });
    });
}
function getAccessToken(config) {
    return __awaiter(this, void 0, void 0, function () {
        var rat, ratResp, err, accessToken, e_22;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 3, , 4]);
                    return [4 /*yield*/, fetch("https://api-campaign-".concat(config.region, "-").concat(config.pod, ".goacoustic.com/oauth/token"), {
                            method: 'POST',
                            body: new URLSearchParams({
                                refresh_token: config.refreshToken,
                                client_id: config.clientId,
                                client_secret: config.clientSecret,
                                grant_type: 'refresh_token',
                            }),
                            headers: {
                                'Content-Type': 'application/x-www-form-urlencoded',
                                'User-Agent': 'S3DropBucket GetAccessToken',
                            },
                        })];
                case 1:
                    rat = _a.sent();
                    return [4 /*yield*/, rat.json()];
                case 2:
                    ratResp = (_a.sent());
                    if (rat.status != 200) {
                        err = ratResp;
                        console.error("Problem retrieving Access Token (".concat(rat.status, ") Error: ").concat(err.error, " \nDescription: ").concat(err.error_description));
                        //  {
                        //  error: "invalid_client",
                        //  error_description: "Unable to find matching client for 1d99f8d8-0897-4090-983a-c517cc54032e",
                        //  }
                        throw new Error("Problem - Retrieving Access Token:   ".concat(rat.status, " - ").concat(err.error, "  - \n").concat(err.error_description));
                    }
                    accessToken = ratResp.access_token;
                    return [2 /*return*/, { accessToken: accessToken }.accessToken];
                case 3:
                    e_22 = _a.sent();
                    throw new Error("Exception - On GetAccessToken: \n ".concat(e_22));
                case 4: return [2 /*return*/];
            }
        });
    });
}
exports.getAccessToken = getAccessToken;
function postToCampaign(xmlCalls, config, count) {
    var _a, _b;
    return __awaiter(this, void 0, void 0, function () {
        var c, _c, _d, at, l, redactAT, at, l, redactAT, myHeaders, requestOptions, host, postRes;
        var _this = this;
        return __generator(this, function (_e) {
            switch (_e.label) {
                case 0:
                    c = config.Customer;
                    if (!(process.env["".concat(c, "_accessToken")] === undefined ||
                        process.env["".concat(c, "_accessToken")] === null ||
                        process.env["".concat(c, "_accessToken")] === '')) return [3 /*break*/, 2];
                    _c = process.env;
                    _d = "".concat(c, "_accessToken");
                    return [4 /*yield*/, getAccessToken(config)];
                case 1:
                    _c[_d] = (_e.sent());
                    at = (_a = process.env["".concat(c, "_accessToken\"")]) !== null && _a !== void 0 ? _a : '';
                    l = at.length;
                    redactAT = '.......' + at.substring(l - 10, l);
                    if (s3dbLogDebug)
                        console.info("Generated a new AccessToken: ".concat(redactAT));
                    return [3 /*break*/, 3];
                case 2:
                    at = (_b = process.env["accessToken"]) !== null && _b !== void 0 ? _b : '';
                    l = at.length;
                    redactAT = '.......' + at.substring(l - 8, l);
                    if (s3dbLogDebug)
                        console.info("Access Token already stored: ".concat(redactAT));
                    _e.label = 3;
                case 3:
                    myHeaders = new Headers();
                    myHeaders.append('Content-Type', 'text/xml');
                    myHeaders.append('Authorization', 'Bearer ' + process.env["".concat(c, "_accessToken")]);
                    myHeaders.append('Content-Type', 'text/xml');
                    myHeaders.append('Connection', 'keep-alive');
                    myHeaders.append('Accept', '*/*');
                    myHeaders.append('Accept-Encoding', 'gzip, deflate, br');
                    requestOptions = {
                        method: 'POST',
                        headers: myHeaders,
                        body: xmlCalls,
                        redirect: 'follow',
                    };
                    host = "https://api-campaign-".concat(config.region, "-").concat(config.pod, ".goacoustic.com/XMLAPI");
                    if (s3db_cc.SelectiveDebug.indexOf("_905,") > -1)
                        console.info("Selective Debug 905 - Updates to POST are: ".concat(xmlCalls));
                    return [4 /*yield*/, fetch(host, requestOptions)
                            .then(function (response) { return response.text(); })
                            .then(function (result) { return __awaiter(_this, void 0, void 0, function () {
                            var faults, f, fl, msg, m, l;
                            return __generator(this, function (_a) {
                                faults = [];
                                //const f = result.split( /<FaultString><!\[CDATA\[(.*)\]\]/g )
                                //Add this fail
                                //<RESULT>
                                //    <SUCCESS>false</SUCCESS>
                                //    < /RESULT>
                                //    < Fault >
                                //    <Request/>
                                //    < FaultCode />
                                //    <FaultString> <![ CDATA[ Local part of Email Address is Blocked.]]> </FaultString>
                                //        < detail >
                                //        <error>
                                //        <errorid> 121 < /errorid>
                                //        < module />
                                //        <class> SP.Recipients < /class>
                                //        < method />
                                //        </error>
                                //        < /detail>
                                //        < /Fault>
                                if (result.toLowerCase().indexOf('max number of concurrent') > -1 ||
                                    result.toLowerCase().indexOf('access token has expired') > -1 ||
                                    result.toLowerCase().indexOf('Error saving row') > -1) {
                                    console.warn("Temporary Failure - POST Updates - Marked for Retry. \n".concat(result));
                                    return [2 /*return*/, 'retry'];
                                }
                                else if (result.indexOf('<FaultString><![CDATA[') > -1) {
                                    f = result.split(/<FaultString><!\[CDATA\[(.*)\]\]/g);
                                    if (f && (f === null || f === void 0 ? void 0 : f.length) > 0) {
                                        for (fl in f) {
                                            faults.push(f[fl]);
                                        }
                                    }
                                    debugger;
                                    console.warn("Partially Successful POST of the Updates (".concat(f.length, " FaultStrings on ").concat(count, " updates) - \nResults\n ").concat(JSON.stringify(faults)));
                                    return [2 /*return*/, "Partially Successful - (".concat(f.length, " FaultStrings on ").concat(count, " updates) \n").concat(JSON.stringify(faults))];
                                }
                                //Add this Fail 
                                //    //<SUCCESS> true < /SUCCESS>
                                //    //    < FAILURES >
                                //    //    <FAILURE failure_type="permanent" description = "There is no column registeredAdvisorTitle" >
                                //    const m = result.match( /<FAILURE failure_(.*)"/gm )
                                else if (result.indexOf("<FAILURE  failure_type") > -1) {
                                    msg = '';
                                    m = result.match(/<FAILURE (.*)>$/g);
                                    if (m && (m === null || m === void 0 ? void 0 : m.length) > 0) {
                                        for (l in m) {
                                            // "<FAILURE failure_type=\"permanent\" description=\"There is no column name\">"
                                            //Actual message is ambiguous, changing it to read less confusingly:
                                            l.replace("There is no column ", "There is no column named ");
                                            msg += l;
                                        }
                                        console.error("Unsuccessful POST of the Updates (".concat(m.length, " of ").concat(count, ") - \nFailure Msg: ").concat(JSON.stringify(msg)));
                                        return [2 /*return*/, "Error - Unsuccessful POST of the Updates (".concat(m.length, " of ").concat(count, ") - \nFailure Msg: ").concat(JSON.stringify(msg))];
                                    }
                                }
                                result = result.replace('\n', ' ');
                                return [2 /*return*/, "Successfully POSTed (".concat(count, ") Updates - Result: ").concat(result)];
                            });
                        }); })
                            .catch(function (e) {
                            //console.error( `Error - Temporary failure to POST the Updates - Marked for Retry. ${ e }` )
                            if (e.indexOf('econnreset') > -1) {
                                console.error("Error - Temporary failure to POST the Updates - Marked for Retry. ".concat(e));
                                return 'retry';
                            }
                            else {
                                console.error("Error - Unsuccessful POST of the Updates: ".concat(e));
                                //throw new Error( `Exception - Unsuccessful POST of the Updates \n${ e }` )
                                return 'Unsuccessful POST of the Updates';
                            }
                        })
                        //retry
                        //unsuccessful post
                        //partially successful
                        //successfully posted
                    ];
                case 4:
                    // try
                    // {
                    postRes = _e.sent();
                    //retry
                    //unsuccessful post
                    //partially successful
                    //successfully posted
                    return [2 /*return*/, postRes];
            }
        });
    });
}
exports.postToCampaign = postToCampaign;
function updateConnect() {
    //query getDataSetById() 
}
function checkMetadata() {
    //Pull metadata for table/db defined in config
    // confirm updates match Columns
    //ToDo:  Log where Columns are not matching
}
function getAnS3ObjectforTesting(bucket) {
    return __awaiter(this, void 0, void 0, function () {
        var s3Key, listReq;
        var _this = this;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    s3Key = '';
                    if (testS3Key !== null)
                        return [2 /*return*/];
                    listReq = {
                        Bucket: bucket,
                        MaxKeys: 101,
                        Prefix: s3db_cc.prefixFocus
                    };
                    return [4 /*yield*/, s3.send(new client_s3_1.ListObjectsV2Command(listReq))
                            .then(function (s3ListResult) { return __awaiter(_this, void 0, void 0, function () {
                            var i, kc;
                            var _a, _b, _c, _d;
                            return __generator(this, function (_e) {
                                i = 0;
                                if (s3ListResult.Contents) {
                                    kc = s3ListResult.KeyCount - 1;
                                    if (kc = 0)
                                        throw new Error("No S3 Objects to retrieve as Test Data, exiting");
                                    if (kc > 10) {
                                        i = Math.floor(Math.random() * (10 - 1 + 1) + 1);
                                    }
                                    if (kc = 1)
                                        i = 0;
                                    s3Key = (_b = (_a = s3ListResult.Contents) === null || _a === void 0 ? void 0 : _a.at(i)) === null || _b === void 0 ? void 0 : _b.Key;
                                    while (s3Key.toLowerCase().indexOf('aggregat') > -1) {
                                        i++;
                                        s3Key = (_d = (_c = s3ListResult.Contents) === null || _c === void 0 ? void 0 : _c.at(i)) === null || _d === void 0 ? void 0 : _d.Key;
                                    }
                                    // console.info(`S3 List: \n${ JSON.stringify(s3ListResult.Contents) } `)
                                    // if (tcLogDebug)
                                    console.info("TestRun(".concat(i, ") Retrieved ").concat(s3Key, " for this Test Run"));
                                }
                                else
                                    throw new Error("No S3 Object available for Testing: ".concat(bucket, " "));
                                return [2 /*return*/, s3Key];
                            });
                        }); })
                            .catch(function (e) {
                            console.error("Exception - On S3 List Command for Testing Objects from ".concat(bucket, ": ").concat(e, " "));
                        })
                        // .finally(() => {
                        //     console.info(`S3 List Finally...`)
                        // })
                        // } catch (e)
                        // {
                        //     console.error(`Exception - Processing S3 List Command: ${ e } `)
                        // }
                    ];
                case 1:
                    _a.sent();
                    // .finally(() => {
                    //     console.info(`S3 List Finally...`)
                    // })
                    // } catch (e)
                    // {
                    //     console.error(`Exception - Processing S3 List Command: ${ e } `)
                    // }
                    return [2 /*return*/, s3Key
                        // return 'pura_2023_11_12T01_43_58_170Z.csv'
                    ];
            }
        });
    });
}
function saveSampleJSON(body) {
    var path = "Saved/";
    saveS3Work("".concat(path, "sampleJSON_").concat(Date.now().toString(), ".json"), body, s3db_cc.S3DropBucketConfigs);
    // s3://s3dropbucket-configs/Saved/
}
function purgeBucket(count, bucket) {
    return __awaiter(this, void 0, void 0, function () {
        var listReq, d, r, e_23;
        var _this = this;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    listReq = {
                        Bucket: bucket,
                        MaxKeys: count,
                    };
                    d = 0;
                    r = '';
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 3, , 4]);
                    return [4 /*yield*/, s3.send(new client_s3_1.ListObjectsV2Command(listReq)).then(function (s3ListResult) { return __awaiter(_this, void 0, void 0, function () {
                            var _this = this;
                            var _a;
                            return __generator(this, function (_b) {
                                (_a = s3ListResult.Contents) === null || _a === void 0 ? void 0 : _a.forEach(function (listItem) { return __awaiter(_this, void 0, void 0, function () {
                                    return __generator(this, function (_a) {
                                        switch (_a.label) {
                                            case 0:
                                                d++;
                                                return [4 /*yield*/, deleteS3Object(listItem.Key, bucket)];
                                            case 1:
                                                r = _a.sent();
                                                if (r !== '204')
                                                    console.error("Non Successful return (Expected 204 but received ".concat(r, " ) on Delete of ").concat(listItem.Key, " "));
                                                return [2 /*return*/];
                                        }
                                    });
                                }); });
                                return [2 /*return*/];
                            });
                        }); })];
                case 2:
                    _a.sent();
                    return [3 /*break*/, 4];
                case 3:
                    e_23 = _a.sent();
                    console.error("Exception - Attempting Purge of Bucket ".concat(bucket, ": \n").concat(e_23, " "));
                    return [3 /*break*/, 4];
                case 4:
                    console.info("Deleted ".concat(d, " Objects from ").concat(bucket, " "));
                    return [2 /*return*/, "Deleted ".concat(d, " Objects from ").concat(bucket, " ")];
            }
        });
    });
}
function maintainS3DropBucket(cust) {
    return __awaiter(this, void 0, void 0, function () {
        var bucket, limit, ContinuationToken, reProcess, deleteSource, concurrency, copyFile, d, a, _loop_4, l;
        var _this = this;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    bucket = s3db_cc.S3DropBucket;
                    limit = 0;
                    if (bucket.indexOf('-process') > -1)
                        limit = s3db_cc.S3DropBucketWorkQueueMaintLimit;
                    else
                        limit = s3db_cc.S3DropBucketMaintLimit;
                    reProcess = [];
                    deleteSource = false;
                    concurrency = s3db_cc.S3DropBucketMaintConcurrency;
                    copyFile = function (sourceKey) { return __awaiter(_this, void 0, void 0, function () {
                        return __generator(this, function (_a) {
                            switch (_a.label) {
                                case 0:
                                    // const targetKey = sourceKey.replace(sourcePrefix, targetPrefix)
                                    debugger;
                                    return [4 /*yield*/, s3.send(new client_s3_1.CopyObjectCommand({
                                            Bucket: bucket,
                                            Key: sourceKey,
                                            CopySource: "".concat(bucket, "/").concat(sourceKey),
                                            MetadataDirective: 'COPY',
                                            // CopySourceIfUnmodifiedSince: dd
                                        }))
                                            .then(function (res) {
                                            return res;
                                        })
                                            .catch(function (err) {
                                            // console.error(`Error - Maintain S3DropBucket - Copy of ${sourceKey} \n${e}`)
                                            reProcess.push("Copy Error on ".concat(sourceKey, "  -->  \n").concat(JSON.stringify(err)));
                                            if (err !== undefined && err !== '' && err.indexOf('NoSuchKey') > -1)
                                                reProcess.push("S3DropBucket Maintenance - Reprocess Error - MaintainS3DropBucket - File Not Found(".concat(sourceKey, " \nException ").concat(err, " "));
                                            else
                                                reProcess.push("S3DropBucket Maintenance - Reprocess Error for ".concat(sourceKey, " --> \n").concat(JSON.stringify(err)));
                                        })
                                        // if ( deleteSource )
                                        // {
                                        //     await s3.send(
                                        //         new DeleteObjectCommand( {
                                        //             Bucket: bucket,
                                        //             Key: sourceKey,
                                        //         } ),
                                        //     )
                                        //         .then( ( res ) => {
                                        //             // console.info(`${JSON.stringify(res)}`)
                                        //             reProcess.push( `Delete of ${ sourceKey }  -->  \n${ JSON.stringify( res ) }` )
                                        //         } )
                                        //         .catch( ( e ) => {
                                        //             console.error( `Error - Maintain S3DropBucket - Delete of ${ sourceKey } \n${ e }` )
                                        //             reProcess.push( `Delete Error on ${ sourceKey }  -->  \n${ JSON.stringify( e ) }` )
                                        //         } )
                                        // }
                                    ];
                                case 1:
                                    _a.sent();
                                    // if ( deleteSource )
                                    // {
                                    //     await s3.send(
                                    //         new DeleteObjectCommand( {
                                    //             Bucket: bucket,
                                    //             Key: sourceKey,
                                    //         } ),
                                    //     )
                                    //         .then( ( res ) => {
                                    //             // console.info(`${JSON.stringify(res)}`)
                                    //             reProcess.push( `Delete of ${ sourceKey }  -->  \n${ JSON.stringify( res ) }` )
                                    //         } )
                                    //         .catch( ( e ) => {
                                    //             console.error( `Error - Maintain S3DropBucket - Delete of ${ sourceKey } \n${ e }` )
                                    //             reProcess.push( `Delete Error on ${ sourceKey }  -->  \n${ JSON.stringify( e ) }` )
                                    //         } )
                                    // }
                                    return [2 /*return*/, reProcess];
                            }
                        });
                    }); };
                    debugger;
                    d = new Date();
                    a = 3600000 * s3db_cc.S3DropBucketMaintHours //Older Than X Hours 
                    ;
                    _loop_4 = function () {
                        var _b, _c, Contents, NextContinuationToken, lastMod, sourceKeys;
                        return __generator(this, function (_d) {
                            switch (_d.label) {
                                case 0: return [4 /*yield*/, s3.send(new client_s3_1.ListObjectsV2Command({
                                        Bucket: bucket,
                                        //Prefix: cust.Customer,
                                        ContinuationToken: ContinuationToken,
                                        //MaxKeys: limit,
                                    }))];
                                case 1:
                                    _b = _d.sent(), _c = _b.Contents, Contents = _c === void 0 ? [] : _c, NextContinuationToken = _b.NextContinuationToken;
                                    lastMod = Contents.map(function (_a) {
                                        var LastModified = _a.LastModified;
                                        return LastModified;
                                    });
                                    sourceKeys = Contents.map(function (_a) {
                                        var Key = _a.Key;
                                        return Key;
                                    });
                                    return [4 /*yield*/, Promise.all(new Array(concurrency).fill(null).map(function () { return __awaiter(_this, void 0, void 0, function () {
                                            var key, mod, s3d, df, rp;
                                            var _a;
                                            return __generator(this, function (_b) {
                                                switch (_b.label) {
                                                    case 0:
                                                        if (!sourceKeys.length) return [3 /*break*/, 3];
                                                        key = (_a = sourceKeys.pop()) !== null && _a !== void 0 ? _a : "";
                                                        mod = lastMod.pop();
                                                        s3d = new Date(mod);
                                                        df = d.getTime() - s3d.getTime();
                                                        if (!(df > a)) return [3 /*break*/, 2];
                                                        return [4 /*yield*/, copyFile(key)];
                                                    case 1:
                                                        rp = _b.sent();
                                                        reProcess.push("Copy of ".concat(key, "  -->  \n").concat(JSON.stringify(rp)));
                                                        if (reProcess.length >= limit)
                                                            ContinuationToken = '';
                                                        _b.label = 2;
                                                    case 2: return [3 /*break*/, 0];
                                                    case 3: return [2 /*return*/];
                                                }
                                            });
                                        }); }))];
                                case 2:
                                    _d.sent();
                                    ContinuationToken = NextContinuationToken !== null && NextContinuationToken !== void 0 ? NextContinuationToken : "";
                                    return [2 /*return*/];
                            }
                        });
                    };
                    _a.label = 1;
                case 1: return [5 /*yield**/, _loop_4()];
                case 2:
                    _a.sent();
                    _a.label = 3;
                case 3:
                    if (ContinuationToken) return [3 /*break*/, 1];
                    _a.label = 4;
                case 4:
                    console.info("S3DropBucketMaintenance - Copy Log: ".concat(JSON.stringify(reProcess)));
                    l = reProcess.length;
                    return [2 /*return*/, [l, reProcess]];
            }
        });
    });
}
function maintainS3DropBucketQueueBucket() {
    return __awaiter(this, void 0, void 0, function () {
        var bucket, ContinuationToken, reQueue, concurrency, d, age, _loop_5, l;
        var _this = this;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    bucket = s3db_cc.S3DropBucketWorkBucket;
                    reQueue = [];
                    concurrency = s3db_cc.S3DropBucketWorkQueueMaintConcurrency;
                    d = new Date();
                    age = 3600000 * s3db_cc.S3DropBucketMaintHours //Older Than X Hours 
                    ;
                    _loop_5 = function () {
                        var _b, _c, Contents, NextContinuationToken, lastMod, sourceKeys;
                        return __generator(this, function (_d) {
                            switch (_d.label) {
                                case 0: return [4 /*yield*/, s3.send(new client_s3_1.ListObjectsV2Command({
                                        Bucket: bucket,
                                        //Prefix: config.Customer,   //Need some option to limit millions of records 
                                        ContinuationToken: ContinuationToken,
                                    }))];
                                case 1:
                                    _b = _d.sent(), _c = _b.Contents, Contents = _c === void 0 ? [] : _c, NextContinuationToken = _b.NextContinuationToken;
                                    lastMod = Contents.map(function (_a) {
                                        var LastModified = _a.LastModified;
                                        return LastModified;
                                    });
                                    sourceKeys = Contents.map(function (_a) {
                                        var Key = _a.Key;
                                        return Key;
                                    });
                                    console.info("S3DropBucket Maintenance - Processing ".concat(Contents.length, " records"));
                                    return [4 /*yield*/, Promise.all(new Array(concurrency).fill(null).map(function () { return __awaiter(_this, void 0, void 0, function () {
                                            var key, mod, cc, s3d, tdf, batch, updates, r1, rm, r2, marker, qa, e_24;
                                            var _a, _b, _c;
                                            return __generator(this, function (_d) {
                                                switch (_d.label) {
                                                    case 0:
                                                        if (!sourceKeys.length) return [3 /*break*/, 6];
                                                        key = (_a = sourceKeys.pop()) !== null && _a !== void 0 ? _a : "";
                                                        mod = lastMod.pop();
                                                        return [4 /*yield*/, getCustomerConfig(key)];
                                                    case 1:
                                                        cc = _d.sent();
                                                        s3d = new Date(mod);
                                                        tdf = d.getTime() - s3d.getTime();
                                                        if (!(tdf > age)) return [3 /*break*/, 5];
                                                        batch = '';
                                                        updates = '';
                                                        r1 = new RegExp(/json_update_(.*)_/g);
                                                        rm = (_b = r1.exec(key)) !== null && _b !== void 0 ? _b : "";
                                                        batch = rm[1];
                                                        r2 = new RegExp(/json_update_.*?_(.*)\./g);
                                                        rm = (_c = r2.exec(key)) !== null && _c !== void 0 ? _c : "";
                                                        updates = rm[1];
                                                        marker = "ReQueued on: " + new Date();
                                                        _d.label = 2;
                                                    case 2:
                                                        _d.trys.push([2, 4, , 5]);
                                                        return [4 /*yield*/, addWorkToSQSWorkQueue(cc, key, '', parseInt(batch), updates, marker)];
                                                    case 3:
                                                        qa = _d.sent();
                                                        console.info("Return from Maintenance - AddWorkToSQSWorkQueue: ".concat(JSON.stringify(qa)));
                                                        return [3 /*break*/, 5];
                                                    case 4:
                                                        e_24 = _d.sent();
                                                        debugger;
                                                        return [3 /*break*/, 5];
                                                    case 5: return [3 /*break*/, 0];
                                                    case 6: return [2 /*return*/];
                                                }
                                            });
                                        }); }))];
                                case 2:
                                    _d.sent();
                                    ContinuationToken = NextContinuationToken !== null && NextContinuationToken !== void 0 ? NextContinuationToken : "";
                                    return [2 /*return*/];
                            }
                        });
                    };
                    _a.label = 1;
                case 1: return [5 /*yield**/, _loop_5()];
                case 2:
                    _a.sent();
                    _a.label = 3;
                case 3:
                    if (ContinuationToken) return [3 /*break*/, 1];
                    _a.label = 4;
                case 4:
                    // const listReq = {
                    //     Bucket: bucket,
                    //     MaxKeys: 1000,
                    //     Prefix: `config.Customer`,
                    //     ContinuationToken,
                    // } as ListObjectsV2CommandInput
                    // await s3.send( new ListObjectsV2Command( listReq ) )
                    //     .then( async ( s3ListResult: ListObjectsV2CommandOutput ) => {
                    //         // 3,600,000 millisecs = 1 hour
                    //         const a = 3600000 * tcc.S3DropBucketWorkQueueMaintHours  //Older Than X Hours 
                    //         const d: Date = new Date()
                    //         try
                    //         {
                    //             for ( const o in s3ListResult.Contents )
                    //             {
                    //                 const n = parseInt( o )
                    //                 const s3d: Date = new Date( s3ListResult.Contents[ n ].LastModified ?? new Date() )
                    //                 const df = d.getTime() - s3d.getTime()
                    //                 const dd = s3d.setHours( -tcc.S3DropBucketMaintHours )
                    //                 let rm: string[] = []
                    //                 if ( df > a ) 
                    //                 {
                    //                     const obj = s3ListResult.Contents[ n ]
                    //                     const key = obj.Key ?? ""
                    //                     let versionId = ''
                    //                     let batch = ''
                    //                     let updates = ''
                    //                     const r1 = new RegExp( /json_update_(.*)_/g )
                    //                     let rm = r1.exec( key ) ?? ""
                    //                     batch = rm[ 1 ]
                    //                     const r2 = new RegExp( /json_update_.*?_(.*)\./g )
                    //                     rm = r2.exec( key ) ?? ""
                    //                     updates = rm[ 1 ]
                    //                     const marker = "ReQueued on: " + new Date()
                    //                     debugger
                    //                     //build SQS Entry
                    //                     const qa = await addWorkToSQSWorkQueue( config, key, versionId, batch, updates, marker )
                    //                     reQueue.push( "ReQueue Work" + key + " --> " + JSON.stringify( qa ) )
                    //                 }
                    //             }
                    //         } catch ( e )
                    //         {
                    //             throw new Error( `Exception - Maintain S3DropBucket - List Results processing - \n${ e }` )
                    //         }
                    //     } )
                    //     .catch( ( e ) => {
                    //         console.error( `Exception - On S3 List Command for Maintaining Objects on ${ bucket }: ${ e } ` )
                    //     } )
                    debugger;
                    l = reQueue.length;
                    return [2 /*return*/, [l, reQueue]];
            }
        });
    });
}
