"use strict";

const Restore = require("./lib/restore");
const Backup = require("./lib/backup");
const Deploy = require("./lib/deploy");

const fullStackName = "dev-joeTest-stack-euw1";
const datastore_name = "qa-limits-dynamoDb-euw1";
const timestamp = "dd-MM-yyyy-HH-mm-ss";

const dynamoConfig = {
  S3Bucket: "qa-copydowndata-s3-euw1" /* required */,
  S3Prefix: `backup/qa-LimitsDb-stack-euw1/DYNAMO/qa-limits-dynamoDb-euw1_DYNAMO_18-08-2023-11-22-16.json` /* optional */,
  S3Region: "eu-west-1" /* required */,
  DbTable: datastore_name /* required */,
  DbRegion: "eu-west-1" /* required */,
  mergedFiles: true,
};

Restore(dynamoConfig);
//new Backup(dynamoConfig).full();

module.exports = {
  Backup: Backup,
  Restore: Restore,
  Deploy: Deploy,
};
