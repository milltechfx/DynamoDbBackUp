"use strict";

const Restore = require("./lib/restore");
const Backup = require("./lib/backup");
const Deploy = require("./lib/deploy");

const table = "dev-joeTest-dynamoDb-euw1";
const merged = false;
const dynamoConfig = {
  S3Bucket: "dev-copydowndata-s3-euw1" /* required */,
  S3Prefix: `dev-joeTest-stack-euw1/backup/DYNAMO/${table}${
    merged == true ? "_MERGED" : ""
  }_DYNAMO` /* optional */,
  S3Region: "eu-west-1" /* required */,
  DbTable: table /* required */,
  DbRegion: "eu-west-1" /* required */,
};

//new Backup(dynamoConfig).full(merged);
Restore(dynamoConfig, merged);

module.exports = {
  Backup: Backup,
  Restore: Restore,
  Deploy: Deploy,
};
