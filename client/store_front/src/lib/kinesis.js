// =============================
// src/kinesis.js
// =============================
import { KinesisClient, PutRecordCommand, PutRecordsCommand } from "@aws-sdk/client-kinesis";
import { fromCognitoIdentityPool } from "@aws-sdk/credential-providers";

export function parseKinesisArn(arn) {
  if (!arn) return { region: "", accountId: "", streamName: "" };
  if (!arn.startsWith("arn:aws:kinesis")) return { region: "us-east-1", accountId: "", streamName: arn };

  const parts = arn.split(":");
  const region = parts[3] || "us-east-1";
  const accountId = parts[4] || "";
  const resource = parts.slice(5).join(":");
  const streamName = resource.startsWith("stream/") ? resource.replace("stream/", "") : resource;

  return { region, accountId, streamName };
}

function encodeData(value) {
  return new TextEncoder().encode(value);
}

function randomPartitionKey() {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

export function createKinesisClient(identityPoolId, region = "us-east-1") {
  if (!identityPoolId) {
    throw new Error("Identity Pool ID is required for Kinesis Client");
  }

  return new KinesisClient({
    region,
    credentials: fromCognitoIdentityPool({
      clientConfig: { region },
      identityPoolId: identityPoolId,
    }),
  });
}

export async function putKinesisRecord({ client, streamName, data, partitionKey }) {
  const command = new PutRecordCommand({
    StreamName: streamName,
    PartitionKey: partitionKey || randomPartitionKey(),
    Data: encodeData(JSON.stringify(data)),
  });

  return client.send(command);
}

export async function putKinesisRecords({ client, streamName, records }) {
  if (!records || records.length === 0) return;

  const kinesisRecords = records.map((record) => ({
    Data: encodeData(JSON.stringify(record.data)),
    PartitionKey: record.partitionKey || randomPartitionKey(),
  }));

  const command = new PutRecordsCommand({
    StreamName: streamName,
    Records: kinesisRecords,
  });

  return client.send(command);
}