// =============================
// src/kinesis.js
// =============================
import { KinesisClient, PutRecordCommand, PutRecordsCommand } from "@aws-sdk/client-kinesis";
import { fromCognitoIdentityPool } from "@aws-sdk/credential-providers";

export const KINESIS_STREAM_ARN = "arn:aws:kinesis:us-east-1:327444422515:stream/UserMetricsStream";
export const IDENTITY_POOL_ID = "us-east-1:4b78870f-3948-4123-83f0-4520ee62e51a";

export function parseKinesisArn(arn) {
  const parts = arn.split(":");
  const region = parts[3] || "";
  const accountId = parts[4] || "";
  const resource = parts.slice(5).join(":");
  const streamName = resource.startsWith("stream/") ? resource.replace("stream/", "") : "";

  return { region, accountId, streamName };
}

function encodeData(value) {
  return new TextEncoder().encode(value);
}

function randomPartitionKey() {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

export function createKinesisClient() {
  const { region, streamName } = parseKinesisArn(KINESIS_STREAM_ARN);

  if (!region || !streamName) {
    throw new Error(
      "Invalid Kinesis ARN. Expected arn:aws:kinesis:REGION:ACCOUNT_ID:stream/STREAM_NAME"
    );
  }

  return new KinesisClient({
    region,
    credentials: fromCognitoIdentityPool({
      clientConfig: { region },
      identityPoolId: IDENTITY_POOL_ID,
    }),
  });
}

export async function putKinesisRecord({ client, data, partitionKey }) {
  const { streamName } = parseKinesisArn(KINESIS_STREAM_ARN);

  const command = new PutRecordCommand({
    StreamName: streamName,
    PartitionKey: partitionKey || randomPartitionKey(),
    Data: encodeData(JSON.stringify(data)),
  });

  return client.send(command);
}

export async function putKinesisRecords({ client, data, partitionKey, count = 1 }) {
  const { streamName } = parseKinesisArn(KINESIS_STREAM_ARN);
  const safeCount = Math.max(1, Math.min(500, Number(count) || 1));

  const records = Array.from({ length: safeCount }).map((_, i) => ({
    Data: encodeData(
      JSON.stringify({
        ...data,
        batchIndex: i,
        ts: new Date().toISOString(),
      })
    ),
    PartitionKey: `${partitionKey || "demo-key"}-${i}`,
  }));

  const command = new PutRecordsCommand({
    StreamName: streamName,
    Records: records,
  });

  return client.send(command);
}

export function getKinesisStreamInfo() {
  return parseKinesisArn(KINESIS_STREAM_ARN);
}