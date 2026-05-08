// =============================
// src/kinesis.js
// =============================
import { KinesisClient, PutRecordCommand, PutRecordsCommand } from "@aws-sdk/client-kinesis";

export const KINESIS_STREAM_ARN = "";
export const AWS_ACCESS_KEY_ID = "";
export const AWS_SECRET_ACCESS_KEY = "";

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

  if (
    AWS_ACCESS_KEY_ID === "YOUR_ACCESS_KEY_ID" || AWS_SECRET_ACCESS_KEY === "YOUR_SECRET_ACCESS_KEY"
  ) {
    throw new Error("Replace AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in src/kinesis.js first.");
  }

  return new KinesisClient({
    region,
    credentials: {
      accessKeyId: AWS_ACCESS_KEY_ID,
      secretAccessKey: AWS_SECRET_ACCESS_KEY,
    },
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