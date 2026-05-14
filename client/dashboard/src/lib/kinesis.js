/*Brendan Nichols - CSC 258 Project Dashboard
This is the kinesis.js file for the dashboard. It pulls the data from the AWS Kinesis Data Stream so that it can then be processed in Dashboard.jsx
*/
import { KinesisClient, ListShardsCommand, GetShardIteratorCommand, GetRecordsCommand } from "@aws-sdk/client-kinesis";

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

export async function getShardIterator({ client, shardId }) {
  const { streamName } = parseKinesisArn(KINESIS_STREAM_ARN);
  
  const command = new GetShardIteratorCommand({
    StreamName: streamName,
    ShardId: shardId,
    ShardIteratorType: "LATEST", // Using LATEST for real-time info
  });

  const response = await client.send(command);
  return response.ShardIterator;
}

export async function getKinesisRecords({ client, shardIterator }) {
  const command = new GetRecordsCommand({
    ShardIterator: shardIterator,
    Limit: 100,
  });

  const response = await client.send(command);
  
  // Decoder to handle the encoded data sent by store_front
  const decoder = new TextDecoder("utf-8");

  const records = response.Records.map((record) => {
    const jsonString = decoder.decode(record.Data);
    return JSON.parse(jsonString);
  });

  return {
    records,
    nextIterator: response.NextShardIterator,
    millisBehindLatest: response.MillisBehindLatest,
  };
}

//For the Active Shards section under the Dashboard's "System Health"
export async function listActiveShards(client) {
  const { streamName } = parseKinesisArn(KINESIS_STREAM_ARN);
  const command = new ListShardsCommand({ StreamName: streamName });
  const response = await client.send(command);
  return response.Shards; //The length of this array will be equal to the number of active shards
}