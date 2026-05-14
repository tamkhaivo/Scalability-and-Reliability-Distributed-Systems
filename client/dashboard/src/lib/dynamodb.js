/*Brendan Nichols - CSC 258 Project Dashboard
This is the dynamodb.js file for the dashboard. It pulls aggregated sliding window data from AWS DynamoDB.
*/
import { DynamoDBClient, ScanCommand } from "@aws-sdk/client-dynamodb";
import { fromCognitoIdentityPool } from "@aws-sdk/credential-providers";

export const DYNAMODB_TABLE_NAME = "UserTelemetryAggregations";

export function createDynamoClient(identityPoolId, region = "us-east-1") {
  if (!identityPoolId) {
    throw new Error("Identity Pool ID is required for DynamoDB Client");
  }

  return new DynamoDBClient({
    region: region,
    credentials: fromCognitoIdentityPool({
      clientConfig: { region: region },
      identityPoolId: identityPoolId,
    }),
  });
}

export async function getRecentAggregations({ client, secondsAgo = 60 }) {
  if (!client) return [];

  const cutoffTime = Date.now() - (secondsAgo * 1000);

  // Note: For a real production app, use Query on an index, but Scan is fine for prototype with TTL
  const command = new ScanCommand({
    TableName: DYNAMODB_TABLE_NAME,
    FilterExpression: "window_timestamp >= :cutoff",
    ExpressionAttributeValues: {
      ":cutoff": { S: cutoffTime.toString() }
    }
  });

  const response = await client.send(command);
  
  // Guard: DynamoDB returns undefined Items when no records match the filter
  const items = response.Items ?? [];
  
  return items.map(item => ({
    timestamp: parseInt(item.window_timestamp.S),
    type: item.metric_type.S,
    count: parseInt(item.count.N)
  })).sort((a, b) => a.timestamp - b.timestamp);
}
