import boto3
import time
from datetime import datetime, timedelta

# Configuration
TABLE_NAME = "UserTelemetryAggregations"
REGION = "us-east-1"
LOOKBACK_MINUTES = 5

dynamodb = boto3.resource('dynamodb', region_name=REGION)
table = dynamodb.Table(TABLE_NAME)

def get_p99_metrics():
    # We'll scan for latency_p99 metrics in the last few minutes
    # In a production environment, we'd use a Global Secondary Index on metric_type and window_timestamp
    # For this test, we scan and filter.
    
    now_ms = int(time.time() * 1000)
    lookback_ms = now_ms - (LOOKBACK_MINUTES * 60 * 1000)
    
    print(f"Querying p99 metrics from DynamoDB (last {LOOKBACK_MINUTES} minutes)...")
    
    response = table.scan(
        FilterExpression="#mt = :mt AND window_timestamp >= :ts",
        ExpressionAttributeNames={"#mt": "metric_type"},
        ExpressionAttributeValues={
            ":mt": "latency_p99",
            ":ts": str(lookback_ms)
        }
    )
    
    items = response.get('Items', [])
    if not items:
        print("No p99 metrics found in the specified window.")
        return
    
    # Sort items by timestamp
    items.sort(key=lambda x: int(x['window_timestamp']))
    
    print("-" * 50)
    print(f"{'Timestamp':<25} | {'p99 Latency (ms)':<20}")
    print("-" * 50)
    
    max_p99 = 0
    for item in items:
        ts = int(item['window_timestamp'])
        dt = datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
        p99 = int(item['count'])
        print(f"{dt:<25} | {p99:<20}")
        if p99 > max_p99:
            max_p99 = p99
            
    print("-" * 50)
    print(f"Peak p99 Latency observed: {max_p99} ms")
    print("-" * 50)

if __name__ == "__main__":
    get_p99_metrics()
