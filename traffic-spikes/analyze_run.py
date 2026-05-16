import boto3
import time
import sys
from datetime import datetime, timedelta

# Configuration
TABLE_NAME = "UserTelemetryAggregations"
REGION = "us-east-1"

dynamodb = boto3.resource('dynamodb', region_name=REGION)
table = dynamodb.Table(TABLE_NAME)

def analyze_window(duration_minutes, run_id, scale_type):
    now_ms = int(time.time() * 1000)
    lookback_ms = now_ms - (duration_minutes * 60 * 1000)
    
    print(f"Analyzing {scale_type} Run {run_id} (last {duration_minutes} minutes)...")
    
    response = table.scan(
        FilterExpression="window_timestamp >= :ts",
        ExpressionAttributeValues={":ts": str(lookback_ms)}
    )
    
    items = response.get('Items', [])
    if not items:
        print("No metrics found.")
        return None
    
    # Group by timestamp
    windows = {}
    for item in items:
        ts = item['window_timestamp']
        if ts not in windows:
            windows[ts] = {'events': 0, 'p99': 0, 'shards': 0}
        
        m_type = item['metric_type']
        count = int(item['count'])
        
        if m_type in ['mouse_click', 'mouse_move', 'product_add_to_cart', 'scroll']:
            windows[ts]['events'] += count
        elif m_type == 'latency_p99':
            windows[ts]['p99'] = count
        elif m_type == 'active_shards':
            windows[ts]['shards'] = count

    # Analysis
    sorted_ts = sorted(windows.keys(), key=lambda x: int(x))
    peak_p99 = max([w['p99'] for w in windows.values()] or [0])
    total_events = sum([w['events'] for w in windows.values()])
    
    # Availability: Gaps in timestamps > 2 seconds
    gaps = 0
    for i in range(1, len(sorted_ts)):
        diff = (int(sorted_ts[i]) - int(sorted_ts[i-1])) / 1000.0
        if diff > 2.0:
            gaps += (diff - 1.0) # Deduct 1s for normal interval

    avg_throughput = total_events / (duration_minutes * 60.0)
    
    results = {
        'run_id': run_id,
        'type': scale_type,
        'peak_p99_ms': peak_p99,
        'total_events': total_events,
        'avg_throughput_eps': round(avg_throughput, 2),
        'downtime_sec': round(gaps, 2)
    }
    
    return results

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 analyze_run.py <run_id> <scale_type>")
        sys.exit(1)
    
    res = analyze_window(5, sys.argv[1], sys.argv[2])
    if res:
        print(f"RESULT|{res['type']}|{res['run_id']}|{res['peak_p99_ms']}|{res['total_events']}|{res['avg_throughput_eps']}|{res['downtime_sec']}")
