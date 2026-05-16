import boto3
import json
import time
import random
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

# Configuration
STREAM_NAME = "UserMetricsStream"
REGION = "us-east-1"
BATCH_SIZE = 500

kinesis = boto3.client('kinesis', region_name=REGION)

def generate_record():
    event_types = ['click', 'view', 'add-to-cart', 'scroll']
    return {
        'eventType': random.choice(event_types),
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'userId': f"user-{random.randint(1, 10000)}",
        'sessionId': f"session-{random.randint(1, 5000)}"
    }

def send_batch(batch_size):
    records = []
    for _ in range(batch_size):
        data = generate_record()
        records.append({
            'Data': json.dumps(data),
            'PartitionKey': data['sessionId']
        })
    
    try:
        response = kinesis.put_records(
            StreamName=STREAM_NAME,
            Records=records
        )
    except Exception as e:
        # Silently fail to keep pacing if Kinesis throttles
        pass

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 generate_spike.py <target_eps> <duration_seconds>")
        sys.exit(1)
        
    target_eps = int(sys.argv[1])
    duration = int(sys.argv[2])
    
    print(f"Starting sustained load: {target_eps} EPS for {duration} seconds into {STREAM_NAME}...")
    start_time = time.time()
    
    batches_per_sec = target_eps // BATCH_SIZE
    remainder = target_eps % BATCH_SIZE
    
    # We use more threads to ensure we can hit the target rate even with network latency
    with ThreadPoolExecutor(max_workers=50) as executor:
        while time.time() - start_time < duration:
            loop_start = time.time()
            
            futures = []
            for _ in range(batches_per_sec):
                futures.append(executor.submit(send_batch, BATCH_SIZE))
            if remainder > 0:
                futures.append(executor.submit(send_batch, remainder))
                
            # Wait for this second's batches to be submitted
            # We don't necessarily need to wait for completion to maintain throughput
            # but we wait a tiny bit to avoid overwhelming the local machine's memory
            
            elapsed = time.time() - loop_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
                
    print("Sustained load complete.")

if __name__ == "__main__":
    main()
