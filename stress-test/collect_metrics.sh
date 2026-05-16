#!/bin/bash

DURATION=${1:-300}
INTERVAL=5
OUTPUT_FILE="metrics_results.csv"

echo "Timestamp,PodName,CPU(m),Memory(Mi),Lag(ms)" > "$OUTPUT_FILE"

echo "Collecting metrics for $DURATION seconds (Interval: $INTERVAL s)..."

END_TIME=$((SECONDS + DURATION))

while [ $SECONDS -lt $END_TIME ]; do
    TIMESTAMP=$(date +"%Y-%m-%dT%H:%M:%S")
    
    # Get CPU and Memory usage
    TOP_OUTPUT=$(kubectl top pods -l app=analytics-worker --no-headers 2>/dev/null)
    
    if [ -n "$TOP_OUTPUT" ]; then
        while read -r line; do
            POD_NAME=$(echo "$line" | awk '{print $1}')
            CPU=$(echo "$line" | awk '{print $2}' | sed 's/m//')
            MEM=$(echo "$line" | awk '{print $3}' | sed 's/Mi//')
            
            # Get latest lag from logs
            # We take the most recent "lag: " entry
            LAG=$(kubectl logs "$POD_NAME" --tail=50 2>/dev/null | grep "lag: " | tail -n 1 | sed -E 's/.*lag: ([0-9]+)ms.*/\1/')
            
            if [ -z "$LAG" ]; then LAG=0; fi
            
            echo "$TIMESTAMP,$POD_NAME,$CPU,$MEM,$LAG" >> "$OUTPUT_FILE"
        done <<< "$TOP_OUTPUT"
    else
        echo "$TIMESTAMP,None,0,0,0" >> "$OUTPUT_FILE"
    fi
    
    sleep $INTERVAL
done

echo "Metrics collection complete. Results saved to $OUTPUT_FILE"
