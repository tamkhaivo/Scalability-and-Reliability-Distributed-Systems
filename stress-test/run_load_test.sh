#!/bin/bash

if [ "$#" -lt 1 ]; then
    echo "Usage: ./run_load_test.sh <URL> [num_instances] [eps_per_instance] [duration_seconds]"
    echo "Example: ./run_load_test.sh http://localhost:3000 5 100 300"
    exit 1
fi

URL=$1
NUM_INSTANCES=${2:-5}
EPS=${3:-100}
DURATION=${4:-300}

echo "Starting $NUM_INSTANCES load generator instances..."
echo "Target: $URL"
echo "Throughput: $EPS EPS per instance (Total: $(($NUM_INSTANCES * $EPS)) EPS)"
echo "Duration: $DURATION seconds"

# Path to the selenium script
SCRIPT_PATH="../client/store_front/automate_browse.py"

for i in $(seq 1 $NUM_INSTANCES); do
    echo "Launching instance $i..."
    python3 "$SCRIPT_PATH" "$URL" "$EPS" "$DURATION" --headless > "instance_$i.log" 2>&1 &
done

echo "All instances launched. Waiting for duration..."
sleep $DURATION
echo "Load test duration reached. Cleaning up..."

# Kill any remaining python processes related to the script
pkill -f "python3 $SCRIPT_PATH"

echo "Load test complete."
