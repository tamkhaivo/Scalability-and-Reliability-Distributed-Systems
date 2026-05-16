#!/bin/bash
# Master Benchmark: Horizontal vs Vertical Scalability (5 Iterations)

mkdir -p traffic-spikes
rm -f traffic-spikes/benchmark_raw.txt

# Phase 1: Horizontal Benchmark
echo "Starting Horizontal Scaling Benchmark..."
for i in {1..5}; do
  echo "--- Horizontal Run $i ---"
  kubectl delete hpa --all && kubectl delete deployment analytics-worker-horizontal && kubectl delete deployment analytics-worker-vertical
  kubectl apply -f k8s/worker-horizontal-test.yaml && kubectl apply -f k8s/worker-hpa.yaml
  kubectl set image deployment/analytics-worker-horizontal worker=327444422515.dkr.ecr.us-east-1.amazonaws.com/analytics-processor:latest
  kubectl wait --for=condition=available deployment/analytics-worker-horizontal --timeout=300s
  sleep 30
  /opt/homebrew/opt/python@3.11/bin/python3.11 traffic-spikes/generate_spike.py 4000 120
  sleep 30
  /opt/homebrew/opt/python@3.11/bin/python3.11 traffic-spikes/analyze_run.py $i "Horizontal" >> traffic-spikes/benchmark_raw.txt
done

# Phase 2: Vertical Benchmark
echo "Starting Vertical Scaling Benchmark..."
for i in {1..5}; do
  echo "--- Vertical Run $i ---"
  kubectl delete hpa --all && kubectl delete deployment analytics-worker-horizontal && kubectl delete deployment analytics-worker-vertical
  kubectl apply -f k8s/worker-vertical-test.yaml
  kubectl set image deployment/analytics-worker-vertical worker=327444422515.dkr.ecr.us-east-1.amazonaws.com/analytics-processor:latest
  kubectl wait --for=condition=available deployment/analytics-worker-vertical --timeout=300s
  sleep 30
  /opt/homebrew/opt/python@3.11/bin/python3.11 traffic-spikes/generate_spike.py 4000 120 & PID=$!
  sleep 45
  echo "Simulating VPA Scale Up..."
  kubectl patch deployment analytics-worker-vertical --patch '{"spec": {"template": {"spec": {"containers": [{"name": "worker", "resources": {"requests": {"cpu": "1500m", "memory": "1Gi"}, "limits": {"cpu": "2000m", "memory": "2Gi"}}}]}}}}'
  wait $PID
  sleep 30
  /opt/homebrew/opt/python@3.11/bin/python3.11 traffic-spikes/analyze_run.py $i "Vertical" >> traffic-spikes/benchmark_raw.txt
done

echo "Benchmark complete. Results in traffic-spikes/benchmark_raw.txt"
