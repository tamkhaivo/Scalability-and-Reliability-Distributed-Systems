# Dynamic Traffic Spike Test (Auto-VPA vs HPA)

## Objective
Compare **Dynamic Horizontal Scaling (HPA)** and **Dynamic Vertical Scaling (Auto-VPA)** to determine which architecture better preserves p99 end-to-end latency *during* an active scale-up event triggered by a sustained spike.

## Hypothesis
- **HPA**: Will experience a latency plateau while new pods start and negotiate KCL shard leases. p99 should stay relatively bounded as load is distributed.
- **VPA (Auto)**: Will likely suffer a **massive p99 spike** because Auto-VPA typically evicts the existing pod to recreate it with larger resources, causing temporary total downtime for that consumer.

## Methodology

### Phase 1: Dynamic Horizontal Scaling (Scale Out)
1. **Kinesis**: Ensure `UserMetricsStream` has **4 Shards**.
2. **Deploy**: Apply `k8s/worker-horizontal-test.yaml` and `k8s/worker-hpa.yaml`.
3. **Wait**: Ensure 1 pod is running and stable.
4. **Traffic**: Run `python3 traffic-spikes/generate_spike.py 4000 300` (4,000 EPS for 5 minutes).
5. **Monitor**:
   - `kubectl get hpa -w`
   - `kubectl get pods -l app=analytics-worker`
6. **Measure**: Run `python3 traffic-spikes/measure_p99.py` after the test. Record peak p99.

### Phase 2: Dynamic Vertical Scaling (Scale Up)
1. **Kinesis**: Ensure `UserMetricsStream` has **4 Shards**.
2. **Deploy**: Apply `k8s/worker-vertical-test.yaml` and `k8s/worker-vpa.yaml`.
3. **Wait**: Ensure 1 pod is running and stable.
4. **Traffic**: Run `python3 traffic-spikes/generate_spike.py 4000 300`.
5. **Monitor**:
   - `kubectl get vpa -w`
   - `kubectl get pods -l app=analytics-worker` (Watch for eviction/restarts)
6. **Measure**: Run `python3 traffic-spikes/measure_p99.py` after the test. Record peak p99.

## Evaluation Metrics
- **Peak p99 Latency**: The highest latency recorded during the scaling transition.
- **Settled p99 Latency**: The latency once the target scale (4 pods or 1 large pod) is reached.
- **Consumer Downtime**: Duration where zero metrics were being pushed during the scaling event.
