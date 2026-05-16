package com.analytics.aggregator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.*;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import jakarta.annotation.PostConstruct;

@RestController
@RequestMapping("/api/metrics")
@Profile("aggregator")
public class AggregatorService {

    private static final Logger log = LoggerFactory.getLogger(AggregatorService.class);
    private final Map<String, AtomicLong> globalCounts = new ConcurrentHashMap<>();
    private final Map<String, Long> sessionLastSeen = new ConcurrentHashMap<>();
    private final Map<String, Long> shardLastSeen = new ConcurrentHashMap<>();
    private final java.util.List<Long> latencies = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
    private final AtomicLong maxLag = new AtomicLong(0);
    private DynamoDbClient dynamoDbClient;

    private static final String TABLE_NAME = "UserTelemetryAggregations";

    @PostConstruct
    public void init() {
        log.info("Starting Aggregator Service...");
        dynamoDbClient = DynamoDbClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }

    @PostMapping("/partial")
    public void receivePartialMetrics(@RequestBody Map<String, Object> payload) {
        if (payload.containsKey("counts")) {
            Map<String, Number> partialCounts = (Map<String, Number>) payload.get("counts");
            log.debug("Received {} partial counts", partialCounts.size());
            partialCounts.forEach((type, count) -> {
                globalCounts.computeIfAbsent(type, k -> new AtomicLong(0)).addAndGet(count.longValue());
            });
        }
        
        if (payload.containsKey("activeSessions")) {
            java.util.List<String> sessions = (java.util.List<String>) payload.get("activeSessions");
            log.debug("Received {} partial sessions", sessions.size());
            long now = System.currentTimeMillis();
            sessions.forEach(sessionId -> sessionLastSeen.put(sessionId, now));
        }

        if (payload.containsKey("shardId")) {
            String shardId = (String) payload.get("shardId");
            shardLastSeen.put(shardId, System.currentTimeMillis());
        }

        if (payload.containsKey("lag")) {
            long lag = ((Number) payload.get("lag")).longValue();
            synchronized (maxLag) {
                if (lag > maxLag.get()) {
                    maxLag.set(lag);
                }
            }
        }

        if (payload.containsKey("latencies")) {
            java.util.List<Number> partialLatencies = (java.util.List<Number>) payload.get("latencies");
            partialLatencies.forEach(l -> latencies.add(l.longValue()));
        }
    }

    @Scheduled(fixedRate = 1000)
    public void compileAndPush() {
        long now = System.currentTimeMillis();
        
        // Expire entries older than 10 seconds
        sessionLastSeen.entrySet().removeIf(entry -> (now - entry.getValue()) > 10000);
        shardLastSeen.entrySet().removeIf(entry -> (now - entry.getValue()) > 10000);

        if (globalCounts.isEmpty() && sessionLastSeen.isEmpty() && shardLastSeen.isEmpty() && maxLag.get() == 0 && latencies.isEmpty()) return;

        String timestamp = String.valueOf(Instant.now().toEpochMilli());
        Map<String, Long> snapshottedCounts = new ConcurrentHashMap<>();
        
        // Reset counts for the next window while snapshotting current window
        globalCounts.forEach((type, atomicCount) -> snapshottedCounts.put(type, atomicCount.getAndSet(0)));
        snapshottedCounts.put("active_sessions", (long) sessionLastSeen.size());
        snapshottedCounts.put("active_shards", (long) shardLastSeen.size());
        snapshottedCounts.put("stream_lag", maxLag.getAndSet(0));

        // Calculate percentiles
        java.util.List<Long> snapshotLatencies;
        synchronized (latencies) {
            snapshotLatencies = new java.util.ArrayList<>(latencies);
            latencies.clear();
        }

        if (!snapshotLatencies.isEmpty()) {
            java.util.Collections.sort(snapshotLatencies);
            int size = snapshotLatencies.size();
            long p99 = snapshotLatencies.get((int) (size * 0.99));
            long p999 = snapshotLatencies.get((int) (size * 0.999));
            snapshottedCounts.put("latency_p99", p99);
            snapshottedCounts.put("latency_p999", p999);
        }

        snapshottedCounts.forEach((type, count) -> {
            if (count > 0 || type.equals("stream_lag") || type.startsWith("latency_")) {
                try {
                    Map<String, AttributeValue> item = Map.of(
                            "window_timestamp", AttributeValue.builder().s(timestamp).build(),
                            "metric_type", AttributeValue.builder().s(type).build(),
                            "count", AttributeValue.builder().n(String.valueOf(count)).build()
                    );

                    dynamoDbClient.putItem(PutItemRequest.builder()
                            .tableName(TABLE_NAME)
                            .item(item)
                            .build());
                } catch (Exception e) {
                    log.error("Failed to write to DynamoDB", e);
                }
            }
        });

        log.info("Pushed compiled analytics to DynamoDB for timestamp: {}", timestamp);
    }
}
