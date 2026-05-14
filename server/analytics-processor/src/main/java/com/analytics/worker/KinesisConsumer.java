package com.analytics.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
@Profile("worker")
public class KinesisConsumer {

    private static final Logger log = LoggerFactory.getLogger(KinesisConsumer.class);

    @Value("${kinesis.stream.name:UserMetricsStream}")
    private String streamName;

    @Value("${aggregator.url:http://dashboard-ingestion-service.default.svc.cluster.local:8080/api/metrics/partial}")
    private String aggregatorUrl;

    private Scheduler scheduler;

    @PostConstruct
    public void start() {
        log.info("Starting Kinesis Worker Consumer...");
        KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(
                KinesisAsyncClient.builder().region(Region.US_EAST_1)
                        .credentialsProvider(DefaultCredentialsProvider.create()));
        DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create()).build();
        CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create()).build();

        ConfigsBuilder configsBuilder = new ConfigsBuilder(streamName, "analytics-processor", kinesisClient,
                dynamoClient, cloudWatchClient, UUID.randomUUID().toString(), new AnalyticsRecordProcessorFactory());

        scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig());

        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.setDaemon(true);
        schedulerThread.start();
    }

    @PreDestroy
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    private class AnalyticsRecordProcessorFactory implements ShardRecordProcessorFactory {
        @Override
        public ShardRecordProcessor shardRecordProcessor() {
            return new AnalyticsRecordProcessor();
        }
    }

    private class AnalyticsRecordProcessor implements ShardRecordProcessor {
        private String shardId;
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final RestTemplate restTemplate = new RestTemplate();

        // Local state
        private final Map<String, AtomicLong> localCounts = new ConcurrentHashMap<>();
        private final java.util.Set<String> localSessions = ConcurrentHashMap.newKeySet();
        private final java.util.List<Long> latencies = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        private long lastFlushTime = System.currentTimeMillis();
        private long currentLag = 0;

        @Override
        public void initialize(InitializationInput initializationInput) {
            this.shardId = initializationInput.shardId();
            log.info("Initializing record processor for shard: {}", shardId);
        }

        @Override
        public void processRecords(ProcessRecordsInput processRecordsInput) {
            this.currentLag = processRecordsInput.millisBehindLatest() != null ? processRecordsInput.millisBehindLatest() : 0;
            long now = System.currentTimeMillis();
            
            processRecordsInput.records().forEach(record -> {
                try {
                    byte[] bytes = new byte[record.data().remaining()];
                    record.data().get(bytes);
                    String data = new String(bytes, StandardCharsets.UTF_8);
                    
                    Map<String, Object> telemetryEvent = objectMapper.readValue(data, Map.class);
                    String type = (String) telemetryEvent.getOrDefault("eventType", "unknown");
                    localCounts.computeIfAbsent(type, k -> new AtomicLong(0)).incrementAndGet();

                    if (telemetryEvent.containsKey("timestamp")) {
                        String eventTimestampStr = (String) telemetryEvent.get("timestamp");
                        try {
                            long eventTimestamp = java.time.Instant.parse(eventTimestampStr).toEpochMilli();
                            latencies.add(now - eventTimestamp);
                        } catch (Exception e) {
                            log.warn("Failed to parse timestamp: {}", eventTimestampStr);
                        }
                    }

                    if (record.partitionKey() != null) {
                        localSessions.add(record.partitionKey());
                    }
                } catch (Exception e) {
                    log.error("Failed to parse record", e);
                }
            });

            long currentTime = System.currentTimeMillis();
            if (currentTime - lastFlushTime >= 1000) {
                flushLocalCounts();
                lastFlushTime = currentTime;

                try {
                    processRecordsInput.checkpointer().checkpoint();
                } catch (InvalidStateException | ShutdownException e) {
                    log.error("Error checkpointing", e);
                }
            }
        }

        private void flushLocalCounts() {
            if (localCounts.isEmpty() && localSessions.isEmpty() && currentLag == 0 && latencies.isEmpty())
                return;

            try {
                Map<String, Long> payloadCounts = new ConcurrentHashMap<>();
                localCounts.forEach((k, v) -> payloadCounts.put(k, v.getAndSet(0)));

                java.util.Set<String> payloadSessions = new java.util.HashSet<>(localSessions);
                localSessions.clear();

                java.util.List<Long> payloadLatencies;
                synchronized (latencies) {
                    payloadLatencies = new java.util.ArrayList<>(latencies);
                    latencies.clear();
                }

                Map<String, Object> payload = new java.util.HashMap<>();
                payload.put("counts", payloadCounts);
                payload.put("activeSessions", payloadSessions);
                payload.put("shardId", shardId);
                payload.put("lag", currentLag);
                payload.put("latencies", payloadLatencies);

                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                HttpEntity<Map<String, Object>> request = new HttpEntity<>(payload, headers);

                restTemplate.postForObject(aggregatorUrl, request, String.class);
                log.debug("Flushed partial metrics for shard {} (lag: {}ms, events: {}) to aggregator.", 
                        shardId, currentLag, payloadLatencies.size());
            } catch (Exception e) {
                log.error("Failed to flush local counts to aggregator", e);
            }
        }

        @Override
        public void leaseLost(LeaseLostInput leaseLostInput) {
            log.info("Lease lost for shard: {}", shardId);
        }

        @Override
        public void shardEnded(ShardEndedInput shardEndedInput) {
            log.info("Shard ended: {}", shardId);
            try {
                shardEndedInput.checkpointer().checkpoint();
            } catch (InvalidStateException | ShutdownException e) {
                log.error("Error checkpointing", e);
            }
        }

        @Override
        public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
            log.info("Shutdown requested for shard: {}", shardId);
            try {
                shutdownRequestedInput.checkpointer().checkpoint();
            } catch (InvalidStateException | ShutdownException e) {
                log.error("Error checkpointing", e);
            }
        }
    }
}
