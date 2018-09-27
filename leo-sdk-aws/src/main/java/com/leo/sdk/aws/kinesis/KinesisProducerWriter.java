package com.leo.sdk.aws.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.leo.sdk.ExecutorManager;
import com.leo.sdk.StreamStats;
import com.leo.sdk.config.ConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Singleton
public final class KinesisProducerWriter {
    private static final Logger log = LoggerFactory.getLogger(KinesisProducerWriter.class);
    private final KinesisResults resultsProcessor;
    private final KinesisProducer kinesis;
    private final String stream;
    private final ExecutorManager executorManager;
    private final List<CompletableFuture<Void>> pendingWrites = new LinkedList<>();
    private final Lock lock = new ReentrantLock();
    private final Condition asyncUpload = lock.newCondition();

    @Inject
    public KinesisProducerWriter(ConnectorConfig config, ExecutorManager executorManager,
                                 KinesisResults resultsProcessor) {
        this.stream = config.value("Stream.Name");
        KinesisProducerConfiguration kCfg = new KinesisProducerConfiguration()
                .setCredentialsProvider(credentials(config))
                .setRegion(config.valueOrElse("Region", "us-east-1"))
                .setAggregationEnabled(false)
                .setRecordMaxBufferedTime(200L)
                .setRequestTimeout(60000)
                .setMaxConnections(48)
                .setCredentialsRefreshDelay(100)
                .setMetricsNamespace("LEO Java SDK")
                .setLogLevel("info");
        this.kinesis = new KinesisProducer(kCfg);
        this.executorManager = executorManager;
        this.resultsProcessor = resultsProcessor;
    }

    public void write(ByteBuffer payload) {
        lock.lock();
        try {
            Executor e = executorManager.get();
            CompletableFuture<Void> cf = CompletableFuture
                    .supplyAsync(() -> addRecord(payload), e)
                    .thenAcceptAsync(r -> resultsProcessor.add(r, payload.array().length), e)
                    .thenRunAsync(this::removeCompleted, e);
            pendingWrites.add(cf);
        } finally {
            lock.unlock();
        }
    }

    void flush() {
        kinesis.flushSync();
    }

    private UserRecordResult addRecord(ByteBuffer payload) {
        try {
            return kinesis.addUserRecord(stream, "0", payload).get();
        } catch (Exception e) {
            resultsProcessor.addFailure(e);
            return null;
        }
    }

    StreamStats end() {
        completePendingTasks();

        try {
            log.info("Flushing Kinesis pipeline");
            kinesis.flushSync();
        } catch (Exception e) {
            log.warn("Unable to flush kinesis pipeline: {}", e.getMessage());
        }
        try {
            log.info("Stopping Kinesis writer ({} outstanding)", kinesis.getOutstandingRecordsCount());
            kinesis.destroy();
            log.info("Stopped Kinesis writer");
        } catch (Exception e) {
            log.warn("Unable to stop Kinesis writer: {}", e.getMessage());
        }
        return getStats();
    }

    private void completePendingTasks() {
        while (!pendingWrites.isEmpty()) {
            lock.lock();
            try {
                asyncUpload.await(100, MILLISECONDS);
            } catch (InterruptedException i) {
                log.warn("Stopped Kinesis upload manager with incomplete pending tasks");
                pendingWrites.clear();
            } finally {
                lock.unlock();
            }
            removeCompleted();
        }
        lock.lock();
        try {
            long inFlight = pendingWrites.parallelStream()
                    .map(CompletableFuture::join)
                    .count();
            log.info("Waited for {} Kinesis upload{} to complete", inFlight, inFlight == 1 ? "" : "s");
        } finally {
            lock.unlock();
        }
    }

    private void removeCompleted() {
        lock.lock();
        try {
            pendingWrites.removeIf(CompletableFuture::isDone);
            asyncUpload.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private AWSCredentialsProvider credentials(ConnectorConfig config) {
        try {
            return Optional.of(config.valueOrElse("AwsProfile", ""))
                    .map(String::trim)
                    .filter(profile -> !profile.isEmpty())
                    .map(ProfileCredentialsProvider::new)
                    .filter(p -> p.getCredentials() != null)
                    .map(AWSCredentialsProvider.class::cast)
                    .orElse(DefaultAWSCredentialsProviderChain.getInstance());
        } catch (Exception e) {
            return DefaultAWSCredentialsProviderChain.getInstance();
        }
    }

    private StreamStats getStats() {
        return new StreamStats() {
            @Override
            public Long successes() {
                return resultsProcessor.successes();
            }

            @Override
            public Long failures() {
                return resultsProcessor.failures();
            }

            @Override
            public Duration totalTime() {
                return Duration.between(resultsProcessor.start(), Instant.now());
            }
        };
    }
}
