package io.leoplatform.sdk.aws.kinesis;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.SimpleStats;
import io.leoplatform.sdk.StreamStats;
import io.leoplatform.sdk.aws.AWSResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
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
    public KinesisProducerWriter(ExecutorManager executorManager, KinesisResults resultsProcessor,
                                 AWSResources credentials) {
        this.stream = credentials.kinesisStream();
        KinesisProducerConfiguration kCfg = new KinesisProducerConfiguration()
            .setCredentialsProvider(credentials.credentials())
            .setRegion(credentials.region())
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
            throw new RuntimeException("Error adding record");
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
            if (inFlight > 0) {
                log.info("Waited for {} Kinesis upload{} to complete", inFlight, inFlight == 1 ? "" : "s");
            }
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

    private StreamStats getStats() {
        return new SimpleStats(resultsProcessor.successes(), resultsProcessor.failures(), resultsProcessor.start());
    }
}
