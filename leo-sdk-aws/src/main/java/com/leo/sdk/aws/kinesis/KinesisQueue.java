package com.leo.sdk.aws.kinesis;

import com.leo.sdk.AsyncWorkQueue;
import com.leo.sdk.ExecutorManager;
import com.leo.sdk.StreamStats;
import com.leo.sdk.TransferStyle;
import com.leo.sdk.aws.payload.CompressionWriter;
import com.leo.sdk.config.ConnectorConfig;
import com.leo.sdk.payload.EventPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.leo.sdk.TransferStyle.STREAM;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Singleton
public final class KinesisQueue implements AsyncWorkQueue {
    private static final Logger log = LoggerFactory.getLogger(KinesisQueue.class);

    private final long maxBatchAge;
    private final int maxBatchRecords;
    private final ExecutorManager executorManager;
    private final CompressionWriter compression;
    private final KinesisProducerWriter writer;
    private final BlockingQueue<EventPayload> payloads = new LinkedBlockingQueue<>();
    private final List<CompletableFuture<Void>> pendingWrites = new LinkedList<>();
    private final AtomicBoolean running;
    private final Lock lock = new ReentrantLock();
    private final Condition batchSend = lock.newCondition();

    @Inject
    public KinesisQueue(ConnectorConfig config, ExecutorManager executorManager,
                        CompressionWriter compression, KinesisProducerWriter writer) {
        maxBatchAge = config.longValueOrElse("Stream.MaxBatchAge", 400L);
        maxBatchRecords = config.intValueOrElse("Stream.MaxBatchRecords", 1000);
        this.executorManager = executorManager;
        this.compression = compression;
        this.writer = writer;
        running = new AtomicBoolean(true);
        pendingWrites.add(CompletableFuture.runAsync(this::asyncBatchSend, executorManager.get()));
    }

    @Override
    public void addEntity(EventPayload entity) {
        if (running.get()) {
            add(entity);
            if (exceedsMaxRecords()) {
                signalBatch();
            }
        } else {
            log.warn("Attempt to add kinesis payload to a stopped queue");
        }
    }

    @Override
    public StreamStats end() {
        running.set(false);
        signalBatch();
        sendAll();
        completePendingTasks();
        return writer.end();
    }

    private void add(EventPayload entity) {
        lock.lock();
        try {
            payloads.add(entity);
        } finally {
            lock.unlock();
        }
    }

    private void asyncBatchSend() {
        while (running.get()) {
            lock.lock();
            try {
                batchSend.await(maxBatchAge, MILLISECONDS);
            } catch (InterruptedException i) {
                running.set(false);
                log.info("Kinesis queue stopped with {} pending", payloads.size());
            } finally {
                lock.unlock();
            }
            sendBatch();
        }
    }

    private void sendBatch() {
        do {
            send();
        } while (exceedsMaxRecords());
    }

    private void sendAll() {
        while (!payloads.isEmpty()) {
            send();
        }
    }

    private void completePendingTasks() {
        removeCompleted();
        lock.lock();
        try {
            long inFlight = pendingWrites.parallelStream()
                    .filter(w -> !w.isDone())
                    .count();
            log.info("Waiting for {} kinesis background task{} to complete", inFlight, inFlight == 1 ? "" : "s");
        } finally {
            lock.unlock();
        }
        while (!pendingWrites.isEmpty()) {
            lock.lock();
            try {
                batchSend.await(100, MILLISECONDS);
            } catch (InterruptedException i) {
                log.warn("Stopped with incomplete pending Kinesis batch tasks");
                pendingWrites.clear();
            } finally {
                lock.unlock();
            }
            removeCompleted();
        }
    }

    private void send() {
        Set<EventPayload> toSend = drainToSet();
        if (!toSend.isEmpty()) {
            lock.lock();
            try {
                Executor e = executorManager.get();
                CompletableFuture<Void> cf = CompletableFuture
                        .supplyAsync(() -> compression.compressWithNewlines(toSend), e)
                        .thenAcceptAsync(p -> writer.write(p.getPayload()), e)
                        .thenRunAsync(this::removeCompleted, e);
                pendingWrites.add(cf);
            } finally {
                lock.unlock();
            }
        }
    }

    private Set<EventPayload> drainToSet() {
        Set<EventPayload> toSend = new LinkedHashSet<>();
        lock.lock();
        try {
            payloads.drainTo(toSend, maxBatchRecords);
        } finally {
            lock.unlock();
        }
        return toSend;
    }

    private void signalBatch() {
        lock.lock();
        try {
            batchSend.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void removeCompleted() {
        lock.lock();
        try {
            pendingWrites.removeIf(CompletableFuture::isDone);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public TransferStyle style() {
        return STREAM;
    }

    private boolean exceedsMaxRecords() {
        return payloads.size() >= maxBatchRecords;
    }
}
