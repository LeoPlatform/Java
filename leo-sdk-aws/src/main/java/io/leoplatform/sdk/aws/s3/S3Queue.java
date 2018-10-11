package io.leoplatform.sdk.aws.s3;

import io.leoplatform.sdk.AsyncWorkQueue;
import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.StreamStats;
import io.leoplatform.sdk.TransferStyle;
import io.leoplatform.sdk.aws.payload.CompressionWriter;
import io.leoplatform.sdk.payload.EventPayload;
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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Singleton
public final class S3Queue implements AsyncWorkQueue {
    private static final Logger log = LoggerFactory.getLogger(S3Queue.class);

    private final int maxBatchRecords = 1000;
    private final CompressionWriter compression;
    private final ExecutorManager executorManager;
    private final S3Writer s3Writer;
    private final BlockingQueue<EventPayload> payloads = new LinkedBlockingQueue<>();
    private final List<CompletableFuture<Void>> pendingWrites = new LinkedList<>();
    private final AtomicBoolean running;
    private final Lock lock = new ReentrantLock();
    private final Condition batchSend = lock.newCondition();

    @Inject
    public S3Queue(ExecutorManager executorManager, CompressionWriter compression, S3Writer s3Writer) {
        this.compression = compression;
        this.executorManager = executorManager;
        this.s3Writer = s3Writer;
        running = new AtomicBoolean(true);
        CompletableFuture.runAsync(this::asyncBatchSend, executorManager.get());
    }

    @Override
    public void addEntity(EventPayload entity) {
        if (running.get()) {
            add(entity);
            if (exceedsMaxRecords()) {
                signalBatch();
            }
        } else {
            log.warn("Attempt to add S3 payload to a stopped queue");
        }
    }

    @Override
    public void flush() {
        signalBatch();
        sendAll();
        completePendingWrites();
        s3Writer.flush();
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
                batchSend.await(200L, MILLISECONDS);
            } catch (InterruptedException i) {
                running.set(false);
                log.info("S3 queue stopped with {} pending", payloads.size());
            } finally {
                lock.unlock();
            }
            sendBatch();
        }
    }

    @Override
    public StreamStats end() {
        flush();
        running.set(false);
        signalBatch();
        return s3Writer.end();
    }

    private void send() {
        Set<EventPayload> toSend = drainToSet();
        if (!toSend.isEmpty()) {
            lock.lock();
            try {
                Executor e = executorManager.get();
                CompletableFuture<Void> cf = CompletableFuture
                        .supplyAsync(() -> compression.compressWithOffsets(toSend), e)
                        .thenAcceptAsync(s3Writer::write, e)
                        .thenRunAsync(this::removeCompleted, e);
                pendingWrites.add(cf);
            } finally {
                lock.unlock();
            }
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

    private void completePendingWrites() {
        removeCompleted();
        lock.lock();
        try {
            long inFlight = pendingWrites.parallelStream()
                    .filter(w -> !w.isDone())
                    .count();
            log.info("Waiting for {} S3 background task{} to stop", inFlight, inFlight == 1 ? "" : "s");
        } finally {
            lock.unlock();
        }
        while (!pendingWrites.isEmpty()) {
            lock.lock();
            try {
                batchSend.await(100, MILLISECONDS);
            } catch (InterruptedException i) {
                log.warn("Stopped with incomplete pending S3 batch tasks");
                pendingWrites.clear();
            } finally {
                lock.unlock();
            }
            removeCompleted();
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

    private void signalBatch() {
        lock.lock();
        try {
            batchSend.signalAll();
        } finally {
            lock.unlock();
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

    private boolean exceedsMaxRecords() {
        return payloads.size() >= maxBatchRecords;
    }

    @Override
    public TransferStyle style() {
        return TransferStyle.STORAGE;
    }
}
