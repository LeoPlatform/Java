package io.leoplatform.sdk.aws.kinesis;

import io.leoplatform.sdk.AsyncWorkQueue;
import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.StreamStats;
import io.leoplatform.sdk.TransferStyle;
import io.leoplatform.sdk.aws.payload.CompressionWriter;
import io.leoplatform.sdk.config.ConnectorConfig;
import io.leoplatform.sdk.payload.EventPayload;
import io.leoplatform.sdk.payload.FileSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toCollection;

@Singleton
public final class KinesisQueue implements AsyncWorkQueue {
    private static final Logger log = LoggerFactory.getLogger(KinesisQueue.class);

    private final long maxBatchAge;
    private final int maxBatchRecords;
    private final long maxBatchSize;
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
        maxBatchSize = config.longValueOrElse("Stream.MaxBatchSize", 1_048_576L);
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
    public void flush() {
        writer.flush();
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
        Executor e = executorManager.get();
        CompletableFuture<Void> cf = CompletableFuture
                .supplyAsync(() -> compressPayloads(toSend), e)
                .thenAcceptAsync(this::toKinesis, e)
                .thenRunAsync(this::removeCompleted, e);
        lock.lock();
        try {
            pendingWrites.add(cf);
        } finally {
            lock.unlock();
        }
    }

    private Queue<FileSegment> compressPayloads(Set<EventPayload> toSend) {
        FileSegment compressedBatch = compression.compressWithOffsets(toSend);
        if (compressedBatch.getOffset().getGzipSize() > maxBatchSize) {
            log.warn("Compressed payload batch exceeds {} bytes", compressedBatch.getOffset().getGzipSize());
            return toSend.parallelStream()
                    .map(Collections::singletonList)
                    .map(compression::compressWithOffsets)
                    .peek(this::checkSize)
                    .filter(s -> s.getOffset().getGzipSize() <= maxBatchSize)
                    .collect(toCollection(LinkedList::new));
        } else {
            return Stream.of(compressedBatch)
                    .collect(toCollection(LinkedList::new));
        }
    }

    private void checkSize(FileSegment fileSegment) {
        Long compressed = fileSegment.getOffset().getGzipSize();
        if (compressed > maxBatchSize) {
            log.error("Skipping {} byte payload which exceeds maximum of {} bytes", compressed, maxBatchSize);
        }
    }

    private void toKinesis(Queue<FileSegment> segments) {
        segments.stream()
                .filter(Objects::nonNull)
                .map(FileSegment::getSegment)
                .filter(b -> b.length > 0)
                .map(ByteBuffer::wrap)
                .forEachOrdered(writer::write);
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
        return TransferStyle.STREAM;
    }

    private boolean exceedsMaxRecords() {
        return payloads.size() >= maxBatchRecords;
    }
}
