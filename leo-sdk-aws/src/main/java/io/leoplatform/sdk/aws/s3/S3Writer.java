package io.leoplatform.sdk.aws.s3;

import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.StreamStats;
import io.leoplatform.sdk.config.ConnectorConfig;
import io.leoplatform.sdk.payload.FileSegment;
import io.leoplatform.sdk.payload.StorageEventOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Singleton
public final class S3Writer {
    private static final Logger log = LoggerFactory.getLogger(S3Writer.class);

    private static final DateTimeFormatter eidFormat = DateTimeFormatter
            .ofPattern("uuuu'/'MM'/'dd'/'HH'/'mm")
            .withZone(UTC);

    private final long maxBatchAge;
    private final int maxBatchRecords;
    private final long maxRecordSize;
    private final S3BufferStyle bufferStyle;
    private final S3TransferManager transferManager;

    private final Queue<FileSegment> payloads = new LinkedList<>();
    private final AtomicBoolean running;
    private final Lock lock = new ReentrantLock();
    private final Condition batchSend = lock.newCondition();

    @Inject
    public S3Writer(ConnectorConfig config, S3TransferManager transferManager,
                    ExecutorManager executorManager) {
        maxBatchAge = config.longValueOrElse("Storage.MaxBatchAge", 4000L);
        maxBatchRecords = config.intValueOrElse("Storage.MaxBatchRecords", 6000);
        maxRecordSize = config.longValueOrElse("Storage.MaxBatchSize", 5017600L);
        bufferStyle = S3BufferStyle.fromName(config.valueOrElse("Storage.BufferStyle", "Memory"));
        this.transferManager = transferManager;
        running = new AtomicBoolean(true);
        CompletableFuture.runAsync(this::asyncBatchSend, executorManager.get());
    }

    void write(FileSegment fileSegment) {
        if (running.get()) {
            add(fileSegment);
            signalBatch();
        } else {
            log.warn("Attempt to add file segment to a stopped batch process");
        }
    }

    void flush() {
        signalBatch();
        sendAll();
        transferManager.flush();
    }

    private void add(FileSegment segment) {
        lock.lock();
        try {
            payloads.add(segment);
        } finally {
            lock.unlock();
        }
    }

    StreamStats end() {
        log.info("Stopping S3 writer");
        flush();
        running.set(false);
        signalBatch();
        log.info("Stopped S3 writer");
        return transferManager.end();
    }

    private void signalBatch() {
        lock.lock();
        try {
            batchSend.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void asyncBatchSend() {
        Instant lastUpload = Instant.now();
        while (running.get()) {
            lock.lock();
            try {
                batchSend.await(maxBatchAge, MILLISECONDS);
            } catch (InterruptedException i) {
                running.set(false);
                log.info("S3 batch writer stopped with {} pending", payloads.size());
            } finally {
                lock.unlock();
            }
            if (Instant.now().isAfter(lastUpload.plusMillis(maxBatchAge))) {
                send();
                lastUpload = Instant.now();
            }
        }
    }

    private void send() {
        AtomicLong fileCount = new AtomicLong();
        Instant now = Instant.now();
        lock.lock();
        try {
            while (!payloads.isEmpty()) {
                Queue<FileSegment> segments = drainToMaximum();
                Optional.of(segments)
                        .map(Queue::peek)
                        .map(FileSegment::getOffset)
                        .ifPresent(o -> transferAsync(fileCount.incrementAndGet(), now, segments, o));
            }
        } finally {
            lock.unlock();
        }
    }

    private void transferAsync(long fileCount, Instant now, Queue<FileSegment> segments, StorageEventOffset o) {
        String fileName = fileName(o, now, fileCount);
        if (bufferStyle == S3BufferStyle.DISK) {
            transferManager.enqueue(new PendingFileUpload(fileName, segments));
        } else {
            transferManager.enqueue(new PendingMemoryUpload(fileName, segments));
        }
    }

    private void sendAll() {
        while (!payloads.isEmpty()) {
            send();
        }
    }

    private Queue<FileSegment> drainToMaximum() {
        Queue<FileSegment> current = new LinkedList<>();
        do {
            if (!payloads.isEmpty()) {
                current.add(payloads.remove());
            }
        } while (!payloads.isEmpty() && belowMax(current, payloads.peek()));
        return current;
    }

    private boolean belowMax(Queue<FileSegment> current, FileSegment next) {
        long currentSize = current.stream()
                .map(FileSegment::getOffset)
                .mapToLong(StorageEventOffset::getGzipSize)
                .sum();
        long nextSize = next.getOffset().getGzipSize();

        long currentRecords = current.stream()
                .map(FileSegment::getOffset)
                .mapToLong(StorageEventOffset::getRecords)
                .sum();
        long nextRecords = next.getOffset().getRecords();

        return currentSize + nextSize < maxRecordSize && currentRecords + nextRecords < maxBatchRecords;
    }

    private String fileName(StorageEventOffset offset, Instant time, long fileNum) {
        String queue = offset.getEvent();
        String formattedTime = eidFormat.format(time);
        String fileNumPad = padWithZeros(fileNum);
        return String.format("bus/%s/z/%s/%d-%s.gz", queue, formattedTime, time.toEpochMilli(), fileNumPad);
    }

    private String padWithZeros(long value) {
        String val = String.valueOf(value);
        return String.format("%0" + (7 - val.length()) + "d%s", 0, val);
    }
}
