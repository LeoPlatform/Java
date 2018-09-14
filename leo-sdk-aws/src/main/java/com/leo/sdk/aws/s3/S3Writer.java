package com.leo.sdk.aws.s3;

import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.leo.sdk.ExecutorManager;
import com.leo.sdk.StreamStats;
import com.leo.sdk.bus.LoadingBot;
import com.leo.sdk.config.ConnectorConfig;
import com.leo.sdk.payload.FileSegment;
import com.leo.sdk.payload.StorageEventOffset;
import com.leo.sdk.payload.StorageStats;
import com.leo.sdk.payload.StorageUnits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

@Singleton
public final class S3Writer {
    private static final Logger log = LoggerFactory.getLogger(S3Writer.class);

    private static final DateTimeFormatter eidFormat = DateTimeFormatter
            .ofPattern("'z/'uuuu'/'MM'/'dd'/'HH'/'mm'/'")
            .withZone(UTC);

    private final long maxBatchAge;
    private final int maxBatchRecords;
    private final long maxRecordSize;
    private final S3TransferManager transferManager;
    private final ExecutorManager executorManager;
    private final S3Results resultsProcessor;
    private final LoadingBot bot;

    private final Queue<FileSegment> payloads = new LinkedList<>();
    private final AtomicBoolean running;
    private final Lock lock = new ReentrantLock();
    private final Condition batchSend = lock.newCondition();

    @Inject
    public S3Writer(ConnectorConfig config, S3TransferManager transferManager,
                    ExecutorManager executorManager, S3Results resultsProcessor,
                    LoadingBot bot) {
        maxBatchAge = config.longValueOrElse("Storage.MaxBatchAge", 4000L);
        maxBatchRecords = config.intValueOrElse("Storage.MaxBatchRecords", 6000);
        maxRecordSize = config.longValueOrElse("Storage.MaxRecordSize", 5017600L);
        this.transferManager = transferManager;
        this.executorManager = executorManager;
        this.resultsProcessor = resultsProcessor;
        this.bot = bot;
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
        running.set(false);
        signalBatch();
        sendAll();
        transferManager.end();
        log.info("Stopped S3 writer");
        return getStats();
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
        Upload upload = transferManager.transfer(fileName, segments);
        Executor e = executorManager.get();
        CompletableFuture.runAsync(() -> processResult(upload, segments), e);
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
        return String.format("bus/%s/%s%d-%s.gz", queue, formattedTime, time.toEpochMilli(), fileNumPad);
    }

    private String padWithZeros(long value) {
        String val = String.valueOf(value);
        return String.format("%0" + (7 - val.length()) + "d%s", 0, val);
    }

    private void processResult(Upload upload, Queue<FileSegment> segments) {
        try {
            UploadResult result = upload.waitForUploadResult();
            S3Payload s3Payload = toS3Payload(result, segments);
            resultsProcessor.addSuccess(s3Payload, result);
        } catch (Exception e) {
            resultsProcessor.addFailure(upload.getDescription(), e);
        }
    }

    private S3Payload toS3Payload(UploadResult result, Queue<FileSegment> segments) {
        String queue = getEvent(segments);
        Long gzipSize = segments.stream().map(FileSegment::getOffset).mapToLong(StorageEventOffset::getGzipSize).sum();
        Long size = segments.stream().map(FileSegment::getOffset).mapToLong(StorageEventOffset::getSize).sum();
        Long records = segments.stream().map(FileSegment::getOffset).mapToLong(StorageEventOffset::getRecords).sum();
        S3LocationPayload location = new S3LocationPayload(result.getBucketName(), result.getKey());
        List<StorageEventOffset> offsets = accumulateOffsets(segments);
        StorageStats stats = new StorageStats(Collections.singletonMap(bot.name(), new StorageUnits(records)));
        return new S3Payload(queue, null, null, location, offsets, gzipSize, size, records, stats);
    }

    private List<StorageEventOffset> accumulateOffsets(Queue<FileSegment> segments) {
        AtomicLong startAccumulator = new AtomicLong();
        AtomicLong offsetAccumulator = new AtomicLong();
        AtomicLong gzipOffsetAccumulator = new AtomicLong();
        return segments.stream()
                .map(FileSegment::getOffset)
                .map(o -> {
                    Long start = startAccumulator.getAndAdd(o.getRecords());
                    Long end = start + o.getRecords() - 1;
                    Long offset = offsetAccumulator.getAndAdd(o.getSize());
                    Long gzipOffset = gzipOffsetAccumulator.getAndAdd(o.getGzipSize());
                    return new StorageEventOffset(o.getEvent(), start, end, o.getSize(), offset, o.getRecords(), o.getGzipSize(), gzipOffset);
                })
                .collect(toList());
    }

    private String getEvent(Queue<FileSegment> segments) {
        return Optional.of(segments)
                .map(Queue::peek)
                .map(FileSegment::getOffset)
                .map(StorageEventOffset::getEvent)
                .orElseThrow(() -> new IllegalArgumentException("Missing storage event"));
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
