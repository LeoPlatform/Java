package com.leo.sdk.aws.kinesis;

import com.leo.sdk.AsyncWorkQueue;
import com.leo.sdk.StreamStats;
import com.leo.sdk.TransferStyle;
import com.leo.sdk.aws.payload.CompressionWriter;
import com.leo.sdk.bus.LoadingBot;
import com.leo.sdk.config.ConnectorConfig;
import com.leo.sdk.payload.EventPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import static com.leo.sdk.TransferStyle.STREAM;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class KinesisQueue implements AsyncWorkQueue {

    private static final Logger log = LoggerFactory.getLogger(KinesisQueue.class);
    private final TransferStyle style = STREAM;
    private final long maxBatchAge;
    private final LoadingBot bot;
    private final long maxBatchRecords;
    private final Queue<EventPayload> payloads = new ConcurrentLinkedQueue<>();
    private final Lock lock = new ReentrantLock();
    private final Condition sendNow = lock.newCondition();
    private final AtomicLong lastWritten;
    private final ScheduledExecutorService asyncLoad = Executors.newSingleThreadScheduledExecutor();
    private final CompressionWriter compression;

    @Inject
    public KinesisQueue(ConnectorConfig config, CompressionWriter compression, LoadingBot bot) {
        this.bot = bot;
        this.compression = compression;
        maxBatchAge = config.longValueOrElse("Stream.MaxBatchAge", 400L);
        lastWritten = new AtomicLong(System.currentTimeMillis());
        asyncLoad.scheduleWithFixedDelay(checkThresholds(), 100L, maxBatchAge, MILLISECONDS);
        maxBatchRecords = config.longValueOrElse("Stream.MaxBatchRecords", 1000L);
    }

    @Override
    public void addEntity(EventPayload entity) {
        payloads.add(entity);

//        CompletableFuture
//                .supplyAsync(() -> compression.compress(entity), asyncCompress)
//                .thenApply(byteBuffer -> new PayloadIdentifier(entity, byteBuffer))
//                .thenAccept(kinesisWriter::write);
    }

    private Runnable checkThresholds() {
        return () -> {
            if (maxAgeReached() || maxRecordsReached()) {
                asyncLoad.submit(this::sendToKinesis);
            }
        };
    }

    private Runnable sendToKinesis() {
        return () -> {
            Set<EventPayload> toSend = new LinkedHashSet<>();
            while (toSend.size() <= maxBatchRecords && !payloads.isEmpty()) {
                toSend.add(payloads.remove());
            }
            if (!toSend.isEmpty()) {
                compression.compress(toSend);
            }
        };
    }

    @Override
    public StreamStats end() {
        asyncLoad.shutdown();
        try {
            if (!asyncLoad.awaitTermination(30L, SECONDS)) {
                asyncLoad.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("Could not shutdown async compression pool");
            throw new IllegalStateException("Unable to shutdown Kinesis writers", e);
        }
        return emptyStats();
    }

    @Override
    public TransferStyle style() {
        return style;
    }

    private boolean maxAgeReached() {
        return System.currentTimeMillis() - lastWritten.get() > maxBatchAge;
    }

    private boolean maxRecordsReached() {
        return payloads.size() >= maxBatchRecords;
    }

    private StreamStats emptyStats() {
        return new StreamStats() {
            @Override
            public Stream<String> successIds() {
                return Stream.empty();
            }

            @Override
            public Stream<String> failedIds() {
                return Stream.empty();
            }

            @Override
            public Duration totalTime() {
                return Duration.ofMillis(0L);
            }
        };
    }
}
