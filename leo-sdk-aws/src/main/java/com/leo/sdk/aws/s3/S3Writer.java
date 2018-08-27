package com.leo.sdk.aws.s3;

import com.amazonaws.services.s3.transfer.Upload;
import com.leo.sdk.AsyncPayloadWriter;
import com.leo.sdk.PayloadIdentifier;
import com.leo.sdk.StreamStats;
import com.leo.sdk.TransferStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.leo.sdk.TransferStyle.STORAGE;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MINUTES;

public final class S3Writer implements AsyncPayloadWriter {
    private static final Logger log = LoggerFactory.getLogger(S3Writer.class);

    private static final DateTimeFormatter eidFormat = DateTimeFormatter
            .ofPattern("'z/'uuuu'/'MM'/'dd'/'HH'/'mm'/'")
            .withZone(UTC);

    private final S3TransferManager transferManager;
    private final S3Results resultsProcessor;
    private final ExecutorService asyncWrite = Executors.newWorkStealingPool();
    private final AtomicLong fileCount = new AtomicLong();

    @Inject
    public S3Writer(S3TransferManager transferManager, S3Results resultsProcessor) {
        this.transferManager = transferManager;
        this.resultsProcessor = resultsProcessor;

    }

    @Override
    public void write(PayloadIdentifier payload) {
        CompletableFuture
                .supplyAsync(() -> addRecord(payload), asyncWrite)
                .whenComplete(processResult());
    }

    @Override
    public StreamStats end() {
        log.info("Stopping S3 writer");
        asyncWrite.shutdown();
        try {
            if (!asyncWrite.awaitTermination(4L, MINUTES)) {
                asyncWrite.shutdownNow();
            }
            transferManager.end();
            log.info("Stopped S3 writer");
        } catch (InterruptedException e) {
            log.warn("Could not shutdown S3 async writer pool");
        }
        return getStats();
    }

    @Override
    public TransferStyle style() {
        return STORAGE;
    }

    private Upload addRecord(PayloadIdentifier id) {
        String uniqueName = fileName(id);
        PayloadIdentifier unique = new PayloadIdentifier(uniqueName, id.getEvent(), id.getTimestamp(), id.getPayload());
        return transferManager.transfer(unique);
    }


    private String fileName(PayloadIdentifier payloadIdentifier) {
        String queue = payloadIdentifier.getEvent();
        Instant payloadTime = Instant.ofEpochMilli(payloadIdentifier.getTimestamp());
        String formattedTime = eidFormat.format(payloadTime);
        String fileNumPad = padZeros(fileCount.incrementAndGet());
        return String.format("bus/%s/z/%s%d-%s.gz", queue, formattedTime, payloadTime.toEpochMilli(), fileNumPad);
    }

    private String padZeros(long value) {
        String val = String.valueOf(value);
        return String.format("%0" + (7 - val.length()) + "d%s", 0, val);
    }

    private BiConsumer<Upload, Throwable> processResult() {
        return (result, t) -> {
            if (success(t)) {
//                resultsProcessor.addSuccess(result.getKey(), result.getValue());
            } else {
//                resultsProcessor.addFailure(result.getKey(), t);
            }
        };
    }

    private StreamStats getStats() {
        return new StreamStats() {
            @Override
            public Stream<String> successIds() {
//                return resultsProcessor.successes();
                return Stream.empty();
            }

            @Override
            public Stream<String> failedIds() {
//                return resultsProcessor.failures();
                return Stream.empty();
            }

            @Override
            public Duration totalTime() {
                return Duration.between(Instant.now(), Instant.now());
            }
        };
    }

    private boolean success(Throwable throwable) {
        return !Optional.ofNullable(throwable).isPresent();
    }
}
