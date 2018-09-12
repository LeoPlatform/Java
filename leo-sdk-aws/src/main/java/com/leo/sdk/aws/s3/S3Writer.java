package com.leo.sdk.aws.s3;

import com.amazonaws.services.s3.transfer.Upload;
import com.leo.sdk.ExecutorManager;
import com.leo.sdk.PayloadIdentifier;
import com.leo.sdk.StreamStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static java.time.ZoneOffset.UTC;

public final class S3Writer {
    private static final Logger log = LoggerFactory.getLogger(S3Writer.class);

    private static final DateTimeFormatter eidFormat = DateTimeFormatter
            .ofPattern("'z/'uuuu'/'MM'/'dd'/'HH'/'mm'/'")
            .withZone(UTC);

    private final S3TransferManager transferManager;
    private final ExecutorManager executorManager;
    private final S3Results resultsProcessor;
    private final AtomicLong fileCount = new AtomicLong();

    @Inject
    public S3Writer(S3TransferManager transferManager, ExecutorManager executorManager, S3Results resultsProcessor) {
        this.transferManager = transferManager;
        this.executorManager = executorManager;
        this.resultsProcessor = resultsProcessor;

    }

    public void write(PayloadIdentifier payload) {
        CompletableFuture
                .supplyAsync(() -> addRecord(payload), executorManager.get())
                .whenComplete(processResult());
    }

    public StreamStats end() {
        log.info("Stopping S3 writer");
        transferManager.end();
        log.info("Stopped S3 writer");
        return getStats();
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
        String fileNumPad = padWithZeros(fileCount.incrementAndGet());
        return String.format("bus/%s/z/%s%d-%s.gz", queue, formattedTime, payloadTime.toEpochMilli(), fileNumPad);
    }

    private String padWithZeros(long value) {
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
            public Long successes() {
//                return resultsProcessor.successes();
                return 0L;
            }

            @Override
            public Long failures() {
//                return resultsProcessor.failures();
                return 0L;
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
