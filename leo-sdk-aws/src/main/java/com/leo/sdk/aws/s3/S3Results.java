package com.leo.sdk.aws.s3;

import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.leo.sdk.aws.kinesis.KinesisProducerWriter;
import com.leo.sdk.aws.payload.CompressionWriter;
import com.leo.sdk.payload.ThresholdMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@Singleton
public final class S3Results {
    private static final Logger log = LoggerFactory.getLogger(S3Results.class);

    private final AtomicLong successes = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();
    private final Instant start = Instant.now();
    private final CompressionWriter compressionWriter;
    private final KinesisProducerWriter kinesis;
    private final ThresholdMonitor thresholdMonitor;

    @Inject
    public S3Results(CompressionWriter compressionWriter, KinesisProducerWriter kinesis, ThresholdMonitor thresholdMonitor) {
        this.compressionWriter = compressionWriter;
        this.kinesis = kinesis;
        this.thresholdMonitor = thresholdMonitor;
    }

    void addSuccess(S3Payload payload, UploadResult result) {
        successes.incrementAndGet();
        logSuccess(payload.getRecords(), result);
        thresholdMonitor.addBytes(payload.getGzipSize());
        ByteBuffer b = compressionWriter.compress(payload);
        kinesis.write(b);
    }

    Long successes() {
        return successes.get();
    }

    void addFailure(String desc, Exception ex) {
        failures.incrementAndGet();
        log.error("Could not upload {} to S3: {}", desc, ex);
    }

    Long failures() {
        return failures.get();
    }

    Instant start() {
        return start;
    }

    private void logSuccess(Long records, UploadResult result) {
        String key = Optional.ofNullable(result)
                .map(UploadResult::getKey)
                .orElse("Unknown");
        log.info("Creating Kinesis pointer to {} for {} records", key, records);
    }
}
