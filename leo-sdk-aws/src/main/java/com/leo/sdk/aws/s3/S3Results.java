package com.leo.sdk.aws.s3;

import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr353.JSR353Module;
import com.leo.sdk.aws.kinesis.KinesisProducerWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.ALWAYS;
import static java.nio.charset.StandardCharsets.UTF_8;

@Singleton
public final class S3Results {
    private static final Logger log = LoggerFactory.getLogger(S3Results.class);

    private final ObjectMapper mapper = buildMapper();
    private final AtomicLong successes = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();
    private final Instant start = Instant.now();
    private final KinesisProducerWriter kinesis;

    @Inject
    public S3Results(KinesisProducerWriter kinesis) {
        this.kinesis = kinesis;
    }

    void addSuccess(S3Payload payload, UploadResult result) {
        successes.incrementAndGet();
        logSuccess(payload.getRecords(), result);
        byte[] b = toJsonString(payload).getBytes(UTF_8);
        kinesis.write(ByteBuffer.wrap(b));
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

    public String toJsonString(S3Payload s3Payload) {
        try {
            return mapper.writeValueAsString(s3Payload);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create S3 JSON payload");
        }
    }

    private static ObjectMapper buildMapper() {
        return new ObjectMapper()
                .setSerializationInclusion(ALWAYS)
                .registerModule(new JSR353Module());
    }
}
