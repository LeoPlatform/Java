package com.leo.sdk.aws.kinesis;

import com.amazonaws.services.kinesis.producer.UserRecordResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@Singleton
public final class KinesisResults {
    private static final Logger log = LoggerFactory.getLogger(KinesisResults.class);

    private final AtomicLong successes = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();
    private final Instant start = Instant.now();


    void add(UserRecordResult recordResult) {
        Optional.ofNullable(recordResult)
                .ifPresent(this::log);
    }

    void addFailure(Exception e) {
        failures.incrementAndGet();
        log.error("Unable to add payload to Kinesis", e);
    }

    private void log(UserRecordResult r) {
        if (r.isSuccessful()) {
            successes.incrementAndGet();
            logSuccess(r);
        } else {
            failures.incrementAndGet();
            logFailure(r);
        }
    }

    Long successes() {
        return successes.get();
    }

    Long failures() {
        return failures.get();
    }

    Instant start() {
        return start;
    }

    private void logSuccess(UserRecordResult result) {
        String succ = Optional.ofNullable(result)
                .map(UserRecordResult::isSuccessful)
                .filter(s -> s)
                .map(s -> "Successfully")
                .orElse("Unsuccessfully");
        String seq = Optional.ofNullable(result)
                .map(UserRecordResult::getSequenceNumber)
                .orElse("Unknown");
        String shard = Optional.ofNullable(result)
                .map(UserRecordResult::getShardId)
                .orElse("Unknown");
        String att = Optional.ofNullable(result)
                .map(UserRecordResult::getAttempts)
                .map(List::size)
                .map(String::valueOf)
                .orElse("Unknown");
        String plu = Optional.of(att)
                .filter(a -> a.equals("1"))
                .map(a -> "")
                .orElse("s");
        log.info("{} uploaded record {} to {} in {} attempt{}", succ, seq, shard, att, plu);
    }

    private void logFailure(UserRecordResult result) {
        String att = Optional.ofNullable(result)
                .map(UserRecordResult::getAttempts)
                .map(List::size)
                .map(String::valueOf)
                .orElse("Unknown");
        String plu = Optional.of(att)
                .filter(a -> a.equals("1"))
                .map(a -> "")
                .orElse("s");
        log.error("Could not upload record to Kinesis in {} attempt{}", att, plu);
    }
}
