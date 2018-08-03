package com.leo.sdk.aws.kinesis;

import com.amazonaws.services.kinesis.producer.UserRecordResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class KinesisResults {
    private static final Logger log = LoggerFactory.getLogger(KinesisResults.class);
    private final Instant start = Instant.now();
    private final Map<String, UserRecordResult> successes = new ConcurrentHashMap<>();
    private final Map<String, Throwable> failures = new ConcurrentHashMap<>();

    void addSuccess(String id, UserRecordResult recordResult) {
        logSuccess(id, recordResult);
    }

    Stream<String> successes() {
        Set<String> s = new HashSet<>(successes.keySet());
        successes.clear();
        return s.parallelStream();
    }

    void addFailure(String id, Throwable throwable) {
        logFailure(id, throwable);
    }

    Stream<String> failures() {
        Set<String> s = new HashSet<>(failures.keySet());
        failures.clear();
        return s.parallelStream();
    }

    Instant start() {
        return start;
    }

    private void logSuccess(String id, UserRecordResult result) {
        successes.put(id, result);
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
        log.info("{} uploaded {} as record {} to Kinesis shard {} in {} attempt{}", succ, id, seq, shard, att, plu);
    }

    private void logFailure(String id, Throwable throwable) {
        failures.put(id, throwable);
        log.error("Could not upload record {} to Kinesis", id, throwable);
    }

}
