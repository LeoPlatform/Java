package com.leo.sdk.aws.s3;

import com.amazonaws.services.s3.transfer.model.UploadResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

@Singleton
public final class S3Results {
    private static final Logger log = LoggerFactory.getLogger(S3Results.class);

    private static final int MAX_RESULT_ENTRIES = 10_000;
    private final Map<String, UploadResult> successes = successMap();
    private final Map<String, Throwable> failures = failureMap();
    private final Instant start = Instant.now();


    void addSuccess(String id, UploadResult result) {
        successes.put(id, result);
        logSuccess(id, result);
    }

    Stream<String> successes() {
        Set<String> s = new HashSet<>(successes.keySet());
        successes.clear();
        return s.parallelStream();
    }

    void addFailure(String id, Throwable throwable) {
        failures.put(id, throwable);
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

    private void logSuccess(String id, UploadResult result) {
//        String succ = Optional.ofNullable(result)
//                .map(UserRecordResult::isSuccessful)
//                .filter(s -> s)
//                .map(s -> "Successfully")
//                .orElse("Unsuccessfully");
//        String seq = Optional.ofNullable(result)
//                .map(UserRecordResult::getSequenceNumber)
//                .orElse("Unknown");
//        String shard = Optional.ofNullable(result)
//                .map(UserRecordResult::getShardId)
//                .orElse("Unknown");
//        String att = Optional.ofNullable(result)
//                .map(UserRecordResult::getAttempts)
//                .map(List::size)
//                .map(String::valueOf)
//                .orElse("Unknown");
//        String plu = Optional.of(att)
//                .filter(a -> a.equals("1"))
//                .map(a -> "")
//                .orElse("s");
//        log.info("{} uploaded {} as record {} to Kinesis shard {} in {} attempt{}", succ, id, seq, shard, att, plu);
    }

    private void logFailure(String id, Throwable throwable) {
        log.error("Could not upload record {} to Kinesis", id, throwable);
    }


    private Map<String, UploadResult> successMap() {
        return Collections.synchronizedMap(new LinkedHashMap<String, UploadResult>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, UploadResult> eldest) {
                return size() > MAX_RESULT_ENTRIES;
            }
        });
    }

    private Map<String, Throwable> failureMap() {
        return Collections.synchronizedMap(new LinkedHashMap<String, Throwable>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Throwable> eldest) {
                return size() > MAX_RESULT_ENTRIES;
            }
        });
    }
}
