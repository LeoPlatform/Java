package com.leo.sdk.aws.s3;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class S3Batch {
    private final String id;
    private final AtomicLong age;
    private final AtomicLong batchRecords;
    private final AtomicLong batchSize;
    private final AtomicLong recordSize;
    private final AtomicLong uploadAttempts;

    public S3Batch(String id, Instant start, long batchRecords, long batchSize, long recordSize, long uploadAttempts) {
        this(id, Duration.between(start, Instant.now()), batchRecords, batchSize, recordSize, uploadAttempts);
    }

    public S3Batch(String id, Duration age, long batchRecords, long batchSize, long recordSize, long uploadAttempts) {
        this.id = id;
        long ageMillis = Optional.ofNullable(age)
                .map(Duration::toMillis)
                .orElseThrow(() -> new IllegalArgumentException("Invalid age"));
        this.age = new AtomicLong(ageMillis);
        this.batchRecords = new AtomicLong(batchRecords);
        this.batchSize = new AtomicLong(batchSize);
        this.recordSize = new AtomicLong(recordSize);
        this.uploadAttempts = new AtomicLong(uploadAttempts);
    }

    public Duration getAge() {
        return Duration.ofMillis(age.get());
    }

    public Duration addAge(Duration age) {
        return Duration.ofMillis(this.age.addAndGet(age.toMillis()));
    }

    public long getBatchRecords() {
        return batchRecords.get();
    }

    public long addBatchRecords(long batchRecords) {
        return this.batchRecords.addAndGet(batchRecords);
    }

    public long getBatchSize() {
        return batchSize.get();
    }

    public long addBatchSize(long batchSize) {
        return this.batchSize.addAndGet(batchSize);
    }

    public long getRecordSize() {
        return recordSize.get();
    }

    public long addRecordSize(long recordSize) {
        return this.recordSize.addAndGet(recordSize);
    }

    public long getUploadAttempts() {
        return uploadAttempts.get();
    }

    public long addUploadAttempts(long uploadAttempts) {
        return this.uploadAttempts.addAndGet(uploadAttempts);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        S3Batch s3Batch = (S3Batch) o;
        return Objects.equals(id, s3Batch.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return String.format("S3Batch{id='%s', ageMillis=%d, batchRecords=%d, batchSize=%d, recordSize=%d, uploadAttempts=%d}",
                id, age.get(), batchRecords.get(), batchSize.get(), recordSize.get(), uploadAttempts.get());
    }
}
