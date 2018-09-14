package com.leo.sdk.aws.s3;

import com.leo.sdk.payload.StorageEventOffset;
import com.leo.sdk.payload.StoragePayload;
import com.leo.sdk.payload.StreamCorrelation;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class S3Payload implements StoragePayload {
    private final String event;
    private final String start;
    private final String end;
    private final S3LocationPayload s3;
    private final List<StorageEventOffset> offsets;
    private final Long gzipSize;
    private final Long size;
    private final Long records;
    private final List<StreamCorrelation> correlations = Collections.emptyList();

    public S3Payload(String event, String start, String end, S3LocationPayload s3,
                     List<StorageEventOffset> offsets, Long gzipSize, Long size, Long records) {
        this.event = event;
        this.start = start;
        this.end = end;
        this.s3 = s3;
        this.offsets = offsets;
        this.gzipSize = gzipSize;
        this.size = size;
        this.records = records;
    }

    @Override
    public String getEvent() {
        return event;
    }

    @Override
    public String getStart() {
        return start;
    }

    @Override
    public String getEnd() {
        return end;
    }

    public S3LocationPayload getS3() {
        return s3;
    }

    @Override
    public List<StorageEventOffset> getOffsets() {
        return offsets;
    }

    @Override
    public Long getGzipSize() {
        return gzipSize;
    }

    @Override
    public Long getSize() {
        return size;
    }

    @Override
    public Long getRecords() {
        return records;
    }

    @Override
    public List<StreamCorrelation> getCorrelations() {
        return correlations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        S3Payload s3Payload = (S3Payload) o;
        return Objects.equals(event, s3Payload.event) &&
                Objects.equals(start, s3Payload.start) &&
                Objects.equals(end, s3Payload.end) &&
                Objects.equals(s3, s3Payload.s3) &&
                Objects.equals(offsets, s3Payload.offsets) &&
                Objects.equals(gzipSize, s3Payload.gzipSize) &&
                Objects.equals(size, s3Payload.size) &&
                Objects.equals(records, s3Payload.records);
    }

    @Override
    public int hashCode() {
        return Objects.hash(event, start, end, s3, offsets, gzipSize, size, records);
    }

    @Override
    public String toString() {
        return String
                .format("S3Payload{event='%s', start=%d, end=%d, s3=%s, offsets=%s, gzipSize=%d, size=%d, records=%d}",
                        event, start, end, s3, offsets, gzipSize, size, records);
    }
}
