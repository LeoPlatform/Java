package io.leoplatform.sdk.aws.s3;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import io.leoplatform.sdk.payload.FileSegment;
import io.leoplatform.sdk.payload.StorageEventOffset;
import io.leoplatform.sdk.payload.StorageStats;
import io.leoplatform.sdk.payload.StorageUnits;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.stream.Collectors.toList;

public final class PendingMemoryUpload implements PendingS3Upload {

    private final String fileName;
    private final Queue<FileSegment> segments;

    PendingMemoryUpload(String fileName, Queue<FileSegment> segments) {
        this.fileName = fileName;
        this.segments = segments;
    }

    @Override
    public String filename() {
        return fileName;
    }

    @Override
    public PutObjectRequest s3PutRequest(String name) {
        byte[] file = concat(segments);

        ObjectMetadata meta = new ObjectMetadata();
        byte[] resultByte = DigestUtils.md5(file);
        String streamMD5 = new String(Base64.encodeBase64(resultByte));
        meta.setContentMD5(streamMD5);
        meta.setContentLength(file.length);

        return new PutObjectRequest(name, fileName, new ByteArrayInputStream(file), meta);
    }

    @Override
    public S3Payload s3Payload(UploadResult result, String botName) {
        String queue = getEvent(segments);
        Long gzipSize = segments.stream().map(FileSegment::getOffset).mapToLong(StorageEventOffset::getGzipSize).sum();
        Long size = segments.stream().map(FileSegment::getOffset).mapToLong(StorageEventOffset::getSize).sum();
        Long records = segments.stream().map(FileSegment::getOffset).mapToLong(StorageEventOffset::getRecords).sum();
        S3LocationPayload location = new S3LocationPayload(result.getBucketName(), result.getKey());
        List<StorageEventOffset> offsets = accumulateOffsets(segments);
        StorageStats stats = new StorageStats(Collections.singletonMap(botName, new StorageUnits(records)));
        return new S3Payload(queue, null, null, location, offsets, gzipSize, size, records, stats);
    }

    private String getEvent(Queue<FileSegment> segments) {
        return Optional.of(segments)
                .map(Queue::peek)
                .map(FileSegment::getOffset)
                .map(StorageEventOffset::getEvent)
                .orElseThrow(() -> new IllegalArgumentException("Missing storage event"));
    }

    private List<StorageEventOffset> accumulateOffsets(Queue<FileSegment> segments) {
        AtomicLong startAccumulator = new AtomicLong();
        AtomicLong offsetAccumulator = new AtomicLong();
        AtomicLong gzipOffsetAccumulator = new AtomicLong();
        return segments.stream()
                .map(FileSegment::getOffset)
                .map(o -> {
                    Long offset = offsetAccumulator.getAndAdd(o.getSize());
                    Long gzipOffset = gzipOffsetAccumulator.getAndAdd(o.getGzipSize());
                    Long start = startAccumulator.getAndAdd(o.getRecords());
                    Long end = start + o.getRecords() - 1;
                    return new StorageEventOffset(o.getEvent(), start, end, o.getSize(), offset, o.getRecords(), o.getGzipSize(), gzipOffset);
                })
                .collect(toList());
    }

    private byte[] concat(Queue<FileSegment> segments) {
        byte[] all = new byte[segments.stream()
                .map(FileSegment::getSegment)
                .mapToInt(b -> b.length)
                .sum()];
        AtomicInteger pos = new AtomicInteger(0);
        segments.stream()
                .map(FileSegment::getSegment)
                .forEachOrdered(b1 -> System.arraycopy(b1, 0, all, pos.getAndAdd(b1.length), b1.length));
        return all;
    }
}
