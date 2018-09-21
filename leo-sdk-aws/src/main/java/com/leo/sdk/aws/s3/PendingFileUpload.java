package com.leo.sdk.aws.s3;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.leo.sdk.payload.FileSegment;
import com.leo.sdk.payload.StorageEventOffset;
import com.leo.sdk.payload.StorageStats;
import com.leo.sdk.payload.StorageUnits;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

class PendingFileUpload implements PendingS3Upload {
    private final String fileName;
    private final Path cachedFile;
    private final Queue<StorageEventOffset> storageEventOffsets;

    PendingFileUpload(String fileName, Queue<FileSegment> segments) {
        this.fileName = fileName;
        this.storageEventOffsets = segments.stream()
                .map(FileSegment::getOffset)
                .collect(toCollection(LinkedList::new));
        this.cachedFile = toTempFile(segments);
    }

    @Override
    public String filename() {
        return fileName;
    }

    @Override
    public PutObjectRequest s3PutRequest(String name) {
        try {
            ObjectMetadata meta = new ObjectMetadata();
            long fileSize = Files.size(cachedFile);
            meta.setContentLength(fileSize);

            try (InputStream is = new BufferedInputStream(Files.newInputStream(cachedFile), 512)) {
                byte[] resultByte = DigestUtils.md5(is);
                String streamMD5 = new String(Base64.encodeBase64(resultByte));
                meta.setContentMD5(streamMD5);
            }
            InputStream is = new BufferedInputStream(Files.newInputStream(cachedFile, DELETE_ON_CLOSE), 512);
            return new PutObjectRequest(name, fileName, is, meta);
        } catch (IOException e) {
            throw new IllegalStateException("Could not read S3 temporary file", e);
        }
    }

    @Override
    public S3Payload s3Payload(UploadResult result, String botName) {
        String queue = getEvent();
        Long gzipSize = storageEventOffsets.stream().mapToLong(StorageEventOffset::getGzipSize).sum();
        Long size = storageEventOffsets.stream().mapToLong(StorageEventOffset::getSize).sum();
        Long records = storageEventOffsets.stream().mapToLong(StorageEventOffset::getRecords).sum();
        S3LocationPayload location = new S3LocationPayload(result.getBucketName(), result.getKey());
        List<StorageEventOffset> offsets = accumulateOffsets();
        StorageStats stats = new StorageStats(Collections.singletonMap(botName, new StorageUnits(records)));
        return new S3Payload(queue, null, null, location, offsets, gzipSize, size, records, stats);
    }

    private String getEvent() {
        return Optional.of(storageEventOffsets)
                .map(Queue::peek)
                .map(StorageEventOffset::getEvent)
                .orElseThrow(() -> new IllegalArgumentException("Missing storage event"));
    }

    private List<StorageEventOffset> accumulateOffsets() {
        AtomicLong startAccumulator = new AtomicLong();
        AtomicLong offsetAccumulator = new AtomicLong();
        AtomicLong gzipOffsetAccumulator = new AtomicLong();
        return storageEventOffsets.stream()
                .map(o -> {
                    Long start = startAccumulator.getAndAdd(o.getRecords());
                    Long end = start + o.getRecords() - 1;
                    Long offset = offsetAccumulator.getAndAdd(o.getSize());
                    Long gzipOffset = gzipOffsetAccumulator.getAndAdd(o.getGzipSize());
                    return new StorageEventOffset(o.getEvent(), start, end, o.getSize(), offset, o.getRecords(), o.getGzipSize(), gzipOffset);
                })
                .collect(toList());
    }

    private Path toTempFile(Queue<FileSegment> segments) {
        try {
            Path tempFile = Files.createTempFile("java-sdk-", ".gz");
            try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(tempFile), 512)) {
                FileSegment fs;
                while ((fs = segments.poll()) != null) {
                    os.write(fs.getSegment());
                }
                os.flush();
            }
            return tempFile;
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create temporary file");
        }
    }
}
