package com.leo.sdk.aws.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import com.leo.sdk.config.ConnectorConfig;
import com.leo.sdk.payload.FileSegment;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.amazonaws.event.ProgressEventType.TRANSFER_COMPLETED_EVENT;
import static com.amazonaws.services.s3.transfer.Transfer.TransferState.Completed;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

public class S3TransferManager {
    private static final Logger log = LoggerFactory.getLogger(S3TransferManager.class);

    private final String name;
    private final TransferManager s3TransferManager;

    private final Queue<Upload> pendingUploads = new LinkedList<>();
    private final Lock lock = new ReentrantLock();
    private final Condition uploadProgress = lock.newCondition();
    private final S3ProgressListener listener = new XferListener();

    @Inject
    public S3TransferManager(ConnectorConfig config) {
        this.name = config.value("Storage.Name");
        this.s3TransferManager = TransferManagerBuilder.standard()
                .withS3Client(client(config.valueOrElse("AwsProfile", "")))
                .withDisableParallelDownloads(false)
                .build();
    }

    Upload transfer(String fileName, Queue<FileSegment> segments) {
        byte[] file = concat(segments);

        ObjectMetadata meta = new ObjectMetadata();
        byte[] resultByte = DigestUtils.md5(file);
        String streamMD5 = new String(Base64.encodeBase64(resultByte));
        meta.setContentMD5(streamMD5);
        meta.setContentLength(file.length);

        PutObjectRequest req = new PutObjectRequest(name, fileName, new ByteArrayInputStream(file), meta);
        Upload upload = s3TransferManager.upload(req, listener);
        lock.lock();
        try {
            pendingUploads.add(upload);
        } finally {
            lock.unlock();
        }
        return upload;
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

    void end() {
        while (!pendingUploads.isEmpty()) {
            lock.lock();
            try {
                uploadProgress.await(100, MILLISECONDS);
            } catch (InterruptedException e) {
                log.warn("S3 transfer manager unexpectedly stopped");
                pendingUploads.clear();
            } finally {
                lock.unlock();
            }
            removeCompleted();
        }
        s3TransferManager.shutdownNow();
    }

    private AmazonS3 client(String awsProfile) {
        return AmazonS3ClientBuilder
                .standard()
                .withCredentials(credentials(awsProfile))
                .build();
    }

    private AWSCredentialsProvider credentials(String awsProfile) {
        return Optional.of(awsProfile)
                .map(String::trim)
                .filter(profile -> !profile.isEmpty())
                .map(ProfileCredentialsProvider::new)
                .map(AWSCredentialsProvider.class::cast)
                .orElse(DefaultAWSCredentialsProviderChain.getInstance());
    }

    private final class XferListener implements S3ProgressListener {

        @Override
        public void onPersistableTransfer(PersistableTransfer persistableTransfer) {
            log.info("Perishable xfer");
        }

        @Override
        public void progressChanged(ProgressEvent progressEvent) {
            if (progressEvent.getEventType() == TRANSFER_COMPLETED_EVENT) {
                removeCompleted();
                lock.lock();
                try {
                    uploadProgress.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    private void removeCompleted() {
        lock.lock();
        try {
            List<Upload> completed = pendingUploads.stream()
                    .filter(u -> u.getState() == Completed)
                    .collect(toList());
            completed.forEach(c -> log.info("{} complete", c.getDescription()));
            pendingUploads.removeAll(completed);
        } finally {
            lock.unlock();
        }
    }
}
