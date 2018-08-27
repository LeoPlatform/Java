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
import com.leo.sdk.PayloadIdentifier;
import com.leo.sdk.config.ConnectorConfig;

import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class S3TransferManager {
    private final String name;
    private final ScheduledExecutorService asyncTransfer;
    private final S3Batch batchMaximums;
    private final TransferManager s3TransferManager;
    private final LinkedList<S3Batch> batches = new LinkedList<>();
    private final S3ProgressListener listener = new XferListener();

    @Inject
    public S3TransferManager(ConnectorConfig config) {
        this.name = config.value("Storage.Name");
        this.asyncTransfer = Executors.newScheduledThreadPool(config.longValueOrElse("Storage.MaxConcurrentUploads", 64L).intValue());
        Duration maxAge = Duration.ofMillis(config.longValueOrElse("Storage.MaxAge", 400L));
        Long maxBatchRecords = config.longValueOrElse("Storage.MaxBatchRecords", 4000L);
        Long maxBatchSize = config.longValueOrElse("Storage.MaxBatchSize", 3_900_000L);
        Long maxRecordSize = config.longValueOrElse("Storage.MaxRecordSize", 4_900_000L);
        Long maxUploadAttempts = config.longValueOrElse("Storage.MaxUploadAttempts", 10L);
        this.batchMaximums = new S3Batch("Batch Maximums", maxAge, maxBatchRecords,
                maxBatchSize, maxRecordSize, maxUploadAttempts);
        String awsProfile = config.valueOrElse("AwsProfile", "");
        this.s3TransferManager = TransferManagerBuilder.standard()
                .withExecutorFactory(() -> asyncTransfer)
                .withS3Client(client(awsProfile))
                .withDisableParallelDownloads(false)
                .build();
    }

    public Upload transfer(PayloadIdentifier payload) {
        updateCurrentBatch(payload.getPayload());
        ObjectMetadata meta = new ObjectMetadata();
        PutObjectRequest req = new PutObjectRequest(name, payload.getId(), toStream(payload.getPayload()), meta);
        return s3TransferManager.upload(req, listener);
    }

    private void updateCurrentBatch(ByteBuffer payload) {
        S3Batch currentBatch = batches.getLast();
        currentBatch.addBatchRecords(1);
        currentBatch.addBatchSize(payload.array().length);
//        currentBatch.addRecordSize()
    }

    private ByteArrayInputStream toStream(ByteBuffer bb) {
        return new ByteArrayInputStream(bb.array());
    }

    public void end() {
        //shutdown nicely...

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

    private static final class XferListener implements S3ProgressListener {

        @Override
        public void onPersistableTransfer(PersistableTransfer persistableTransfer) {

        }

        @Override
        public void progressChanged(ProgressEvent progressEvent) {

        }
    }

}
