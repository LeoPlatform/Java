package com.leo.sdk.aws.s3;

import com.leo.sdk.*;
import com.leo.sdk.aws.kinesis.KinesisCompression;
import com.leo.sdk.aws.kinesis.KinesisProducerWriter;
import com.leo.sdk.payload.EntityPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.leo.sdk.TransferStyle.STORAGE;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class S3Queue implements AsyncWorkQueue {
    private static final Logger log = LoggerFactory.getLogger(S3Queue.class);

    private final TransferStyle style = STORAGE;
    private final ExecutorService asyncCompress = Executors.newWorkStealingPool();
    private final KinesisCompression compression;
    private final AsyncPayloadWriter kinesisWriter;

    @Inject
    public S3Queue(KinesisCompression compression, KinesisProducerWriter kinesisWriter) {
        this.compression = compression;
        this.kinesisWriter = kinesisWriter;
    }

    @Override
    public void addEntity(EntityPayload entity) {
        CompletableFuture
                .supplyAsync(() -> compression.compress(entity), asyncCompress)
                .thenApply(byteBuffer -> new PayloadIdentifier(entity.getId(), byteBuffer))
                .thenAccept(kinesisWriter::write);
    }

    @Override
    public StreamStats end() {
        asyncCompress.shutdown();
        try {
            if (!asyncCompress.awaitTermination(30L, SECONDS)) {
                asyncCompress.shutdownNow();
            }
            return kinesisWriter.end();
        } catch (InterruptedException e) {
            log.warn("Could not shutdown async compression pool");
            throw new IllegalStateException("Unable to shutdown Kinesis writers", e);
        }
    }

    @Override
    public TransferStyle style() {
        return style;
    }
}
