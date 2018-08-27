package com.leo.sdk.aws.s3;

import com.leo.sdk.*;
import com.leo.sdk.aws.payload.PayloadCompression;
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

    private final ExecutorService asyncCompress = Executors.newWorkStealingPool();
    private final PayloadCompression compression;
    private final AsyncPayloadWriter s3Writer;

    @Inject
    public S3Queue(PayloadCompression compression, S3Writer s3Writer) {
        this.compression = compression;
        this.s3Writer = s3Writer;
    }

    @Override
    public void addEntity(EntityPayload entity) {
        CompletableFuture
                .supplyAsync(() -> compression.compress(entity), asyncCompress)
                .thenApply(byteBuffer -> new PayloadIdentifier(entity, byteBuffer))
                .thenAccept(s3Writer::write);
    }

    @Override
    public StreamStats end() {
        asyncCompress.shutdown();
        try {
            if (!asyncCompress.awaitTermination(30L, SECONDS)) {
                asyncCompress.shutdownNow();
            }
            return s3Writer.end();
        } catch (InterruptedException e) {
            log.warn("Could not shutdown s3 compression pool");
            throw new IllegalStateException("Unable to shutdown Kinesis writers", e);
        }
    }

    @Override
    public TransferStyle style() {
        return STORAGE;
    }
}
