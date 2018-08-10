package com.leo.sdk.aws.kinesis;

import com.leo.sdk.*;
import com.leo.sdk.payload.EntityPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.leo.sdk.TransferStyle.STREAM;
import static java.util.concurrent.TimeUnit.SECONDS;

public class KinesisQueue implements AsyncWorkQueue {

    private static final Logger log = LoggerFactory.getLogger(KinesisQueue.class);
    private final TransferStyle style = STREAM;
    private final ExecutorService asyncCompress = Executors.newWorkStealingPool();
    private final KinesisCompression compression;
    private final AsyncPayloadWriter kinesisWriter;

    @Inject
    public KinesisQueue(KinesisCompression compression, KinesisProducerWriter kinesisWriter) {
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
