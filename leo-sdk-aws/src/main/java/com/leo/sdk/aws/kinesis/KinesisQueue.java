package com.leo.sdk.aws.kinesis;

import com.leo.sdk.StreamStats;
import com.leo.sdk.Uploader;
import com.leo.sdk.WriteQueue;
import com.leo.sdk.aws.payload.PayloadIdentifier;
import com.leo.sdk.payload.SimplePayload;
import com.leo.sdk.payload.StreamCorrelation;
import com.leo.sdk.payload.StreamPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.leo.sdk.Uploader.STREAM;
import static java.util.concurrent.TimeUnit.MINUTES;

public class KinesisQueue implements WriteQueue {

    private static final Logger log = LoggerFactory.getLogger(KinesisQueue.class);
    private final ExecutorService asyncCompress = Executors.newWorkStealingPool();
    private final KinesisCompression compression;
    private final KinesisWrite kinesisWriter;

    @Inject
    public KinesisQueue(KinesisCompression compression, KinesisWrite kinesisWriter) {
        this.compression = compression;
        this.kinesisWriter = kinesisWriter;
    }

    @Override
    public void addEntity(SimplePayload entity) {
        StreamPayload wrapper = new StreamPayload(entity, new StreamCorrelation("", 0L, 0L, 0L), "");
        addEntity(wrapper);
    }

    @Override
    public void addEntity(StreamPayload entity) {
        CompletableFuture
                .supplyAsync(() -> compression.compress(entity), asyncCompress)
                .thenApply(byteBuffer -> new PayloadIdentifier(entity.getId(), byteBuffer))
                .thenAccept(kinesisWriter::write);
    }

    @Override
    public StreamStats end() {
        asyncCompress.shutdown();
        try {
            if (!asyncCompress.awaitTermination(1L, MINUTES)) {
                asyncCompress.shutdownNow();
            }
            return kinesisWriter.end();
        } catch (InterruptedException e) {
            log.warn("Could not shutdown async compression pool");
            throw new IllegalStateException("Unable to shutdown Kinesis writers", e);
        }
    }

    @Override
    public Uploader type() {
        return STREAM;
    }
}
