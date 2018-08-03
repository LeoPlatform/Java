package com.leo.sdk.aws;

import com.leo.sdk.PlatformStream;
import com.leo.sdk.StreamStats;
import com.leo.sdk.payload.SimplePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;

public class AWSStream implements PlatformStream {
    private static final Logger log = LoggerFactory.getLogger(AWSStream.class);
    private final AsyncUpload asyncUploader;

    @Inject
    public AWSStream(AsyncUpload asyncUploader) {
        this.asyncUploader = asyncUploader;
    }

    @Override
    public void write(SimplePayload payload) {
        asyncUploader.upload(payload);
        log.debug("Registered {} for write", payload.entity().toString());
    }

    @Override
    public CompletableFuture<StreamStats> end() {
        log.info("Completed");
        return CompletableFuture.supplyAsync(asyncUploader::end);
    }
}
