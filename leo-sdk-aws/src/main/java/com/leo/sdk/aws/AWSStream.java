package com.leo.sdk.aws;

import com.leo.sdk.AsyncWorkQueue;
import com.leo.sdk.PlatformStream;
import com.leo.sdk.StreamStats;
import com.leo.sdk.payload.EntityPayload;
import com.leo.sdk.payload.SimplePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class AWSStream implements PlatformStream {
    private static final Logger log = LoggerFactory.getLogger(AWSStream.class);
    private final AsyncWorkQueue transferProxy;

    @Inject
    public AWSStream(AsyncWorkQueue transferProxy) {
        this.transferProxy = transferProxy;
    }

    @Override
    public void transfer(SimplePayload payload) {
        transferProxy.addEntity(payload);
        log.debug("Registered {} for transfer", payload.entity().toString());
    }

    @Override
    public Stream<EntityPayload> process() {
        return Stream.empty();
    }

    @Override
    public CompletableFuture<StreamStats> end() {
        log.info("Completed");
        return CompletableFuture.supplyAsync(transferProxy::end);
    }
}
