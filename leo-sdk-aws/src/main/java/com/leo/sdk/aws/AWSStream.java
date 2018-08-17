package com.leo.sdk.aws;

import com.leo.sdk.AsyncWorkQueue;
import com.leo.sdk.PlatformStream;
import com.leo.sdk.StreamStats;
import com.leo.sdk.bus.LoadingBot;
import com.leo.sdk.payload.EntityPayload;
import com.leo.sdk.payload.SimplePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class AWSStream implements PlatformStream {
    private static final Logger log = LoggerFactory.getLogger(AWSStream.class);
    private final AsyncWorkQueue transferProxy;
    private final LoadingBot bot;

    public AWSStream(AsyncWorkQueue transferProxy, LoadingBot bot) {
        this.transferProxy = transferProxy;
        this.bot = bot;
    }

    @Override
    public void write(SimplePayload payload) {
        transferProxy.addEntity(new EntityPayload(payload, bot));
    }

    @Override
    public Stream<EntityPayload> process() {
        return Stream.empty();
    }

    @Override
    public CompletableFuture<StreamStats> end() {
        log.info("Stopping platform stream");
        return CompletableFuture.supplyAsync(transferProxy::end);
    }
}
