package com.leo.sdk.aws;

import com.leo.sdk.AsyncWorkQueue;
import com.leo.sdk.PlatformStream;
import com.leo.sdk.StreamStats;
import com.leo.sdk.bus.LoadingBot;
import com.leo.sdk.payload.EntityPayload;
import com.leo.sdk.payload.SimplePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.Json;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public final class AWSStream implements PlatformStream {
    private static final Logger log = LoggerFactory.getLogger(AWSStream.class);
    private final AsyncWorkQueue transferProxy;
    private final LoadingBot bot;
    private final AtomicBoolean streaming;

    public AWSStream(AsyncWorkQueue transferProxy, LoadingBot bot) {
        this.transferProxy = transferProxy;
        this.bot = bot;
        this.streaming = new AtomicBoolean(true);
    }

    @Override
    public void load(SimplePayload payload) {
        transferProxy.addEntity(new EntityPayload(payload, bot));
    }

    @Override
    public SimplePayload enhance(EntityPayload payload) {
        return () -> Json.createObjectBuilder().build();
    }

    @Override
    public Stream<EntityPayload> offload() {
        return Stream.empty();
    }

    @Override
    public CompletableFuture<StreamStats> end() {
        if (streaming.getAndSet(false)) {
            log.info("Stopping platform stream");
            return CompletableFuture.supplyAsync(transferProxy::end);
        } else {
            return noStats();
        }
    }

    private CompletableFuture<StreamStats> noStats() {
        return CompletableFuture.completedFuture(new StreamStats() {
            @Override
            public Stream<String> successIds() {
                return Stream.empty();
            }

            @Override
            public Stream<String> failedIds() {
                return Stream.empty();
            }

            @Override
            public Duration totalTime() {
                Instant now = Instant.now();
                return Duration.between(now, now);
            }
        });
    }
}
