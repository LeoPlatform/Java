package com.leo.sdk.aws;

import com.leo.sdk.AsyncWorkQueue;
import com.leo.sdk.ExecutorManager;
import com.leo.sdk.LoadingStream;
import com.leo.sdk.StreamStats;
import com.leo.sdk.payload.EventPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

@Singleton
public final class AWSLoadingStream implements LoadingStream {
    private static final Logger log = LoggerFactory.getLogger(AWSLoadingStream.class);

    private final AsyncWorkQueue transferProxy;
    private final ExecutorManager executorManager;
    private final AtomicBoolean streaming;

    @Inject
    public AWSLoadingStream(@Named("Proxy") AsyncWorkQueue transferProxy, ExecutorManager executorManager) {
        this.transferProxy = transferProxy;
        this.executorManager = executorManager;
        this.streaming = new AtomicBoolean(true);
    }

    @Override
    public void load(EventPayload payload) {
        if (streaming.get()) {
            EventPayload sanitized = Optional.ofNullable(payload)
                    .filter(p -> Objects.nonNull(p.payload()))
                    .orElseThrow(() -> new IllegalArgumentException("Invalid payload: " + payload));
            transferProxy.addEntity(sanitized);
        } else {
            log.warn("Attempt to load payload on a closed stream");
        }
    }

    @Override
    public void load(Stream<EventPayload> payload) {
        payload.forEachOrdered(this::load);
    }

    @Override
    public CompletableFuture<StreamStats> end() {
        if (streaming.getAndSet(false)) {
            log.info("Stopping platform stream");
            return CompletableFuture.supplyAsync(() -> {
                StreamStats stats = transferProxy.end();
                executorManager.end();
                return stats;
            });
        } else {
            return noStats();
        }
    }

    private CompletableFuture<StreamStats> noStats() {
        return CompletableFuture.completedFuture(new StreamStats() {
            @Override
            public Long successes() {
                return 0L;
            }

            @Override
            public Long failures() {
                return 0L;
            }

            @Override
            public Duration totalTime() {
                Instant now = Instant.now();
                return Duration.between(now, now);
            }
        });
    }
}
