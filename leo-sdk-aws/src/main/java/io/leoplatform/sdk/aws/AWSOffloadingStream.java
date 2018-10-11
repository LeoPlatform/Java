package io.leoplatform.sdk.aws;

import io.leoplatform.sdk.ExecutorManager;
import io.leoplatform.sdk.OffloadingStream;
import io.leoplatform.sdk.StreamStats;
import io.leoplatform.sdk.payload.EntityPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

@Singleton
public final class AWSOffloadingStream implements OffloadingStream {
    private static final Logger log = LoggerFactory.getLogger(AWSOffloadingStream.class);

    private final ExecutorManager executorManager;
    private final AtomicBoolean streaming;

    @Inject
    public AWSOffloadingStream(ExecutorManager executorManager) {
        this.executorManager = executorManager;
        this.streaming = new AtomicBoolean(true);
    }

    @Override
    public Stream<EntityPayload> offload() {
        return Stream.empty();
    }

    @Override
    public CompletableFuture<StreamStats> end() {
        if (streaming.getAndSet(false)) {
            log.info("Stopping offload stream");
            return null;
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
                return Duration.between(Instant.now(), Instant.now());
            }
        });
    }
}
