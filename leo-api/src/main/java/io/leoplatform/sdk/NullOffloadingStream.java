package io.leoplatform.sdk;

import io.leoplatform.sdk.payload.EntityPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

@Singleton
public final class NullOffloadingStream implements OffloadingStream {
    private static final Logger log = LoggerFactory.getLogger(NullOffloadingStream.class);

    @Inject
    public NullOffloadingStream() {
        log.info("No events will be read from the LEO bus");
    }

    @Override
    public CompletableFuture<StreamStats> end() {
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
                return Duration.ZERO;
            }
        });
    }

    @Override
    public Stream<EntityPayload> offload() {
        return Stream.empty();
    }
}
