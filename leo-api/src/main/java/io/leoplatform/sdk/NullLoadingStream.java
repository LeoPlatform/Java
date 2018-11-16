package io.leoplatform.sdk;

import io.leoplatform.sdk.payload.EventPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

@Singleton
public class NullLoadingStream implements LoadingStream {
    private static final Logger log = LoggerFactory.getLogger(NullLoadingStream.class);

    @Inject
    public NullLoadingStream() {
        log.info("No events will be written to the LEO bus");
    }

    @Override
    public void load(EventPayload payload) {
    }

    @Override
    public void load(Stream<EventPayload> payload) {
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
}
