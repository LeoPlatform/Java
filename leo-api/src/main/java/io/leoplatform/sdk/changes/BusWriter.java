package io.leoplatform.sdk.changes;

import io.leoplatform.sdk.LoadingStream;
import io.leoplatform.sdk.payload.EventPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.json.JsonObject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Singleton
public final class BusWriter implements PayloadWriter {
    private static final Logger log = LoggerFactory.getLogger(BusWriter.class);

    private final LoadingStream stream;

    @Inject
    public BusWriter(LoadingStream stream) {
        this.stream = stream;
    }

    @Override
    public void write(List<JsonObject> payloads) {
        log.info("Writing {} payloads to bus", payloads.size());
        payloads.stream()
            .map(this::toPayload)
            .forEach(stream::load);
    }

    @Override
    public void end() {
        Instant begin = Instant.now();
        stream.end().join();
        log.info("Stopped writer in {}ms", Duration.between(begin, Instant.now()).toMillis());
    }

    private EventPayload toPayload(JsonObject jsonObject) {
        return () -> jsonObject;
    }

}
