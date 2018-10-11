package io.leoplatform.sdk;

import io.leoplatform.sdk.payload.EventPayload;

import java.util.stream.Stream;

public interface LoadingStream extends PlatformStream {

    void load(EventPayload payload);

    void load(Stream<EventPayload> payload);
}
