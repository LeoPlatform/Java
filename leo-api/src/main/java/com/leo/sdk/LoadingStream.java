package com.leo.sdk;

import com.leo.sdk.payload.EventPayload;

import java.util.stream.Stream;

public interface LoadingStream extends PlatformStream {

    void load(EventPayload payload);

    void load(Stream<EventPayload> payload);
}
