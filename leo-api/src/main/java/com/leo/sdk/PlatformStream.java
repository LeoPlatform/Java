package com.leo.sdk;

import com.leo.sdk.payload.EntityPayload;
import com.leo.sdk.payload.EventPayload;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public interface PlatformStream {
    void load(EventPayload payload);

    EventPayload enhance(EntityPayload payload);

    Stream<EntityPayload> offload();

    CompletableFuture<StreamStats> end();
}
