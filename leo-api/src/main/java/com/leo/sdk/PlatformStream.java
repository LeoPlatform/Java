package com.leo.sdk;

import com.leo.sdk.payload.EntityPayload;
import com.leo.sdk.payload.SimplePayload;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public interface PlatformStream {
    void write(SimplePayload payload);

    Stream<EntityPayload> process();

    CompletableFuture<StreamStats> end();
}
