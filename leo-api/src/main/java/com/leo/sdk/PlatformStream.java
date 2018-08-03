package com.leo.sdk;

import com.leo.sdk.payload.SimplePayload;

import java.util.concurrent.CompletableFuture;

public interface PlatformStream {
    void write(SimplePayload payload);

    CompletableFuture<StreamStats> end();
}
