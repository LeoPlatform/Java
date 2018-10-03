package com.leo.sdk;

import java.util.concurrent.CompletableFuture;

public interface PlatformStream {

    CompletableFuture<StreamStats> end();

}
