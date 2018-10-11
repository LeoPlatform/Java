package io.leoplatform.sdk;

import java.util.concurrent.CompletableFuture;

public interface PlatformStream {

    CompletableFuture<StreamStats> end();

}
