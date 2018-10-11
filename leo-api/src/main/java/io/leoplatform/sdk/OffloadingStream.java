package io.leoplatform.sdk;

import io.leoplatform.sdk.payload.EntityPayload;

import java.util.stream.Stream;

public interface OffloadingStream extends PlatformStream {

    Stream<EntityPayload> offload();

}
