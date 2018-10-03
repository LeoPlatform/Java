package com.leo.sdk;

import com.leo.sdk.payload.EntityPayload;

import java.util.stream.Stream;

public interface OffloadingStream extends PlatformStream {

    Stream<EntityPayload> offload();

}
