package com.leo.sdk.aws.payload;

import com.leo.sdk.payload.EventPayload;

import java.nio.ByteBuffer;
import java.util.Collection;

public interface CompressionWriter {
    ByteBuffer compress(Collection<EventPayload> payload);
}
