package com.leo.sdk.aws.payload;

import com.leo.sdk.payload.EventPayload;

import java.util.Collection;

public interface CompressionWriter {
    void compress(Collection<EventPayload> payload);
}
