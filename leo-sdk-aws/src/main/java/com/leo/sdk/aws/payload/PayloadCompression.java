package com.leo.sdk.aws.payload;

import com.leo.sdk.payload.EntityPayload;

import java.nio.ByteBuffer;

public interface PayloadCompression {
    ByteBuffer compress(EntityPayload payload);
}
