package com.leo.sdk.aws.kinesis;

import com.leo.sdk.payload.EntityPayload;

import java.nio.ByteBuffer;

public interface KinesisCompression {
    ByteBuffer compress(EntityPayload payload);
}
