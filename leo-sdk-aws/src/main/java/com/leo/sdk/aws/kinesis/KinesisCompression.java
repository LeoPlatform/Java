package com.leo.sdk.aws.kinesis;

import com.leo.sdk.payload.StreamPayload;

import java.nio.ByteBuffer;

public interface KinesisCompression {
    ByteBuffer compress(StreamPayload payload);
}
