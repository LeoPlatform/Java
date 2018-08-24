package com.leo.sdk.aws.s3;

import com.leo.sdk.payload.EntityPayload;

import java.nio.ByteBuffer;

public interface S3Compression {
    ByteBuffer compress(EntityPayload payload);
}
