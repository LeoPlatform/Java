package com.leo.sdk.aws.payload;

import com.leo.sdk.aws.s3.S3Payload;
import com.leo.sdk.payload.EventPayload;
import com.leo.sdk.payload.FileSegment;

import java.nio.ByteBuffer;
import java.util.Collection;

public interface CompressionWriter {
    FileSegment compressWithOffsets(Collection<EventPayload> payload);

    ByteBuffer compress(S3Payload payload);
}
