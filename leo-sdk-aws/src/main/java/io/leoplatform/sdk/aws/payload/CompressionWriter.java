package io.leoplatform.sdk.aws.payload;

import io.leoplatform.sdk.aws.s3.S3Payload;
import io.leoplatform.sdk.payload.EventPayload;
import io.leoplatform.sdk.payload.FileSegment;

import java.nio.ByteBuffer;
import java.util.Collection;

public interface CompressionWriter {
    FileSegment compressWithOffsets(Collection<EventPayload> payload);

    ByteBuffer compress(S3Payload payload);
}
