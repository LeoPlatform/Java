package com.leo.sdk.aws.payload;

import com.leo.sdk.payload.EventPayload;
import com.leo.sdk.payload.FileSegment;

import java.util.Collection;

public interface CompressionWriter {
    FileSegment compressWithOffsets(Collection<EventPayload> payload);
}
