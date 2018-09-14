package com.leo.sdk.aws.payload;

import com.leo.sdk.PayloadIdentifier;
import com.leo.sdk.payload.EventPayload;
import com.leo.sdk.payload.FileSegment;

import java.util.Collection;

public interface CompressionWriter {
    PayloadIdentifier compressWithNewlines(Collection<EventPayload> payload);

    FileSegment compressWithOffsets(Collection<EventPayload> payload);
}
