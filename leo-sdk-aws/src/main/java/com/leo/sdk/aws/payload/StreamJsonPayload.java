package com.leo.sdk.aws.payload;

import com.leo.sdk.aws.s3.S3Payload;
import com.leo.sdk.payload.EntityPayload;
import com.leo.sdk.payload.EventPayload;

public interface StreamJsonPayload {
    String toJsonString(EventPayload entityPayload);

    String toJsonString(EntityPayload entityPayload);

    String toJsonString(S3Payload s3Payload);

    EntityPayload toEntity(EventPayload eventPayload);
}
