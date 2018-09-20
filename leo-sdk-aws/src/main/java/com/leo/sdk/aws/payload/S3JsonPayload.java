package com.leo.sdk.aws.payload;

import com.leo.sdk.aws.s3.S3Payload;
import com.leo.sdk.payload.StreamJsonPayload;

public interface S3JsonPayload extends StreamJsonPayload {
    String toJsonString(S3Payload s3Payload);
}
