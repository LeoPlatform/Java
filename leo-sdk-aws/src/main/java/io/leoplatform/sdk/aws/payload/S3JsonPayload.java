package io.leoplatform.sdk.aws.payload;

import io.leoplatform.sdk.aws.s3.S3Payload;
import io.leoplatform.sdk.payload.StreamJsonPayload;

public interface S3JsonPayload extends StreamJsonPayload {
    String toJsonString(S3Payload s3Payload);
}
