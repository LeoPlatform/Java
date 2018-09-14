package com.leo.sdk.aws.s3;

public class S3LocationPayload {
    private final String bucket;
    private final String key;

    public S3LocationPayload(String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
    }

    public String getBucket() {
        return bucket;
    }

    public String getKey() {
        return key;
    }
}
