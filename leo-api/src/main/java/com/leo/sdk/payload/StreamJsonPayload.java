package com.leo.sdk.payload;

public interface StreamJsonPayload {
    String toJsonString(EventPayload eventPayload);
}
