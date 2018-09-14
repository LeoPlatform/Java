package com.leo.sdk.payload;

public interface StreamJsonPayload {
    String toJsonString(EventPayload entityPayload);

    String toJsonString(EntityPayload entityPayload);

    EntityPayload toEntity(EventPayload eventPayload);
}
