package com.leo.sdk;

import com.leo.sdk.payload.EntityPayload;

import java.nio.ByteBuffer;

public final class PayloadIdentifier {
    private final String id;
    private final String event;
    private final Long timestamp;
    private final ByteBuffer payload;

    public PayloadIdentifier(String id, String event, Long timestamp, ByteBuffer payload) {
        this.id = id;
        this.event = event;
        this.timestamp = timestamp;
        this.payload = payload;
    }

    public PayloadIdentifier(EntityPayload entity, ByteBuffer payload) {
        this.id = entity.getId();
        this.event = entity.getEvent();
        this.timestamp = entity.getTimestamp();
        this.payload = payload;
    }

    public String getId() {
        return id;
    }

    public String getEvent() {
        return event;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public ByteBuffer getPayload() {
        return payload;
    }
}
