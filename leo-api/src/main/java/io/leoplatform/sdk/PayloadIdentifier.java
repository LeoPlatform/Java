package io.leoplatform.sdk;

import io.leoplatform.sdk.payload.EntityPayload;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public final class PayloadIdentifier {
    private final String id;
    private final String event;
    private final Long timestamp;
    private final ByteBuffer payload;
    private final List<Long> offsets;

    public PayloadIdentifier(String id, String event, Long timestamp, ByteBuffer payload) {
        this.id = id;
        this.event = event;
        this.timestamp = timestamp;
        this.payload = payload;
        this.offsets = Collections.emptyList();
    }

    public PayloadIdentifier(EntityPayload entity, ByteBuffer payload) {
        this(entity, payload, Collections.emptyList());
    }

    public PayloadIdentifier(EntityPayload entity, ByteBuffer payload, List<Long> offsets) {
        this.id = entity.getId();
        this.event = entity.getEvent();
        this.timestamp = entity.getTimestamp();
        this.payload = payload;
        this.offsets = offsets;
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

    public List<Long> getOffsets() {
        return offsets;
    }
}
