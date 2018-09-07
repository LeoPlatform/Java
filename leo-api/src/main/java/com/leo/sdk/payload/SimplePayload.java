package com.leo.sdk.payload;

import javax.json.JsonObject;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public class SimplePayload implements EventPayload {

    private final String id;
    private final Instant eventTime;
    private final JsonObject payload;

    public SimplePayload(JsonObject payload) {
        this(Instant.now(), payload);
    }

    public SimplePayload(String id, JsonObject payload) {
        this(id, Instant.now(), payload);
    }

    public SimplePayload(Instant eventTime, JsonObject payload) {
        this(UUID.randomUUID().toString(), eventTime, payload);
    }

    public SimplePayload(String id, Instant eventTime, JsonObject payload) {
        this.id = id;
        this.eventTime = eventTime;
        this.payload = payload;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public Instant eventTime() {
        return eventTime;
    }

    @Override
    public JsonObject payload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SimplePayload that = (SimplePayload) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return String.format("SimplePayload{id='%s', eventTime=%s, payload=%s}", id, eventTime, payload);
    }
}
