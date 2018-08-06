package com.leo.sdk.payload;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import javax.json.JsonObject;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static java.time.ZoneOffset.UTC;

@JsonPropertyOrder({"event", "id", "eid", "payload", "correlation_id", "event_source_timestamp", "timestamp"})
public class EntityPayload {
    private static final String fmt = "'z/'uuuu'/'MM'/'dd'/'HH'/'mm'/'ss'/%d-0000000'";
    private static final DateTimeFormatter eidFormat = DateTimeFormatter.ofPattern(fmt).withZone(UTC);
    private final JsonObject payload;
    private final StreamCorrelation correlation_id;
    private final String eid;
    private final String id;
    private final String event;
    private final Long event_source_timestamp;
    private final Long timestamp;

    public EntityPayload(SimplePayload simplePayload, StreamCorrelation streamCorrelation, String event) {
        this.payload = simplePayload.entity();
        this.correlation_id = streamCorrelation;
        Instant now = Instant.now();
        this.eid = String.format(eidFormat.format(now), now.toEpochMilli());
        this.id = simplePayload.id();
        this.event = event;
        this.event_source_timestamp = Optional.of(simplePayload)
                .map(SimplePayload::eventTime)
                .orElse(now)
                .toEpochMilli();
        this.timestamp = now.toEpochMilli();
    }

    public String getEvent() {
        return event;
    }

    public String getId() {
        return id;
    }

    public String getEid() {
        return eid;
    }

    public JsonObject getPayload() {
        return payload;
    }

    public StreamCorrelation getCorrelation_id() {
        return correlation_id;
    }

    public Long getEvent_source_timestamp() {
        return event_source_timestamp;
    }

    public Long getTimestamp() {
        return timestamp;
    }
}
