package com.leo.sdk.payload;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.leo.sdk.bus.LoadingBot;

import javax.json.JsonObject;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static java.time.ZoneOffset.UTC;

@JsonPropertyOrder({"payload", "id", "event", "event_source_timestamp", "timestamp", "eid", "correlation_id"})
@JsonInclude(NON_NULL)
public class EntityPayload {
    private static final String fmt = "'z/'uuuu'/'MM'/'dd'/'HH'/'mm'/'ss'/%d-0000000'";
    private static final DateTimeFormatter eidFormat = DateTimeFormatter
            .ofPattern(fmt)
            .withZone(UTC);

    private final JsonObject payload;
    private final String id;
    private final String event;
    private final Long event_source_timestamp;
    private final Long timestamp;
    private final String eid;
    private final StreamCorrelation correlation_id = null;

    public EntityPayload(SimplePayload simplePayload, LoadingBot bot) {
        Instant now = Instant.now();
        this.payload = simplePayload.entity();
        this.id = bot.name();
        this.event = bot.destination().name();
        this.event_source_timestamp = Optional.of(simplePayload)
                .map(SimplePayload::eventTime)
                .orElse(now)
                .toEpochMilli();
        this.timestamp = now.toEpochMilli();
        this.eid = String.format(eidFormat.format(now), now.toEpochMilli());
    }

    public JsonObject getPayload() {
        return payload;
    }

    public String getId() {
        return id;
    }

    public String getEvent() {
        return event;
    }

    public Long getEvent_source_timestamp() {
        return event_source_timestamp;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public String getEid() {
        return eid;
    }

    public StreamCorrelation getCorrelation_id() {
        return correlation_id;
    }
}
