package io.leoplatform.sdk.aws.payload;

import io.leoplatform.sdk.bus.Bots;
import io.leoplatform.sdk.bus.LoadingBot;
import io.leoplatform.sdk.payload.EntityPayload;
import io.leoplatform.sdk.payload.SimplePayload;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.StringReader;
import java.time.Instant;

public class JacksonPayloadTest {

    private final LoadingBot bot = Bots.ofLoading("loading-bot-name", "queue-name");
    private final JacksonPayload jacksonPayload = new JacksonPayload(bot);

    @Test
    public void testJsonEventTime() {
        Instant now = Instant.now();
        String eventPayloadJson = jacksonPayload.toJsonString(new SimplePayload(now, payload()));
        long timestamp = Json.createReader(new StringReader(eventPayloadJson))
                .readObject()
                .getJsonNumber("event_source_timestamp")
                .longValueExact();
        Assert.assertEquals(timestamp, now.toEpochMilli(), "Invalid JSON event time");
    }

    @Test
    public void testJsonTime() {
        EntityPayload entityPayload = new EntityPayload(new SimplePayload(payload()), bot);
        String eventPayloadJson = jacksonPayload.toJsonString(entityPayload);
        long timestamp = Json.createReader(new StringReader(eventPayloadJson))
                .readObject()
                .getJsonNumber("timestamp")
                .longValueExact();
        Assert.assertEquals(timestamp, entityPayload.getEvent_source_timestamp().longValue(), "Invalid JSON time");
    }

    @Test
    public void testEntity() {
        String eventPayloadJson = jacksonPayload.toJsonString(new SimplePayload(payload()));
        int abcVal = Json.createReader(new StringReader(eventPayloadJson))
                .readObject()
                .getJsonObject("payload")
                .getJsonNumber("abc")
                .intValue();
        Assert.assertEquals(abcVal, 123, "Invalid payload value");
    }

    @Test
    public void testEventId() {
        String eventPayloadJson = jacksonPayload.toJsonString(new SimplePayload(payload()));
        String jsonId = Json.createReader(new StringReader(eventPayloadJson))
                .readObject()
                .getJsonString("id")
                .getString();
        Assert.assertEquals(jsonId, bot.name(), "Invalid JSON id");
    }

    @Test
    public void testEvent() {
        EntityPayload entityPayload = new EntityPayload(new SimplePayload(payload()), bot);
        String eventPayloadJson = jacksonPayload.toJsonString(entityPayload);
        String event = Json.createReader(new StringReader(eventPayloadJson))
                .readObject()
                .getJsonString("event")
                .getString();
        Assert.assertEquals(event, bot.destination().name(), "Invalid entity event");
    }

    private JsonObject payload() {
        return Json.createObjectBuilder()
                .add("abc", 123)
                .build();
    }
}