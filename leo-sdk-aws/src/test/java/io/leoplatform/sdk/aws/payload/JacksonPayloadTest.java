package io.leoplatform.sdk.aws.payload;

import io.leoplatform.sdk.bus.Bots;
import io.leoplatform.sdk.bus.LoadingBot;
import io.leoplatform.sdk.payload.EntityPayload;
import io.leoplatform.sdk.payload.SimplePayload;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.StringReader;
import java.time.Instant;
import java.util.Random;

public class JacksonPayloadTest {

    private final Random r = new Random();
    private final LoadingBot bot = Bots.ofLoading("loading-bot-name", "queue-name");
    private final JacksonPayload jacksonPayload = new JacksonPayload(bot);

    @Test(dataProvider = "TimeGenerator")
    public void testJsonTimestamp(Instant randomTime) {
        EntityPayload entityPayload = new EntityPayload(new SimplePayload(randomTime, payload()), bot);
        long expectedTimestamp = entityPayload.getTimestamp();
        String eventPayloadJson = jacksonPayload.toJsonString(entityPayload);
        long actualTimestamp = Json.createReader(new StringReader(eventPayloadJson))
            .readObject()
            .getJsonNumber("timestamp")
            .longValueExact();
        Assert.assertEquals(actualTimestamp, expectedTimestamp, "Invalid JSON timestamp");
    }

    @Test(dataProvider = "TimeGenerator")
    public void testEventSourceTimestamp(Instant randomTime) {
        EntityPayload entityPayload = new EntityPayload(new SimplePayload(randomTime, payload()), bot);
        long expectedSourceTime = entityPayload.getEvent_source_timestamp();
        String eventPayloadJson = jacksonPayload.toJsonString(entityPayload);
        long actualSourceTime = Json.createReader(new StringReader(eventPayloadJson))
            .readObject()
            .getJsonNumber("event_source_timestamp")
            .longValueExact();
        Assert.assertEquals(actualSourceTime, expectedSourceTime, "Invalid JSON event source time");
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

    @DataProvider(name = "TimeGenerator")
    public Object[][] time() {
        return r.longs(10, 10_000, 1_000_000)
            .map(r -> System.currentTimeMillis() + r)
            .mapToObj(Instant::ofEpochMilli)
            .map(instant -> new Object[]{instant})
            .toArray(Object[][]::new);
    }
}