package io.leoplatform.sdk.aws.payload;

import io.leoplatform.sdk.aws.DaggerAWSLoadingPlatform;
import io.leoplatform.sdk.bus.Bots;
import io.leoplatform.sdk.payload.EntityPayload;
import io.leoplatform.sdk.payload.EventPayload;
import org.testng.Assert;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.StringReader;
import java.time.Instant;

public class JacksonNewlinePayloadTest {

    private S3JsonPayload jp = DaggerAWSLoadingPlatform.builder()
            .loadingBot(Bots.ofLoading("my-bot", "my-queue"))
            .build()
            .s3JsonPayload();

    //    @Test
    public void testJsonTime() {
        Instant now = Instant.now();
        EventPayload eventPayload = eventPayload(now);
        EntityPayload sp = new EntityPayload(eventPayload, Bots.ofLoading("my-bot", "my-queue"));
        long timestamp = Json.createReader(new StringReader(jp.toJsonString(eventPayload)))
                .readObject()
                .getJsonNumber("timestamp")
                .longValueExact();
        Assert.assertEquals(timestamp, now.toEpochMilli(), "Invalid JSON time");
    }

    //    @Test
    public void testEntity() {
        EventPayload simplePayload = eventPayload(Instant.now());
        EntityPayload sp = new EntityPayload(simplePayload, Bots.ofLoading("my-bot", "my-queue"));
        int abcVal = Json.createReader(new StringReader(jp.toJsonString(simplePayload)))
                .readObject()
                .getJsonObject("payload")
                .getJsonNumber("abc")
                .intValue();
        Assert.assertEquals(abcVal, 123, "Invalid JSON time");
    }

    private EventPayload eventPayload(Instant time) {
        return new EventPayload() {
            @Override
            public Instant eventTime() {
                return time;
            }

            @Override
            public JsonObject payload() {
                return Json.createObjectBuilder()
                        .add("abc", 123)
                        .build();
            }
        };
    }
}