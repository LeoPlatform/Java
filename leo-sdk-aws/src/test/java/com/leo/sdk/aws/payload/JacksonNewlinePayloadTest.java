package com.leo.sdk.aws.payload;

import com.leo.sdk.aws.DaggerAWSPlatform;
import com.leo.sdk.payload.EntityPayload;
import com.leo.sdk.payload.SimplePayload;
import com.leo.sdk.payload.StreamCorrelation;
import com.leo.sdk.payload.StreamJsonPayload;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.StringReader;
import java.time.Instant;

public class JacksonNewlinePayloadTest {

    private StreamJsonPayload jp = DaggerAWSPlatform.builder().build().streamJsonPayload();

    @Test
    public void testJsonId() {
        SimplePayload simplePayload = simplePayload(Instant.now());
        EntityPayload sp = new EntityPayload(simplePayload, streamCorrelation(), "my-event");
        String id = Json.createReader(new StringReader(jp.toJsonString(sp)))
                .readObject()
                .getJsonString("id")
                .getString();
        Assert.assertEquals(id, simplePayload.id(), "Invalid JSON id");
    }

    @Test
    public void testJsonTime() {
        Instant now = Instant.now();
        SimplePayload simplePayload = simplePayload(now);
        EntityPayload sp = new EntityPayload(simplePayload, streamCorrelation(), "my-event");
        long timestamp = Json.createReader(new StringReader(jp.toJsonString(sp)))
                .readObject()
                .getJsonNumber("timestamp")
                .longValueExact();
        Assert.assertEquals(timestamp, now.toEpochMilli(), "Invalid JSON time");
    }

    @Test
    public void testEntity() {
        SimplePayload simplePayload = simplePayload(Instant.now());
        EntityPayload sp = new EntityPayload(simplePayload, streamCorrelation(), "my-event");
        int abcVal = Json.createReader(new StringReader(jp.toJsonString(sp)))
                .readObject()
                .getJsonObject("payload")
                .getJsonNumber("abc")
                .intValue();
        Assert.assertEquals(abcVal, 123, "Invalid JSON time");
    }

    private SimplePayload simplePayload(Instant time) {
        return new SimplePayload() {
            @Override
            public String id() {
                return "Jack";
            }

            @Override
            public Instant eventTime() {
                return time;
            }

            @Override
            public JsonObject entity() {
                return Json.createObjectBuilder()
                        .add("abc", 123)
                        .build();
            }
        };
    }

    private StreamCorrelation streamCorrelation() {
        return new StreamCorrelation("my-src", 1L, 2L, 3L);
    }
}