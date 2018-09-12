package com.leo.sdk.payload;

import com.leo.sdk.bus.SimpleLoadingBot;
import org.testng.annotations.Test;

import javax.json.Json;

import static org.testng.Assert.assertEquals;

public class EntityPayloadTest {

    @Test
    public void testGetPayload() {
        EntityPayload sp = new EntityPayload(simplePayload(), new SimpleLoadingBot("my-bot", "my-queue"));
        assertEquals(sp.getPayload().toString(), "{\"simple\":\"payload\"}", "JSON payload mismatch");
    }

    private EventPayload simplePayload() {
        return () -> Json.createObjectBuilder()
                .add("simple", "payload")
                .build();
    }
}