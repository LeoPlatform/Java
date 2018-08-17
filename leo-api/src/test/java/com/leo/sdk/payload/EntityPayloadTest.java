package com.leo.sdk.payload;

import com.leo.sdk.bus.SimpleLoadingBot;
import org.testng.annotations.Test;

import javax.json.Json;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class EntityPayloadTest {

    @Test
    public void testGetEid() {
        EntityPayload sp = new EntityPayload(simplePayload(), new SimpleLoadingBot("my-bot", "my-queue"));
        String e = sp.getEid();
        boolean formatted = e.startsWith("z/20") && e.endsWith("-0000000") && e.length() == 43;
        assertTrue(formatted, "Invalid eid date format");
    }

    @Test
    public void testGetPayload() {
        EntityPayload sp = new EntityPayload(simplePayload(), new SimpleLoadingBot("my-bot", "my-queue"));
        assertEquals(sp.getPayload().toString(), "{\"simple\":\"payload\"}", "JSON payload mismatch");
    }

    private SimplePayload simplePayload() {
        return () -> Json.createObjectBuilder()
                .add("simple", "payload")
                .build();
    }
}