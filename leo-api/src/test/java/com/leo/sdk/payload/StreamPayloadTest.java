package com.leo.sdk.payload;

import org.testng.annotations.Test;

import javax.json.Json;
import javax.json.JsonObject;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class StreamPayloadTest {

    @Test
    public void testGetEid() {
        StreamPayload sp = new StreamPayload(simplePayload(), streamCorrelation(), "my-event");
        String e = sp.getEid();
        boolean formatted = e.startsWith("z/20") && e.endsWith("-0000000") && e.length() == 43;
        assertTrue(formatted, "Invalid eid date format");
    }

    @Test
    public void testGetPayload() {
        StreamPayload sp = new StreamPayload(simplePayload(), streamCorrelation(), "my-event");
        assertEquals(sp.getPayload().toString(), "{\"simple\":\"payload\"}", "JSON payload mismatch");
    }

    private SimplePayload simplePayload() {
        return new SimplePayload() {
            @Override
            public String id() {
                return "abc123";
            }

            @Override
            public JsonObject entity() {
                return Json.createObjectBuilder()
                        .add("simple", "payload")
                        .build();
            }
        };
    }

    private StreamCorrelation streamCorrelation() {
        return new StreamCorrelation("my-src", 1L, 2L, 3L);
    }
}