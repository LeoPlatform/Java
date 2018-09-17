package com.leo.sdk.aws.payload;

import com.leo.sdk.aws.DaggerAWSLoadingPlatform;
import com.leo.sdk.bus.Bots;
import com.leo.sdk.payload.EventPayload;
import org.testng.annotations.BeforeClass;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPInputStream;

import static java.time.temporal.ChronoUnit.MINUTES;

public class JSDKGzipPayloadTest {

    private CompressionWriter compressor;

    @BeforeClass
    void setUp() {
        System.setProperty("JAVA_ENV", "DevBus");
        compressor = null;
        DaggerAWSLoadingPlatform.builder()
                .loadingBot(Bots.ofLoading("my-bot", "my-queue"))
                .build()
                .kinesisCompression();
    }

    //    @Test
    public void testCompressLen() {
//        ByteBuffer compressed = compressor.compress(eventPayload(Instant.now()));
//        int len = compressed.limit();
//        assertTrue(len > 800 && len < 900, "Invalid compressed payload length");
    }

    //    @Test
    public void testSymmetric() {

        Instant now = Instant.now();
        List<EventPayload> expectedPayload = Collections.singletonList(eventPayload(now));

//        ByteBuffer compressedPayload = compressor.compress(expectedPayload);
//        EventPayload inflatedPayload = eventPayload(now, inflate(compressedPayload));

//        String expectedId = expectedPayload.getPayload()
//                .getJsonObject("fullobj")
//                .getJsonString("_id")
//                .getString();
//        String actualId = inflatedPayload.getPayload()
//                .getJsonObject("payload")
//                .getJsonObject("fullobj")
//                .getJsonString("_id")
//                .getString();
//
//        assertEquals(actualId, expectedId, "Compressed and inflated payload mismatch");
    }

    private JsonObject inflate(ByteBuffer compressed) {
        try (InputStream is = new GZIPInputStream(new ByteArrayInputStream(compressed.array()))) {
            return Json.createReader(is).readObject();
        } catch (IOException e) {
            throw new IllegalStateException("Could not inflate payload", e);
        }
    }

    private EventPayload eventPayload(Instant now) {
        return eventPayload(now, simpleJson());
    }

    private EventPayload eventPayload(Instant time, JsonObject simpleJson) {
        return simplePayload(time, simpleJson);
//        return new EntityPayload(simplePayload(time, simpleJson), new SimpleLoadingBot("my-bot", "my-queue"));
    }

    private EventPayload simplePayload(Instant time, JsonObject simpleJson) {
        return new EventPayload() {
            @Override
            public Instant eventTime() {
                return time;
            }

            @Override
            public JsonObject payload() {
                return simpleJson;
            }
        };
    }

    private JsonObject simpleJson() {
        Instant now = Instant.now();
        long id = randomLong(10000, 1000000000);
        String retailerCreated = Instant.now().minus(randomLong(1440, 2880), MINUTES).toString();
        String shipLate = Instant.now().plus(randomLong(5000, 10000), MINUTES).toString();
        List<String> carriers = Arrays.asList("USPS", "FedEx", "UPS", "OnTrack", "Other");

        JsonObject orderJson = orderJson();
        JsonObjectBuilder lastActor = Json
                .createObjectBuilder(orderJson.getJsonObject("fullobj").getJsonObject("last_actor"))
                .add("update_date", Json.createValue(now.toString()));
        JsonObjectBuilder dates = Json
                .createObjectBuilder(orderJson.getJsonObject("fullobj").getJsonObject("_dates"))
                .add("created", Json.createValue(now.toString()))
                .add("last_status_update", Json.createValue(now.toString()))
                .add("last_update", Json.createValue(now.toString()))
                .add("ship_late", Json.createValue(shipLate))
                .add("retailer_create", Json.createValue(retailerCreated));
        JsonObjectBuilder fullObj = Json
                .createObjectBuilder(orderJson.getJsonObject("fullobj"))
                .add("_id", Json.createValue(String.valueOf(id)))
                .add("suborder_id", Json.createValue(id))
                .add("order_id", Json.createValue(id))
                .add("retailer_id", Json.createValue(String.valueOf(1000000000 + (id % 100))))
                .add("supplier_id", Json.createValue(String.valueOf(1000000000 + (id % 100) + 1000)))
                .add("po_number", Json.createValue("SG" + String.valueOf(id)))
                .add("create_date", Json.createValue(now.toString()))
                .add("ship_late_date", Json.createValue(shipLate))
                .add("retailer_create_date", Json.createValue(retailerCreated))
                .add("requested_ship_carrier", Json.createValue(carriers.get((int) randomLong(0, 4))))
                .add("last_actor", lastActor)
                .add("_dates", dates);

        return Json.createObjectBuilder()
                .add("op", "update")
                .add("fullobj", fullObj)
                .build();
    }

    private JsonObject orderJson() {
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            FileReader r = new FileReader(Objects.requireNonNull(classLoader.getResource("new_order.json")).getFile());
            return Json.createReader(r).readObject();
        } catch (IOException e) {
            throw new IllegalStateException("new_order.json not found");
        }
    }

    private long randomLong(long min, long max) {
        return min + (long) (Math.random() * (max - min));
    }
}