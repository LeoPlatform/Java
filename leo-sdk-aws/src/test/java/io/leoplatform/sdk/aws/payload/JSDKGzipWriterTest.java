package io.leoplatform.sdk.aws.payload;

import io.leoplatform.sdk.aws.s3.S3LocationPayload;
import io.leoplatform.sdk.aws.s3.S3Payload;
import io.leoplatform.sdk.bus.Bots;
import io.leoplatform.sdk.bus.LoadingBot;
import io.leoplatform.sdk.payload.*;
import org.testng.annotations.Test;

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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public final class JSDKGzipWriterTest {

    private final LoadingBot bot = Bots.ofLoading("loading-bot-name", "queue-name");
    private S3JsonPayload s3JsonPayload = new JacksonPayload(bot);
    private JSDKGzipWriter writer = new JSDKGzipWriter(s3JsonPayload, nullThresholdMonitor());

    @Test
    public void testCompressLen() {
        ByteBuffer bb = writer.compress(s3Payload());
        assertTrue(bb.array().length > 150 && bb.array().length < 250, "Invalid compressed payload length");
    }

    @Test
    public void testSymmetricS3Compress() {
        ByteBuffer bb = writer.compress(s3Payload());
        JsonObject inflated = inflate(bb);
        assertEquals(inflated.getJsonString("event").getString(), "my_event", "Invalid event on symmetric inflate");
    }

    @Test
    public void testSymmetricOffsetCompress() {
        FileSegment fs = writer.compressWithOffsets(Collections.singletonList(new SimplePayload(simpleJson())));
        JsonObject inflated = inflate(ByteBuffer.wrap(fs.getSegment()));
        assertEquals(inflated.getJsonObject("payload").getJsonString("op").getString(), "update", "Invalid op on symmetric offset inflate");
    }

    private JsonObject inflate(ByteBuffer compressed) {
        try (InputStream is = new GZIPInputStream(new ByteArrayInputStream(compressed.array()))) {
            return Json.createReader(is).readObject();
        } catch (IOException e) {
            throw new IllegalStateException("Could not inflate payload", e);
        }
    }

    private S3Payload s3Payload() {
        return new S3Payload("my_event", "b", "c", new S3LocationPayload("d", "e"),
                Collections.singletonList(new StorageEventOffset("f", 1L, 2L, 3L, 4L, 5L, 6L, 7L)),
                8L, 9L, 10L, new StorageStats(Collections.singletonMap("g", new StorageUnits(11L))));
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
            .add("po_number", Json.createValue("SG" + id))
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

    private ThresholdMonitor nullThresholdMonitor() {
        return new ThresholdMonitor() {
            @Override
            public void addBytes(Long bytes) {
            }

            @Override
            public boolean isFailover() {
                return false;
            }

            @Override
            public void end() {
            }
        };
    }
}