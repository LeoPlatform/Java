package com.leo.sdk.aws.payload;

import com.leo.sdk.aws.DaggerAWSPlatform;
import com.leo.sdk.aws.kinesis.KinesisCompression;
import com.leo.sdk.payload.SimplePayload;
import com.leo.sdk.payload.StreamCorrelation;
import com.leo.sdk.payload.StreamPayload;
import org.testng.annotations.Test;

import javax.json.Json;
import javax.json.JsonObject;
import java.io.*;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.zip.GZIPInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class JSDKGzipPayloadTest {

    private KinesisCompression compressor = DaggerAWSPlatform.builder().build().kinesisCompression();

    @Test
    public void testCompressLen() {
        ByteBuffer compressed = compressor.compress(streamPayload(Instant.now()));
        int len = compressed.limit();
        assertTrue(len > 800 && len < 900, "Invalid compressed payload length");
    }

    @Test
    public void testSymmetric() {
        Instant now = Instant.now();
        StreamPayload payload = streamPayload(now);
        SimplePayload inflatedPayload = simplePayload(now, inflate(compressor.compress(payload)));

        String expectedId = payload.getPayload()
                .getJsonObject("fullobj")
                .getJsonString("_id")
                .getString();
        String actualId = inflatedPayload.entity()
                .getJsonObject("payload")
                .getJsonObject("fullobj")
                .getJsonString("_id")
                .getString();

        assertEquals(actualId, expectedId, "Compressed and inflated payload mismatch");
    }

    private String inflate(ByteBuffer compressed) {
        try (InputStream is = new GZIPInputStream(new ByteArrayInputStream(compressed.array()))) {
            try (ByteArrayOutputStream os = new ByteArrayOutputStream(compressed.capacity() * 2)) {
                byte[] b = new byte[0xFF];
                int len;
                while ((len = is.read(b, 0, b.length)) != -1) {
                    os.write(b, 0, len);
                }
                return new String(os.toByteArray(), UTF_8);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Could not inflate payload", e);
        }
    }

    private StreamPayload streamPayload(Instant now) {
        return new StreamPayload(simplePayload(now), streamCorrelation(), "my-event");
    }

    private SimplePayload simplePayload(Instant time) {
        return simplePayload(time, rawJson());
    }

    private SimplePayload simplePayload(Instant time, String raw) {
        return new SimplePayload() {
            @Override
            public String id() {
                return "JSDK";
            }

            @Override
            public Instant eventTime() {
                return time;
            }

            @Override
            public JsonObject entity() {
                return Json.createReader(new StringReader(raw))
                        .readObject();
            }
        };
    }

    private StreamCorrelation streamCorrelation() {
        return new StreamCorrelation("my-src", 1L, 2L, 3L);
    }

    private String rawJson() {
        return "{\n" +
                "  \"op\": \"update\",\n" +
                "  \"fullobj\": {\n" +
                "    \"_id\": \"259546265\",\n" +
                "    \"suborder_id\": 259546265,\n" +
                "    \"order_id\": 259546265,\n" +
                "    \"retailer_id\": \"1000000065\",\n" +
                "    \"supplier_id\": \"1000001065\",\n" +
                "    \"po_number\": \"SG259546265\",\n" +
                "    \"supplier_order_number\": null,\n" +
                "    \"consumer_order_number\": null,\n" +
                "    \"cancel_after_date\": null,\n" +
                "    \"create_date\": \"2018-08-03T17:49:34.721579Z\",\n" +
                "    \"ship_late_date\": \"2018-08-10T15:31:34.739153Z\",\n" +
                "    \"retailer_create_date\": \"2018-08-02T17:03:34.721587Z\",\n" +
                "    \"provisional_shipments\": [],\n" +
                "    \"items\": [\n" +
                "      {\n" +
                "        \"item_id\": 1028695022,\n" +
                "        \"line_number\": 1,\n" +
                "        \"title\": \"CAYE NAPPA BUCKLE BOOT\",\n" +
                "        \"quantity\": 1,\n" +
                "        \"expected_cost\": 143.82,\n" +
                "        \"cost\": 143.82,\n" +
                "        \"consumer_price\": 156,\n" +
                "        \"partner_sku\": \"401082326017\",\n" +
                "        \"sku\": \"804608133388\",\n" +
                "        \"sub_statuses\": [],\n" +
                "        \"activity\": [\n" +
                "          {\n" +
                "            \"action\": \"add\",\n" +
                "            \"update_date\": \"2018-07-25T10:53:58+00:00\"\n" +
                "          }\n" +
                "        ]\n" +
                "      },\n" +
                "      {\n" +
                "        \"item_id\": 1028663477,\n" +
                "        \"line_number\": 2,\n" +
                "        \"title\": \"MOTT\",\n" +
                "        \"quantity\": 2,\n" +
                "        \"expected_cost\": 94.5,\n" +
                "        \"cost\": 95.5,\n" +
                "        \"consumer_price\": 225,\n" +
                "        \"partner_sku\": \"401075358964\",\n" +
                "        \"sku\": \"401075358964\",\n" +
                "        \"sub_statuses\": [],\n" +
                "        \"activity\": [\n" +
                "          {\n" +
                "            \"action\": \"add\",\n" +
                "            \"update_date\": \"2018-07-25T10:53:57+00:00\"\n" +
                "          }\n" +
                "        ]\n" +
                "      }\n" +
                "    ],\n" +
                "    \"channel\": null,\n" +
                "    \"requested_ship_carrier\": \"UPS\",\n" +
                "    \"requested_ship_method\": \"Ground\",\n" +
                "    \"requested_shipping_service_level_code\": \"FECG\",\n" +
                "    \"days_to_arrive\": 7,\n" +
                "    \"shipping\": null,\n" +
                "    \"status\": \"shipment_pending\",\n" +
                "    \"required_ship_date\": null,\n" +
                "    \"retailer_account_id\": null,\n" +
                "    \"retailer_shipping_account\": null,\n" +
                "    \"signature_required_flag\": null,\n" +
                "    \"vendor_id\": null,\n" +
                "    \"test_flag\": 0,\n" +
                "    \"currency_code\": null,\n" +
                "    \"shipments\": [],\n" +
                "    \"last_actor\": {\n" +
                "      \"account_id\": null,\n" +
                "      \"account_type\": null,\n" +
                "      \"source\": \"SYSTEM\",\n" +
                "      \"system_op\": true,\n" +
                "      \"employee_id\": null,\n" +
                "      \"user_id\": null,\n" +
                "      \"ip\": null,\n" +
                "      \"process_id\": null,\n" +
                "      \"job_id\": null,\n" +
                "      \"update_date\": \"2018-08-03T17:49:34.721579Z\",\n" +
                "      \"mt\": 1515525670.646138,\n" +
                "      \"trigger\": \"Operation.HandleShippoResponse\"\n" +
                "    },\n" +
                "    \"_dates\": {\n" +
                "      \"created\": \"2018-08-03T17:49:34.721579Z\",\n" +
                "      \"shipment_pending\": null,\n" +
                "      \"last_status_update\": \"2018-08-03T17:49:34.721579Z\",\n" +
                "      \"last_update\": \"2018-08-03T17:49:34.721579Z\",\n" +
                "      \"ship_late\": \"2018-08-10T15:31:34.739153Z\",\n" +
                "      \"retailer_create\": \"2018-08-02T17:03:34.721587Z\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
    }
}