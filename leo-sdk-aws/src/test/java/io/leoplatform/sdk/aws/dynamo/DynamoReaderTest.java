package io.leoplatform.sdk.aws.dynamo;

import io.leoplatform.sdk.bus.SimpleOffloadingBot;
import io.leoplatform.sdk.config.ConnectorConfig;
import io.leoplatform.sdk.payload.EntityPayload;

import java.util.stream.Stream;

public final class DynamoReaderTest {

    DynamoReader dynamoReader = new DynamoReader(testConfig());

    //    @Test
    public void testEvents() {
        Stream<EntityPayload> p = dynamoReader.events(new SimpleOffloadingBot("test_bot", "test_queue"));
    }

    private ConnectorConfig testConfig() {
        return new ConnectorConfig() {
            @Override
            public String value(String key) {
                return key.equals("Cron") ? "LeoDev-Bus-NQQPNFKOGCH0-LeoCron-MHLFTV8UVHHM" : "LeoDev-Bus-NQQPNFKOGCH0-LeoEvent-1DQB1MJ403WL6";
            }

            @Override
            public Long longValue(String key) {
                return null;
            }

            @Override
            public Integer intValue(String key) {
                return null;
            }

            @Override
            public String valueOrElse(String key, String orElse) {
                return key.equals("AwsProfile") ? "leo_test" : "us-west-1";
            }

            @Override
            public Long longValueOrElse(String key, Long orElse) {
                return 0L;
            }

            @Override
            public Integer intValueOrElse(String key, Integer orElse) {
                return null;
            }
        };
    }
}