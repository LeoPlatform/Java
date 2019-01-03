package io.leoplatform.sdk.aws.dynamo;

import io.leoplatform.sdk.aws.ConfigurationResources;
import io.leoplatform.sdk.bus.SimpleOffloadingBot;
import io.leoplatform.sdk.config.ConnectorConfig;
import io.leoplatform.sdk.config.FileConfig;
import io.leoplatform.sdk.payload.EntityPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.stream.Stream;

import static org.testng.Assert.assertNotNull;

public final class DynamoReaderTest {
    private static final Logger log = LoggerFactory.getLogger(DynamoReaderTest.class);

    private DynamoReader dynamoReader;

    @BeforeMethod
    private void init() {
        dynamoReader = new DynamoReader(new ConfigurationResources(testConfig()));
    }

    @AfterMethod
    void destroy() {
        dynamoReader.end();
    }

    @Test
    public void testEvents() {
//        Stream<EntityPayload> p = dynamoReader.events(new SimpleOffloadingBot("order_webhooks", "ModifiedOrder"));
        Stream<EntityPayload> p = dynamoReader.events(new SimpleOffloadingBot("modified_order_to_dw", "ModifiedOrder"));
        p.forEach(e -> log.info(e.toString()));
        assertNotNull(p, "Empty or missing events");
    }

    private ConnectorConfig testConfig() {
        System.setProperty("JAVA_ENV", "DevBus");
        return new FileConfig();
    }
}