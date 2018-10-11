package io.leoplatform.sdk.config;

import io.leoplatform.sdk.DaggerSDKPlatform;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class FileConfigTest {

    private ConnectorConfig fileConfig;

    @BeforeClass
    void setUp() {
        System.setProperty("JAVA_ENV", "DevBus");
        fileConfig = DaggerSDKPlatform.create().connectorConfig();
    }

    @Test
    public void testValue() {
        assertEquals(fileConfig.value("Writer"), "Stream", "DevBus.Writer property not found");
    }

    @Test
    public void testLongValue() {
        assertEquals(fileConfig.longValue("Stream.LoadingThreads").longValue(), 3L, "LoadingThreads config error");
    }

    @Test
    public void testValueOrElse() {
        assertEquals(fileConfig.valueOrElse("Stream.ABCXYZ", "HELLO"), "HELLO", "Incorrect String orElse");
    }

    @Test
    public void testLongValueOrElse() {
        assertEquals(fileConfig.longValueOrElse("Stream.ABCXYZ", 44L).longValue(), 44L, "Incorrect Long orElse");
    }
}