package com.leo.sdk.config;

public interface ConnectorConfig {
    String value(String key);

    Long longValue(String key);

    String valueOrElse(String key, String orElse);

    Long longValueOrElse(String key, Long orElse);
}
