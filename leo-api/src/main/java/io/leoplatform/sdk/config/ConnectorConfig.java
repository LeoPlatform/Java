package io.leoplatform.sdk.config;

public interface ConnectorConfig {
    String value(String key);

    Long longValue(String key);

    Integer intValue(String key);

    String valueOrElse(String key, String orElse);

    Long longValueOrElse(String key, Long orElse);

    Integer intValueOrElse(String key, Integer orElse);
}
