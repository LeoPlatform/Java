package io.leoplatform.sdk.config;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

import javax.inject.Singleton;
import java.util.Optional;

@Singleton
public class FileConfig implements ConnectorConfig {
    private static final String PROFILE_ENV = "JAVA_ENV";
    private static final Config cfg = ConfigFactory.load("leo_config.properties");

    private final String env;

    public FileConfig() {
        this.env = environmentProfile();
    }

    @Override
    public String value(String key) {
        String fullKey = withEnvironment(key);
        return cfg.getString(fullKey);
    }

    @Override
    public Long longValue(String key) {
        String fullKey = withEnvironment(key);
        return cfg.getLong(fullKey);
    }

    @Override
    public Integer intValue(String key) {
        String fullKey = withEnvironment(key);
        return cfg.getInt(fullKey);
    }

    @Override
    public String valueOrElse(String key, String orElse) {
        String fullKey = withEnvironment(key);
        try {
            return cfg.getString(fullKey);
        } catch (ConfigException e) {
            return orElse;
        }
    }

    @Override
    public Long longValueOrElse(String key, Long orElse) {
        String fullKey = withEnvironment(key);
        try {
            return cfg.getLong(fullKey);
        } catch (ConfigException e) {
            return orElse;
        }
    }

    @Override
    public Integer intValueOrElse(String key, Integer orElse) {
        String fullKey = withEnvironment(key);
        try {
            return cfg.getInt(fullKey);
        } catch (ConfigException e) {
            return orElse;
        }
    }

    private String withEnvironment(String unsafeKey) {
        String safeKey = safeKey(unsafeKey);
        return String.format("%s.%s", env, safeKey);
    }

    private String safeKey(String unsafeKey) {
        return Optional.ofNullable(unsafeKey)
                .map(String::trim)
                .filter(e -> !e.isEmpty())
                .orElseThrow(() -> new IllegalArgumentException("Missing or empty config key: " + unsafeKey));
    }

    private String environmentProfile() {
        return Optional.ofNullable(findProperty())
                .map(String::trim)
                .filter(e -> !e.isEmpty())
                .orElse("dev");
    }

    private String findProperty() {
        return Optional.ofNullable(System.getenv(PROFILE_ENV))
                .orElse(System.getProperty(PROFILE_ENV));
    }
}
