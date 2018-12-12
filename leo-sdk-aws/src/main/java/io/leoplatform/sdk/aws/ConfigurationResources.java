package io.leoplatform.sdk.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import io.leoplatform.sdk.config.ConnectorConfig;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Optional;

@Singleton
public final class ConfigurationResources implements AWSResources {

    private final ConnectorConfig config;

    @Inject
    public ConfigurationResources(ConnectorConfig config) {
        this.config = config;
    }

    @Override
    public AWSCredentialsProvider credentials() {
        try {
            return Optional.of(config.valueOrElse("AwsProfile", ""))
                .map(String::trim)
                .filter(profile -> !profile.isEmpty())
                .map(ProfileCredentialsProvider::new)
                .filter(p -> p.getCredentials() != null)
                .map(AWSCredentialsProvider.class::cast)
                .orElse(DefaultAWSCredentialsProviderChain.getInstance());
        } catch (Exception e) {
            return DefaultAWSCredentialsProviderChain.getInstance();
        }
    }

    @Override
    public String region() {
        return config.valueOrElse("Region", "us-east-1");
    }

    @Override
    public String kinesisStream() {
        return config.value("Stream.Name");
    }

    @Override
    public String cronTable() {
        return config.value("Cron");
    }

    @Override
    public String eventTable() {
        return config.value("Event");
    }

    @Override
    public String storage() {
        return config.value("Storage.Name");
    }
}
