package io.leoplatform.sdk;

import dagger.Component;
import io.leoplatform.sdk.config.ConnectorConfig;

import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
@Component(modules = SDKModule.class)
public interface SDKPlatform {

    ConnectorConfig connectorConfig();

    @Named("Internal")
    ExecutorManager executorManager();
}
