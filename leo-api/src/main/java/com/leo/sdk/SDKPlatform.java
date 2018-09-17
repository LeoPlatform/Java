package com.leo.sdk;

import com.leo.sdk.config.ConnectorConfig;
import dagger.Component;

import javax.inject.Named;
import javax.inject.Singleton;

@Singleton
@Component(modules = SDKModule.class)
public interface SDKPlatform {

    ConnectorConfig connectorConfig();

    @Named("Internal")
    ExecutorManager executorManager();
}
