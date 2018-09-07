package com.leo.sdk;

import com.leo.sdk.config.ConnectorConfig;
import dagger.Component;

@Component(modules = SDKModule.class)
public interface SDKPlatform {

    ConnectorConfig connectorConfig();

    ExecutorManager executorManager();
}
