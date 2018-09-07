package com.leo.sdk;

import com.leo.sdk.config.ConnectorConfig;
import com.leo.sdk.config.FileConfig;
import dagger.Module;
import dagger.Provides;

@Module
public final class SDKModule {

    @Provides
    public static ConnectorConfig provideConnectorConfig() {
        return new FileConfig();
    }

    @Provides
    public static ExecutorManager provideExecutorManager(ConnectorConfig config) {
        return new ExecutorManager(config);
    }
}
