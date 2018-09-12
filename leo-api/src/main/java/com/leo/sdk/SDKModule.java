package com.leo.sdk;

import com.leo.sdk.config.ConnectorConfig;
import com.leo.sdk.config.FileConfig;
import dagger.Module;
import dagger.Provides;

import javax.inject.Singleton;

@Module
public final class SDKModule {

    @Singleton
    @Provides
    static ConnectorConfig provideConnectorConfig() {
        return new FileConfig();
    }
}
