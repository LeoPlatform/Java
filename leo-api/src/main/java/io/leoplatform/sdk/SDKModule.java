package io.leoplatform.sdk;

import dagger.Module;
import dagger.Provides;
import io.leoplatform.sdk.config.ConnectorConfig;
import io.leoplatform.sdk.config.FileConfig;

import javax.inject.Named;
import javax.inject.Singleton;

@Module
public class SDKModule {
    @Singleton
    @Provides
    public static ConnectorConfig provideConnectorConfig() {
        return new FileConfig();
    }

    @Singleton
    @Provides
    @Named("Internal")
    public static ExecutorManager provideExecutorManager(ConnectorConfig config) {
        return new InternalExecutorManager(config);
    }
}
