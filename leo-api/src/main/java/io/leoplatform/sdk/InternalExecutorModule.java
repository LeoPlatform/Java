package io.leoplatform.sdk;

import dagger.Module;
import dagger.Provides;
import io.leoplatform.sdk.config.ConnectorConfig;

import javax.inject.Named;
import javax.inject.Singleton;

@Module
public final class InternalExecutorModule {
    @Singleton
    @Provides
    @Named("Internal")
    static ExecutorManager provideExecutorManager(ConnectorConfig config) {
        return new InternalExecutorManager(config);
    }
}
